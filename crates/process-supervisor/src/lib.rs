//! Owner of a command's process group and continuously drained output.
//! Drop, cancellation and timeout use the same TERM / 500 ms / KILL sequence.
use std::{
    future::Future,
    io,
    process::{ExitStatus, Output, Stdio},
    sync::atomic::{AtomicBool, Ordering},
    task::Poll,
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    process::{Child, Command},
};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, Copy)]
pub enum Capture {
    All,
    Tail(usize),
    Prefix(usize),
}
#[derive(Debug, Clone, Copy)]
pub enum CompletionPolicy {
    /// Wait for the leader and for both pipes to close; stop nothing after a
    /// normal exit (Git: a hook that outlives Git is not ours to stop).
    Drain,
    /// Stop the leader's process group once the leader exits, then drain for
    /// at most [`POST_EXIT_DRAIN`]: a descendant that left the group (`setsid`)
    /// and still holds a pipe can neither stall the command nor turn a
    /// finished command into a timeout.
    StopDescendants,
    /// Leave descendants running after a normal exit and bound only the
    /// post-exit drain, independently of the command limit. Incomplete drain
    /// is evidence, never size truncation.
    DrainFor(Duration),
}
/// How long a finished command's pipes are read when a descendant keeps them
/// open.
pub const POST_EXIT_DRAIN: Duration = Duration::from_secs(2);
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Termination {
    Exited,
    TimedOut,
    Cancelled,
}
pub struct ProcessOutput {
    pub status: ExitStatus,
    pub termination: Termination,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub stdout_truncated: bool,
    pub stderr_truncated: bool,
    pub descendants_stopped: bool,
    /// The command's process group (the leader's id). A caller that left the
    /// descendants running passes it to [`stop_group`] when its run ends.
    pub group: Option<u32>,
    pub stdout_drain_incomplete: bool,
    pub stderr_drain_incomplete: bool,
}

struct Group {
    child: Child,
    leader: Option<u32>,
}
impl Group {
    fn stop(&mut self) {
        let Some(id) = self.leader.take().filter(|id| *id > 1) else {
            return;
        };
        #[cfg(unix)]
        {
            if signal("TERM", id) {
                let until = Instant::now() + Duration::from_millis(500);
                // Do not forget the group when the leader exits: a grandchild
                // can ignore TERM after the shell has already been reaped.
                while Instant::now() < until && signal("0", id) {
                    let _ = self.child.try_wait();
                    std::thread::sleep(Duration::from_millis(5));
                }
                signal("KILL", id);
            }
        }
        #[cfg(windows)]
        {
            let _ = std::process::Command::new("taskkill")
                .args(["/F", "/T", "/PID", &id.to_string()])
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();
        }
        let _ = self.child.start_kill();
    }
}
impl Drop for Group {
    fn drop(&mut self) {
        self.stop();
    }
}
#[cfg(unix)]
fn signal(signal: &str, id: u32) -> bool {
    std::process::Command::new("kill")
        .args([&format!("-{signal}"), "--", &format!("-{id}")])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|status| status.success())
}
#[cfg(unix)]
fn group_alive(id: u32) -> Option<bool> {
    std::process::Command::new("kill")
        .args(["-0", "--", &format!("-{id}")])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .ok()
        .map(|status| status.success())
}
/// Stop a process group a finished command left running, with the same
/// TERM / 500 ms / KILL sequence, and say whether it is really gone: `true`
/// only when the group has no member left afterwards. Blocking (up to about
/// three seconds); call it off the async runtime.
///
/// Limits: ids 0 and 1 are never signalled; a descendant that left the group
/// (`setsid`) is not seen and not stopped. The id of a group whose every
/// member already exited can in principle be taken by an unrelated new group
/// of the same user before this is called; that group would be signalled.
#[cfg(unix)]
pub fn stop_group(id: u32) -> bool {
    if id <= 1 {
        return false;
    }
    match group_alive(id) {
        None => return false,
        Some(false) => return true,
        Some(true) => {}
    }
    signal("TERM", id);
    let until = Instant::now() + Duration::from_millis(500);
    while Instant::now() < until && group_alive(id) == Some(true) {
        std::thread::sleep(Duration::from_millis(5));
    }
    signal("KILL", id);
    // A killed member is gone once its parent (or init) has reaped it.
    let until = Instant::now() + Duration::from_secs(2);
    loop {
        match group_alive(id) {
            Some(false) => return true,
            None => return false,
            Some(true) if Instant::now() >= until => return false,
            Some(true) => std::thread::sleep(Duration::from_millis(10)),
        }
    }
}
#[cfg(not(unix))]
pub fn stop_group(_id: u32) -> bool {
    false
}
#[derive(Default)]
struct Stream {
    bytes: Vec<u8>,
    truncated: bool,
    eof: bool,
}
impl Stream {
    fn keep(&mut self, capture: Capture, chunk: &[u8]) {
        let count = chunk.len();
        match capture {
            Capture::All => self.bytes.extend_from_slice(chunk),
            Capture::Prefix(limit) => {
                let size = count.min(limit.saturating_sub(self.bytes.len()));
                self.truncated |= size < count;
                self.bytes.extend_from_slice(&chunk[..size]);
            }
            Capture::Tail(limit) => {
                self.truncated |= self.bytes.len().saturating_add(count) > limit;
                if count >= limit {
                    self.bytes.clear();
                    self.bytes.extend_from_slice(&chunk[count - limit..]);
                } else {
                    let remove = self.bytes.len().saturating_add(count).saturating_sub(limit);
                    self.bytes.drain(..remove);
                    self.bytes.extend_from_slice(chunk);
                }
            }
        }
    }
}
async fn drain(
    pipe: &mut (impl AsyncRead + Unpin),
    capture: Capture,
    retained: &mut Stream,
) -> io::Result<()> {
    let mut chunk = [0; 8192];
    loop {
        let count = pipe.read(&mut chunk).await?;
        if count == 0 {
            retained.eof = true;
            return Ok(());
        }
        retained.keep(capture, &chunk[..count]);
    }
}
/// The most a bounded drain still takes from a pipe after its bound: more
/// than any pipe buffer holds, so a descendant that never stops writing
/// cannot keep a finished command reading.
const FINAL_DRAIN_BYTES: usize = 4 * 1024 * 1024;
/// A drain bound is wall-clock time, and a reader that was not scheduled
/// during it has read nothing. So when a bound ends the wait, what the pipe
/// already holds is still taken: read without waiting until end of file, a
/// read that would block, or [`FINAL_DRAIN_BYTES`].
async fn drain_ready(pipe: &mut (impl AsyncRead + Unpin), capture: Capture, retained: &mut Stream) {
    let mut chunk = [0; 8192];
    let mut budget = FINAL_DRAIN_BYTES;
    while !retained.eof && budget > 0 {
        let polled = {
            // Exempt from the cooperative budget: a spent budget reports
            // "pending" for a pipe that has bytes.
            let read = tokio::task::unconstrained(pipe.read(&mut chunk));
            tokio::pin!(read);
            std::future::poll_fn(|context| Poll::Ready(read.as_mut().poll(context))).await
        };
        match polled {
            Poll::Ready(Ok(0)) => retained.eof = true,
            Poll::Ready(Ok(count)) => {
                retained.keep(capture, &chunk[..count]);
                budget = budget.saturating_sub(count);
            }
            Poll::Ready(Err(_)) | Poll::Pending => return,
        }
    }
}

/// Output is drained continuously, so a full pipe never stalls the command or
/// changes its verdict. The deadline bounds the leader; what happens to
/// descendants after a normal exit is the caller's [`CompletionPolicy`]. Git
/// keeps its established normal-exit semantics via `output` below.
///
/// Limits: the group is the child's own (`process_group(0)`), never the
/// caller's, and ids 0 and 1 are never signalled. A descendant that moved to
/// another session or group (`setsid`) is outside the group and is not
/// signalled; it is only detached from the pipes.
pub async fn run(
    command: &mut Command,
    capture: Capture,
    deadline: Option<Instant>,
    cancel: &CancellationToken,
    completion: CompletionPolicy,
) -> io::Result<ProcessOutput> {
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    #[cfg(unix)]
    command.process_group(0).kill_on_drop(false);
    #[cfg(not(unix))]
    command.kill_on_drop(true);
    let mut child = command.spawn()?;
    let mut stdout_pipe = child.stdout.take().expect("piped stdout");
    let mut stderr_pipe = child.stderr.take().expect("piped stderr");
    let leader = child.id();
    let mut group = Group { child, leader };
    let mut stdout = Stream::default();
    let mut stderr = Stream::default();
    let stop_descendants = matches!(completion, CompletionPolicy::StopDescendants);
    let leader_exited = AtomicBool::new(false);
    let (exited, finished) = tokio::sync::oneshot::channel::<()>();
    let post_exit_bound = match completion {
        CompletionPolicy::Drain => None,
        CompletionPolicy::StopDescendants => Some(POST_EXIT_DRAIN),
        CompletionPolicy::DrainFor(bound) => Some(bound),
    };
    let post_exit_timeout = async {
        match post_exit_bound {
            Some(bound) => {
                let _ = finished.await;
                tokio::time::sleep(bound).await;
            }
            None => std::future::pending::<()>().await,
        }
    };
    let timeout = async {
        match deadline {
            Some(deadline) => {
                tokio::time::sleep_until(deadline.into()).await;
                // A leader that already exited keeps its exit status: only
                // the bounded post-exit drain is still pending.
                if post_exit_bound.is_some() && leader_exited.load(Ordering::Acquire) {
                    std::future::pending::<()>().await;
                }
            }
            None => std::future::pending().await,
        }
    };
    let mut status = None;
    let (termination, status) = {
        let drains_done = AtomicBool::new(false);
        let drains = async {
            let result = tokio::try_join!(
                drain(&mut stdout_pipe, capture, &mut stdout),
                drain(&mut stderr_pipe, capture, &mut stderr)
            );
            drains_done.store(true, Ordering::Release);
            result.map(|_| ())
        };
        tokio::pin!(drains);
        let termination = {
            let work = async {
                let wait = async {
                    status = Some(group.child.wait().await?);
                    leader_exited.store(true, Ordering::Release);
                    let _ = exited.send(());
                    if stop_descendants {
                        group.stop();
                    }
                    Ok::<_, io::Error>(())
                };
                tokio::try_join!(wait, &mut drains)?;
                Ok::<_, io::Error>(())
            };
            tokio::select! {
                biased;
                _=cancel.cancelled()=>Termination::Cancelled,
                _=timeout=>Termination::TimedOut,
                result=work=>{result?;Termination::Exited},
                _=post_exit_timeout=>Termination::Exited,
            }
        };
        if termination != Termination::Exited {
            group.stop();
            // Keep the pipes alive through TERM/KILL and drain final buffered
            // diagnostics, including a shell's TERM trap. No reader outlives
            // the guard, and incomplete drainage never changes the verdict.
            if !drains_done.load(Ordering::Acquire) {
                let _ = tokio::time::timeout(Duration::from_secs(2), &mut drains).await;
            }
        }
        let status = match status {
            Some(status) => status,
            None => group.child.wait().await?,
        };
        (termination, status)
    };
    // Every bounded path ends here with the readers stopped. Let the I/O
    // driver report what arrived meanwhile, then take it.
    if !(stdout.eof && stderr.eof) {
        tokio::task::yield_now().await;
        drain_ready(&mut stdout_pipe, capture, &mut stdout).await;
        drain_ready(&mut stderr_pipe, capture, &mut stderr).await;
    }
    // No uncancelled Git descendants are killed by guard destruction.
    group.leader = None;
    Ok(ProcessOutput {
        status,
        termination,
        stdout: stdout.bytes,
        stderr: stderr.bytes,
        stdout_truncated: stdout.truncated,
        stderr_truncated: stderr.truncated,
        descendants_stopped: stop_descendants || termination != Termination::Exited,
        group: leader.filter(|id| *id > 1),
        stdout_drain_incomplete: !stdout.eof,
        stderr_drain_incomplete: !stderr.eof,
    })
}

pub async fn output(command: &mut Command) -> io::Result<Output> {
    output_bounded(command, None).await
}
pub async fn output_bounded(command: &mut Command, limit: Option<usize>) -> io::Result<Output> {
    let result = run(
        command,
        limit.map_or(Capture::All, Capture::Prefix),
        None,
        &CancellationToken::new(),
        CompletionPolicy::Drain,
    )
    .await?;
    if result.stdout_truncated || result.stderr_truncated {
        return Err(io::Error::other(
            "review command output exceeds size budget",
        ));
    }
    Ok(Output {
        status: result.status,
        stdout: result.stdout,
        stderr: result.stderr,
    })
}

#[cfg(all(test, unix))]
mod tests;
