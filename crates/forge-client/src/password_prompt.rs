use std::io::{self, IsTerminal, Read, Write};

use anyhow::{bail, Context, Result};
use crossterm::terminal::{disable_raw_mode, enable_raw_mode};

pub(crate) fn stdin_is_terminal() -> bool {
    io::stdin().is_terminal()
}

pub(crate) fn prompt_password() -> Result<String> {
    if !stdin_is_terminal() {
        bail!("cannot prompt for a password because stdin is not a terminal; use --password-stdin");
    }

    // Do not make the prompt observable until terminal echo is disabled. A
    // fast reader can respond as soon as the prompt is flushed; enabling raw
    // mode afterward would briefly expose the password in the terminal.
    let mut terminal = TerminalGuard::enter()?;
    let mut stderr = io::stderr().lock();
    write!(stderr, "Password: ").context("write password prompt")?;
    stderr.flush().context("flush password prompt")?;
    let password_result = read_hidden_password(&mut io::stdin().lock());
    let restore_result = terminal.restore();
    let newline_result = writeln!(stderr).context("finish password prompt");

    restore_result?;
    newline_result?;
    password_result
}

fn read_hidden_password(input: &mut impl Read) -> Result<String> {
    let mut password = String::new();
    let mut pending_utf8 = Vec::new();
    let mut byte = [0_u8; 1];

    loop {
        let read = input
            .read(&mut byte)
            .context("read hidden password input")?;
        if read == 0 {
            bail!("password input ended before a password was entered");
        }

        match byte[0] {
            b'\r' | b'\n' => return Ok(password),
            0x03 => bail!("password input cancelled"),
            0x04 => bail!("password input ended before a password was entered"),
            0x08 | 0x7f => {
                pending_utf8.clear();
                password.pop();
            }
            0x1b => bail!("password input cancelled"),
            byte if byte.is_ascii_control() => {}
            byte => {
                pending_utf8.push(byte);
                match std::str::from_utf8(&pending_utf8) {
                    Ok(character) => {
                        password.push_str(character);
                        pending_utf8.clear();
                    }
                    Err(error) if error.error_len().is_none() => {}
                    Err(_) => bail!("password input is not valid UTF-8"),
                }
            }
        }
    }
}

struct TerminalGuard {
    active: bool,
}

impl TerminalGuard {
    fn enter() -> Result<Self> {
        enable_raw_mode().context("hide password input")?;
        Ok(Self { active: true })
    }

    fn restore(&mut self) -> Result<()> {
        if self.active {
            disable_raw_mode().context("restore terminal after password input")?;
            self.active = false;
        }
        Ok(())
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        if self.active {
            let _ = disable_raw_mode();
            self.active = false;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct FailingReader;

    impl Read for FailingReader {
        fn read(&mut self, _buffer: &mut [u8]) -> io::Result<usize> {
            Err(io::Error::other("test read failure"))
        }
    }

    #[test]
    fn hidden_reader_returns_password_on_enter_without_echoing() {
        let mut input = "secret-value\r".as_bytes();
        assert_eq!(
            read_hidden_password(&mut input).expect("password reads"),
            "secret-value"
        );
    }

    #[test]
    fn hidden_reader_treats_closed_input_as_eof() {
        let mut input = &b"secret-value"[..];
        let error = read_hidden_password(&mut input).expect_err("closed input must fail");

        assert_eq!(
            error.to_string(),
            "password input ended before a password was entered"
        );
    }

    #[test]
    fn hidden_reader_treats_ctrl_c_as_cancellation() {
        let mut input = &b"secret-value\x03"[..];
        let error = read_hidden_password(&mut input).expect_err("ctrl-c must cancel");

        assert_eq!(error.to_string(), "password input cancelled");
    }

    #[test]
    fn hidden_reader_reports_read_failures() {
        let error = read_hidden_password(&mut FailingReader).expect_err("read failure must fail");

        assert_eq!(error.to_string(), "read hidden password input");
    }

    #[test]
    fn hidden_reader_handles_utf8_and_backspace() {
        let mut input = "pässwod\x7frd\r".as_bytes();
        assert_eq!(
            read_hidden_password(&mut input).expect("password reads"),
            "pässword"
        );
    }
}
