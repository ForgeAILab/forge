//! Seeded workflow actions, fixed seeds and run-cap case generation.

// ---------------------------------------------------------------------------
// Seeded generator
// ---------------------------------------------------------------------------

/// splitmix64: small, seedable and good enough to pick actions.
pub(super) struct Rng(pub(super) u64);

impl Rng {
    pub(super) fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    pub(super) fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Outcome {
    /// Commit the Task's deliverable (and reconcile a handed-off conflict).
    Success,
    /// Commit the deliverable and an edit to a file every Task shares, so a
    /// second Task doing the same conflicts at merge.
    Conflict,
    /// Commit something else: the Task's required check fails in review.
    CiFail,
    /// The executor reports a failed run.
    Fail,
    /// The provider reports an exhausted quota.
    UsageLimit,
}

/// Tasks are named by their creation index, never by ID, so a sequence
/// replays on a fresh database. An index is taken modulo the number of Tasks
/// and an action that does not apply is skipped, so every subsequence of a
/// sequence is itself a valid sequence (which is what makes shrinking simple).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Action {
    Create,
    /// Build the breakdown before the first dispatcher pass.
    CreateLeadRoot,
    CreateChild(usize),
    /// The first Task waits for the second.
    Depend(usize, usize),
    Claim(usize),
    Finish(usize, Outcome),
    /// Script the verdict of the Task's next agent reviews.
    Verdict(usize, bool),
    /// Apply the n-th offer the API advertises for the Task.
    Offer(usize, usize),
    Cancel(usize),
    /// Apply the offer with this verb, when the API advertises it.
    Take(usize, &'static str),
    PauseProject,
    ResumeProject,
    /// Save the Project unchanged: any edit moves its version.
    EditProject,
    /// Make the scripted lead the planner of the Task (a coordination root).
    AssignLead(usize),
    RemoveLead(usize),
    /// Pause and resume the coder Agent.
    PauseAgent,
    ResumeAgent,
    /// Every retry, backoff and cooldown deadline passes.
    AdvanceClock,
    /// Restart on the same database after a quiescent state. Executions that
    /// were in flight lose their process.
    Crash,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Step {
    pub(super) action: Action,
    /// Crash after the action commits and before any Task step it queued
    /// runs, then restart and recover.
    pub(super) crash_before_drain: bool,
}

pub(super) fn step(action: Action) -> Step {
    Step {
        action,
        crash_before_drain: false,
    }
}

pub(super) fn crash_after(action: Action) -> Step {
    Step {
        action,
        crash_before_drain: true,
    }
}

pub(super) fn generate(seed: u64, steps: usize) -> Vec<Step> {
    let mut rng = Rng(seed);
    let mut sequence = vec![step(Action::CreateLeadRoot), step(Action::AssignLead(0))];
    let mut tasks = 3_u64;
    while sequence.len() < steps {
        // Half of the picks go to the newest Task: older ones settle.
        let task = if rng.below(2) == 0 {
            tasks - 1
        } else {
            rng.below(tasks)
        } as usize;
        let action = match rng.below(100) {
            0..=13 if tasks < 6 => {
                tasks += 1;
                Action::Create
            }
            14 => Action::EditProject,
            15 if tasks < 6 => {
                tasks += 1;
                Action::CreateChild(0)
            }
            16 => Action::AssignLead(0),
            17 => Action::RemoveLead(0),
            18..=22 => Action::Depend(task, rng.below(tasks) as usize),
            23..=26 => Action::Claim(task),
            0..=57 => Action::Finish(
                task,
                match rng.below(10) {
                    0..=3 => Outcome::Success,
                    4 | 5 => Outcome::Conflict,
                    6 => Outcome::CiFail,
                    7 | 8 => Outcome::Fail,
                    _ => Outcome::UsageLimit,
                },
            ),
            58..=63 => Action::Verdict(task, rng.below(3) == 0),
            64..=81 => Action::Offer(task, rng.below(8) as usize),
            82..=84 => Action::Cancel(task),
            85 | 86 => Action::PauseProject,
            87 | 88 => Action::ResumeProject,
            89 => Action::PauseAgent,
            90 => Action::ResumeAgent,
            91..=95 => Action::AdvanceClock,
            _ => Action::Crash,
        };
        let crash_before_drain = action != Action::Crash && rng.below(10) == 0;
        sequence.push(Step {
            action,
            crash_before_drain,
        });
    }
    sequence
}

/// Seeds every run covers, so a regression in one of them is a stable failure.
pub(super) const FIXED_SEEDS: [u64; 17] = [
    1, 2, 3, 5, 8, 909358760, 213412165, 974622785, 637240885, 277839211, 580812352, 610875980,
    207044633, 749274245, 263753536, 625967944, 931180934,
];

/// The sequences the extra caps walk: every Task lifecycle shape (plain
/// success, a failed run retried, a review sent back, two Tasks for one slot,
/// a crash in flight) and two generated seeds. Fewer than the default cap's
/// set, to keep the target's runtime in bounds.
pub(super) fn cap_matrix_cases() -> Vec<(String, Vec<Step>)> {
    use Action::{Claim, Create, Finish, Verdict};
    let mut cases = vec![
        (
            "one Task runs, is checked, reviewed and merged".to_owned(),
            vec![step(Create), step(Finish(0, Outcome::Success))],
        ),
        (
            "a failed run is retried".to_owned(),
            vec![step(Create), step(Finish(0, Outcome::Fail))],
        ),
        (
            "a usage limit is waited out".to_owned(),
            vec![
                step(Create),
                step(Claim(0)),
                step(Finish(0, Outcome::UsageLimit)),
            ],
        ),
        (
            "review sends the Task back".to_owned(),
            vec![
                step(Create),
                step(Verdict(0, false)),
                step(Finish(0, Outcome::Success)),
            ],
        ),
        (
            "three Tasks share the slots".to_owned(),
            vec![
                step(Create),
                step(Create),
                step(Create),
                step(Finish(0, Outcome::Success)),
                step(Finish(1, Outcome::Fail)),
            ],
        ),
        (
            "crash with a run in flight".to_owned(),
            vec![step(Create), crash_after(Create)],
        ),
    ];
    cases.extend([3, 5].map(|seed| (format!("FORGE_MODEL_SEED={seed}"), generate(seed, 16))));
    cases
}
