//! Test instrumentation of the one-writer-per-merge rule across an upgrade.
//!
//! Three things can act on the merge of a Task that was merging the old way
//! when the queue became active: the old merge's own hooks step (it finishes
//! the rebase or reports the merge it recorded), the import continuation
//! step, and the queue worker's head driver. Each marks the span in which it
//! may act on the merge; the counter records how many held one Task's merge
//! at once. The hand-over point of the two Task steps is admission: their
//! span ends before the admission that makes an attempt claimable.
//!
//! Counted per database (tests run many servers in one process, some on
//! fixtures that share Task ids) and per kind of holder: two sessions of one
//! kind (the driver of a server a test "crashed" and its successor) are not
//! two writers. Compiled to nothing outside tests (`test-support`).

/// Who holds a Task's merge.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum MergeHolder {
    LegacyHooksStep,
    ImportContinuation,
    WorkerDriver,
}

#[cfg(any(test, feature = "test-support"))]
mod counted {
    use super::MergeHolder;
    use std::{
        collections::{BTreeSet, HashMap},
        sync::Mutex,
    };

    fn key(db: &db::SqliteDb, task_id: &str) -> String {
        format!(
            "{}|{task_id}",
            db.pool().connect_options().get_filename().display()
        )
    }

    #[derive(Default)]
    struct Held {
        now: Vec<MergeHolder>,
        most: usize,
        overlap: Option<Vec<MergeHolder>>,
        seen: BTreeSet<MergeHolder>,
    }
    static HELD: Mutex<Option<HashMap<String, Held>>> = Mutex::new(None);

    /// The span of one holder; it ends when this is dropped.
    #[derive(Debug)]
    pub struct MergeHold {
        key: String,
        holder: MergeHolder,
    }
    impl Drop for MergeHold {
        fn drop(&mut self) {
            let mut held = HELD.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            if let Some(task) = held.as_mut().and_then(|held| held.get_mut(&self.key)) {
                if let Some(at) = task.now.iter().position(|holder| *holder == self.holder) {
                    task.now.remove(at);
                }
            }
        }
    }
    pub fn hold(db: &db::SqliteDb, task_id: &str, holder: MergeHolder) -> MergeHold {
        let key = key(db, task_id);
        let mut held = HELD.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let task = held
            .get_or_insert_with(HashMap::new)
            .entry(key.clone())
            .or_default();
        task.now.push(holder);
        task.seen.insert(holder);
        let kinds = task.now.iter().copied().collect::<BTreeSet<_>>();
        if kinds.len() > task.most {
            task.most = kinds.len();
            if kinds.len() > 1 {
                task.overlap = Some(kinds.into_iter().collect());
            }
        }
        MergeHold { key, holder }
    }
    /// `(most kinds of holder at one instant, the kinds of that instant if
    /// more than one, every kind seen)` for a Task of the server on `db`.
    pub fn merge_holders(
        db: &db::SqliteDb,
        task_id: &str,
    ) -> (usize, Option<Vec<MergeHolder>>, Vec<MergeHolder>) {
        let key = key(db, task_id);
        let held = HELD.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        held.as_ref()
            .and_then(|held| held.get(&key))
            .map(|task| {
                (
                    task.most,
                    task.overlap.clone(),
                    task.seen.iter().copied().collect(),
                )
            })
            .unwrap_or((0, None, Vec::new()))
    }
}
#[cfg(any(test, feature = "test-support"))]
pub use counted::{hold, merge_holders, MergeHold};

#[cfg(not(any(test, feature = "test-support")))]
mod uncounted {
    /// Nothing is counted in a production build.
    #[derive(Debug)]
    pub struct MergeHold;
    #[inline(always)]
    pub fn hold(_db: &db::SqliteDb, _task_id: &str, _holder: super::MergeHolder) -> MergeHold {
        MergeHold
    }
}
#[cfg(not(any(test, feature = "test-support")))]
pub use uncounted::{hold, MergeHold};
