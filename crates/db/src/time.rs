#[must_use]
pub fn now_rfc3339() -> String {
    #[cfg(feature = "test-template")]
    if let Some(now) = test_clock::next() {
        return now;
    }
    chrono::Utc::now().to_rfc3339()
}

#[cfg(feature = "test-template")]
pub mod test_clock {
    use chrono::{DateTime, Duration, SecondsFormat, Utc};
    use std::{cell::RefCell, marker::PhantomData, rc::Rc};

    thread_local! {
        static NEXT: RefCell<Option<DateTime<Utc>>> = const { RefCell::new(None) };
    }

    /// Restores this thread's previous clock when dropped, including during unwinding.
    #[must_use]
    pub struct Guard {
        previous: Option<DateTime<Utc>>,
        // A guard must be dropped on the thread whose clock it replaced.
        _thread: PhantomData<Rc<()>>,
    }

    /// Starts a microsecond clock on the current thread. Use a current-thread runtime
    /// when holding the guard across an await.
    pub fn freeze(base: DateTime<Utc>) -> Guard {
        assert_eq!(base.timestamp_subsec_nanos() % 1_000, 0);
        Guard {
            previous: NEXT.with(|next| next.replace(Some(base))),
            _thread: PhantomData,
        }
    }

    impl Drop for Guard {
        fn drop(&mut self) {
            NEXT.with(|next| next.replace(self.previous));
        }
    }

    pub(super) fn next() -> Option<String> {
        NEXT.with(|next| {
            let mut next = next.borrow_mut();
            let now = next.as_mut()?;
            // Keep six digits even when the sequence crosses a whole millisecond.
            let output = now.to_rfc3339_opts(SecondsFormat::Micros, false);
            *now += Duration::microseconds(1);
            Some(output)
        })
    }
}

#[cfg(all(test, feature = "test-template"))]
mod tests {
    use super::*;
    use chrono::{DateTime, Duration, Utc};

    fn base() -> DateTime<Utc> {
        "2026-10-25T00:00:00.999998+00:00".parse().unwrap()
    }

    #[test]
    fn frozen_clock_has_fixed_width_and_microsecond_steps() {
        let _clock = test_clock::freeze(base());
        let mut previous = base() - Duration::microseconds(1);
        for expected in [
            "2026-10-25T00:00:00.999998+00:00",
            "2026-10-25T00:00:00.999999+00:00",
            "2026-10-25T00:00:01.000000+00:00",
            "2026-10-25T00:00:01.000001+00:00",
        ] {
            let actual = now_rfc3339();
            assert_eq!(actual.len(), 32);
            assert_eq!(actual, expected);
            let instant = actual.parse::<DateTime<Utc>>().unwrap();
            assert_eq!(instant - previous, Duration::microseconds(1));
            previous = instant;
        }
        assert_eq!(base().to_rfc3339(), "2026-10-25T00:00:00.999998+00:00");
    }

    #[test]
    fn nested_clock_restores_sequence_after_unwind() {
        assert!(test_clock::next().is_none());
        let clock = test_clock::freeze(base());
        assert_eq!(now_rfc3339(), base().to_rfc3339());
        let panic = std::panic::catch_unwind(|| {
            let _nested = test_clock::freeze(base() - Duration::days(1));
            assert_eq!(now_rfc3339(), (base() - Duration::days(1)).to_rfc3339());
            panic!("drop nested clock");
        });
        assert!(panic.is_err());
        assert_eq!(
            now_rfc3339(),
            (base() + Duration::microseconds(1)).to_rfc3339()
        );
        drop(clock);
        assert!(test_clock::next().is_none());
    }

    #[test]
    fn frozen_clock_is_thread_local() {
        let _clock = test_clock::freeze(base());
        std::thread::spawn(|| {
            assert!(test_clock::next().is_none());
            let _clock = test_clock::freeze(base() - Duration::days(1));
            assert_eq!(now_rfc3339(), (base() - Duration::days(1)).to_rfc3339());
        })
        .join()
        .unwrap();
        assert_eq!(now_rfc3339(), base().to_rfc3339());
    }

    #[test]
    fn unguarded_clock_uses_wall_time() {
        assert!(test_clock::next().is_none());
        let before = Utc::now();
        let actual = now_rfc3339().parse::<DateTime<Utc>>().unwrap();
        assert!(actual >= before && actual <= Utc::now());
    }
}
