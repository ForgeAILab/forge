//! Captures journal fields without installing a process-global subscriber.
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

pub type JournalEvent = (tracing::Level, BTreeMap<String, String>);

#[derive(Clone, Default)]
pub struct JournalEvents {
    pub debug: bool,
    pub rows: Arc<Mutex<Vec<JournalEvent>>>,
}

impl tracing::Subscriber for JournalEvents {
    // Concurrent captures use different filters; a callsite is not globally
    // always/never enabled just because one capture accepted/rejected it.
    fn register_callsite(
        &self,
        _: &'static tracing::Metadata<'static>,
    ) -> tracing::subscriber::Interest {
        tracing::subscriber::Interest::sometimes()
    }
    fn max_level_hint(&self) -> Option<tracing::level_filters::LevelFilter> {
        Some(tracing::level_filters::LevelFilter::DEBUG)
    }
    fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
        metadata.target() == "forge_session_journal"
            && (self.debug || *metadata.level() <= tracing::Level::WARN)
    }
    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn event(&self, event: &tracing::Event<'_>) {
        struct Fields(BTreeMap<String, String>);
        impl tracing::field::Visit for Fields {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                self.0.insert(field.name().into(), format!("{value:?}"));
            }
        }
        let mut fields = Fields(BTreeMap::new());
        event.record(&mut fields);
        self.rows
            .lock()
            .unwrap()
            .push((*event.metadata().level(), fields.0));
    }
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
}
