//! One charged LRU for scoped index directories and decoded body packs.
use super::codec::Directory;
use agent_runtime::core::{ids::SessionId, journal::*};
use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

pub(super) type Scope = (JournalDomain, SessionId);
pub(super) const DEFAULT_BUDGET: usize = 64 * 1024 * 1024;

#[derive(Clone, PartialEq, Eq, Hash)]
pub(super) struct PackKey {
    pub scope: Scope,
    pub id: i64,
    pub generation: u64,
    pub count: usize,
    pub retained: usize,
    pub owner: String,
}
#[derive(Clone)]
pub(super) struct IndexEntry {
    pub key: PackKey,
    pub directory: Arc<Directory>,
}
#[derive(Clone, Default)]
pub(super) struct ScopeDirectory {
    pub packs: BTreeMap<i64, IndexEntry>,
}
impl ScopeDirectory {
    fn charge(&self) -> usize {
        self.packs
            .values()
            .map(|p| {
                // Include retained prefix slots, BTree nodes, keys and scope strings.
                256 + p.key.owner.capacity()
                    + p.key.scope.0.storage_id.as_str().len()
                    + p.key.scope.1.as_str().len()
                    + p.directory.charge()
            })
            .sum::<usize>()
            + 128
    }
}
#[derive(Clone, PartialEq, Eq, Hash)]
enum Key {
    Directory(Scope),
    Body(PackKey),
}
#[derive(Clone)]
enum Value {
    Directory(Arc<ScopeDirectory>),
    Body(Arc<Vec<JournalStoredObject>>),
}
struct Entry {
    value: Value,
    charge: usize,
    touched: u64,
}

pub(super) struct Cache {
    entries: HashMap<Key, Entry>,
    pub budget: usize,
    pub charge: usize,
    clock: u64,
    pub index_decodes: u64,
    pub body_decodes: u64,
    pub pack_queries: u64,
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub oversize: u64,
}
impl Cache {
    pub fn new(budget: usize) -> Self {
        Self {
            entries: HashMap::new(),
            budget,
            charge: 0,
            clock: 0,
            index_decodes: 0,
            body_decodes: 0,
            pack_queries: 0,
            hits: 0,
            misses: 0,
            evictions: 0,
            oversize: 0,
        }
    }
    fn get(&mut self, key: &Key) -> Option<Value> {
        self.clock = self.clock.wrapping_add(1);
        let Some(entry) = self.entries.get_mut(key) else {
            self.misses += 1;
            return None;
        };
        self.hits += 1;
        entry.touched = self.clock;
        Some(entry.value.clone())
    }
    fn put(&mut self, key: Key, value: Value, charge: usize) {
        if let Some(old) = self.entries.remove(&key) {
            self.charge -= old.charge;
        }
        if charge > self.budget {
            self.oversize += 1;
            return;
        }
        while self.charge > self.budget - charge {
            let Some(oldest) = self
                .entries
                .iter()
                .min_by_key(|(_, e)| e.touched)
                .map(|(k, _)| k.clone())
            else {
                break;
            };
            if let Some(old) = self.entries.remove(&oldest) {
                self.evictions += 1;
                self.charge -= old.charge;
            }
        }
        self.clock = self.clock.wrapping_add(1);
        self.charge += charge;
        self.entries.insert(
            key,
            Entry {
                value,
                charge,
                touched: self.clock,
            },
        );
    }
    #[cfg(feature = "test-support")]
    pub fn directory_charge(&self) -> usize {
        self.entries
            .iter()
            .filter(|(key, _)| matches!(key, Key::Directory(_)))
            .map(|(_, entry)| entry.charge)
            .sum()
    }
    pub fn directory(&mut self, scope: &Scope) -> Option<Arc<ScopeDirectory>> {
        match self.get(&Key::Directory(scope.clone())) {
            Some(Value::Directory(v)) => Some(v),
            _ => None,
        }
    }
    pub fn put_directory(&mut self, scope: Scope, value: Arc<ScopeDirectory>) {
        let charge =
            value.charge() + scope.0.storage_id.as_str().len() + scope.1.as_str().len() + 128;
        self.put(Key::Directory(scope), Value::Directory(value), charge);
    }
    pub fn body(&mut self, key: &PackKey) -> Option<Arc<Vec<JournalStoredObject>>> {
        match self.get(&Key::Body(key.clone())) {
            Some(Value::Body(v)) => Some(v),
            _ => None,
        }
    }
    pub fn put_body(&mut self, key: PackKey, value: Arc<Vec<JournalStoredObject>>) {
        let charge = 256
            + key.owner.capacity()
            + key.scope.0.storage_id.as_str().len()
            + key.scope.1.as_str().len()
            + value.capacity() * std::mem::size_of::<JournalStoredObject>()
            + value.iter().map(|o| o.bytes.len() + 128).sum::<usize>();
        self.put(Key::Body(key), Value::Body(value), charge);
    }
    pub fn invalidate(&mut self, scope: &Scope) {
        self.entries.retain(|key, entry| {
            let remove = match key {
                Key::Directory(s) => s == scope,
                Key::Body(p) => &p.scope == scope,
            };
            if remove {
                self.charge -= entry.charge;
            }
            !remove
        });
    }
    #[cfg(feature = "test-support")]
    pub fn resize(&mut self, budget: usize) {
        self.entries.clear();
        self.charge = 0;
        self.budget = budget;
    }
}
