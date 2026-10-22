//! Versioned row-bound AEAD and domain-separated keyed metadata lookups.
use agent_runtime::core::{error::RuntimeError, ids::SessionId, journal::*};
use chacha20poly1305::{
    XChaCha20Poly1305, XNonce,
    aead::{Aead, KeyInit, OsRng, Payload, rand_core::RngCore},
};
use hkdf::Hkdf;
use hmac::{Hmac, Mac};
use sha2_journal::Sha256;

use super::corrupt;

pub(super) const FORMAT: i64 = 1;
const ENVELOPE: &[u8] = b"FORGE-JOURNAL\0\x01";

/// Codes 4 and 5 belonged to the superseded domain-owned pack units and are
/// never reused: a session-scoped pack unit cannot alias a domain-bound one.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Unit {
    Session = 1,
    Pin = 2,
    Catalog = 3,
    Operations = 6,
    PackIndex = 7,
    PackBody = 8,
}
impl Unit {
    fn table(self) -> &'static [u8] {
        match self {
            Self::Session => b"session",
            Self::Pin => b"pin",
            Self::Catalog => b"catalog",
            Self::PackIndex | Self::PackBody => b"session_pack",
            Self::Operations => b"operation_pack",
        }
    }
    /// Body packs belong to a runtime session and are shared by its domains.
    fn session_scoped(self) -> bool {
        matches!(self, Self::PackIndex | Self::PackBody)
    }
}

/// Every field is length-delimited or fixed width. Owner binds the host mapping
/// too, so a row cannot be relabeled to a different Forge session. Metadata
/// units bind their domain; pack units are session-scoped and carry `None`.
pub(super) struct Binding<'a> {
    pub unit: Unit,
    pub domain: Option<&'a JournalDomain>,
    pub session: &'a SessionId,
    pub owner: &'a str,
    pub row: &'a [u8],
    pub generation: u64,
}

#[derive(Clone)]
pub(super) struct Keys {
    cipher: XChaCha20Poly1305,
    lookup: Hmac<Sha256>,
    pub revision: i64,
}

pub(super) struct Sealed {
    pub nonce: Vec<u8>,
    pub payload: Vec<u8>,
}

pub(super) fn role(domain: &JournalDomain) -> &'static str {
    match domain.role {
        JournalRole::Ordinary => "ordinary",
        JournalRole::Protected => "protected",
    }
}

fn field(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(&(bytes.len() as u64).to_be_bytes());
    out.extend_from_slice(bytes);
}

impl Keys {
    /// `master` is the exact key passed to SqliteProtectedRuntimeStore::new,
    /// SHA-256 of Forge's existing protected key material (server JWT secret).
    pub fn new(master: &[u8; 32], revision: i64) -> Result<Self, RuntimeError> {
        if revision != 1 {
            return Err(corrupt()); // key rotation is deliberately unsupported
        }
        let hkdf = Hkdf::<Sha256>::new(None, master);
        let mut aead = [0; 32];
        let mut lookup = [0; 32];
        hkdf.expand(b"forge/session-journal/aead/v1", &mut aead)
            .map_err(|_| corrupt())?;
        hkdf.expand(b"forge/session-journal/lookup/v1", &mut lookup)
            .map_err(|_| corrupt())?;
        Ok(Self {
            cipher: XChaCha20Poly1305::new((&aead).into()),
            lookup: <Hmac<Sha256> as Mac>::new_from_slice(&lookup).map_err(|_| corrupt())?,
            revision,
        })
    }

    fn aad(&self, binding: &Binding<'_>) -> Result<Vec<u8>, RuntimeError> {
        let mut out = ENVELOPE.to_vec();
        out.extend_from_slice(&(FORMAT as u32).to_be_bytes());
        out.extend_from_slice(&(self.revision as u64).to_be_bytes());
        field(&mut out, binding.unit.table());
        out.push(binding.unit as u8);
        match (binding.unit.session_scoped(), binding.domain) {
            (false, Some(domain)) => {
                field(&mut out, domain.storage_id.as_str().as_bytes());
                field(&mut out, role(domain).as_bytes());
            }
            // A fixed scope marker replaces the two domain fields.
            (true, None) => field(&mut out, b"runtime-session"),
            _ => return Err(corrupt()),
        }
        field(&mut out, binding.session.as_str().as_bytes());
        field(&mut out, binding.owner.as_bytes());
        field(&mut out, binding.row);
        out.extend_from_slice(&binding.generation.to_be_bytes());
        Ok(out)
    }

    pub fn seal(&self, binding: &Binding<'_>, bytes: &[u8]) -> Result<Sealed, RuntimeError> {
        #[cfg(feature = "test-support")]
        let _profile = crate::session_journal::profile::timer(
            format!("aead.seal.{}", binding.unit as u8),
            bytes.len(),
        );
        let mut nonce = vec![0; 24];
        OsRng.fill_bytes(&mut nonce);
        let aad = self.aad(binding)?;
        let ciphertext = self
            .cipher
            .encrypt(
                XNonce::from_slice(&nonce),
                Payload {
                    msg: bytes,
                    aad: &aad,
                },
            )
            .map_err(|_| corrupt())?;
        let mut payload = ENVELOPE.to_vec();
        payload.extend(ciphertext);
        Ok(Sealed { nonce, payload })
    }

    pub fn open(
        &self,
        binding: &Binding<'_>,
        nonce: &[u8],
        payload: &[u8],
        max: usize,
    ) -> Result<Vec<u8>, RuntimeError> {
        #[cfg(feature = "test-support")]
        let _profile = crate::session_journal::profile::timer(
            format!("aead.open.{}", binding.unit as u8),
            payload.len(),
        );
        if nonce.len() != 24
            || payload.len() < ENVELOPE.len() + 16
            || payload.len() - ENVELOPE.len() - 16 > max
            || !payload.starts_with(ENVELOPE)
        {
            return Err(corrupt());
        }
        let aad = self.aad(binding)?;
        self.cipher
            .decrypt(
                XNonce::from_slice(nonce),
                Payload {
                    msg: &payload[ENVELOPE.len()..],
                    aad: &aad,
                },
            )
            .map_err(|_| corrupt())
    }

    pub fn lookup(
        &self,
        unit: Unit,
        domain: &JournalDomain,
        session: &SessionId,
        id: &[u8],
    ) -> Vec<u8> {
        #[cfg(feature = "test-support")]
        let _profile = crate::session_journal::profile::timer("aead.lookup", id.len());
        let mut context = b"forge/session-journal/lookup/v1\0".to_vec();
        // pin and catalog contexts cannot alias even for identical input bytes.
        field(&mut context, unit.table());
        field(&mut context, domain.storage_id.as_str().as_bytes());
        field(&mut context, role(domain).as_bytes());
        field(&mut context, session.as_str().as_bytes());
        field(&mut context, id);
        let mut mac = self.lookup.clone();
        mac.update(&context);
        mac.finalize().into_bytes().to_vec()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn real_aead_tampering_nonces_and_every_aad_binding() {
        let keys = Keys::new(&[7; 32], 1).unwrap();
        let domain = JournalDomain {
            storage_id: JournalStorageId::new("protected"),
            role: JournalRole::Protected,
        };
        let session = SessionId::from("session");
        let row = 42u64.to_be_bytes();
        // Metadata units stay domain-bound: every domain field is authenticated.
        let binding = Binding {
            unit: Unit::Session,
            domain: Some(&domain),
            session: &session,
            owner: "owner",
            row: &row,
            generation: 1,
        };
        let first = keys.seal(&binding, b"secret").unwrap();
        let second = keys.seal(&binding, b"secret").unwrap();
        assert_ne!(first.nonce, second.nonce);
        assert_ne!(first.payload, second.payload);
        assert_eq!(
            keys.open(&binding, &first.nonce, &first.payload, 6)
                .unwrap(),
            b"secret"
        );
        assert!(
            keys.open(&binding, &first.nonce, &first.payload, 5)
                .is_err()
        );
        for i in 0..first.payload.len() {
            let mut payload = first.payload.clone();
            payload[i] ^= 1;
            assert!(keys.open(&binding, &first.nonce, &payload, 6).is_err());
        }
        let other = JournalDomain {
            storage_id: JournalStorageId::new("ordinary"),
            role: JournalRole::Ordinary,
        };
        let other_role = JournalDomain {
            storage_id: domain.storage_id.clone(),
            role: JournalRole::Ordinary,
        };
        let other_session = SessionId::from("other");
        for swapped in [
            Binding {
                unit: Unit::Pin,
                ..binding
            },
            Binding {
                domain: Some(&other),
                ..binding
            },
            Binding {
                domain: Some(&other_role),
                ..binding
            },
            // A domain-bound unit can never be opened as a session-scoped one.
            Binding {
                unit: Unit::PackIndex,
                domain: None,
                ..binding
            },
            Binding {
                session: &other_session,
                ..binding
            },
            Binding {
                owner: "another",
                ..binding
            },
            Binding {
                row: &43u64.to_be_bytes(),
                ..binding
            },
            Binding {
                generation: 2,
                ..binding
            },
        ] {
            assert!(
                keys.open(&swapped, &first.nonce, &first.payload, 6)
                    .is_err()
            );
        }
        assert!(
            Keys::new(&[8; 32], 1)
                .unwrap()
                .open(&binding, &first.nonce, &first.payload, 6)
                .is_err()
        );
        assert!(keys.open(&binding, &[0; 23], &first.payload, 6).is_err());
        assert!(Keys::new(&[7; 32], 2).is_err());
    }

    #[test]
    fn session_pack_units_bind_session_owner_row_and_generation_not_domain() {
        let keys = Keys::new(&[7; 32], 1).unwrap();
        let domain = JournalDomain {
            storage_id: JournalStorageId::new("protected"),
            role: JournalRole::Protected,
        };
        let session = SessionId::from("session");
        let other_session = SessionId::from("other");
        let row = 42u64.to_be_bytes();
        let binding = Binding {
            unit: Unit::PackIndex,
            domain: None,
            session: &session,
            owner: "owner",
            row: &row,
            generation: 1,
        };
        let sealed = keys.seal(&binding, b"secret").unwrap();
        assert_eq!(
            keys.open(&binding, &sealed.nonce, &sealed.payload, 6)
                .unwrap(),
            b"secret"
        );
        for swapped in [
            Binding {
                unit: Unit::PackBody,
                ..binding
            },
            Binding {
                session: &other_session,
                ..binding
            },
            Binding {
                owner: "another",
                ..binding
            },
            Binding {
                row: &43u64.to_be_bytes(),
                ..binding
            },
            Binding {
                generation: 2,
                ..binding
            },
            // Domain-bound shapes cannot open, or be sealed as, pack units.
            Binding {
                unit: Unit::Session,
                domain: Some(&domain),
                ..binding
            },
        ] {
            assert!(
                keys.open(&swapped, &sealed.nonce, &sealed.payload, 6)
                    .is_err()
            );
        }
        assert!(
            keys.seal(
                &Binding {
                    domain: Some(&domain),
                    ..binding
                },
                b"secret"
            )
            .is_err()
        );
        assert!(
            keys.seal(
                &Binding {
                    unit: Unit::Session,
                    domain: None,
                    ..binding
                },
                b"secret"
            )
            .is_err()
        );
    }

    #[test]
    fn metadata_units_and_keyed_lookups_are_separated_and_opaque() {
        let keys = Keys::new(&[7; 32], 1).unwrap();
        let domain = JournalDomain {
            storage_id: JournalStorageId::new("scope"),
            role: JournalRole::Ordinary,
        };
        let session = SessionId::from("s");
        let id = [0xab; 32];
        let pin = keys.lookup(Unit::Pin, &domain, &session, &id);
        let catalog = keys.lookup(Unit::Catalog, &domain, &session, &id);
        assert_eq!(pin.len(), 32);
        assert_ne!(pin, catalog);
        assert_ne!(pin, id);
        assert_ne!(
            pin,
            Keys::new(&[8; 32], 1)
                .unwrap()
                .lookup(Unit::Pin, &domain, &session, &id)
        );
        for unit in [
            Unit::Session,
            Unit::Pin,
            Unit::Catalog,
            Unit::Operations,
            Unit::PackIndex,
            Unit::PackBody,
        ] {
            let b = Binding {
                unit,
                domain: (!unit.session_scoped()).then_some(&domain),
                session: &session,
                owner: "o",
                row: &pin,
                generation: 0,
            };
            let sealed = keys.seal(&b, &id).unwrap();
            assert!(!sealed.payload.windows(4).any(|w| w == &id[..4]));
            assert_eq!(
                keys.open(&b, &sealed.nonce, &sealed.payload, 32).unwrap(),
                id
            );
        }
    }
    #[test]
    fn sealed_pack_contains_no_clear_ids_or_prefixes() {
        let keys = Keys::new(&[7; 32], 1).unwrap();
        let session = SessionId::from("s");
        let mut objects: Vec<_> = [42269, 139372]
            .into_iter()
            .map(|n| {
                let o = JournalObject::encode(JournalObjectKind::JsonInline, &n).unwrap();
                JournalStoredObject {
                    id: o.id,
                    kind: JournalObjectKind::JsonInline,
                    bytes: o.bytes.into(),
                }
            })
            .collect();
        objects.sort_by(|a, b| a.id.cmp(&b.id));
        let pack = super::super::codec::encode(&objects).unwrap();
        for (unit, raw) in [(Unit::PackIndex, &pack.index), (Unit::PackBody, &pack.body)] {
            let b = Binding {
                unit,
                domain: None,
                session: &session,
                owner: "o",
                row: &1u64.to_be_bytes(),
                generation: 0,
            };
            let sealed = keys.seal(&b, raw).unwrap();
            for object in &objects {
                let id = object.id.to_bytes();
                assert!(!sealed.payload.windows(4).any(|w| w == &id[..4]));
                assert!(
                    !sealed
                        .payload
                        .windows(64)
                        .any(|w| w == object.id.as_str().as_bytes())
                );
            }
            assert!(!sealed.payload.windows(7).any(|w| w == b"JRPACK\0"));
            assert_eq!(
                keys.open(&b, &sealed.nonce, &sealed.payload, raw.len())
                    .unwrap(),
                *raw
            );
        }
    }
}
