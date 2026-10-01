-- Runtime behavior is direct-merge-only, but this stored value is immutable
-- provenance for readiness, evidence, and release digests created before the
-- upgrade. New repositories continue to use the direct_merge column default.

UPDATE pr_provider_config
SET token_secret_ref = NULL;
