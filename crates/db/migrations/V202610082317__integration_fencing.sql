-- Additive owner/effect evidence. Repository and Project CASCADE is unchanged.
ALTER TABLE integration_attempt ADD COLUMN owner_fence_json TEXT
    CHECK(owner_fence_json IS NULL OR (json_valid(owner_fence_json) AND length(CAST(owner_fence_json AS BLOB)) <= 16384));
ALTER TABLE integration_attempt ADD COLUMN effect_intent_json TEXT
    CHECK(effect_intent_json IS NULL OR (json_valid(effect_intent_json) AND length(CAST(effect_intent_json AS BLOB)) <= 65536));
ALTER TABLE integration_attempt ADD COLUMN effect_receipts_json TEXT NOT NULL DEFAULT '[]'
    CHECK(json_valid(effect_receipts_json) AND json_type(effect_receipts_json) = 'array' AND length(CAST(effect_receipts_json AS BLOB)) <= 1048576);
