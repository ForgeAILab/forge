-- Pricing is configured as an adjustment on top of models.dev, not as a
-- hand-maintained per-model binding list.
--
-- Before this migration a provider entry or CLI runtime was priced only once
-- its owner opened a pricing dialog and bound every runtime model to one
-- catalog row or a manual rate. Nothing was inferred from the model name, so
-- most work stayed unpriced.
--
-- Forge now resolves the models.dev row automatically at admission and
-- materializes the binding itself. An owner only states how their price
-- differs from the list price:
--
-- * on a provider entry or CLI runtime, for every agent that uses it, and
-- * on an agent, overriding its provider's setting.
--
-- `mode`:
--   list      catalog rates unchanged
--   discount  catalog rates reduced by `discount_bps` (1/100 of a percent)
--   fixed     the four fixed rates, catalog ignored
--
-- `catalog_provider_id` / `catalog_model_id` pin the models.dev row when
-- inference cannot pick one (an OpenAI-compatible relay, a model listed by
-- several providers). A provider-scope pin names the catalog provider only;
-- the model comes from each agent's runtime model.
CREATE TABLE pricing_adjustment (
    id                               TEXT NOT NULL PRIMARY KEY,
    owner_user_id                    TEXT NOT NULL REFERENCES user(id) ON DELETE CASCADE,
    scope_kind                       TEXT NOT NULL
                                         CHECK (scope_kind IN (
                                             'provider_entry', 'cli_runtime', 'agent'
                                         )),
    provider_entry_id                TEXT REFERENCES credential_handle(id) ON DELETE CASCADE,
    daemon_id                        TEXT,
    executor_type                    TEXT,
    agent_id                         TEXT REFERENCES agent_identity(id) ON DELETE CASCADE,
    mode                             TEXT NOT NULL
                                         CHECK (mode IN ('list', 'discount', 'fixed')),
    discount_bps                     INTEGER
                                         CHECK (discount_bps IS NULL
                                             OR (typeof(discount_bps) = 'integer'
                                                 AND discount_bps >= 0
                                                 AND discount_bps <= 10000)),
    input_nano_usd_per_million       INTEGER
                                         CHECK (input_nano_usd_per_million IS NULL
                                             OR (typeof(input_nano_usd_per_million) = 'integer'
                                                 AND input_nano_usd_per_million >= 0
                                                 AND input_nano_usd_per_million <= 1000000000000000)),
    output_nano_usd_per_million      INTEGER
                                         CHECK (output_nano_usd_per_million IS NULL
                                             OR (typeof(output_nano_usd_per_million) = 'integer'
                                                 AND output_nano_usd_per_million >= 0
                                                 AND output_nano_usd_per_million <= 1000000000000000)),
    cache_read_nano_usd_per_million  INTEGER
                                         CHECK (cache_read_nano_usd_per_million IS NULL
                                             OR (typeof(cache_read_nano_usd_per_million) = 'integer'
                                                 AND cache_read_nano_usd_per_million >= 0
                                                 AND cache_read_nano_usd_per_million <= 1000000000000000)),
    cache_write_nano_usd_per_million INTEGER
                                         CHECK (cache_write_nano_usd_per_million IS NULL
                                             OR (typeof(cache_write_nano_usd_per_million) = 'integer'
                                                 AND cache_write_nano_usd_per_million >= 0
                                                 AND cache_write_nano_usd_per_million <= 1000000000000000)),
    catalog_provider_id              TEXT CHECK (catalog_provider_id IS NULL
                                         OR length(trim(catalog_provider_id)) > 0),
    catalog_model_id                 TEXT CHECK (catalog_model_id IS NULL
                                         OR length(trim(catalog_model_id)) > 0),
    version                          INTEGER NOT NULL DEFAULT 1
                                         CHECK (typeof(version) = 'integer' AND version >= 1),
    created_at                       TEXT NOT NULL CHECK (length(trim(created_at)) > 0),
    updated_at                       TEXT NOT NULL CHECK (length(trim(updated_at)) > 0),
    CHECK (
        (scope_kind = 'provider_entry'
            AND provider_entry_id IS NOT NULL
            AND daemon_id IS NULL AND executor_type IS NULL AND agent_id IS NULL
            AND catalog_model_id IS NULL)
        OR
        (scope_kind = 'cli_runtime'
            AND provider_entry_id IS NULL
            AND daemon_id IS NOT NULL AND length(trim(daemon_id)) > 0
            AND executor_type IS NOT NULL AND length(trim(executor_type)) > 0
            AND agent_id IS NULL
            AND catalog_model_id IS NULL)
        OR
        (scope_kind = 'agent'
            AND provider_entry_id IS NULL
            AND daemon_id IS NULL AND executor_type IS NULL
            AND agent_id IS NOT NULL)
    ),
    CHECK (catalog_model_id IS NULL OR catalog_provider_id IS NOT NULL),
    CHECK ((mode = 'discount') = (discount_bps IS NOT NULL)),
    CHECK (
        mode = 'fixed'
        OR (input_nano_usd_per_million IS NULL
            AND output_nano_usd_per_million IS NULL
            AND cache_read_nano_usd_per_million IS NULL
            AND cache_write_nano_usd_per_million IS NULL)
    ),
    CHECK (
        mode != 'fixed'
        OR input_nano_usd_per_million IS NOT NULL
        OR output_nano_usd_per_million IS NOT NULL
        OR cache_read_nano_usd_per_million IS NOT NULL
        OR cache_write_nano_usd_per_million IS NOT NULL
    )
);

CREATE UNIQUE INDEX ux_pricing_adjustment_provider_entry
    ON pricing_adjustment(owner_user_id, provider_entry_id)
    WHERE scope_kind = 'provider_entry';
CREATE UNIQUE INDEX ux_pricing_adjustment_cli_runtime
    ON pricing_adjustment(owner_user_id, daemon_id, executor_type)
    WHERE scope_kind = 'cli_runtime';
CREATE UNIQUE INDEX ux_pricing_adjustment_agent
    ON pricing_adjustment(owner_user_id, agent_id)
    WHERE scope_kind = 'agent';

-- A binding now belongs to a scope inside its subject: '' is the provider-
-- wide binding every agent shares, 'agent:<id>' is materialized for one agent
-- that carries its own adjustment. Admission resolves exactly one scope.
ALTER TABLE pricing_subject_binding ADD COLUMN scope_key TEXT NOT NULL DEFAULT '';

DROP INDEX ux_pricing_subject_binding_active;
CREATE UNIQUE INDEX ux_pricing_subject_binding_active
    ON pricing_subject_binding(subject_id, subject_revision_id, scope_key, source_kind, runtime_model)
    WHERE state = 'active';

CREATE TRIGGER pricing_subject_binding_scope_immutable_update
BEFORE UPDATE OF scope_key ON pricing_subject_binding
WHEN OLD.scope_key IS NOT NEW.scope_key
BEGIN
    SELECT RAISE(ABORT, 'pricing bindings cannot change scope');
END;

-- Carry explicit bindings forward as agent adjustments for the agents that
-- currently run that exact subject and model, so no configured price is
-- lost. A manual rate becomes a fixed adjustment; an explicit catalog choice
-- becomes a pin. Catalog rows Forge created itself for the reviewed direct
-- providers are not carried over: admission resolves the same row again.
INSERT OR IGNORE INTO pricing_adjustment (
    id, owner_user_id, scope_kind, agent_id, mode,
    input_nano_usd_per_million, output_nano_usd_per_million,
    cache_read_nano_usd_per_million, cache_write_nano_usd_per_million,
    version, created_at, updated_at
)
SELECT
    'pricing-adjustment:migrated:' || ai.id,
    b.owner_user_id,
    'agent',
    ai.id,
    'fixed',
    r.input_nano_usd_per_million,
    r.output_nano_usd_per_million,
    r.cache_read_nano_usd_per_million,
    r.cache_write_nano_usd_per_million,
    1,
    b.updated_at,
    b.updated_at
FROM pricing_subject_binding b
JOIN pricing_rate_revision r ON r.id = b.rate_revision_id
JOIN pricing_subject s ON s.id = b.subject_id
JOIN agent_profile ap ON ap.model = b.runtime_model
JOIN agent_identity ai ON ai.selected_profile_id = ap.id AND ai.owner_id = b.owner_user_id
WHERE b.state = 'active'
  AND b.source_kind = 'manual_override'
  AND (r.input_nano_usd_per_million IS NOT NULL
       OR r.output_nano_usd_per_million IS NOT NULL
       OR r.cache_read_nano_usd_per_million IS NOT NULL
       OR r.cache_write_nano_usd_per_million IS NOT NULL)
  AND (
      (s.subject_kind = 'provider_entry' AND ap.credential_ref = s.provider_entry_id)
      OR (s.subject_kind = 'cli_runtime' AND ap.credential_ref IS NULL
          AND ap.daemon_id = s.daemon_id AND ap.executor_type = s.executor_type)
  );

INSERT OR IGNORE INTO pricing_adjustment (
    id, owner_user_id, scope_kind, agent_id, mode,
    catalog_provider_id, catalog_model_id,
    version, created_at, updated_at
)
SELECT
    'pricing-adjustment:migrated:' || ai.id,
    b.owner_user_id,
    'agent',
    ai.id,
    'list',
    b.catalog_provider_id,
    b.catalog_model_id,
    1,
    b.updated_at,
    b.updated_at
FROM pricing_subject_binding b
JOIN pricing_subject s ON s.id = b.subject_id
JOIN pricing_subject_revision sr ON sr.id = b.subject_revision_id
JOIN agent_profile ap ON ap.model = b.runtime_model
JOIN agent_identity ai ON ai.selected_profile_id = ap.id AND ai.owner_id = b.owner_user_id
WHERE b.state = 'active'
  AND b.source_kind = 'models_dev_catalog'
  AND (
      (s.subject_kind = 'provider_entry' AND ap.credential_ref = s.provider_entry_id)
      OR (s.subject_kind = 'cli_runtime' AND ap.credential_ref IS NULL
          AND ap.daemon_id = s.daemon_id AND ap.executor_type = s.executor_type)
  )
  AND NOT (
      s.subject_kind = 'provider_entry'
      AND b.catalog_model_id = b.runtime_model
      AND b.catalog_provider_id = CASE sr.provider_kind
          WHEN 'openai' THEN 'openai'
          WHEN 'xai' THEN 'xai'
          WHEN 'gemini' THEN 'google'
          WHEN 'openrouter' THEN 'openrouter'
      END
  );

-- A subject whose only explicit price was one manual rate keeps it as the
-- subject's fixed adjustment, so an entry no agent currently runs does not
-- lose its configured price. Subjects with several manual rates cannot be
-- expressed as one adjustment; their agents were carried over above.
INSERT OR IGNORE INTO pricing_adjustment (
    id, owner_user_id, scope_kind, provider_entry_id, daemon_id, executor_type,
    mode, input_nano_usd_per_million, output_nano_usd_per_million,
    cache_read_nano_usd_per_million, cache_write_nano_usd_per_million,
    version, created_at, updated_at
)
SELECT
    'pricing-adjustment:migrated:' || s.id,
    s.owner_user_id,
    s.subject_kind,
    s.provider_entry_id,
    s.daemon_id,
    s.executor_type,
    'fixed',
    r.input_nano_usd_per_million,
    r.output_nano_usd_per_million,
    r.cache_read_nano_usd_per_million,
    r.cache_write_nano_usd_per_million,
    1,
    b.updated_at,
    b.updated_at
FROM pricing_subject s
JOIN pricing_subject_binding b ON b.subject_id = s.id
    AND b.state = 'active' AND b.source_kind = 'manual_override'
JOIN pricing_rate_revision r ON r.id = b.rate_revision_id
WHERE s.state = 'active'
  AND (s.subject_kind != 'provider_entry'
       OR EXISTS (SELECT 1 FROM credential_handle c WHERE c.id = s.provider_entry_id))
  AND (r.input_nano_usd_per_million IS NOT NULL
       OR r.output_nano_usd_per_million IS NOT NULL
       OR r.cache_read_nano_usd_per_million IS NOT NULL
       OR r.cache_write_nano_usd_per_million IS NOT NULL)
  AND (SELECT COUNT(*) FROM pricing_subject_binding other
       WHERE other.subject_id = s.id AND other.state = 'active'
         AND other.source_kind = 'manual_override') = 1;

-- Admission matches a runtime model across every provider in the active
-- snapshot.
CREATE INDEX idx_pricing_rate_catalog_snapshot_model
    ON pricing_rate_revision(catalog_snapshot_id, catalog_model_id)
    WHERE catalog_snapshot_id IS NOT NULL;
