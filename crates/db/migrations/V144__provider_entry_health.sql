-- Live health of a provider entry, learned from real provider calls.
--
-- Before this table the only health signal was a one-time `GET /models` probe
-- when an agent was created. A provider that started returning 429s, 5xx, or
-- 401s kept showing "Connected", its agents stayed eligible, and every queued
-- chat turn burned its retry budget against the failing endpoint.
--
-- Transient failures extend `backoff_until` exponentially. Exhausted usage
-- waits for its reset. Auth failures remain blocked until a successful manual
-- connection test. After a timed backoff lapses, the next call is the trial.
CREATE TABLE provider_entry_health (
    credential_id         TEXT NOT NULL PRIMARY KEY
                              REFERENCES credential_handle(id) ON DELETE CASCADE,
    owner_user_id         TEXT NOT NULL REFERENCES user(id) ON DELETE CASCADE,
    status                TEXT NOT NULL
                              CHECK (status IN ('healthy', 'backoff', 'error')),
    consecutive_failures  INTEGER NOT NULL DEFAULT 0
                              CHECK (typeof(consecutive_failures) = 'integer'
                                  AND consecutive_failures >= 0),
    last_error_kind       TEXT CHECK (last_error_kind IS NULL OR last_error_kind IN (
                              'rate_limited', 'usage_exhausted', 'server_error',
                              'network', 'auth'
                          )),
    last_error_message    TEXT CHECK (last_error_message IS NULL
                              OR length(last_error_message) <= 500),
    last_failure_at       TEXT,
    last_success_at       TEXT,
    backoff_until         TEXT,
    version               INTEGER NOT NULL DEFAULT 1 CHECK (version >= 1),
    updated_at            TEXT NOT NULL CHECK (length(trim(updated_at)) > 0),
    CHECK (
        (status = 'healthy' AND consecutive_failures = 0
            AND last_error_kind IS NULL AND backoff_until IS NULL)
        OR (status = 'backoff' AND consecutive_failures > 0
            AND last_error_kind IN ('rate_limited', 'server_error', 'network')
            AND backoff_until IS NOT NULL)
        OR (status = 'error' AND consecutive_failures > 0
            AND ((last_error_kind = 'auth' AND backoff_until IS NULL)
                OR (last_error_kind = 'usage_exhausted' AND backoff_until IS NOT NULL)))
    )
);

CREATE INDEX idx_provider_entry_health_owner
    ON provider_entry_health(owner_user_id);
