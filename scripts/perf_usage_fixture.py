"""Local usage-read fixtures and mutations; all production SQLite guards stay enabled."""
from __future__ import annotations
import hashlib
import json
import sqlite3
import uuid
from perf_fixture import SqlBuilder, fixture_id, timestamp, validate_database


def insert(db: sqlite3.Connection, table: str, row: dict) -> None:
    db.execute(f'INSERT INTO {table} ({",".join(row)}) VALUES ({",".join("?" for _ in row)})', tuple(row.values()))


def pricing(db: sqlite3.Connection, owner: str, project: str) -> tuple[str,str,str]:
    sql = SqlBuilder(db)
    catalog, rate, preview = (fixture_id('usage-bench-' + key) for key in ('catalog','rate','preview'))
    payload = json.dumps({'synthetic': {'models': {'synthetic-model': {'cost': {'input': 1, 'output': 2}}}}}, sort_keys=True)
    sql.insert('pricing_catalog_snapshot', id=catalog, source_kind='models_dev_catalog', source_url='https://models.dev/api.json', payload_sha256=hashlib.sha256(payload.encode()).hexdigest(), parser_revision='usage-bench', revision_digest='usage-bench-catalog', payload_json=payload, fetched_at=timestamp(), created_at=timestamp())
    sql.insert('pricing_rate_revision', id=rate, source_kind='models_dev_catalog', catalog_snapshot_id=catalog, catalog_provider_id='synthetic', catalog_model_id='synthetic-model', source_model_key='synthetic/synthetic-model', input_nano_usd_per_million=1_000_000_000, output_nano_usd_per_million=2_000_000_000, cache_read_nano_usd_per_million=0, cache_write_nano_usd_per_million=0, rate_digest='usage-bench-rate', effective_at=timestamp(), created_at=timestamp())
    sql.insert('cost_estimation_preview', id=preview, owner_user_id=owner, project_id=project, catalog_snapshot_id=catalog, catalog_freshness='fresh', usage_set_digest='usage-bench-set', idempotency_key='usage-bench-preview', expires_at=timestamp(1_000_000), created_at=timestamp(), updated_at=timestamp())
    return catalog,rate,preview


def revision(db: sqlite3.Connection, event: dict, provenance: tuple[str,str,str], ordinal: int, *, run: str | None = None) -> None:
    catalog,rate,preview = provenance
    identity = str(uuid.uuid4())
    if run is None:
        run = identity
        SqlBuilder(db).insert('cost_estimation_run', id=run, owner_user_id=event['owner_user_id'], project_id=event['project_id'], preview_id=preview, catalog_snapshot_id=catalog, usage_set_digest='usage-bench-set', status='committed', idempotency_key=identity, created_at=timestamp(), updated_at=timestamp(), completed_at=timestamp())
    old = db.execute('SELECT MAX(revision) FROM cost_estimate_revision WHERE usage_event_id=?', (event['id'],)).fetchone()[0] or 0
    SqlBuilder(db).insert('cost_estimate_revision', id=identity, run_id=run, owner_user_id=event['owner_user_id'], project_id=event['project_id'], usage_event_id=event['id'], revision=old+1, state='applied', rate_revision_id=rate, catalog_snapshot_id=catalog, estimated_nano_usd=30_000_000+ordinal*1000, formula_revision='usage-bench-formula', retrospective=1, estimate_digest=identity, created_at=timestamp(ordinal))


def seed_estimates(db: sqlite3.Connection, owner: str, project: str) -> None:
    provenance=pricing(db,owner,project)
    run=fixture_id('usage-bench-seed-run')
    SqlBuilder(db).insert('cost_estimation_run', id=run, owner_user_id=owner, project_id=project, preview_id=provenance[2], catalog_snapshot_id=provenance[0], usage_set_digest='usage-bench-set', status='committed', idempotency_key='usage-bench-seed-run', created_at=timestamp(), updated_at=timestamp(), completed_at=timestamp())
    db.row_factory=sqlite3.Row
    events=[dict(row) for row in db.execute('SELECT * FROM usage_event ORDER BY rowid')]
    for ordinal,event in enumerate(events):
        # Stable identities keep this named fixture deterministic.
        identity=fixture_id('usage-bench-seed-revision',ordinal)
        SqlBuilder(db).insert('cost_estimate_revision', id=identity, run_id=run, owner_user_id=owner, project_id=project, usage_event_id=event['id'], revision=1, state='applied', rate_revision_id=provenance[1], catalog_snapshot_id=provenance[0], estimated_nano_usd=30_000_000+ordinal*1000, formula_revision='usage-bench-formula', retrospective=1, estimate_digest=identity, created_at=timestamp(ordinal))
    db.row_factory=None


def scale(db: sqlite3.Connection, factor: int) -> None:
    if factor==1: return
    db.row_factory=sqlite3.Row
    tables={table:[dict(row) for row in db.execute('SELECT * FROM '+table)] for table in ('execution','pricing_selection','usage_invocation','usage_event','cost_estimate_revision')}
    db.row_factory=None
    for copy in range(1,factor):
        maps={table:{row['id']:str(uuid.uuid5(uuid.NAMESPACE_URL,f'usage-scale-{copy}-{table}-{row["id"]}')) for row in rows} for table,rows in tables.items()}
        em,im,pm,evm,rm=(maps[table] for table in ('execution','usage_invocation','pricing_selection','usage_event','cost_estimate_revision'))
        for table in tables:
            for old in tables[table]:
                row=old.copy();row['id']=maps[table][old['id']]
                for column,mapping in [('parent_execution_id',em),('source_id',em),('execution_id',em),('pricing_selection_id',pm),('invocation_id',im),('usage_event_id',evm),('supersedes_revision_id',rm)]:
                    if column in row: row[column]=mapping.get(row[column],row[column])
                if table=='pricing_selection':row['invocation_id']=None
                for column in ('selection_digest','domain_idempotency_key','event_idempotency_key','estimate_digest','legacy_source_id'):
                    if column in row and row[column] is not None:row[column]=f'scale-{copy}-{row[column]}'
                insert(db,table,row)
    db.commit();validate_database(db)
