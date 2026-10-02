#!/usr/bin/env python3
"""Build deterministic, local-only Forge performance data. Python 3.10+, stdlib."""
from __future__ import annotations

import argparse
from contextlib import closing
import hashlib
import http.client
import json
import os
from pathlib import Path
import sqlite3
import socket
import subprocess
import tempfile
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

NAMESPACE = uuid.UUID('52af8ea8-8a8c-5d9a-ae85-e7c843216aa5')
EPOCH = datetime(2025, 1, 1, tzinfo=timezone.utc)
EMAIL = 'perf@example.test'
PASSWORD = 'Forge-perf-test-123!'
HEAVY_ROOTS = 12
HEAVY_CHILDREN = 4
HEAVY_EXECUTIONS = 10
HEAVY_REVIEWS = 3
HEAVY_TRANSITIONS = 15
HEAVY_ATTEMPTS = 3
HEAVY_EVENTS = 50_000
HEAVY_MESSAGES = 200
# The heavy Project carries no deferred-dispatch metadata, so its task list keeps
# its validator. A second, small Project carries it: there the validator is off.
DEFERRED_TASKS = 6
DEFERRED_EVERY = 3
IDLE_TABLES = ('task', 'execution', 'transition_log', 'workspace',
               'agent_chat_message', 'agent_chat_turn_job', 'domain_event')


def fixture_id(kind: str, index: int | str = 0) -> str:
    return str(uuid.uuid5(NAMESPACE, f'{kind}:{index}'))


def timestamp(index: int = 0) -> str:
    return (EPOCH + timedelta(seconds=index)).isoformat(timespec='seconds')


def encode(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False)


def quote(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


class SqlBuilder:
    """Check every required column, including future schema additions, before insert."""
    def __init__(self, connection: sqlite3.Connection):
        self.connection = connection
        self.schemas: dict[str, list] = {}
        self.tables: set[str] = set()

    def insert(self, table: str, **values: Any) -> None:
        if table not in self.schemas:
            self.schemas[table] = self.connection.execute(
                f'PRAGMA table_info({quote(table)})').fetchall()
        schema = self.schemas[table]
        if not schema:
            raise ValueError(f'baseline has no target table: {table}')
        columns = {row[1] for row in schema}
        for _, name, _, required, default, _ in schema:
            if required and default is None and name not in values:
                raise ValueError(f'cannot fill unknown NOT NULL column {table}.{name}')
        unknown = values.keys() - columns
        if unknown:
            raise ValueError(f'baseline lacks column {table}.{sorted(unknown)[0]}')
        names = list(values)
        sql = (f'INSERT INTO {quote(table)} ({", ".join(map(quote, names))}) '
               f'VALUES ({", ".join("?" for _ in names)})')
        try:
            self.connection.execute(sql, [values[name] for name in names])
        except sqlite3.Error as error:
            raise ValueError(f'inserting {table}: {error}') from error
        self.tables.add(table)


class LocalClient:
    """Persistent loopback HTTP connection; no proxy, redirects or remote hosts."""
    def __init__(self, port: int):
        self.connection = http.client.HTTPConnection('127.0.0.1', port, timeout=30)
        self.token: str | None = None

    def request(self, path: str, data: Any = None,
                headers: dict[str, str] | None = None) -> tuple[int, dict, bytes]:
        if not path.startswith('/') or path.startswith('//'):
            raise ValueError('request must be a local absolute path')
        combined = dict(headers or {})
        if self.token:
            combined['Authorization'] = f'Bearer {self.token}'
        body = None if data is None else encode(data).encode()
        if body is not None:
            combined['Content-Type'] = 'application/json'
        try:
            self.connection.request('GET' if data is None else 'POST', path,
                                    body=body, headers=combined)
            response = self.connection.getresponse()
            result = response.status, dict(response.getheaders()), response.read()
            return result
        except (OSError, http.client.HTTPException):
            self.connection.close()
            raise

    def json(self, path: str, data: Any = None) -> Any:
        status, _, body = self.request(path, data)
        if not 200 <= status < 300:
            raise ValueError(f'{path}: HTTP {status}')
        return json.loads(body)

    def close(self) -> None:
        self.connection.close()


def binary_info(binary: Path) -> tuple[str, str]:
    version = subprocess.check_output([str(binary), '--version'], text=True).strip()
    help_text = subprocess.check_output([str(binary), '--help'], text=True)
    return version, help_text


class Server:
    def __init__(self, binary: Path, directory: Path, port: int, log_path: Path):
        self.binary, self.directory, self.port = binary, directory, port
        self.log_path = log_path
        self.process: subprocess.Popen | None = None
        self.log: Any = None
        self.home: tempfile.TemporaryDirectory | None = None

    def __enter__(self) -> 'Server':
        if not 1 <= self.port <= 65535:
            raise ValueError('port must be between 1 and 65535')
        with socket.socket() as check:
            check.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            check.bind(('127.0.0.1', self.port))
        _, help_text = binary_info(self.binary)
        command = [str(self.binary), '--data-dir', str(self.directory)]
        if '--no-embedded-daemon' in help_text:
            command.append('--no-embedded-daemon')
        # Override only the child's HOME. FORGE_PERF_HOME permits an explicitly
        # designated test home; otherwise each launch gets an isolated scratch HOME.
        self.home = tempfile.TemporaryDirectory(prefix='forge-perf-home-')
        home = Path(os.environ.get('FORGE_PERF_HOME', self.home.name))
        home.mkdir(parents=True, exist_ok=True)
        env = dict(os.environ, HOME=str(home), FORGE_DATA_DIR=str(self.directory),
                   FORGE_SERVER_BIND=f'127.0.0.1:{self.port}', RUST_LOG='warn')
        # Ignore inherited production config locations; this is a synthetic server.
        for key in ('FORGE_CONFIG', 'FORGE_CONFIG_FILE', 'XDG_CONFIG_HOME', 'XDG_DATA_HOME'):
            env.pop(key, None)
        env['XDG_CONFIG_HOME'] = str(home / '.config')
        env['XDG_DATA_HOME'] = str(home / '.local/share')
        self.log = self.log_path.open('wb')
        try:
            self.started = time.perf_counter()
            self.process = subprocess.Popen(command, env=env, cwd=self.directory,
                                            stdout=self.log, stderr=self.log)
            client = LocalClient(self.port)
            deadline = self.started + 120
            try:
                while time.perf_counter() < deadline:
                    if self.process.poll() is not None:
                        raise ValueError(f'server exited; inspect local log {self.log_path}')
                    try:
                        status, _, _ = client.request('/healthz')
                        if status == 200:
                            self.healthy_seconds = time.perf_counter() - self.started
                            return self
                    except (OSError, http.client.HTTPException):
                        pass
                    time.sleep(0.05)
                raise ValueError(f'server did not become healthy; inspect {self.log_path}')
            finally:
                client.close()
        except BaseException:
            self.__exit__(None, None, None)
            raise

    def __exit__(self, *_: Any) -> None:
        if self.process is not None and self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=10)
        if self.log is not None:
            self.log.close()
        if self.home is not None:
            self.home.cleanup()


def validate_database(db: sqlite3.Connection) -> None:
    foreign = db.execute('PRAGMA foreign_key_check').fetchall()
    integrity = db.execute('PRAGMA integrity_check').fetchall()
    if foreign or integrity != [('ok',)]:
        raise ValueError(f'database validation failed: foreign_keys={foreign}, integrity={integrity}')


def canonical_dump(db: sqlite3.Connection, tables: list[str] | tuple[str, ...],
                   user_id: str | None = None) -> str:
    """Canonical seeded rows; registration's salted hash/identity/time are excluded."""
    result = {}
    for table in sorted(tables):
        columns = [row[1] for row in db.execute(f'PRAGMA table_info({quote(table)})')]
        keep = [i for i, name in enumerate(columns)
                if table != 'user' or name not in ('password_hash', 'created_at', 'updated_at')]
        rows = []
        for row in db.execute(f'SELECT * FROM {quote(table)}'):
            values = [row[i] for i in keep]
            if user_id:
                values = ['registered-user' if value == user_id else value for value in values]
            rows.append(values)
        result[table] = {'columns': [columns[i] for i in keep],
                         'rows': sorted(rows, key=encode)}
    return encode(result)


def table_digests(db: sqlite3.Connection) -> dict[str, str]:
    return {table: hashlib.sha256(canonical_dump(db, [table]).encode()).hexdigest()
            for table in IDLE_TABLES}


def normalize_bootstrap(db: sqlite3.Connection, user_id: str) -> list[dict]:
    """Retain baseline-created agents/profiles; normalize their generated identity/time."""
    db.execute('PRAGMA defer_foreign_keys=ON')
    # Registration makes an empty account chat. Recreate it with a stable identity.
    db.execute('DELETE FROM agent_chat WHERE account_id=?', (user_id,))
    db.execute('DELETE FROM refresh_token')
    db.row_factory = sqlite3.Row
    agents = [dict(row) for row in db.execute('SELECT * FROM agent_identity ORDER BY name')]
    for index, agent in enumerate(agents):
        old_id, new_id = agent['id'], fixture_id('agent', index)
        profiles = [dict(row) for row in db.execute(
            'SELECT * FROM agent_profile WHERE identity_id=? ORDER BY executor_type, id', (old_id,))]
        selected = agent['selected_profile_id']
        db.execute('UPDATE agent_identity SET selected_profile_id=NULL WHERE id=?', (old_id,))
        db.execute('UPDATE agent_identity SET id=?, paused=1, status=\'idle\', last_heartbeat_at=NULL, '
                   'created_at=?, updated_at=? WHERE id=?', (new_id, timestamp(), timestamp(), old_id))
        for profile_index, profile in enumerate(profiles):
            new_profile = fixture_id('profile', f'{index}:{profile_index}')
            # Profiles prohibit UPDATE. These pristine registration profiles have
            # no history: preserve every baseline field via an explicit reinsert.
            db.execute('DELETE FROM agent_profile WHERE id=?', (profile['id'],))
            original_id = profile['id']
            profile.update(id=new_profile, identity_id=new_id,
                           created_at=timestamp(), updated_at=timestamp())
            SqlBuilder(db).insert('agent_profile', **profile)
            if selected == original_id:
                db.execute('UPDATE agent_identity SET selected_profile_id=? WHERE id=?',
                           (new_profile, new_id))
    agents = [dict(row) for row in db.execute('SELECT * FROM agent_identity ORDER BY name')]
    db.row_factory = None
    if not agents:
        raise ValueError('baseline registration created no default agents')
    return agents


def seed_deferred_project(db: sqlite3.Connection, sql: SqlBuilder, workflow: dict,
                          user_id: str) -> None:
    """A small paused Project whose Tasks carry real deferred-dispatch metadata."""
    project_id, repo_id = fixture_id('deferred-project'), fixture_id('deferred-repo')
    states = [state['name'] for state in workflow['states']]
    sql.insert('project', id=project_id, name='Synthetic deferred-dispatch Project',
               owner_id=user_id, workflow_definition=encode(workflow), workflow_template_name='default',
               paused_at=timestamp(), system_pause_reason=None, created_at=timestamp(), updated_at=timestamp())
    db.execute('UPDATE agent_chat SET id=? WHERE project_id=?', (fixture_id('deferred-project-chat'), project_id))
    db.execute('UPDATE project_agent_binding SET id=? WHERE project_id=?',
               (fixture_id('deferred-project-binding'), project_id))
    sql.insert('repo', id=repo_id, project_id=project_id, name='synthetic-deferred',
               local_path='/nonexistent/forge-perf-fixture-deferred', created_at=timestamp(), updated_at=timestamp())
    db.execute('UPDATE project SET primary_repo_id=? WHERE id=?', (repo_id, project_id))
    sql.insert('project_member', id=fixture_id('deferred-member'), project_id=project_id, user_id=user_id,
               role='owner', created_at=timestamp(), updated_at=timestamp())
    for i in range(DEFERRED_TASKS):
        state = states[i % len(states)]
        metadata: dict[str, Any] = {}
        if i % DEFERRED_EVERY == 1:
            metadata['deferred_dispatch'] = {'not_before': timestamp(3_000_000_000),
                                             'reason': 'synthetic backoff', 'target_state': state}
        sql.insert('task', id=fixture_id('deferred-task', i), project_id=project_id, parent_task_id=None,
                   task_type='task', title=f'Deferred-dispatch Task {i:02d}',
                   description='Synthetic deferred dispatch. ' * 16, status=state, priority=i % 4,
                   board_position=float(i), subtask_order=None, metadata_json=encode(metadata),
                   created_at=timestamp(i * 1000), updated_at=timestamp(i * 1000 + 900))


def seed(db: sqlite3.Connection, workflow: dict, user_id: str) -> list[str]:
    sql = SqlBuilder(db)
    agents = normalize_bootstrap(db, user_id)
    agent = next((row for row in agents if row['name'] == 'Codex Default'), agents[0])
    agent_id, profile_id = agent['id'], agent['selected_profile_id']
    project_id, repo_id, chat_id = (fixture_id(kind) for kind in ('project', 'repo', 'chat'))
    states = [state['name'] for state in workflow['states']]
    if not {'done', 'cancelled', 'review', 'merging'}.issubset(states):
        raise ValueError('baseline default workflow lacks required heavy-profile states')
    sql.insert('project', id=project_id, name='Synthetic heavy performance Project',
               owner_id=user_id, workflow_definition=encode(workflow), workflow_template_name='default',
               paused_at=timestamp(), system_pause_reason=None, created_at=timestamp(), updated_at=timestamp())
    # Project insertion creates its setup chat/binding via baseline SQL triggers.
    # Normalize those generated identities too; do not bypass the triggers.
    db.execute('UPDATE agent_chat SET id=? WHERE project_id=?', (fixture_id('project-chat'), project_id))
    db.execute('UPDATE project_agent_binding SET id=? WHERE project_id=?', (fixture_id('project-binding'), project_id))
    sql.insert('repo', id=repo_id, project_id=project_id, name='synthetic',
               local_path='/nonexistent/forge-perf-fixture', created_at=timestamp(), updated_at=timestamp())
    db.execute('UPDATE project SET primary_repo_id=? WHERE id=?', (repo_id, project_id))
    sql.insert('project_member', id=fixture_id('member'), project_id=project_id, user_id=user_id,
               role='owner', created_at=timestamp(), updated_at=timestamp())
    task_count = HEAVY_ROOTS * (1 + HEAVY_CHILDREN)
    for i in range(task_count):
        task_id = fixture_id('task', i)
        state = states[i % len(states)]
        blocked = failed = barrier = annotation = None
        interruption = {'reason': 'Synthetic review evidence needs attention', 'created_at': timestamp(i),
                        'kind': 'ci_failed', 'source': 'review', 'execution_id': fixture_id('execution', i * HEAVY_EXECUTIONS + 8)}
        if i % 13 == 3:
            blocked = encode(interruption)
            annotation = encode({'type': 'ci_failed', 'blocking_reason': 'ci_failed',
                                 'blocked_at': timestamp(i), 'blocked_by': 'system:review',
                                 'blocked_execution_id': interruption['execution_id'],
                                 'message': interruption['reason'], 'recovery_actions': ['retry_hook']})
        if i % 13 == 4:
            failed = encode(dict(interruption, kind='executor_failed', source='execution'))
        if i % 13 == 5:
            barrier = encode({'state': state, 'status': 'blocked', 'started_at': timestamp(i),
                              'updated_at': timestamp(i), 'blocking_reason': 'synthetic CI step failed'})
        sql.insert('task', id=task_id, project_id=project_id,
                   parent_task_id=None if i < HEAVY_ROOTS else fixture_id('task', (i - HEAVY_ROOTS) // HEAVY_CHILDREN),
                   task_type='task' if i < HEAVY_ROOTS else 'sub_task', title=f'Performance Task {i:02d}',
                   description='Synthetic implementation and review history. ' * 16, status=state,
                   priority=i % 4, board_position=float(i), subtask_order=None if i < HEAVY_ROOTS else (i - HEAVY_ROOTS) % HEAVY_CHILDREN,
                   metadata_json=encode({}), blocked_json=blocked, failed_json=failed,
                   entry_barrier_json=barrier, error_annotation=annotation,
                   created_at=timestamp(i * 1000), updated_at=timestamp(i * 1000 + 900))
        for role in ('planner', 'coder', 'reviewer'):
            sql.insert('task_role_assignment', id=fixture_id('role', f'{i}:{role}'), task_id=task_id,
                       role_name=role, assignee_type='agent', assignee_id=agent_id,
                       created_at=timestamp(), updated_at=timestamp())
        workspace_id = fixture_id('workspace', i)
        sql.insert('workspace', id=workspace_id, task_id=task_id, repo_id=repo_id,
                   worktree_path=f'/nonexistent/forge-perf-fixture/task-{i}', branch=f'perf/task-{i}',
                   status='cleaned', cleanup_after=None, created_at=timestamp(), updated_at=timestamp())
        for j in range(HEAVY_EXECUTIONS):
            k = i * HEAVY_EXECUTIONS + j
            execution_id = fixture_id('execution', k)
            created, finished = timestamp(i * 1000 + j * 60), timestamp(i * 1000 + j * 60 + 50)
            sql.insert('execution', id=execution_id, task_id=task_id, agent_id=agent_id,
                       role='reviewer' if j % 3 == 2 else 'coder', status='completed' if j % 4 else 'failed',
                       summary=f'Synthetic completed attempt {j}', workspace_id=workspace_id,
                       created_at=created, updated_at=finished, stopped_at=finished)
            for attempt in range(HEAVY_ATTEMPTS):
                n = k * HEAVY_ATTEMPTS + attempt
                selection_id, invocation_id = fixture_id('selection', n), fixture_id('invocation', n)
                common = dict(owner_user_id=user_id, project_id=project_id, domain_kind='execution',
                              surface='task_execution', source_id=execution_id, execution_id=execution_id,
                              task_id=task_id, candidate_key=fixture_id('candidate', n), attempt_ordinal=attempt)
                sql.insert('pricing_selection', id=selection_id, **common, selection_status='unpriced',
                           selection_reason='missing_binding', selection_digest=fixture_id('selection-digest', n),
                           admitted_provider_id='synthetic', admitted_model_id='synthetic-model',
                           selected_at=created, created_at=created)
                sql.insert('usage_invocation', id=invocation_id, **common,
                           pricing_selection_id=selection_id, domain_idempotency_key=fixture_id('invocation-key', n),
                           admitted_provider_id='synthetic', admitted_model_id='synthetic-model',
                           agent_id=agent_id, profile_id=profile_id, executor_type='codex', backend_kind='cli',
                           agent_name_snapshot=agent['name'], project_name_snapshot='Synthetic heavy performance Project',
                           lifecycle='settled', telemetry_state='metered', admitted_at=created,
                           started_at=created, settled_at=finished, created_at=created, updated_at=finished)
                db.execute('UPDATE pricing_selection SET invocation_id=? WHERE id=?', (invocation_id, selection_id))
                event_common = {key: value for key, value in common.items() if key != 'domain_kind'}
                sql.insert('usage_event', id=fixture_id('usage', n), **event_common, invocation_id=invocation_id,
                           event_idempotency_key=fixture_id('usage-key', n), source_report_id=fixture_id('report', n),
                           report_mode='final_snapshot', provenance_kind='runtime_report', telemetry_state='metered',
                           provider_id='synthetic', model_id='synthetic-model', agent_id=agent_id, profile_id=profile_id,
                           executor_type='codex', agent_name_snapshot=agent['name'],
                           project_name_snapshot='Synthetic heavy performance Project', input_tokens=10_000 + n, output_tokens=2000 + n % 700,
                           cache_read_tokens=4000, cache_write_tokens=1000, cost_kind='provider_reported',
                           provider_reported_nano_usd=30_000_000 + n * 1000, occurred_at=finished, created_at=finished)
        for j in range(HEAVY_REVIEWS):
            awaiting = state == 'review' and j == HEAVY_REVIEWS - 1 and i % 2 == 0
            sql.insert('review', id=fixture_id('review', f'{i}:{j}'), task_id=task_id,
                       execution_id=fixture_id('execution', i * HEAVY_EXECUTIONS + j * 3 + 1),
                       reviewer_execution_id=fixture_id('execution', i * HEAVY_EXECUTIONS + j * 3 + 2),
                       attempt_number=j + 1, status='awaiting_human' if awaiting else ('passed' if j == 2 else 'failed'),
                       step_results_json=encode([{'index': step, 'command': command, 'exit_code': int(j < 2 and step == 1),
                                                 'stderr_tail': '', 'output_tail': 'Synthetic step result',
                                                 'started_at': timestamp(i * 1000 + j * 180),
                                                 'finished_at': timestamp(i * 1000 + j * 180 + 10)}
                                                for step, command in enumerate(('cargo check', 'cargo test', 'cargo fmt --check'))]),
                       started_at=timestamp(i * 1000 + j * 180), finished_at=None if awaiting else timestamp(i * 1000 + j * 180 + 50),
                       created_at=timestamp(i * 1000 + j * 180), updated_at=timestamp(i * 1000 + j * 180 + 50))
        for j in range(HEAVY_TRANSITIONS):
            sql.insert('transition_log', id=fixture_id('transition', f'{i}:{j}'), task_id=task_id,
                       from_state=states[j % len(states)], to_state=state if j == HEAVY_TRANSITIONS - 1 else states[(j + 1) % len(states)],
                       triggered_by='system:workflow', trigger_reason='Synthetic historical transition',
                       rejection=int(j in (5, 9)), hook_results_json='[]', created_at=timestamp(i * 1000 + j * 60))
    for root in range(HEAVY_ROOTS):
        first = HEAVY_ROOTS + root * HEAVY_CHILDREN
        for child in (1, 2):
            sql.insert('task_dependency', task_id=fixture_id('task', first + child),
                       depends_on_id=fixture_id('task', first + child - 1), created_at=timestamp())
    seed_deferred_project(db, sql, workflow, user_id)
    sql.insert('agent_chat', id=chat_id, kind='account_main', account_id=user_id,
               status='agent_setup_required', message_count=HEAVY_MESSAGES, last_message_at=timestamp(HEAVY_MESSAGES),
               created_at=timestamp(), updated_at=timestamp(HEAVY_MESSAGES))
    for i in range(HEAVY_MESSAGES):
        sql.insert('agent_chat_message', id=fixture_id('message', i), chat_id=chat_id, sequence=i + 1,
                   author_type='user' if i % 2 == 0 else 'agent', author_id=user_id if i % 2 == 0 else agent_id,
                   content=f'Synthetic message {i:03d}. ' + 'Local performance conversation. ' * 12,
                   status='complete', correlation_id=fixture_id('chat-correlation', i // 2), created_at=timestamp(i))
    for i in range(HEAVY_MESSAGES // 2):
        sql.insert('agent_chat_turn_job', id=fixture_id('turn', i), chat_id=chat_id,
                   triggering_message_id=fixture_id('message', i * 2), response_message_id=fixture_id('message', i * 2 + 1),
                   responder_identity_id=agent_id, profile_id=profile_id, canonical_scope_type='agent_chat',
                   canonical_scope_id=chat_id, status='succeeded', attempt_count=1,
                   dedupe_key=fixture_id('turn-dedupe', i), correlation_id=fixture_id('chat-correlation', i),
                   created_at=timestamp(i * 2), updated_at=timestamp(i * 2 + 1))
    event_types = ('task.created', 'task.status_changed', 'execution.completed', 'review.completed',
                   'agent_chat.message.admitted', 'agent_chat.response.completed', 'task.updated', 'agent.updated')
    for i in range(HEAVY_EVENTS):
        sql.insert('domain_event', sequence=i + 1, id=fixture_id('event', i), event_type=event_types[i % len(event_types)],
                   entity_type='task', entity_id=fixture_id('task', i % task_count), actor_type='system',
                   scope_type='project', scope_id=project_id, correlation_id=fixture_id('event-correlation', i),
                   payload_json=encode({'synthetic': True, 'index': i}), created_at=timestamp(i))
    db.execute('UPDATE event_consumer_cursor SET last_sequence=?, updated_at=?', (HEAVY_EVENTS, timestamp(HEAVY_EVENTS)))
    return sorted(sql.tables | {'agent_identity', 'agent_profile', 'project_agent_binding', 'event_consumer_cursor', 'user'})


def build(binary: Path, out: Path, port: int) -> dict:
    if out.exists() and any(out.iterdir()):
        raise ValueError('--out must be an empty data directory')
    out.mkdir(parents=True, exist_ok=True)
    version, _ = binary_info(binary)
    with Server(binary, out, port, out / 'bootstrap.log'):
        client = LocalClient(port)
        try:
            client.token = client.json('/api/v1/auth/register',
                                       {'email': EMAIL, 'password': PASSWORD})['access_token']
            workflow = client.json('/api/v1/workflow-templates/default')['definition']
        finally:
            client.close()
    with closing(sqlite3.connect(out / 'forge.db')) as db, db:
        db.execute('PRAGMA foreign_keys=ON')
        user_id = db.execute('SELECT id FROM user WHERE email=?', (EMAIL,)).fetchone()[0]
        tables = seed(db, workflow, user_id)
        db.commit()
        validate_database(db)
        digest = hashlib.sha256(canonical_dump(db, tables, user_id).encode()).hexdigest()
        counts = {table: db.execute(f'SELECT count(*) FROM {quote(table)}').fetchone()[0] for table in tables}
        db.execute('PRAGMA wal_checkpoint(TRUNCATE)')
    report = {'schema': 'forge.perf-fixture/2', 'profile': 'heavy', 'baseline_version': version,
              'email': EMAIL, 'project_id': fixture_id('project'), 'chat_id': fixture_id('chat'),
              'deferred_project_id': fixture_id('deferred-project'),
              'task_ids': [fixture_id('task', i) for i in (0, 1, 2, 3, 4, 12, 13, 14, 15, 16)],
              'counts': counts, 'seeded_tables': tables, 'canonical_sha256': digest,
              'db_bytes': (out / 'forge.db').stat().st_size,
              'idle_guards': ['manually paused Projects; paused idle default agents; no live leases',
                              'terminal executions and chat jobs; cleaned workspaces with no cleanup deadline',
                              'every baseline consumer cursor at the event head; no plan-publication claims'],
              'notes': ['The heavy Project has no deferred-dispatch metadata; the small deferred Project carries it on two Tasks.',
                        'Usage uses three settled provider attempts per execution with provider-reported costs; no remote pricing catalog required.',
                        'Some reviews await human approval; no reviewer execution is running.',
                        'Registration password is the public test constant PASSWORD in perf_fixture.py.']}
    with (out / 'perf-fixture.json').open('x', encoding='utf-8') as target:
        json.dump(report, target, indent=2, sort_keys=True, allow_nan=False)
        target.write('\n')
    print(f'{version}; heavy fixture; DB {report["db_bytes"]:,} bytes')
    for table, count in sorted(counts.items()):
        print(f'  {table}: {count:,}')
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    command = commands.add_parser('build')
    command.add_argument('--baseline-binary', type=Path, required=True)
    command.add_argument('--out', type=Path, required=True)
    command.add_argument('--profile', choices=('heavy',), default='heavy')
    command.add_argument('--port', type=int, default=18101)
    args = parser.parse_args()
    try:
        build(args.baseline_binary.resolve(), args.out.resolve(), args.port)
    except (OSError, ValueError, sqlite3.Error, subprocess.SubprocessError) as error:
        parser.exit(1, f'Cannot build fixture: {error}\n')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
