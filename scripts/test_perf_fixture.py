"""Focused perf tooling tests; stdlib only, no server needed.

PERF_FIXTURE_A/B optionally compare two already-built real baseline fixtures.
"""
from contextlib import closing
from datetime import datetime
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import tempfile
from types import SimpleNamespace
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit
import unittest
import uuid

from perf_fixture import (DEFERRED_TASKS, SqlBuilder, canonical_dump, fixture_id,
                          seed_deferred_project, timestamp, validate_database)
from perf_bench import (NO_VALIDATOR, VALIDATOR_OFF, compare, cpu_seconds, distribution,
                        idle_window, measure, scenarios)

# The server's own test for "this Project has deferred dispatch": the task list
# validator is off while it matches any Task (crates/db task_list_read.rs).
DEFERRED_DISPATCH_TASKS = ("SELECT count(*) FROM task WHERE project_id = ? AND json_valid(metadata_json) "
                           "AND json_type(metadata_json, '$.deferred_dispatch') IS NOT NULL "
                           "AND deleted_at IS NULL")


class FixtureTests(unittest.TestCase):
    def test_ids_are_stable_namespaced_uuid5(self):
        self.assertEqual(fixture_id('task', 7), 'e65f5917-7f7c-5a30-a02a-e6e45745ef31')
        self.assertEqual(uuid.UUID(fixture_id('task', 7)).version, 5)
        self.assertNotEqual(fixture_id('task', 7), fixture_id('execution', 7))
        self.assertNotEqual(fixture_id('task', 7), fixture_id('task', 8))

    def test_timestamps_use_fixed_epoch(self):
        self.assertEqual(timestamp(), '2025-01-01T00:00:00+00:00')
        self.assertEqual(timestamp(86401), '2025-01-02T00:00:01+00:00')
        self.assertEqual(timestamp(86401), timestamp(86401))

    def test_unknown_required_column_names_table_and_column(self):
        with closing(sqlite3.connect(':memory:')) as db, db:
            db.execute('CREATE TABLE task (id TEXT PRIMARY KEY, future_payload TEXT NOT NULL)')
            with self.assertRaisesRegex(ValueError, r'task\.future_payload'):
                SqlBuilder(db).insert('task', id='test')
            self.assertEqual(db.execute('SELECT count(*) FROM task').fetchone()[0], 0)

    def test_explicit_columns_allow_defaults_and_reordered_schema(self):
        with closing(sqlite3.connect(':memory:')) as db, db:
            db.execute("CREATE TABLE task (future_payload TEXT NOT NULL DEFAULT 'default', title TEXT, id TEXT PRIMARY KEY)")
            builder = SqlBuilder(db)
            builder.insert('task', id='stable-id', title='stable-title')
            self.assertEqual(db.execute('SELECT id,title,future_payload FROM task').fetchone(),
                             ('stable-id', 'stable-title', 'default'))
            with self.assertRaisesRegex(ValueError, 'task.typo'):
                builder.insert('task', id='second', typo='unexpected')

    def test_integrity_checker_rejects_foreign_key_errors(self):
        with closing(sqlite3.connect(':memory:')) as db, db:
            db.execute('CREATE TABLE parent (id TEXT PRIMARY KEY)')
            db.execute('CREATE TABLE child (parent_id TEXT REFERENCES parent(id))')
            db.execute("INSERT INTO child (parent_id) VALUES ('missing')")
            with self.assertRaisesRegex(ValueError, 'foreign_keys'):
                validate_database(db)

    def test_canonical_dump_excludes_only_registration_variations(self):
        def sample(identity, password, reverse):
            with closing(sqlite3.connect(':memory:')) as db, db:
                db.execute('CREATE TABLE user (id TEXT, email TEXT, password_hash TEXT, created_at TEXT, updated_at TEXT)')
                db.execute('CREATE TABLE task (id TEXT, owner_id TEXT, updated_at TEXT)')
                db.execute('INSERT INTO user (id,email,password_hash,created_at,updated_at) VALUES (?,?,?,?,?)',
                           (identity, 'perf@example.test', password, password, password))
                indexes = [2, 1] if reverse else [1, 2]
                for index in indexes:
                    db.execute('INSERT INTO task (id,owner_id,updated_at) VALUES (?,?,?)',
                               (fixture_id('task', index), identity, timestamp(index)))
                return canonical_dump(db, ['task', 'user'], identity)
        self.assertEqual(sample('registration-a', 'salt-a', False),
                         sample('registration-b', 'salt-b', True))
        self.assertIn('updated_at', json.loads(sample('a', 'b', False))['task']['columns'])

    def test_deferred_project_is_small_paused_and_matches_the_server_predicate(self):
        with closing(sqlite3.connect(':memory:')) as db, db:
            db.executescript("""
                CREATE TABLE project (id TEXT PRIMARY KEY, name TEXT NOT NULL, owner_id TEXT,
                    workflow_definition TEXT, workflow_template_name TEXT, paused_at TEXT,
                    system_pause_reason TEXT, primary_repo_id TEXT, created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL);
                CREATE TABLE repo (id TEXT PRIMARY KEY, project_id TEXT NOT NULL, name TEXT NOT NULL,
                    local_path TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
                CREATE TABLE project_member (id TEXT PRIMARY KEY, project_id TEXT NOT NULL,
                    user_id TEXT NOT NULL, role TEXT NOT NULL, created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL);
                CREATE TABLE task (id TEXT PRIMARY KEY, project_id TEXT NOT NULL, parent_task_id TEXT,
                    task_type TEXT NOT NULL, title TEXT NOT NULL, description TEXT, status TEXT NOT NULL,
                    priority INTEGER NOT NULL, board_position REAL NOT NULL, subtask_order INTEGER,
                    metadata_json TEXT, deleted_at TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
                CREATE TABLE agent_chat (id TEXT PRIMARY KEY, project_id TEXT);
                CREATE TABLE project_agent_binding (id TEXT PRIMARY KEY, project_id TEXT);
                CREATE TRIGGER project_chat AFTER INSERT ON project BEGIN
                    INSERT INTO agent_chat VALUES ('generated-chat', NEW.id);
                    INSERT INTO project_agent_binding VALUES ('generated-binding', NEW.id);
                END;
            """)
            workflow = {'states': [{'name': name} for name in ('backlog', 'todo', 'in_progress', 'done')]}
            seed_deferred_project(db, SqlBuilder(db), workflow, 'owner')
            project = fixture_id('deferred-project')
            self.assertNotEqual(project, fixture_id('project'))
            self.assertEqual(db.execute('SELECT id, owner_id, paused_at IS NOT NULL, primary_repo_id FROM project').fetchall(),
                             [(project, 'owner', 1, fixture_id('deferred-repo'))])
            self.assertEqual(db.execute('SELECT count(*) FROM task WHERE project_id = ?', (project,)).fetchone()[0],
                             DEFERRED_TASKS)
            deferred = db.execute(DEFERRED_DISPATCH_TASKS, (project,)).fetchone()[0]
            self.assertEqual(deferred, 2)
            self.assertLess(deferred, DEFERRED_TASKS)
            # Dispatch stays deferred for the life of any run: the fixture must remain idle.
            dates = [json.loads(row[0])['deferred_dispatch']['not_before'] for row in db.execute(
                "SELECT metadata_json FROM task WHERE metadata_json != '{}'")]
            self.assertTrue(all(datetime.fromisoformat(value).year > 2100 for value in dates))
            # Trigger-created identities are normalized, as for the heavy Project.
            self.assertEqual(db.execute('SELECT id FROM agent_chat').fetchall(),
                             [(fixture_id('deferred-project-chat'),)])
            self.assertEqual(db.execute('SELECT id FROM project_agent_binding').fetchall(),
                             [(fixture_id('deferred-project-binding'),)])

    @unittest.skipUnless(os.environ.get('PERF_FIXTURE_A') and os.environ.get('PERF_FIXTURE_B'),
                         'set PERF_FIXTURE_A/B to compare two built baseline fixtures')
    def test_two_real_fixture_builds_have_identical_seeded_rows(self):
        dumps, manifests = [], []
        for variable in ('PERF_FIXTURE_A', 'PERF_FIXTURE_B'):
            directory = Path(os.environ[variable])
            manifest = json.loads((directory / 'perf-fixture.json').read_text())
            manifests.append(manifest)
            with closing(sqlite3.connect((directory / 'forge.db').resolve().as_uri() + '?mode=ro', uri=True)) as db:
                validate_database(db)
                user = db.execute('SELECT id FROM user WHERE email=?', (manifest['email'],)).fetchone()[0]
                dump = canonical_dump(db, manifest['seeded_tables'], user)
                self.assertEqual(hashlib.sha256(dump.encode()).hexdigest(), manifest['canonical_sha256'])
                dumps.append(dump)
                # The heavy Project keeps its task-list validator; only the small one switches it off.
                self.assertEqual(db.execute(DEFERRED_DISPATCH_TASKS, (manifest['project_id'],)).fetchone()[0], 0)
                self.assertEqual(db.execute(DEFERRED_DISPATCH_TASKS, (manifest['deferred_project_id'],)).fetchone()[0], 2)
        self.assertEqual(manifests[0]['baseline_version'], manifests[1]['baseline_version'])
        self.assertEqual(dumps[0], dumps[1])


class BenchmarkTests(unittest.TestCase):
    def test_compare_renders_timings_ratios_and_server_figures(self):
        a = {'label': 'before', 'scenarios': [{'name': 'tasks', 'p50_ms': 10, 'p95_ms': 20}],
             'server': {'first_start_healthy_seconds': 2, 'idle': {'cpu_seconds': 1}}}
        b = {'label': 'after', 'scenarios': [{'name': 'tasks', 'p50_ms': 5, 'p95_ms': 5}],
             'server': {'first_start_healthy_seconds': 1, 'idle': {'cpu_seconds': .5}}}
        table = compare(a, b)
        self.assertIn('| tasks | 10.000 / 20.000 | 5.000 / 5.000 | 0.50× / 0.25× |', table)
        self.assertIn('| First start to healthy (s) | 2 | 1 |', table)
        self.assertIn('| Idle CPU seconds | 1 | 0.5 |', table)

    def test_compare_handles_skips_errors_and_missing_scenarios(self):
        a = {'label': 'a', 'scenarios': [{'name': 'missing', 'skipped': 'HTTP 404'},
                                       {'name': 'error', 'p50_ms': 0, 'p95_ms': 1, 'errors': 100, 'status_counts': {'500': 100}}]}
        b = {'label': 'b', 'scenarios': [{'name': 'error', 'p50_ms': 2, 'p95_ms': 1}]}
        table = compare(a, b)
        self.assertIn('skip: HTTP 404 | absent', table)
        self.assertIn('100 request errors', table)
        self.assertIn('— / 1.00×', table)

    def test_nearest_rank_percentiles(self):
        self.assertEqual(distribution(list(range(1, 101))), {'min': 1, 'p50': 50, 'p95': 95, 'max': 100})

    def test_ps_time_formats(self):
        self.assertEqual(cpu_seconds('00:02.50'), 2.5)
        self.assertEqual(cpu_seconds('01:02:03'), 3723)
        self.assertEqual(cpu_seconds('2-01:02:03'), 176523)

    def test_http_errors_are_results_and_304_is_success(self):
        class Client:
            def __init__(self, status):
                self.status, self.calls = status, 0
            def request(self, *args, **kwargs):
                self.calls += 1
                return self.status, {'Content-Type': 'application/json'}, b'body'
        client = Client(500)
        row = measure(client, 'failed', '/api/v1/test')
        self.assertEqual(row['errors'], 100)
        self.assertEqual(row['status_counts'], {'500': 100})
        self.assertEqual(row['response_bytes']['p50'], 4)
        self.assertEqual(client.calls, 111)  # One availability probe, 10 warmups, 100 samples.
        row = measure(Client(304), 'etag', '/api/v1/test')
        self.assertEqual(row['errors'], 0)
        self.assertEqual(row['share_304'], 1)

    def test_compare_labels_unavailable_process_figures(self):
        sample = {'label': 'restricted', 'scenarios': [],
                  'server': {'idle': {'cpu_seconds': None, 'peak_sampled_rss_bytes': None,
                                      'ps_error': 'ps unavailable: denied'}}}
        table = compare(sample, sample)
        self.assertIn('| Idle CPU seconds | unavailable | unavailable |', table)
        self.assertIn('ps unavailable: denied', table)

    def test_scenario_analytics_windows_are_rfc3339_and_include_fixture_epoch(self):
        class Client:
            def request(self, *args, **kwargs):
                return 200, {}, b'{}'
        manifest = {'project_id': fixture_id('project'), 'chat_id': fixture_id('chat'),
                    'task_ids': [fixture_id('task', i) for i in range(10)]}
        def measured(client, name, path, headers=None):
            return {'name': name, 'path': path}
        with patch('perf_bench.measure', side_effect=measured):
            rows = scenarios(Client(), manifest)
        analytics = [row for row in rows if row['name'] in ('project analytics', 'account usage')]
        self.assertEqual(len(analytics), 2)
        for row in analytics:
            query = parse_qs(urlsplit(row['path']).query)
            start, finish = [datetime.fromisoformat(query[key][0]) for key in ('from', 'to')]
            self.assertIsNotNone(start.tzinfo)
            self.assertEqual(start.isoformat(), timestamp())
            self.assertEqual((finish - start).total_seconds(), 2 * 86400)

    def task_list_rows(self, manifest, etag_for):
        class Client:
            def request(self, path, data=None, headers=None):
                found = {'ETag': 'W/"tasks-1"'} if '/tasks?' in path and etag_for(path) else {}
                return 200, found, b'{}'
        def measured(client, name, path, headers=None):
            return {'name': name, 'path': path, 'headers': headers}
        with patch('perf_bench.measure', side_effect=measured):
            rows = scenarios(Client(), manifest)
        return {row['name']: row for row in rows if row['name'].startswith('tasks')}

    def test_conditional_list_is_measured_and_deferred_project_reports_validator_off(self):
        heavy, deferred = fixture_id('project'), fixture_id('deferred-project')
        manifest = {'project_id': heavy, 'deferred_project_id': deferred, 'chat_id': fixture_id('chat'),
                    'task_ids': [fixture_id('task', i) for i in range(10)]}
        rows = self.task_list_rows(manifest, lambda path: heavy in path)
        self.assertEqual(list(rows), ['tasks limit=20', 'tasks ETag limit=20', 'tasks limit=50',
                                      'tasks ETag limit=50', 'tasks limit=100', 'tasks ETag limit=100',
                                      'tasks deferred limit=20', 'tasks deferred ETag limit=20'])
        for limit in (20, 50, 100):
            self.assertIsNone(rows[f'tasks limit={limit}']['headers'])
            conditional = rows[f'tasks ETag limit={limit}']
            self.assertEqual(conditional['headers'], {'If-None-Match': 'W/"tasks-1"'})
            self.assertIn(f'/projects/{heavy}/tasks?limit={limit}', conditional['path'])
        self.assertIn(f'/projects/{deferred}/tasks?limit=20', rows['tasks deferred limit=20']['path'])
        self.assertIsNone(rows['tasks deferred limit=20']['headers'])
        self.assertEqual(rows['tasks deferred ETag limit=20']['skipped'], VALIDATOR_OFF)

    def test_build_without_validator_and_validator_kept_on_are_reported_as_such(self):
        manifest = {'project_id': fixture_id('project'), 'deferred_project_id': fixture_id('deferred-project'),
                    'chat_id': fixture_id('chat'), 'task_ids': [fixture_id('task', i) for i in range(10)]}
        rows = self.task_list_rows(manifest, lambda path: False)
        for name in ('tasks ETag limit=20', 'tasks ETag limit=50', 'tasks ETag limit=100',
                     'tasks deferred ETag limit=20'):
            self.assertEqual(rows[name]['skipped'], NO_VALIDATOR)
        rows = self.task_list_rows(manifest, lambda path: True)
        self.assertEqual(rows['tasks deferred ETag limit=20']['headers'], {'If-None-Match': 'W/"tasks-1"'})

    def test_fixture_without_deferred_project_has_no_deferred_scenario(self):
        manifest = {'project_id': fixture_id('project'), 'chat_id': fixture_id('chat'),
                    'task_ids': [fixture_id('task', i) for i in range(10)]}
        rows = self.task_list_rows(manifest, lambda path: True)
        self.assertEqual(len(rows), 6)
        self.assertFalse(any('deferred' in name for name in rows))

    def test_denied_ps_keeps_idle_commit_and_row_measurements(self):
        with tempfile.TemporaryDirectory(prefix='perf-unit-') as temporary:
            path = Path(temporary) / 'forge.db'
            with closing(sqlite3.connect(path)) as db, db:
                db.execute('CREATE TABLE event_consumer_cursor (consumer_name TEXT, last_sequence INTEGER)')
                db.execute('CREATE TABLE domain_event (sequence INTEGER)')
                db.execute('INSERT INTO domain_event (sequence) VALUES (50000)')
            server = SimpleNamespace(process=SimpleNamespace(pid=123, poll=lambda: None))
            with patch('perf_bench.IDLE_SECONDS', .02), \
                    patch('perf_bench.table_digests', return_value={'task': 'stable'}), \
                    patch('perf_bench.process_sample', side_effect=PermissionError('denied')):
                result = idle_window(server, path)
            self.assertEqual(result['changed_tables'], [])
            self.assertEqual(result['unchanged_tables'], ['task'])
            self.assertEqual(result['observed_commits'], 0)
            self.assertEqual(result['event_head'], 50000)
            self.assertIsNone(result['cpu_seconds'])
            self.assertIsNone(result['peak_sampled_rss_bytes'])
            self.assertIn('ps unavailable', result['ps_error'])

    def test_unavailable_route_is_skipped(self):
        class Client:
            def request(self, *args, **kwargs):
                return 404, {'Content-Type': 'application/json'}, b'{}'
        self.assertIn('HTTP 404', measure(Client(), 'missing', '/api/v1/missing')['skipped'])


if __name__ == '__main__':
    unittest.main()
