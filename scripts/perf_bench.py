#!/usr/bin/env python3
"""Compare Forge release builds on a copied synthetic fixture; loopback only."""
from __future__ import annotations

import argparse
from collections import Counter
from contextlib import closing
import hashlib
import http.client
import json
import math
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import tempfile
import time
from typing import Any
from urllib.parse import urlencode

from perf_fixture import (LocalClient, PASSWORD, Server, binary_info, fixture_id,
                          table_digests, timestamp, validate_database)

WARMUPS = 10
REQUESTS = 100
IDLE_SECONDS = 60
POLL_HZ = 50


def distribution(values: list[float | int]) -> dict:
    ordered = sorted(values)
    return {'min': round(ordered[0], 3),
            'p50': round(ordered[math.ceil(len(ordered) * .50) - 1], 3),
            'p95': round(ordered[math.ceil(len(ordered) * .95) - 1], 3),
            'max': round(ordered[-1], 3)}


def measure(client: LocalClient, name: str, path: str,
            headers: dict[str, str] | None = None) -> dict:
    try:
        probe_status, probe_headers, _ = client.request(path, headers=headers)
    except (OSError, http.client.HTTPException):
        probe_status, probe_headers = 0, {}
    # Forge's SPA fallback can answer missing API routes with HTML, not JSON.
    content_type = next((value for key, value in probe_headers.items()
                         if key.lower() == 'content-type'), '')
    if probe_status in (404, 405) or 'text/html' in content_type:
        return {'name': name, 'path': path, 'skipped': f'endpoint unavailable (HTTP {probe_status}, {content_type})'}
    for _ in range(WARMUPS):
        try:
            client.request(path, headers=headers)
        except (OSError, http.client.HTTPException):
            pass
    elapsed, sizes, statuses = [], [], Counter()
    errors = 0
    for _ in range(REQUESTS):
        started = time.perf_counter_ns()
        try:
            status, _, body = client.request(path, headers=headers)
            size = len(body)
        except (OSError, http.client.HTTPException):
            status, size = 0, 0
        elapsed.append((time.perf_counter_ns() - started) / 1_000_000)
        sizes.append(size)
        statuses[str(status)] += 1
        if not (200 <= status < 300 or status == 304):
            errors += 1
    latency = distribution(elapsed)
    return {'name': name, 'path': path, 'requests': REQUESTS, 'warmups': WARMUPS,
            'p50_ms': latency['p50'], 'p95_ms': latency['p95'], 'min_ms': latency['min'],
            'response_bytes': distribution(sizes), 'status_counts': dict(statuses),
            'errors': errors, 'share_304': statuses['304'] / REQUESTS}


NO_VALIDATOR = 'build did not return an ETag'
VALIDATOR_OFF = 'validator off: a Task in this Project carries deferred-dispatch metadata'


def task_list(client: LocalClient, label: str, path: str, limit: int,
              without_etag: str) -> tuple[list[dict], bool]:
    """Time one task list page, then its conditional request when it has a validator."""
    rows = [measure(client, f'{label} limit={limit}', path)]
    try:
        status, headers, _ = client.request(path)
    except (OSError, http.client.HTTPException):
        status, headers = 0, {}
    etag = next((value for key, value in headers.items() if key.lower() == 'etag'), None)
    name = f'{label} ETag limit={limit}'
    if 200 <= status < 300 and etag:
        rows.append(measure(client, name, path, {'If-None-Match': etag}))
    else:
        rows.append({'name': name, 'path': path, 'skipped': without_etag})
    return rows, bool(etag)


def scenarios(client: LocalClient, manifest: dict) -> list[dict]:
    project, chat = manifest['project_id'], manifest['chat_id']
    result = []
    validator = False
    for limit in (20, 50, 100):
        rows, has_etag = task_list(client, 'tasks', f'/api/v1/projects/{project}/tasks?limit={limit}',
                                   limit, NO_VALIDATOR)
        result += rows
        validator = validator or has_etag
    # Fixtures since forge.perf-fixture/2 keep deferred-dispatch metadata in a
    # second, small Project. A build with a validator switches it off there; the
    # skip reason reports that separately from a build that has no validator.
    deferred = manifest.get('deferred_project_id')
    if deferred:
        result += task_list(client, 'tasks deferred', f'/api/v1/projects/{deferred}/tasks?limit=20',
                            20, VALIDATOR_OFF if validator else NO_VALIDATOR)[0]
    # The raw Task route exists on the baseline; next also offers a consolidated
    # detail endpoint. Measure both explicitly, without hiding the difference.
    for index, task in enumerate(manifest['task_ids']):
        result.append(measure(client, f'task {index + 1:02d}', f'/api/v1/tasks/{task}'))
    task = manifest['task_ids'][0]
    execution = fixture_id('execution', 9)
    analytics_window = urlencode({'from': timestamp(), 'to': timestamp(2 * 86400)})
    routes = (
        ('project', f'/projects/{project}'),
        ('project workflow', f'/projects/{project}/workflow'),
        ('project agents', f'/projects/{project}/agents'),
        ('agents', '/agents?limit=100'),
        ('operations status', '/operations/status'),
        ('task detail aggregate', f'/tasks/{task}/detail'),
        ('task relations', f'/tasks/{task}/relations'),
        ('task executions', f'/tasks/{task}/executions?limit=20'),
        ('task reviews', f'/tasks/{task}/reviews'),
        ('task transitions', f'/tasks/{task}/transitions?limit=20'),
        ('task comments', f'/tasks/{task}/comments?limit=20'),
        ('task media', f'/tasks/{task}/media?limit=20'),
        ('task workspace', f'/tasks/{task}/workspace'),
        ('task dependencies', f'/tasks/{task}/dependencies'),
        ('task dependents', f'/tasks/{task}/dependents'),
        ('task usage', f'/tasks/{task}/usage'),
        ('execution usage', f'/executions/{execution}/usage'),
        # Fix the date range so historical fixture usage doesn't disappear as
        # the wall clock advances. These are supplementary board-related GETs.
        ('project analytics', f'/projects/{project}/analytics?{analytics_window}'),
        ('account usage', f'/analytics/usage?{analytics_window}'),
        ('chat list', '/agent-chats'),
        ('chat messages', f'/agent-chats/{chat}/messages?limit=50'),
        ('chat turns', f'/agent-chats/{chat}/turns?limit=50'),
    )
    for name, path in routes:
        result.append(measure(client, name, '/api/v1' + path))
    return result


def cpu_seconds(text: str) -> float:
    """ps TIME is [[days-]hours:]minutes:seconds, with optional fractions."""
    days, _, clock = text.rpartition('-')
    parts = [float(part) for part in (clock or text).split(':')]
    total = 0.0
    for part in parts:
        total = total * 60 + part
    return total + (float(days) * 86400 if days else 0)


def process_sample(pid: int) -> tuple[float, int]:
    output = subprocess.check_output(['ps', '-p', str(pid), '-o', 'time=', '-o', 'rss='],
                                     text=True, timeout=5).split()
    if len(output) != 2:
        raise ValueError('server exited during idle measurement')
    return cpu_seconds(output[0]), int(output[1]) * 1024


def readonly_db(path: Path) -> sqlite3.Connection:
    return sqlite3.connect(path.resolve().as_uri() + '?mode=ro', uri=True, timeout=5)


def idle_window(server: Server, db_path: Path) -> dict:
    assert server.process is not None
    with closing(readonly_db(db_path)) as db:
        before = table_digests(db)
        previous = db.execute('PRAGMA data_version').fetchone()[0]
        ps_error = None
        cpu_start = cpu_end = None
        peak_rss = rss = None
        try:
            cpu_start, rss = process_sample(server.process.pid)
            peak_rss = rss
        except (OSError, ValueError, subprocess.SubprocessError) as error:
            ps_error = f'ps unavailable: {error}'
        changes = polls = ps_samples = 0
        started = time.perf_counter()
        next_poll, next_ps = started, started + 1
        while time.perf_counter() - started < IDLE_SECONDS:
            if server.process.poll() is not None:
                raise ValueError('server exited during idle measurement')
            now = time.perf_counter()
            current = db.execute('PRAGMA data_version').fetchone()[0]
            changes += int(current != previous)
            previous = current
            polls += 1
            if now >= next_ps and ps_error is None:
                try:
                    _, rss = process_sample(server.process.pid)
                    peak_rss = max(peak_rss, rss)
                    ps_samples += 1
                except (OSError, ValueError, subprocess.SubprocessError) as error:
                    ps_error = f'ps unavailable: {error}'
                next_ps = now + 1
            next_poll += 1 / POLL_HZ
            time.sleep(max(0, next_poll - time.perf_counter()))
        duration = time.perf_counter() - started
        if ps_error is None:
            try:
                cpu_end, rss = process_sample(server.process.pid)
                peak_rss = max(peak_rss, rss)
                ps_samples += 2
            except (OSError, ValueError, subprocess.SubprocessError) as error:
                ps_error = f'ps unavailable: {error}'
        after = table_digests(db)
        changed = [table for table in before if before[table] != after[table]]
        cursors = {name: sequence for name, sequence in db.execute(
            'SELECT consumer_name, last_sequence FROM event_consumer_cursor')}
        head = db.execute('SELECT max(sequence) FROM domain_event').fetchone()[0]
    return {'seconds': round(duration, 3), 'polls': polls, 'poll_hz': round(polls / duration, 2),
            'observed_commits': changes, 'observed_commits_per_s': round(changes / duration, 3),
            'cpu_seconds': round(cpu_end - cpu_start, 3) if cpu_end is not None else None,
            'peak_sampled_rss_bytes': peak_rss, 'ps_samples': ps_samples, 'ps_error': ps_error,
            'unchanged_tables': [table for table in before if table not in changed],
            'changed_tables': changed, 'consumer_cursors': cursors, 'event_head': head}


def fingerprint(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open('rb') as source:
        for block in iter(lambda: source.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def run(binary: Path, fixture: Path, label: str, out: Path, port: int) -> dict:
    if out.exists():
        raise ValueError('--out must be a new file')
    manifest = json.loads((fixture / 'perf-fixture.json').read_text())
    db_source = fixture / 'forge.db'
    wal = fixture / 'forge.db-wal'
    if wal.exists() and wal.stat().st_size:
        raise ValueError('fixture has an uncheckpointed WAL; stop its server before copying')
    original = fingerprint(db_source)
    version, _ = binary_info(binary)
    with tempfile.TemporaryDirectory(prefix='forge-perf-bench-') as temporary:
        scratch = Path(temporary)
        data = scratch / 'data'
        shutil.copytree(fixture, data)
        db_path = data / 'forge.db'
        size_before = db_path.stat().st_size
        with Server(binary, data, port, scratch / 'first-start.log') as first:
            first_start = first.healthy_seconds
        with closing(readonly_db(db_path)) as db:
            validate_database(db)
            before_second = table_digests(db)
        with Server(binary, data, port, scratch / 'second-start.log') as second:
            second_start = second.healthy_seconds
            print(f'{label}: healthy in {first_start:.3f}s / {second_start:.3f}s; measuring 60s idle', flush=True)
            idle = idle_window(second, db_path)
            client = LocalClient(port)
            try:
                client.token = client.json('/api/v1/auth/login',
                                           {'email': manifest['email'], 'password': PASSWORD})['access_token']
                measurements = scenarios(client, manifest)
            finally:
                client.close()
        with closing(sqlite3.connect(db_path)) as db, db:
            validate_database(db)
            final = table_digests(db)
            db.execute('PRAGMA wal_checkpoint(TRUNCATE)')
        size_after = db_path.stat().st_size
        changed = [table for table in before_second if before_second[table] != final[table]]
    if fingerprint(db_source) != original:
        raise ValueError('source fixture changed during benchmark')
    report = {'schema': 'forge.perf-bench/1', 'label': label, 'binary_version': version,
              'fixture_sha256': original, 'fixture_canonical_sha256': manifest['canonical_sha256'],
              'profile': manifest['profile'], 'scenarios': measurements,
              'server': {'first_start_healthy_seconds': round(first_start, 6),
                         'second_start_healthy_seconds': round(second_start, 6),
                         'db_bytes_before': size_before, 'db_bytes_after': size_after,
                         'idle': idle, 'changed_tables_second_start_through_requests': changed},
              'notes': ['First start includes forward migrations and normal startup; second uses the migrated copy. The difference is not isolated migration time.',
                        'data_version changes at 50 Hz are a lower bound on commits, not a transaction counter.',
                        'CPU is ps cumulative TIME delta; RSS is the maximum sampled once per second during idle, not lifetime peak RSS.',
                        'Sequential persistent loopback HTTP; ten warmups and 100 timed full-body requests per available scenario.',
                        'Only local aggregates; no tokens, headers, response bodies, or raw logs in this report.']}
    with open(out, 'x', encoding='utf-8', opener=lambda path, flags: os.open(path, flags, 0o600)) as target:
        json.dump(report, target, indent=2, sort_keys=True, allow_nan=False)
        target.write('\n')
    print(f'{label}: {len(measurements)} scenarios; idle changes: {idle["changed_tables"]}; report {out}', flush=True)
    return report


def compare(a: dict, b: dict) -> str:
    def cell(value: Any) -> str:
        return str(value).replace('|', '\\|').replace('\n', ' ')

    def timing(row: dict | None) -> str:
        if row is None:
            return 'absent'
        if 'skipped' in row:
            return 'skip: ' + cell(row['skipped'])
        return f'{row["p50_ms"]:.3f} / {row["p95_ms"]:.3f}'

    left = {row['name']: row for row in a['scenarios']}
    right = {row['name']: row for row in b['scenarios']}
    lines = [f'| Scenario | {cell(a["label"])} p50 / p95 ms | {cell(b["label"])} p50 / p95 ms | b/a p50 / p95 |',
             '| --- | ---: | ---: | ---: |']
    for name in dict.fromkeys([*left, *right]):
        x, y = left.get(name), right.get(name)
        ratio = '—'
        if x and y and 'p50_ms' in x and 'p50_ms' in y:
            ratios = [f'{y[key] / x[key]:.2f}×' if x[key] else '—' for key in ('p50_ms', 'p95_ms')]
            ratio = ' / '.join(ratios)
        lines.append(f'| {cell(name)} | {timing(x)} | {timing(y)} | {ratio} |')
    lines += ['', f'| Server figure | {cell(a["label"])} | {cell(b["label"])} |', '| --- | ---: | ---: |']
    for description, key in (('First start to healthy (s)', 'first_start_healthy_seconds'),
                             ('Second start to healthy (s)', 'second_start_healthy_seconds'),
                             ('DB before (bytes)', 'db_bytes_before'), ('DB after (bytes)', 'db_bytes_after')):
        lines.append(f'| {description} | {a.get("server", {}).get(key, "—")} | {b.get("server", {}).get(key, "—")} |')
    for description, key in (('Idle observed commits/s', 'observed_commits_per_s'),
                             ('Idle CPU seconds', 'cpu_seconds'),
                             ('Idle peak sampled RSS (bytes)', 'peak_sampled_rss_bytes')):
        values = [report.get('server', {}).get('idle', {}).get(key, '—') for report in (a, b)]
        values = ['unavailable' if value is None else value for value in values]
        lines.append(f'| {description} | {values[0]} | {values[1]} |')
    warnings = []
    if a.get('fixture_canonical_sha256') != b.get('fixture_canonical_sha256'):
        warnings.append('Fixture digests differ; results do not use the same seeded rows.')
    for report in (a, b):
        for row in report['scenarios']:
            if row.get('errors'):
                warnings.append(f'{report["label"]} / {row["name"]}: {row["errors"]} request errors, statuses {row["status_counts"]}.')
            if row.get('share_304'):
                warnings.append(f'{report["label"]} / {row["name"]}: {row["share_304"]:.0%} HTTP 304.')
        ps_error = report.get('server', {}).get('idle', {}).get('ps_error')
        if ps_error:
            warnings.append(f'{report["label"]}: {ps_error}.')
        changed = report.get('server', {}).get('changed_tables_second_start_through_requests', [])
        if changed:
            warnings.append(f'{report["label"]}: seeded activity changed in {", ".join(changed)}.')
    return '\n'.join(lines + ([''] + warnings if warnings else []))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    command = commands.add_parser('run')
    command.add_argument('--binary', type=Path, required=True)
    command.add_argument('--fixture', type=Path, required=True)
    command.add_argument('--label', required=True)
    command.add_argument('--out', type=Path, required=True)
    command.add_argument('--port', type=int, default=18101)
    command = commands.add_parser('compare')
    command.add_argument('a', type=Path)
    command.add_argument('b', type=Path)
    args = parser.parse_args()
    try:
        if args.command == 'run':
            run(args.binary.resolve(), args.fixture.resolve(), args.label, args.out.resolve(), args.port)
        else:
            print(compare(json.loads(args.a.read_text()), json.loads(args.b.read_text())))
    except (OSError, ValueError, sqlite3.Error, subprocess.SubprocessError) as error:
        parser.exit(1, f'Cannot benchmark: {error}\n')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
