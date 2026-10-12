"""Exercise the PR gate against push CI without making GitHub API calls."""

import json
import os
import pathlib
import subprocess
import tempfile
import textwrap
import unittest

ROOT = pathlib.Path(__file__).resolve().parent.parent


class IntegrationLaneGateTests(unittest.TestCase):
    def run_gate(self, responses):
        workflow = (ROOT / '.github/workflows/ci.yml').read_text()
        step = workflow.split('      - name: Wait for integration-lane CI\n', 1)[1]
        body = step.split('        run: |\n', 1)[1].split('\n  audit:', 1)[0]
        script = textwrap.dedent(body)
        with tempfile.TemporaryDirectory() as scratch:
            tmp = pathlib.Path(scratch)
            (tmp / 'responses.json').write_text(json.dumps(responses))
            gh = tmp / 'gh'
            gh.write_text(
                '#!/usr/bin/env python3\nimport json, pathlib, sys\n'
                'root = pathlib.Path(__file__).parent\n'
                "with (root / 'calls.jsonl').open('a') as log:\n"
                "    log.write(json.dumps(sys.argv[1:]) + '\\n')\n"
                "path = root / 'responses.json'\n"
                'responses = json.loads(path.read_text())\n'
                "response = responses.pop(0) if responses else {'output': ''}\n"
                'path.write_text(json.dumps(responses))\n'
                "print(response.get('output', ''))\n"
                "sys.exit(response.get('exit', 0))\n"
            )
            gh.chmod(0o755)
            sleep = tmp / 'sleep'
            sleep.write_text('#!/bin/bash\nexit 0\n')
            sleep.chmod(0o755)
            env = dict(os.environ, PATH=str(tmp) + os.pathsep + os.environ['PATH'],
                       REPO='owner/repo', SHA='exact-head-sha', LANE_BRANCH='next/v0.14')
            run = subprocess.run(['/bin/bash', '-c', script], env=env,
                                 text=True, capture_output=True, timeout=10)
            calls = [json.loads(line) for line in (tmp / 'calls.jsonl').read_text().splitlines()]
            for call in calls:
                self.assertEqual(call[:2], ['api', 'repos/owner/repo/actions/workflows/ci.yml/runs'])
                for field in ['head_sha=exact-head-sha', 'event=push', 'branch=next/v0.14', 'per_page=1']:
                    self.assertIn(field, call)
            return run, calls

    def test_waits_for_the_exact_push_run_and_requires_success(self):
        run, calls = self.run_gate([
            {'output': 'in_progress - https://example.test/run'},
            {'output': 'completed success https://example.test/run'},
        ])
        self.assertEqual(run.returncode, 0, run.stderr)
        self.assertEqual(len(calls), 2)
        self.assertIn('passed on exact-head-sha', run.stdout)

    def test_failed_cancelled_or_skipped_push_cannot_make_pr_green(self):
        for conclusion in ['failure', 'cancelled', 'skipped']:
            with self.subTest(conclusion=conclusion):
                run, calls = self.run_gate([
                    {'output': f'completed {conclusion} https://example.test/run'},
                ])
                self.assertEqual(run.returncode, 1, run.stderr)
                self.assertEqual(len(calls), 1)
                self.assertIn(f"finished with '{conclusion}'", run.stdout)

    def test_missing_push_run_fails_after_bounded_retries(self):
        run, calls = self.run_gate([])
        self.assertEqual(run.returncode, 1, run.stderr)
        self.assertEqual(len(calls), 15)
        self.assertIn('No push CI run on next/v0.14 for exact-head-sha', run.stdout)

    def test_transient_api_failure_and_run_registration_are_retried(self):
        run, calls = self.run_gate([
            {'exit': 1}, {'output': ''},
            {'output': 'completed success https://example.test/run'},
        ])
        self.assertEqual(run.returncode, 0, run.stderr)
        self.assertEqual(len(calls), 3)
        self.assertIn('Could not read integration-lane CI; retrying.', run.stdout)


if __name__ == '__main__':
    unittest.main()
