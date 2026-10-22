"""Run the actual Actions guard JavaScript with mocked GitHub API responses."""

import json
import shutil
import subprocess
import textwrap
import unittest
from pathlib import Path

WORKFLOWS = Path(__file__).resolve().parents[1] / '.github/workflows'


@unittest.skipUnless(shutil.which('node'), 'Node is needed for Actions JavaScript guard checks')
class WorkflowGuardTests(unittest.TestCase):
    def guard(self, filename, **overrides):
        text = (WORKFLOWS / filename).read_text()
        body = text.split('          script: |\n', 1)[1]
        lines = []
        for line in body.splitlines():
            if line.strip() and not line.startswith('            '):
                break
            lines.append(line)
        script = textwrap.dedent('\n'.join(lines))
        options = {'repository': 'tapdata/tapdata-connectors', 'ref': 'refs/heads/develop',
                   'source': 'develop', 'protected': True, 'source_protected': True, 'status': 'ahead',
                   'scripts': 'a' * 40, 'source_sha': 'b' * 40,
                   'workflow': 'tapdata/tapdata-connectors/.github/workflows/publish-connectors.yml@refs/heads/develop'}
        options.update(overrides)
        harness = '''
const options = JSON.parse(process.argv[1]);
const [owner, repo] = options.repository.split('/');
const context = {repo: {owner, repo}, ref: options.ref};
process.env.SOURCE_REF = options.source;
process.env.SOURCE_BRANCH = options.source;
process.env.WORKFLOW_REF = options.workflow;
process.env.CALLER_WORKFLOW = options.workflow;
process.env.SCRIPTS_SHA = options.scripts;
process.env.SOURCE_SHA = options.source_sha;
const outputs = {};
const core = {setOutput: (key, value) => outputs[key] = value};
const github = {rest: {repos: {
  getBranch: async ({branch}) => ({data: {name: branch, protected: options.protected && (branch === 'develop' || options.source_protected), commit: {sha: 'c'.repeat(40)}}}),
  compareCommits: async () => ({data: {status: options.status}}),
}}};
(async () => {__GUARD_BODY__})().then(() => console.log(JSON.stringify(outputs))).catch(e => {console.error(e.message); process.exitCode=1;});
'''.replace('__GUARD_BODY__', script)
        return subprocess.run(['node', '-e', harness, json.dumps(options)], capture_output=True, text=True)

    def test_primary_guard_accepts_trusted_protected_branch(self):
        result = self.guard('publish-connectors.yml')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('scripts_sha', json.loads(result.stdout))

    def test_primary_guard_allows_other_protected_source_branches(self):
        result = self.guard('publish-connectors.yml', source='release/v3')
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_primary_guard_rejects_untrusted_refs_before_credentials(self):
        for options in [dict(source='refs/tags/v1'),
                        dict(ref='refs/heads/feature'), dict(protected=False),
                        dict(source='release/v3', source_protected=False),
                        dict(repository='fork/tapdata-connectors'),
                        dict(workflow='tapdata/tapdata-connectors/.github/workflows/publish-connectors.yml@refs/heads/feature')]:
            with self.subTest(options=options):
                self.assertNotEqual(self.guard('publish-connectors.yml', **options).returncode, 0)

    def test_batch_guard_accepts_pinned_ancestor_and_current_commits(self):
        for status in ['ahead', 'identical']:
            result = self.guard('connector-publish-batch.yml', status=status)
            self.assertEqual(result.returncode, 0, result.stderr)

    def test_direct_batch_call_rejects_untrusted_caller_or_commits(self):
        for options in [dict(ref='refs/heads/feature'), dict(protected=False),
                        dict(source='release/v3', source_protected=False),
                        dict(repository='fork/tapdata-connectors'), dict(scripts='develop'),
                        dict(source_sha='refs/heads/develop'), dict(status='diverged'), dict(status='behind'),
                        dict(workflow='tapdata/tapdata-connectors/.github/workflows/other.yml@refs/heads/feature')]:
            with self.subTest(options=options):
                self.assertNotEqual(self.guard('connector-publish-batch.yml', **options).returncode, 0)
