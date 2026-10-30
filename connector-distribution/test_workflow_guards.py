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
                   'scripts': 'a' * 40, 'source_sha': 'b' * 40, 'unprotected_branches': [],
                   'workflow': 'tapdata/tapdata-connectors/.github/workflows/publish-connectors.yml@refs/heads/develop'}
        options.update(overrides)
        harness = '''
const options = JSON.parse(process.argv[1]);
const [owner, repo] = options.repository.split('/');
const context = {repo: {owner, repo}, ref: options.ref};
process.env.SOURCE_REF = options.source;
process.env.SOURCE_REPOSITORY = options.source_repository || 'tapdata-connectors';
process.env.ENTERPRISE_READ_TOKEN = options.enterprise_token || '';
process.env.SOURCE_BRANCH = options.source;
process.env.WORKFLOW_REF = options.workflow;
process.env.CALLER_WORKFLOW = options.workflow;
process.env.SCRIPTS_SHA = options.scripts;
process.env.SOURCE_SHA = options.source_sha;
const outputs = {};
const core = {setOutput: (key, value) => outputs[key] = value};
const github = {rest: {repos: {
  getBranch: async ({branch, repo}) => ({data: {name: branch, protected: options.protected && !(repo === 'tapdata-connectors-enterprise' && options.enterprise_protected === false) && !options.unprotected_branches.includes(branch) && (branch !== options.source || options.source_protected), commit: {sha: (repo === 'tapdata-connectors-enterprise' ? 'e' : branch === 'main' ? 'd' : 'c').repeat(40)}}}),
  compareCommits: async () => ({data: {status: options.status}}),
}}};
(async () => {__GUARD_BODY__})().then(() => console.log(JSON.stringify(outputs))).catch(e => {console.error(e.message); process.exitCode=1;});
'''.replace('__GUARD_BODY__', script)
        return subprocess.run(['node', '-e', harness, json.dumps(options)], capture_output=True, text=True)

    def test_primary_guard_accepts_trusted_protected_branch(self):
        result = self.guard('publish-connectors.yml')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('scripts_sha', json.loads(result.stdout))

    def test_enterprise_source_uses_its_own_protected_branch_and_commit(self):
        options = dict(source_repository='tapdata-connectors-enterprise', enterprise_token='test-token')
        result = self.guard('publish-connectors.yml', **options)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout)['sha'], 'e' * 40)
        self.assertEqual(json.loads(result.stdout)['scripts_sha'], 'c' * 40)
        result = self.guard('publish-connectors.yml', **options, enterprise_protected=False)
        self.assertNotEqual(result.returncode, 0)

    def test_enterprise_missing_token_and_unknown_repository_fail_closed(self):
        for repository in ['tapdata-connectors-enterprise', 'other-repository']:
            result = self.guard('publish-connectors.yml', source_repository=repository)
            self.assertNotEqual(result.returncode, 0)

    def test_primary_guard_accepts_main_and_develop_workflow_and_caller(self):
        for workflow_branch in ['main', 'develop']:
            for caller_branch in ['main', 'develop']:
                with self.subTest(workflow=workflow_branch, caller=caller_branch):
                    result = self.guard(
                        'publish-connectors.yml', ref='refs/heads/' + caller_branch,
                        source=caller_branch,
                        workflow='tapdata/tapdata-connectors/.github/workflows/publish-connectors.yml@refs/heads/' + workflow_branch)
                    self.assertEqual(result.returncode, 0, result.stderr)
                    expected = ('d' if workflow_branch == 'main' else 'c') * 40
                    self.assertEqual(json.loads(result.stdout)['scripts_sha'], expected)

    def test_primary_guard_requires_protected_workflow_caller_and_source(self):
        for branch in ['main', 'develop', 'release/v3']:
            with self.subTest(unprotected=branch):
                result = self.guard(
                    'publish-connectors.yml', ref='refs/heads/develop', source='release/v3',
                    workflow='tapdata/tapdata-connectors/.github/workflows/publish-connectors.yml@refs/heads/main',
                    unprotected_branches=[branch])
                self.assertNotEqual(result.returncode, 0)

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
