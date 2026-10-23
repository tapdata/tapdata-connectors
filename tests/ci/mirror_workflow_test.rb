require 'minitest/autorun'
require 'yaml'
require 'open3'

class MirrorWorkflowTest < Minitest::Test
  ROOT = File.expand_path('../..', __dir__)
  SCRIPT = File.join(ROOT, '.github/scripts/sync-code-to-gitee.sh')

  def load_workflow(name)
    YAML.load_file(File.join(ROOT, '.github/workflows', name))
  end

  def test_public_workflow_graph_is_local_and_preserves_scan_dependencies
    jobs = load_workflow('mr-ci.yaml').fetch('jobs')
    assert_equal './.github/workflows/sync-code-to-office.yaml', jobs.fetch('Sync-Code-to-Office').fetch('uses')
    assert_includes jobs.fetch('Scan-Connectors').fetch('needs'), 'Sync-Code-to-Office'
    mirror = load_workflow('sync-code-to-office.yaml').fetch('jobs').fetch('mirror')
    assert_equal %w[tapdata-connectors docs tapdata-application], mirror.fetch('strategy').fetch('matrix').fetch('repository')
    refute mirror.key?('uses')
  end

  def test_script_syntax_and_required_credentials
    _, err, status = Open3.capture3('bash', '-n', SCRIPT)
    assert status.success?, err
    _, err, status = Open3.capture3({'MIRROR_REPOSITORY' => 'tapdata-connectors', 'SOURCE_GITHUB_TOKEN' => '', 'GITEE_TOKEN' => '', 'GITEE_TOKEN_USER' => ''}, 'bash', SCRIPT)
    refute status.success?
    assert_includes err, 'GitHub source token is required'
  end

  def test_clone_retry_exhaustion_does_not_push_and_success_retries_push
    [true, false].each do |clone_fails|
      out, err, status = Open3.capture3({'MIRROR_REPOSITORY' => 'tapdata-connectors', 'SOURCE_GITHUB_TOKEN' => 'fixture', 'GITEE_TOKEN' => 'fixture', 'GITEE_TOKEN_USER' => 'fixture'}, 'bash', '-c', <<~SH)
        clone_count=0; push_count=0
        timeout() {
          shift 4
          git "$@"
        }
        sleep() { :; }
        git() {
          while [[ "$1" == -c ]]; do shift 2; done
          case "$1" in
            clone)
              clone_count=$((clone_count+1))
              echo CLONE
              #{clone_fails ? 'return 1' : 'mkdir -p "${@: -1}"; return 0'} ;;
            push)
              push_count=$((push_count+1)); echo PUSH
              [[ "$push_count" == 2 ]] ;;
            update-ref) command cat >/dev/null ;;
            *) return 0 ;;
          esac
        }
        source "#{SCRIPT}"
      SH
      if clone_fails
        refute status.success?
        assert_equal 5, out.lines.count { |l| l.strip == 'CLONE' }
        refute_includes out, 'PUSH'
      else
        assert status.success?, err
        assert_equal 2, out.lines.count { |l| l.strip == 'PUSH' }
      end
    end
  end

  def test_regression_suite_has_a_ci_entry
    steps = load_workflow('ci-mirror-regression.yaml').fetch('jobs').fetch('regression').fetch('steps')
    assert steps.any? { |s| s['run'] == 'ruby tests/ci/mirror_workflow_test.rb' }
  end
end
