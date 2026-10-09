#!/usr/bin/env bash
# Keep private application workflow resolution out of this public repository's CI graph.
set -euo pipefail
case "${MIRROR_REPOSITORY:-}" in
  tapdata-connectors|docs|tapdata-application) ;;
  *) echo 'Unsupported mirror repository' >&2; exit 2 ;;
esac
: "${SOURCE_GITHUB_TOKEN:?GitHub source token is required}"
: "${GITEE_TOKEN:?Gitee token is required}"
: "${GITEE_TOKEN_USER:?Gitee token owner is required}"
export SOURCE_GITHUB_TOKEN GITEE_TOKEN GITEE_TOKEN_USER
umask 077
mirror_temp="$(mktemp -d)"
trap 'rm -rf -- "$mirror_temp"' EXIT
export GIT_ASKPASS="$mirror_temp/askpass"
export GIT_TERMINAL_PROMPT=0
cat > "$GIT_ASKPASS" <<'ASKPASS'
#!/usr/bin/env bash
case "$1" in
  *Username*github.com*) printf '%s\n' x-access-token ;;
  *Password*github.com*) printf '%s\n' "$SOURCE_GITHUB_TOKEN" ;;
  *Username*gitee.com*) printf '%s\n' "$GITEE_TOKEN_USER" ;;
  *Password*gitee.com*) printf '%s\n' "$GITEE_TOKEN" ;;
  *) exit 1 ;;
esac
ASKPASS
chmod 700 "$GIT_ASKPASS"

retry_git() {
  local duration="$1" attempt delay=5
  shift
  for attempt in 1 2 3 4 5; do
    echo "Git $1 attempt $attempt/5 (maximum $duration)"
    if timeout --signal=TERM --kill-after=15s "$duration" \
      git -c credential.helper= -c http.lowSpeedLimit=1024 -c http.lowSpeedTime=30 "$@"; then
      return 0
    fi
    if [[ "$1" == clone ]]; then
      # Failed clones must not leave an unusable target for the next attempt.
      rm -rf -- "$mirror_temp/repository.git"
    fi
    [[ "$attempt" == 5 ]] && break
    sleep "$delay"
    delay=$((delay < 20 ? delay * 2 : 20))
  done
  echo "Git $1 failed after 5 attempts" >&2
  return 1
}

retry_git 5m clone --mirror "https://github.com/tapdata/$MIRROR_REPOSITORY.git" "$mirror_temp/repository.git"
cd "$mirror_temp/repository.git"
# GitHub synthetic PR refs are not source branches and must not be mirrored.
git for-each-ref --format='delete %(refname)' refs/pull | git update-ref --stdin
git remote add mirror "https://gitee.com/tapdata_1/$MIRROR_REPOSITORY.git"
retry_git 2m push --mirror mirror
