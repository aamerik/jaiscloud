#!/usr/bin/env bash
#
# gcp-session-start.sh — prepare a clean GCP work session. Called by
# `make gcp-session-start`, and therefore by the `/gcp:new` command.
#
#   1. Refuse to start if the current checkout has uncommitted changes. A
#      previous or concurrent session's work must be committed or stashed first.
#      The guard runs before any fetch/checkout, so refs are never touched over
#      dirty work.
#   2. Fast-forward the local `main` base branch from `upstream/main` (the
#      canonical remote; `origin` is a fork and its branches are stale).
#      `--ff-only` means a diverged base fails loudly instead of resetting local
#      commits. Override the base with GCP_SESSION_BASE=<branch>; the old `gcp`
#      branch is frozen (merged into main via #610) and must not be the base.
#   3. Leave the checkout on the base branch; the session branches the item's
#      Branch off it.
#
# Exits non-zero on a dirty checkout or a failed fetch/fast-forward so the caller
# can abort the session.
set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "$REPO_ROOT"

if [ -n "$(git status --porcelain)" ]; then
  echo "gcp-session-start: refusing to start — the current checkout has uncommitted changes." >&2
  echo "                   Commit or stash them (a previous or concurrent session's work) first." >&2
  echo >&2
  git status --short >&2
  exit 1
fi

echo "gcp-session-start: fetching upstream..."
git fetch upstream --prune

BASE="${GCP_SESSION_BASE:-main}"

if git show-ref --verify --quiet "refs/heads/$BASE"; then
  git checkout "$BASE"
  git merge --ff-only "upstream/$BASE"
else
  echo "gcp-session-start: creating local '$BASE' tracking upstream/$BASE"
  git checkout -b "$BASE" --track "upstream/$BASE"
fi

echo "gcp-session-start: base $BASE at $(git rev-parse --short HEAD) — $(git log --oneline -1 --format=%s)"
