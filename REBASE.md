# Staging Rebase Fix Runbook

This document describes the manual steps to fix a failed cherry-pick reported in a rebase issue.

## Prerequisites

- `GITHUB_TOKEN` environment variable set to a valid GitHub token with repo access
- `gh` CLI configured for `github.com`
- `git` remote `gluten_rebase` pointing to `github.com:IBM/velox.git`

---

## Step 1 — Setup remote and fetch latest remote branches

```bash
# Setup gluten_rebase remote (rename if name conflicts with a different URL)
if git remote | grep -q '^gluten_rebase$'; then
  if [ "$(git remote get-url gluten_rebase)" != "https://github.com/IBM/velox.git" ]; then
    git remote rename gluten_rebase "gluten_rebase_$(date +%Y%m%d)"
    git remote add gluten_rebase https://github.com/IBM/velox.git
  fi
else
  git remote add gluten_rebase https://github.com/IBM/velox.git
fi

git fetch gluten_rebase
```

## Step 2 — Hard reset current repo to `gluten_rebase/main`

```bash
git reset --hard gluten_rebase/main
```

## Step 3 — Find the failed PR from the rebase issue

Fetch the last comment of the rebase issue and extract:
- The failed **PR number** and its **head branch name**
- The **Base time**

```bash
GH_TOKEN=$GITHUB_TOKEN GH_HOST=github.com \
  gh issue view <ISSUE_NUMBER> --repo IBM/velox \
  --comments --json comments --jq '.comments[-1].body'
```

Look for a line like:

```
Failed to cherry-pick item [<PR Title>](https://github.com/IBM/velox/pull/<PR_NUMBER>#issuecomment-...) commit 1/1 - <SHA> onto staging/staging-rebase
```

And a **Base time** line in the same comment:

```
Base time: `2026-08-14T18:18:21Z`
```

> **Note:** Do not pipe through `grep` — you need both the failure line (PR number) and the Base time line from the same output.

## Step 4 — Checkout `staging/staging-rebase`

```bash
git checkout gluten_rebase/staging/staging-rebase -B staging/staging-rebase
```

## Step 5 — Get all commits and head branch from the failed PR

```bash
GH_TOKEN=$GITHUB_TOKEN GH_HOST=github.com \
  gh pr view <PR_NUMBER> --repo IBM/velox \
  --json commits,headRefName --jq '{head: .headRefName, commits: [.commits[].oid]}'
```

Note the **head branch name** — you need it for Steps 7 and 8.

## Step 6 — Cherry-pick all commits from the PR

```bash
git cherry-pick <commit1> <commit2> ... <commit7>
```

If a commit is already present (empty), skip it:

```bash
git cherry-pick --skip
```

If there are merge conflicts, you can resolve them using one of two approaches:

### Option A — Resolve conflicts in a separate, new commit on top (Default)

This is the recommended approach. It keeps the original cherry-picked commits completely clean (matching their original state) and captures the conflict resolution in a dedicated commit on top:

1. Temporarily accept HEAD's or the PR's version to allow the cherry-pick to proceed:
   - **To keep HEAD's version** (e.g., if files were deleted in HEAD):
     ```bash
     git rm <conflicted-file>
     ```
   - **To keep the PR's version temporarily** (or use `--ours` / `--theirs` to resolve modification conflicts):
     ```bash
     git checkout --theirs <conflicted-file>
     git add <conflicted-file>
     ```
2. Continue and finish the cherry-pick:
   ```bash
   git cherry-pick --continue --no-edit
   ```
3. Create a new commit on top to apply the actual resolution (e.g., restoring/modifying the conflicted files):
   ```bash
   # Checkout the original files from the PR's head branch to start resolving:
   git checkout <PR_HEAD_BRANCH> -- <conflicted-file>
   
   # Apply any necessary manual fixes, stage, and commit:
   git add <conflicted-file>
   git commit -m "Resolve rebase conflicts for issue <ISSUE_NUMBER> on $(date +%Y-%m-%d)"
   ```

### Option B — Resolve conflicts directly in the cherry-picked commits (Alternative)

Resolve the conflicts manually in each conflicted file, then stage and continue:

```bash
git add <conflicted-file>
git cherry-pick --continue --no-edit
```

## Step 7 — Force push to the PR's head branch

Push the resolved branch to the PR's original head branch (e.g. `wip_fix_spark40`):

```bash
git push gluten_rebase staging/staging-rebase:<PR_HEAD_BRANCH> --force
```

## Step 8 — Update the PR's base branch

Set the PR's base to `staging/staging-rebase` and confirm head is the PR's branch:

```bash
GH_TOKEN=$GITHUB_TOKEN GH_HOST=github.com \
  gh api repos/IBM/velox/pulls/<PR_NUMBER> \
  -X PATCH -f base=staging/staging-rebase \
  --jq '{number,title,baseRefName:.base.ref,headRefName:.head.ref}'
```

## Step 9 — Comment `alchemy merge @<Base time - 1s>` on the PR

Subtract 1 second from the **Base time** extracted in Step 3, then post the comment:

```bash
GH_TOKEN=$GITHUB_TOKEN GH_HOST=github.com \
  gh pr comment <PR_NUMBER> --repo IBM/velox \
  --body "alchemy merge @<BASE_TIME_MINUS_1S>"
```

Example: if Base time is `2026-08-14T18:18:21Z`, comment `alchemy merge @2026-08-14T18:18:20Z`.

## Step 10 — Wait for the database to acknowledge the alchemy merge comment

After posting the `alchemy merge @<BASE_TIME_MINUS_1S>` comment, the rebase bot must process it and record the new `time_added` in its database before the issue is reopened. Poll the PR comments until you see a bot reply confirming the update — it will contain the exact timestamp you used, e.g.:

```
Added: 2026-08-14T18:38:18Z
```

Poll with a loop (checks every 10 s, exits when the confirmation is found):

```bash
while true; do
  RESULT=$(GH_TOKEN=$GITHUB_TOKEN GH_HOST=github.com \
    gh pr view <PR_NUMBER> --repo IBM/velox \
    --comments --json comments \
    --jq '[.comments[].body | select(contains("Added: <BASE_TIME_MINUS_1S>"))] | length')
  if [ "$RESULT" -gt 0 ]; then
    echo "Database updated — safe to reopen the issue."
    break
  fi
  echo "Waiting for database update..."
  sleep 10
done
```

> **Do not reopen the issue until this confirmation appears.** Reopening too early causes the bot to cherry-pick onto the old `staging/staging-rebase` tip before the new `time_added` is registered, resulting in the same conflict being reported again.

## Step 11 — Reopen the original rebase issue

```bash
GH_TOKEN=$GITHUB_TOKEN GH_HOST=github.com \
  gh issue reopen <ISSUE_NUMBER> --repo IBM/velox
```

