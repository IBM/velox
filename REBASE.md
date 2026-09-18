# Staging Rebase Fix Runbook

This document describes the manual steps to fix a failed cherry-pick reported in a rebase issue.

## Prerequisites

- `GITHUB_TOKEN` environment variable set to a valid GitHub token with repo access
- `gh` CLI configured for `github.com`
- `git` remote `gluten_rebase` pointing to `https://github.com/IBM/velox.git`

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

> **Warning:** `git reset --hard` discards uncommitted changes to tracked files —
> including edits to this runbook. Stash first if you have any:
>
> ```bash
> git stash push -m "pre-rebase-fix" -- REBASE.md   # or: git stash push -u
> ```

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

Note the **head branch name** — you need it for Steps 7 and 8. Step 8 also derives the
PR's new base branch name from it (`<PR_HEAD_BRANCH>_base`).

## Step 6 — Cherry-pick all commits from the PR

> **Note — the bot may have already pushed partial progress.** If the failure was at
> commit `k/N`, commits `1..k-1` are often already on `staging/staging-rebase`. Check
> before cherry-picking, and start from the commit that actually failed:
>
> ```bash
> git log --oneline -25 gluten_rebase/staging/staging-rebase
> for c in <commit1> <commit2> ... <commitN>; do
>   echo "$c: $(git log -1 --format='%s' $c)"
> done
> ```
>
> Match the subjects against the staging log. Cherry-picking an already-applied commit
> produces spurious add/add conflicts on files it created, which is misleading.

```bash
git cherry-pick <commit_k> ... <commitN>
```

If a commit is already present (empty), skip it:

```bash
git cherry-pick --skip
```

> **Note — stale "Resolve rebase conflicts" commits.** A PR that has been through
> earlier rebase rounds may carry `Resolve rebase conflicts for issue <M>` commits.
> These captured a resolution against a much older upstream, so replaying them tries to
> revert current upstream state (older `delta`/`iceberg`/`arrow` versions, a dropped
> `java-25` profile, restored `arrow.deps.scope` machinery, and so on).
>
> Never resolve these hunk-by-hunk. Instead compute the PR's **net intent** for the
> file and apply only that on top of HEAD:
>
> ```bash
> # 1. Do the resolve-commits cancel each other out entirely?
> git diff <first_resolve_commit>^ <last_resolve_commit> -- <file>
>
> # 2. What does the PR actually intend for this file, overall?
> BASE=$(git merge-base <commit1>^ <PR_HEAD_BRANCH>)
> git diff $BASE <PR_HEAD_BRANCH> -- <file>
> ```
>
> Then, for each conflict, reset the file to HEAD and re-apply just the net intent:
>
> ```bash
> git checkout --ours <file>          # discard the wholesale revert
> # ...apply the net-intent lines by hand (often a single property)...
> git add <file>
> git diff --cached HEAD -- <file>    # verify: should show ONLY the net intent
> ```
>
> Commits whose entire content was the revert become empty — skip them with
> `git cherry-pick --skip`. Two outcomes seen in practice:
>
> - **Net diff empty**: every resolve-commit is pure churn. Resolve all of them to HEAD;
>   all are skipped.
> - **Net diff is a real change**: the revert carries one genuine edit. Resolve to HEAD,
>   apply that single edit, and let the trailing resolve-commit become empty.
>
> When the net intent is already satisfied in HEAD, there is nothing to apply.

If there are merge conflicts, you can resolve them using one of two approaches. Make sure to ask user to confirm before commit solved conflicts.

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

Do **not** point the PR's base at `staging/staging-rebase` itself. That branch keeps
moving — the bot rebuilds and force-pushes it every round — and, more importantly, the
bot derives the item's commit list from the PR's diff against its base. With a base
that already contains part of the PR, the bot records only the commits *not* on that
branch and **silently drops the rest** on the next full rebase round.

Instead, pin the base to a **fixed commit**: the commit on `staging/staging-rebase`
immediately *after* the last successfully cherry-picked item that precedes this PR.
Equivalently, it is the **parent of the failed PR's first cherry-picked commit** on
`staging/staging-rebase`.

### 8a — Find the base commit

The last comment from Step 3 lists the items that finished before the failure. Take the
last one; the base commit is the `staging/staging-rebase` commit produced by that pick.

```bash
# <FIRST_PICKED_COMMIT> = the failed PR's commit 1/N as it landed on staging/staging-rebase.
# If the bot made no partial progress, this is your first new commit, so the parent is
# simply the staging tip you branched from.
git log -1 --format='%H %s' <FIRST_PICKED_COMMIT>^
```

Cross-check that its subject matches the last commit of the last successful item in the
Step 3 table, and that it is an ancestor of the branch you are about to push:

```bash
git merge-base --is-ancestor <BASE_COMMIT> staging/staging-rebase && echo ok
```

### 8b — Create and push a base branch at that commit

GitHub requires the base to be a branch, not a raw SHA. Create one named
`<PR_HEAD_BRANCH>_base`:

```bash
git branch -f <PR_HEAD_BRANCH>_base <BASE_COMMIT>
git push gluten_rebase <PR_HEAD_BRANCH>_base:<PR_HEAD_BRANCH>_base --force
```

### 8c — Point the PR at it

```bash
GH_TOKEN=$GITHUB_TOKEN GH_HOST=github.com \
  gh api repos/IBM/velox/pulls/<PR_NUMBER> \
  -X PATCH -f base=<PR_HEAD_BRANCH>_base \
  --jq '{number,title,baseRefName:.base.ref,baseSha:.base.sha,headRefName:.head.ref,headSha:.head.sha,commits,changed_files}'
```

Confirm `baseSha` equals `<BASE_COMMIT>`, and that `commits` / `changed_files` cover the
PR's **complete** set of own changes — not just the ones you added while resolving.

## Step 9 — Post a conflict-resolution summary comment on the PR

Before handing the PR back to the bot, leave a comment explaining what conflicted and
how it was resolved. The force-push in Step 7 rewrote the PR's commits, so without this
the PR's owner has no record of what changed or why — and the next person to hit the
same conflict has nothing to learn from.

Post this **before** the `alchemy merge` comment in Step 10, so the explanation sits
above the bot's bookkeeping rather than buried under it.

```bash
GH_TOKEN=$GITHUB_TOKEN GH_HOST=github.com \
  gh pr comment <PR_NUMBER> --repo IBM/velox --body "$(cat <<'EOF'
## Rebase conflict resolved — issue #<ISSUE_NUMBER>

Rebased onto `staging/staging-rebase` and force-pushed. Base is now
`<PR_HEAD_BRANCH>_base` (pinned at `<BASE_COMMIT>`).

**Conflict:** `<file>` — <modify/delete | content | add/add> at <location>.
<One or two sentences on what each side wanted.>

**Resolution:** <What was kept, what was dropped, and why.>

**Net effect:** <Final commit list / file+line counts. Note any difference from the
original commit's counts and why.>

**Verified:** <Checks actually run — grep for leftover markers, `git diff --cached HEAD`
scoped to the intent, import/type resolution, etc. Say plainly what was *not* run,
e.g. no build or tests.>
EOF
)"
```

Keep it factual and specific. Name the commits, files, and line counts; if the net
result differs from the original commit's diffstat, say why. If anything was left
deliberately unfixed (orphaned files, stale references) or needs follow-up validation,
call it out here rather than leaving it for someone to discover.

<details>
<summary>Worked example — `lakehouse/gluten` issue 1211 / PR 1159</summary>

```markdown
## Rebase conflict resolved — issue #1211

Rebased onto `staging/staging-rebase` and force-pushed as `a5b4f4948`. Base is now
`lakehouse-q24-reuse_base` (pinned at `0e2152d63`).

**Conflict:** `VeloxRuleApi.scala` — content conflict at the end of `injectSpark`.
Both sides appended to the same spot: HEAD added an upstream
`nativeUDFBypassRegistration` block, this PR added its `injectQueryStagePrepRule` call.
`VeloxConfig.scala` and the new 685-line rule file applied cleanly.

**Resolution:** Kept both. They use unrelated injection channels (`injectFunction` vs
`injectQueryStagePrepRule`), so ordering is inert. Wrote the injection on one line (87
cols) instead of the original two-line wrap, since `.scalafmt.conf` sets
`maxColumn = 100` and scalafmt would collapse it anyway — this is why the commit shows
705 insertions against the original 706.

**Net effect:** 1 commit, 3 files, +705/-0.

**Verified:** no conflict markers remain; `org.apache.gluten.extension._` already
covers the new rule so no import was needed; `SparkInjector` declares both methods and
the enclosing method takes a `SparkInjector`. Not run: no build, no tests. The config
this PR adds defaults to `true` and its own doc warns a mis-fire returns wrong results,
so a Q24 correctness run is worth doing before this ships.
```

</details>

## Step 10 — Comment `alchemy merge @<Base time - 1s>` on the PR

> **Order matters — finish Step 8 first.** The bot reads the PR's commit list at the
> moment it processes this comment. If the base is still wrong, it records the wrong
> commit list. Should that happen, fix the base and simply post the comment again; the
> bot removes the bad item and adds a corrected one at the same timestamp.

Subtract 1 second from the **Base time** extracted in Step 3, then post the comment:

```bash
GH_TOKEN=$GITHUB_TOKEN GH_HOST=github.com \
  gh pr comment <PR_NUMBER> --repo IBM/velox \
  --body "alchemy merge @<BASE_TIME_MINUS_1S>"
```

Example: if Base time is `2026-08-14T18:18:21Z`, comment `alchemy merge @2026-08-14T18:18:20Z`.

## Step 11 — Wait for the database to acknowledge the alchemy merge comment

After posting the `alchemy merge @<BASE_TIME_MINUS_1S>` comment, the rebase bot must process it and record the new `time_added` in its database before the issue is reopened. Poll the PR comments until you see a bot reply confirming the update — its `Added new rebase item` block contains the exact timestamp you used.

> **Important:** the bot writes this line as markdown — bold label, timestamp in backticks:
>
> ```
>   - **Added:** `2026-08-14T18:38:18Z` by @Ossprestouser via ...
> ```
>
> Matching the plain string `Added: <TIMESTAMP>` will **never** hit, and the loop below will spin forever. Use a regex that tolerates the `**` and the backticks (as in the loop below), or just match the bare timestamp.

> **Do not match on the `Added:` line alone.** The bot uses that same line inside a
> **`Failed to add new rebase item:`** comment when it rejects the request, and old
> failures stay in the comment history forever — so an `Added:`-only match reports
> success for a failure that may not even be yours. Require the success phrase
> `Added new rebase item`, and check for the failure phrase too so a rejection stops
> the loop instead of hanging it.

Poll with a loop (checks every 10 s, exits on either outcome):

```bash
TS=<BASE_TIME_MINUS_1S>   # e.g. 2026-08-14T18:38:18Z
while true; do
  BODY=$(GH_TOKEN=$GITHUB_TOKEN GH_HOST=github.com \
    gh pr view <PR_NUMBER> --repo IBM/velox \
    --comments --json comments \
    --jq "[.comments[].body
           | select(test(\"Added new rebase item\"))
           | select(test(\"Added:\\\\*{0,2} \`?$TS\"))] | last // \"\"")
  if [ -n "$BODY" ]; then
    echo "Database updated — safe to reopen the issue."
    echo "$BODY" | sed -n '/Added new rebase item/,$p'
    break
  fi
  FAILED=$(GH_TOKEN=$GITHUB_TOKEN GH_HOST=github.com \
    gh pr view <PR_NUMBER> --repo IBM/velox \
    --comments --json comments \
    --jq '[.comments[] | select(.author.login=="Ossprestouser")
           | select(.body | test("Failed to add new rebase item"))] | last.createdAt // ""')
  echo "Waiting for database update... (last rejection seen: ${FAILED:-none})"
  sleep 10
done
```

> **Note:** `last // ""` plus `[ -n ]` avoids comparing a possibly-empty string with
> `[ -gt ]`, which would error out on every iteration after a transient `gh` failure.

> **Note:** do not filter the poll by comment timestamp either. The bot usually replies
> within a couple of seconds, so a `createdAt >` cutoff guessed from the wall clock can
> sit past the reply and loop forever against a confirmation that already arrived. Match
> on body content only, as above.

> **Do not reopen the issue until this confirmation appears.** Reopening too early causes the bot to cherry-pick onto the old `staging/staging-rebase` tip before the new `time_added` is registered, resulting in the same conflict being reported again.

### 11a — Verify the recorded commit list

The bot's reply removes the old item and prints the new one. Check that the new item's
**Commits** are exactly the PR's commits from Step 8c — this is the check that catches
a wrong base branch:

```bash
GH_TOKEN=$GITHUB_TOKEN GH_HOST=github.com \
  gh pr view <PR_NUMBER> --repo IBM/velox \
  --comments --json comments --jq '.comments[-1].body' | sed -n '/Added new rebase item/,$p'

GH_TOKEN=$GITHUB_TOKEN GH_HOST=github.com \
  gh pr view <PR_NUMBER> --repo IBM/velox \
  --json commits --jq '[.commits[].oid]'
```

If the lists differ, go back to Step 8, fix the base, and re-post the Step 10
`alchemy merge` comment.

### 11b — If the bot rejects the request

A `Failed to add new rebase item: ... overlaps with the following existing item` reply
means the timestamp you asked for is unusable, not that your branch is wrong. The bot
treats an item added at `T` as coexisting with any item whose `[Added, removed)`
interval contains `T`; if one of those covers your commits, it refuses. Note the
*currently effective* item is exempt, since the merge would remove it — so the culprits
are older, already-removed items.

Read the `Added:` / `Status: removed` timestamps in the overlap list and pick the first
second after the last one, then re-post Step 10 with that timestamp. Any value still
earlier than the issue's **Base time** keeps the PR in the same pick slot.

For example, when items were added at `18:18:21Z` and removed at `18:18:22Z`,
`@2026-01-01T18:18:21Z` was rejected twice and `@2026-01-01T18:18:23Z` was accepted.

## Step 12 — Reopen the original rebase issue

```bash
GH_TOKEN=$GITHUB_TOKEN GH_HOST=github.com \
  gh issue reopen <ISSUE_NUMBER> --repo IBM/velox
```
