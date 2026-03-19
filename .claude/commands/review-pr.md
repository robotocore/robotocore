# Skill: Review a Pull Request

Review, improve, and merge an open pull request. The submitter did the work — your job is to get their PR merged cleanly, giving them full credit.

**Argument**: PR number or URL (e.g., `/review-pr 191` or `/review-pr https://github.com/robotocore/robotocore/pull/191`)

## Philosophy

The submitter's PR is **their contribution**. Prioritize merging it in the shape they submitted it. Fix only what blocks CI or violates project rules. If you want to make further improvements, do that in a follow-up PR *after* theirs is merged. Never rewrite their approach — if the approach itself is wrong, request changes and explain why.

## Process

### 1. Gather context

```bash
gh pr view $PR --json title,body,state,author,headRefName,baseRefName,mergeable,mergeStateStatus,commits,comments,reviews
gh pr diff $PR
gh pr checks $PR
```

Capture: author login, branch names, current CI status, whether it's from a fork.

### 2. Welcome first-time contributors

Check if this is the author's first PR to this repo:

```bash
gh pr list --author $AUTHOR --state merged --limit 1 --json number
```

If empty (no prior merged PRs), leave a warm welcome comment:

> Welcome to robotocore, @{author}! Thanks for your first contribution — we're glad to have you here. Let me take a look at this.

For returning contributors, a simple "Thanks for this, @{author}" is enough.

### 3. Check for prompt log (gate — exit early if missing)

The PR **must** include a `prompts/*.md` file if it touches `src/`, `tests/`, or `scripts/`. Check:

```bash
gh pr diff $PR --name-only | grep -E '^(src/|tests/|scripts/)'
gh pr diff $PR --name-only | grep '^prompts/'
```

If source files changed but no prompt log exists:
- Leave a review requesting changes with this message:

> This PR changes source/test files but doesn't include a prompt log entry. We track AI and human decision provenance in `prompts/` — see [prompts/PROMPTLOG.md](prompts/PROMPTLOG.md) for the format.
>
> Please add a file at `prompts/{YYYYMMDD-HHMMSS}-{slug}.md` documenting the human prompt and key decisions behind this change. If this was written entirely by a human without AI assistance, a brief entry noting that is fine too — the log is about provenance, not policing tools.

- **Do not approve.** Stop here. The contributor needs to push the prompt log.

**Exception**: If the PR only touches docs, CI config, or non-code files, the prompt log is not required.

### 4. Critical code review

Review the diff from **all of these angles**, picking fresh specific concerns each time (don't repeat the same generic checklist):

**Correctness & AWS fidelity**
- Does the implementation match real AWS behavior? Check botocore's service-2.json for the operation shape.
- Are response fields, error codes, and status codes correct?
- For native providers: does it handle both the happy path and error cases?

**Security**
- No hardcoded secrets, no command injection, no unsanitized XML/HTML.
- Exception handlers don't swallow errors silently (project rule: never `except: pass`).
- Inputs that end up in XML responses use `xml_escape()`.

**Test quality**
- Every test MUST contact the server (no tests that only catch `ParamValidationError`).
- Every test MUST assert something meaningful about the response.
- Tests clean up resources they create (use `try/finally`).
- No speculative xfails — if it doesn't work, don't test it.

**Wire format & protocol**
- For native providers: does the XML/JSON response match botocore's expected shape?
- Are operation names PascalCase matching the AWS API?
- Multi-value query params use the `.member.N` convention where AWS does.

**Project conventions**
- `ruff` clean (line-length 100, rules E/F/I/N/W/UP).
- No `except ...: pass` without logging or comment.
- Imports: stdlib → third-party → local, blank-line separated.
- Test files mirror source structure.

**Integration risk**
- Could this break existing tests? (Check for shared state, hardcoded resource names that collide.)
- Does it register new routes that might shadow existing ones?
- If it modifies Moto vendor code, is the fix on a feature branch and pushed to the fork?

Leave inline review comments on specific lines for anything notable — both praise ("nice catch on the audience validation") and issues. Be specific and constructive, never dismissive.

### 5. Fix what blocks the merge

If CI fails or there are small issues (lint, bare except blocks, missing import), **fix them yourself** by pushing commits to the PR branch:

```bash
# For PRs from the same repo:
git fetch origin $BRANCH && git checkout $BRANCH

# For PRs from forks (check maintainerCanModify first):
gh pr view $PR --json maintainerCanModify,headRepositoryOwner,headRepository
git remote add $FORK_OWNER https://github.com/$FORK_OWNER/$REPO.git
git fetch $FORK_OWNER $BRANCH
git checkout -b pr-$PR FETCH_HEAD
# ... make fixes ...
git push $FORK_OWNER HEAD:$BRANCH
```

**Rules for fixup commits:**
- Only fix what blocks CI or violates hard project rules.
- Keep fixes minimal — don't refactor their code.
- Use clear commit messages: `fix(lint): add logging to bare except block in PR #$PR`
- Always attribute: `Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>`

If the PR needs a prompt log and was submitted by someone who may not know the format, **you may add one for them** — capture their intent from the PR description.

### 6. Approve and auto-merge

Once CI is green and the review is clean:

```bash
gh pr review $PR --approve --body "Looks good! $SUMMARY. Thanks @$AUTHOR."
gh pr merge $PR --merge --auto
```

If CI needs workflow approval (fork PRs):
```bash
# Find and approve pending workflow runs
gh api repos/OWNER/REPO/actions/runs --jq '.workflow_runs[] | select(.head_sha == "$SHA") | select(.status != "completed") | .id' | while read id; do
  gh api repos/OWNER/REPO/actions/runs/$id/approve -X POST
done
```

Wait for CI with `gh pr checks $PR --watch`, then verify the merge landed.

### 7. Post-merge

After the PR merges, if you identified improvements that go beyond the submitter's scope, you may open a **separate follow-up PR** with those changes. Credit the original author in the description:

> Follow-up to #$PR by @$AUTHOR. Their implementation of X was solid — this PR adds Y on top of it.

## What NOT to do

- **Don't rewrite their implementation.** If the approach is fundamentally wrong, request changes and explain. Don't silently replace their code.
- **Don't add features they didn't ask for.** Scope creep in review is disrespectful to the contributor.
- **Don't cherry-pick to another branch.** Merge the PR directly so the author gets credit in git history.
- **Don't block on style nitpicks.** If the code works and CI passes, style preferences are suggestions, not blockers.
- **Don't forget to say thank you.** Every contribution matters.
