# Universal PR completion and workspace ownership (Dev OS)

**Contract:** `DEALALITY_PR_COMPLETION_AND_WORKSPACE_OWNERSHIP_V1`  
**Applies to:** GDI (`ws-gdi` / `ws/gdi`), ADP (`ws-adp` / `ws/adp`), Hotel Explorer (`ws-hotel-explorer` / `ws/hotel-explorer`), Brand Explorer (`ws-brand-explorer` / `ws/brand-explorer`), Integration Controller  

## Non-negotiable

A successful GitHub PR merge is **not** a completed development task until positively verified:

1. Required CI checks passed.
2. The PR merged into `main`.
3. Actual merge SHA recorded.
4. The owning permanent workspace returned to its registered **home branch**.
5. Home branch HEAD equals latest `origin/main` (and workspace HEAD equals `origin/main` when on home).
6. Working-tree status explicitly inspected.
7. Research/runtime evidence preservation **explicitly assessed** (never inferred).
8. Cursor active branch and feature-branch retirement either **verified** or clearly marked `OUTSTANDING: …`.
9. Other workspaces not disturbed.

Opening a PR, passing CI, or merging into GitHub alone is **not sufficient**.

### Integrated delivery (founder review readiness)

Contract: `DEALALITY_INTEGRATED_DELIVERY_V1` ([DEALALITY_INTEGRATED_DELIVERY_V1.md](./DEALALITY_INTEGRATED_DELIVERY_V1.md)).

For product founder review, the normal successful status is:

```text
INTEGRATED_ON_8080 — AWAITING_FOUNDER_REVIEW
```

That requires IC-authorized merge to `main`, post-merge home sync, canonical `:8080` serving the integrated tip, cumulative functionality + inventory verification, and browser evidence. Do **not** routinely treat `PR OPEN` or `AWAITING_INTEGRATION_CONTROLLER` as founder-review ready. Product streams must initiate the established IC remote-merge handoff (`npm run dealality:remote-pr-merge -- --pr <n>`) when CI is green; IC retains exclusive merge authority.

### Fail-closed COMPLETE

`npm run dealality:pr-completion-report` **must not** return COMPLETE when merge/CI/home/preservation/cursor/retirement evidence is null, omitted, unknown, or `NOT_ASSESSED`.

Prefer querying GitHub (`gh pr view`) for merge + CI evidence rather than trusting caller-supplied booleans alone. Offline/test callers may inject verified GitHub-shaped evidence, but null/unknown still fails closed.

## Ownership enforcement

Authoritative ownership source: `docs/ai-build-system/worktree-registry.json`.

Before branch mutations, verify worktree root, owning stream, current branch/HEAD, target branch, unresolved merge/rebase/unmerged-index, dirty/untracked, and preservation needs.

**Fail closed** on:

- Cross-workstream feature branch targets
- **Unclassified** branch targets (not matching the owning stream pattern) unless narrow founder auth:
  - `FOUNDER_AUTH_CROSS_STREAM_BRANCH`
  - `FOUNDER_AUTH_UNCLASSIFIED_BRANCH`
- Product workspace checkout of `main` (use home `ws/*` instead)
- Mutations while MERGE_HEAD / rebase / unmerged index is active

Cursor conversation branch chips are **not** authoritative Git state.

### Enforcement coverage (honest)

**Enforcement level: PARTIAL — not universal.**

| Entry point | Enforcement |
|-------------|-------------|
| `dealality:git-preflight` switch/checkout/merge/rebase/stash | **Machine** fail-closed ownership + unresolved gates (`workspace-ownership.js` + `workspace-ownership-guard-v1.js`) |
| `dealality:post-merge-sync` | **Machine** preserve-first; never auto-switch active tasks; no reset/clean/stash-discard |
| `dealality:pr-completion-report` | **Machine** fail-closed COMPLETE evidence rules |
| Raw `git` without preflight | **Instruction-only** (bypass possible) |
| Cursor `SetActiveBranch` / UI chip | **Instruction-only** (non-Git metadata) |
| Cursor cloud-agent / `move_agent_to_root` / worktree reattachment | **Instruction-only** (not intercepted) |
| Product chat startup prompts | **Instruction-only** |
| Integration Controller `gh pr merge` | **Remote merge OK with dirty Integration checkout**; local sync remains separate |

Unavoidable bypasses: any direct git/IDE path that skips `dealality:git-preflight`; Cursor branch-chip tools; external agents outside Dev OS commands.

Do **not** claim universal enforcement. Print coverage: `npm run dealality:pr-completion-report -- --coverage`.

## Preservation

Do **not** automatically stash, discard, force-checkout, reset, or `git clean` to resolve conflicts.

A clean Source Control display must never take priority over preserving work.

`NOT_ASSESSED` preservation **cannot** be treated as verified preservation and **cannot** yield COMPLETE.

## Integration Controller

Owns: merge authorization/execution, CI verification, remote merges that do not disturb dirty local trees, safe permanent-home sync, post-merge verification, consistent completion reporting.

Product chats must not independently merge to `main` or run disruptive cross-worktree operations.

### Additive development & non-regression review (binding)

Every relevant product PR must include a **non-regression assessment**. Integration Controller **must block acceptance** when previously approved UI layouts, opportunity inventory, data accessibility, presentation, or customer-facing functionality has **unintentionally** regressed.

Required closeout evidence (scale to change surface):

1. Pre-change baseline for affected UI / inventory / workflows  
2. After-change comparison (prefer record IDs over counts alone)  
3. Explicit classification of differences as intentional vs unintended  
4. Unintended regressions resolved before certification  
5. Automated regression coverage added when a defect class is identified  

Intentional removal, replacement, redesign, or material change to previously accepted functionality requires **explicit founder approval**. Binding completion rule: new functionality works **and** previously accepted functionality continues to work. Principles: [DEALALITY_ADDITIVE_NON_REGRESSION_PRINCIPLES.md](./DEALALITY_ADDITIVE_NON_REGRESSION_PRINCIPLES.md).

### Durable intelligence persistence review (binding)

When a PR creates or changes durable business intelligence, Integration Controller must verify the PR closeout includes the [AIRTABLE_PERSISTENCE_GATE.md](./AIRTABLE_PERSISTENCE_GATE.md) package:

1. Airtable source-of-truth mapping  
2. Persistence and retrieval evidence  
3. History/provenance verification  
4. Cross-workstream identity checks  
5. Remaining persistence limitations  
6. Explicit classification: `PROTOTYPE` | `PERSISTENCE_DESIGNED` | `PERSISTENCE_VALIDATED` | `FULL_E2E_VALIDATED` | `CUSTOMER_PUBLISHED`

Do **not** accept `FULL_E2E_VALIDATED` / customer-ready claims when data exists only in a local checkout, temporary runtime cache, untracked files, UI demos, PDFs, or unit tests without proven persistence.

## Standardized completion statuses

| Status | Meaning |
|--------|---------|
| **COMPLETE — MERGED_AND_HOME_SYNCED — CLEAN_WORKTREE** | All required evidence verified; home synced; clean tree |
| **COMPLETE — MERGED_AND_HOME_SYNCED — PRESERVED_DIRTY** | All required evidence verified; intentional local/research artifacts remain (listed) |
| **BLOCKED — EVIDENCE_INCOMPLETE** | Missing/unknown merge SHA, CI, preservation, cursor, or retirement evidence |
| **BLOCKED — PRESERVATION_REQUIRED** | Cannot continue without protecting existing work |
| **BLOCKED — CROSS_WORKSTREAM_BRANCH** | Branch violates registered workspace ownership |
| **BLOCKED — UNCLASSIFIED_BRANCH** | Target/current branch not owned and not authorized |
| **BLOCKED — UNRESOLVED_GIT_OPERATION** | Active merge/rebase/cherry-pick/unmerged index |
| **BLOCKED — HOME_NOT_SYNCED** | Not on home and/or home/HEAD not at `origin/main` |
| **BLOCKED — NOT_MERGED** / **BLOCKED — CI_INCOMPLETE** | Merge or CI gate failed |

Never report completion from PR merge status alone.

## Required final report fields

- PR number and merge SHA
- Owning workstream and worktree path
- Permanent home branch
- `git branch --show-current`
- `git rev-parse HEAD`
- `git rev-parse origin/main`
- HEAD synchronization status
- Tracked modification count
- Visible untracked count
- Ignored evidence preservation status (**assessed**, not NOT_ASSESSED)
- Cursor active branch (verified or `OUTSTANDING: …`)
- Temporary feature branch retirement status (verified or `OUTSTANDING: …`)
- Peer worktree impact
- Final standardized completion status
- Enforcement level (PARTIAL) and known bypasses when relevant

## Tooling (reuse — do not fork)

| Concern | Command / module |
|---------|------------------|
| Ownership + dirty gates | `npm run dealality:git-preflight` → `git-preflight.js` + `workspace-ownership.js` |
| Post-merge home sync | `npm run dealality:post-merge-sync` |
| Completion report | `npm run dealality:pr-completion-report` |
| Ownership audit | `npm run dealality:pr-completion-report -- --audit` |
| Enforcement coverage | `npm run dealality:pr-completion-report -- --coverage` |
| Regression gate | `npm run test:dealality-pr-completion-ownership-v1` |

### Recovery before disruptive enforcement

Live workspace recovery must be inventoried with `--audit` **before** forcing home returns. Fail-closed mutation gates refuse new cross-stream/unclassified switches; they do **not** auto-reset or discard existing dirty trees.

## Related

- `docs/ai-build-system/POST_MERGE_WORKSPACE_SYNC.md`
- `docs/ai-build-system/DEALALITY_DEV_OS.md`
- `.cursor/rules/dealality-worktree-governance.mdc`
- `.cursor/rules/gdi-github-branch-merge-discipline.mdc`
- `.cursor/rules/dealality-pr-completion-workspace-ownership.mdc`
