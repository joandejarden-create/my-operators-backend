# Radar authenticated application acceptance gate (Dev OS)

**Contract:** `DEALALITY_RADAR_AUTHENTICATED_APP_ACCEPTANCE_GATE_V1`  
**Canonical route:** `http://localhost:8080/app#/opportunity-radar`  
**Applies to:** Hotel Explorer / Radar changes that affect hotel visibility, filtering, search, mapping, authentication, or Hotel Explorer navigation from Radar.

## Non-negotiable

A Radar / Hotel Explorer task is **not complete** merely because:

- API requests return HTTP 200.
- Automated tests pass.
- Hotels exist in a JavaScript dataset.
- A standalone Radar/demo/share page works.
- A PR passes GitHub CI.

Those signals are necessary but **not sufficient**. Completion requires end-to-end verification in the **actual authenticated application** at the canonical route above.

## Required acceptance checks

Validate through `/app#/opportunity-radar` (authenticated app shell), not a detached demo page:

1. Authenticated application access succeeds.
2. Required API calls succeed without authorization errors.
3. Expected hotels enter the actual application dataset used by that route.
4. Hotels visibly render and/or appear in accessible clusters and search results.
5. Search and filtering behave correctly for the change under test.
6. Hotel selection reaches the correct property.
7. Hotel Explorer opens the correct record from Radar.
8. Customer / share-mode isolation remains enforced (internal inventory must not leak into customer/share surfaces).

### Dynamic HPC integration

Eligible Open / Pipeline hotels must display through the governed dynamic HPC path **without** hotel-specific allowlists.

## Evidence requirements

Record in the PR (or linked report under `reports/`):

| Evidence | Required |
|----------|----------|
| Git SHA and running server version / checkout identity | Yes |
| Browser URL (must be authenticated `/app#/opportunity-radar`) | Yes |
| Authenticated session type (e.g. founder/internal preview vs customer) | Yes |
| Relevant API responses and error status (no secrets) | Yes |
| Expected vs observed hotels | Yes |
| Browser screenshots or equivalent reproducible visual evidence | Yes |
| Hotel Explorer navigation result from Radar selection | Yes |

**Never** record authentication tokens, cookies, session secrets, or credentials.

## Completion states

| State | Meaning |
|-------|---------|
| **PASS** | All required checks completed successfully in the actual authenticated application. |
| **FAIL** | Observed user-facing behavior does not meet requirements. |
| **BLOCKED** | Authenticated application testing is unavailable or incomplete. |

Automated tests alone **cannot** upgrade **BLOCKED** → **PASS**.

CI green + unit/integration gates may accompany a task, but the authenticated-app gate remains mandatory for in-scope Radar/HE changes.

## PR acceptance checklist (HE / Radar)

Before requesting Integration Controller merge:

- [ ] Automated / path-matrix tests for touched Radar/HE files have been run (`npm run dealality:pr-check-suggest`, relevant `test:adp-phase1-radar-he-gates` / HPC gates as matched).
- [ ] Authenticated app verified at `http://localhost:8080/app#/opportunity-radar`.
- [ ] Checks 1–8 above completed (or explicit **BLOCKED** with reason).
- [ ] Evidence recorded without secrets.
- [ ] For dynamic HPC work: Open/Pipeline eligibility confirmed without hotel-specific allowlists.
- [ ] Completion state declared: **PASS** / **FAIL** / **BLOCKED**.

Do **not** declare the task complete on **FAIL** or **BLOCKED**.

## Governance wiring

- Dev OS overview: `docs/ai-build-system/DEALALITY_DEV_OS.md`
- QA checklist: `docs/ai-build-system/DEALALITY_QA_CHECKLIST.md`
- PR validation matrix: `docs/dealality-pr-validation-matrix.md`
- PR check suggest: `lib/dealality-pr-check-matrix.js` (`adp-hotel-explorer-radar-phase1` + contract gate)
- Cursor rule: `.cursor/rules/dealality-radar-authenticated-acceptance-gate.mdc`
- HE startup: `docs/ai-build-system/PRODUCT_CHAT_STARTUP_PROMPTS.md`

Contract presence gate: `npm run test:radar-authenticated-app-acceptance-gate-v1`
