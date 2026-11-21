# FOUNDER REPORT — Exa Search vs Exa Agent Pilot

Generated: 2026-11-21T01:37:11.178Z (rescored after independent verification tightening)

## 1. PR / HEAD

- **PR:** [#503](https://github.com/joandejarden-create/my-operators-backend/pull/503) — `feat/hotel-intelligence-factory-v5` (OPEN)
- **Parent tip before this commit:** `e1741fafb166dd92e9f70c08dbd141b95368d8bc`
- **Implementation:** optional stage `EXA_AGENT_DEEP_VERIFICATION` + `exa-agent-*-v1.js` + pilot script/report pack
- **Registered workspace:** `C:\Dev\dealality\ws-hotel-explorer` (dirty HE tree preserved; commit-tree onto PR branch)
- **origin/main at report time:** `d6dd99ddc588835c67cf46c8a4089b19089878ec` (V5 branch not FF of latest main — IC merge owns rebase/conflict)

## 2. Agent API connectivity & pricing

- Connectivity OK: **true** (live probe run succeeded)
- Blocker: none
- Endpoint: `POST/GET https://api.exa.ai/agent/runs`
- Auth: `EXA_API_KEY` via `x-api-key` / Bearer
- Async: create run → poll until `completed|failed|cancelled`
- Structured output: `outputSchema` → `output.structured`
- Fixed effort prices (official https://exa.ai/pricing): minimal $0.012 · low $0.025 · medium $0.10 · high $0.50 · xhigh $1.00
- Metered `auto`/`ultra` avoided in this pilot
- Pilot effort: **minimal** probe + **low** for all 30 cases
- Pilot ceiling: **$15** separate from V5 Search/SEC-API $25 — actual Agent spend **$0.762** (remaining $14.238)

## 3. Thirty-case cohort composition

- Requested: A10 / B10 / C10
- Actual: A10 / B10 / C10 (total 30)
- A = historical agreement, current status unknown
- B = missing expiration or notice
- C = ownership/operator or commercial signal gaps
- Source pool: V5 BRAND_CONVERSION / MANAGEMENT_CONTRACT lists + EX-10 persist named hotels
- Note: cohort includes a few low-fit edge names (e.g. Maple Grove RV Resort, Bellagio) inherited from V5 commercial inventory — flagged in false/unsupported where date semantics failed

## 4. Search baseline

- Cases frozen: 30
- Search spend in freeze: $0
- Method: reconstructed from Factory V5 commercial + EX-10 evidence (no live Search spend; Agent budget not double-counted)
- Previously known end dates / evidence URLs are **not** scored as Agent incremental discoveries

## 5. Agent investigation results

- Agent attempts: 30 / OK: 30 / skipped: 0
- Calibration: 3 cases @ low before full 30
- Durable run IDs recorded in `AGENT_INVESTIGATION_RESULTS.json`
- Cache dir: `data/sec-edgar/exa-agent-cache`

## 6–7. Independently verified incremental findings

Credited only after independent verification module (Agent alone **cannot** satisfy `AGREEMENT_EXISTENCE_VERIFIED` / `CONTRACT_TERMS_VERIFIED` / `CURRENT_STATUS_VERIFIED`):

1. **Hampton Inn Joplin** — Agent candidate termination effective **2024-07-18** + Choice conversion listing (Comfort Inn Joplin I-44). Classification: `CLOSED_OR_SUPERSEDED`. Source: Hilton franchise complaint excerpt (court filing). Still needs primary-document reopen before any promotion.
2. **Courtyard San Mateo Foster City** — owner/operator commercial signal (Clarion Partners / Marriott, Jan 2025 evidence claim). Classification: `VERIFIED_COMMERCIAL_SIGNAL`. Not a decision-window.

### False positives / attribution failures (3)

- Hampton Inn Anderson — ungrounded/empty answer
- Comfort Inn & Suites - Madison West — opening/loan date semantic risk
- Maple Grove RV Resort — opening/loan date semantic risk

## 8–9. Dual prospect list deltas (CUSTOMER_BLOCKED)

- Brand conversion Agent deltas: `BRAND_CONVERSION_TARGETS_AGENT_DELTA.json` (2 candidate rows)
- Management contract Agent deltas: `MANAGEMENT_CONTRACT_TARGETS_AGENT_DELTA.json` (0)
- No HPC creates/updates · no customer publication · no schema apply

## 10. Side-by-side metrics

| Metric | Search only | Search + Agent | Incremental gain |
|---|---:|---:|---:|
| Verified expiration dates | 28 | 29 | 1 |
| Verified notice periods | 0 | 0 | 0 |
| Verified current-status resolutions | 0 | 1 | 1 |
| Executed renewals confirmed | 0 | 0 | 0 |
| Verified owner/operator identities | 0 | 1 | 1 |
| Qualified brand-conversion targets | 30 | 32 | 2 |
| Qualified management targets | 0 | 0 | 0 |
| False or unsupported findings | 0 | 3 | 3 |
| Actual expenditure (USD) | 0 | 0.762 | 0.762 |

### Unit economics

- Cost per newly verified contract date: **$0.762**
- Cost per newly verified notice period: n/a
- Cost per resolved contractual status: **$0.762**
- Cost per distinct incremental qualified hotel target: **$0.381** (2 hotels)
- % Agent results requiring manual correction: **10%**
- % Agent runs with no incremental value: **93.3%**

**Verdict:** Exa Search remains the high-volume discovery engine. At low effort, Agent did **not** systematically increase trustworthy conversion/management targets per dollar; it produced **one commercially meaningful status/termination lead** (Joplin) and one ownership signal amid mostly non-incremental runs.

## 11. Recommended permanent Agent routing policy

- **MANUAL_ESCALATION_ONLY**
- Auto-deploy now: **false**
- Keep high-volume discovery on Exa Search + free SEC EDGAR
- Proposed future triggers (not enabled): historical end date with unresolved current status; high-value missing notice after Search exhausted; conflicting renewal evidence; multistep owner/operator research; Search exhausted on an important contractual question

## 12. Remaining research gaps

- `CURRENT_STATUS_VERIFIED` still requires later-filing / primary-document reopen (Joplin complaint ≠ final judgment)
- Zero incremental notice periods in this 30-case sample
- Management-contract cohort under-represented in the sampled V5 brand-heavy pool
- Agent often restates historical CMBS end dates without new corroboration

## 13. Approvals required before broader deployment

- Founder approval of routing policy
- Separate Agent budget allocation beyond this $15 pilot (if expanding)
- Confirmation Agent findings still pass independent verification gates before prospect promotion
- No customer publication / HPC without separate authorization
- IC merge of PR #503 (product chat does not merge)

---

Budget snapshot: spentUsd=$0.762 · ceiling=$15 · effort=low · runs=30+probe
