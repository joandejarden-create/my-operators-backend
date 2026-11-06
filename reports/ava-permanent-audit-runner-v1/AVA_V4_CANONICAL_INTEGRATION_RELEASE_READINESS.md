# AVA V4 Canonical Integration — Release Readiness

**Recommendation:** `AVA_V4_CANONICAL_RELEASE_READY`  
**Date:** 2026-11-06  
**Stop point:** PR prepared — **no merge / no production deploy / no customer publish** in this task.

---

## A. Git reconciliation

| Item | Value |
|---|---|
| Canonical repo | `C:\Dev\dealality\my-operators-backend` |
| Integration worktree | `C:\Dev\dealality\my-operators-backend-ava-v4-canonical` |
| Starting tip | `origin/main` @ `0df6e8831` (includes merged GDI PR #146) |
| Integration branch | `release/ava-v4-canonical-integration-v1` |
| Integration commit (feature) | `67f41cb97ec973274a534afde2e3ea19432e808e` |
| Branch HEAD | `6d72f4ea68f4c6f2e6644872c7efb73794078bcc` |
| Pull request | https://github.com/joandejarden-create/my-operators-backend/pull/147 |
| Prior AVA quality worktree | `my-operators-backend-ava-v4-quality` @ `fix/ava-v4-customer-quality-remediation-v1` |
| Prior AVA release PR | [#145](https://github.com/joandejarden-create/my-operators-backend/pull/145) `release/ava-v4-final-customer-quality-v1` (merged into this branch) |
| Related open PR | [#142](https://github.com/joandejarden-create/my-operators-backend/pull/142) production-source reconcile (separate) |

### What landed on the integration branch

1. Merged `origin/release/ava-v4-final-customer-quality-v1` (certified 38-hotel V4 pack + draft auth tooling).
2. Added permanent AVA audit runner + ADP Airtable history sync + OA gates.
3. Cover title template update + representative regen verification.
4. Lifecycle helper `assertAdpAvaCertificationForCustomerReady`.

### Intentionally excluded from Git

- `.env` / secrets
- Runtime `data/.../audit-runs/` and Airtable mirrors
- OA-generated temporary archive overwrites (restored)
- No GDI methodology changes beyond what already exists on `main`

---

## B. Cover page update

| | |
|---|---|
| Old title (subtitle slot) | `INITIAL DIGITAL PRESENCE ASSESSMENT` |
| Old doc-type | `AI VISIBILITY AUDIT & ACTION PLAN` |
| **New title** | `AI VISIBILITY & DIGITAL PRESENCE ASSESSMENT` |
| **New subtitle** | `AUDIT & ACTION PLAN` |

### Template / CSS

- `lib/dealality-report-family/dealality-report-cover-v1.js` — canonical `AVA_COVER_REPORT_TITLE` / `AVA_COVER_REPORT_SUBTITLE` + legacy remap
- `public/css/adp-ai-visibility-audit-report-v1.css` — slightly tighter letter-spacing / wrap for longer doc-type
- All 38 payload `reporting.coverSubtitle` → `AUDIT & ACTION PLAN`
- Historical archived PDFs **not** rewritten

### Visual QA (representative regen)

Hotels: Spice Island, AC Coruña, Cambridge Beaches, Castillo Son Vida  
Artifact: `reports/ava-permanent-audit-runner-v1/cover-title-regen-v1/`

- New title present; “INITIAL” absent
- Subtitle `AUDIT & ACTION PLAN` present
- Hotel / period preserved; no cover overflow observed on cover PNGs
- Certification labels preserved (Spice `CERTIFIED_MATCH`; Cambridge/AC historical; Castillo remains blocked — truthful)

---

## C. Permanent runner

| Capability | Status |
|---|---|
| PowerShell `scripts/Run-AVA-Audit.ps1` | Present |
| Node CLI `ava:permanent-audit` | Present |
| Execution contract | `AVA_AUDIT_EXECUTION_CONTRACT_V1` |
| ADP↔AVA permanent cert | Wired; Spice = MATCH |
| Immutable archive | `data/.../audit-runs/{propertyId}/{auditRunId}/` |
| Airtable sync | Existing ADP base `appa2cE7FTRmIbB32` (Snapshots / Monitoring / Period Metrics / Hotel Intelligence Evidence) |
| Admin versioning | `/permanent-runs` APIs + UI sync status |

Operational acceptance: **`AVA_PERMANENT_AUDIT_RUNNER_OPERATIONAL_ACCEPTANCE_PASS`**  
(including `AIRTABLE_PERSISTENCE_PASS`)

---

## D. Portfolio verification

| Check | Result |
|---|---|
| 38 hotel identity / dry-run | PASS (`test:ava-permanent-audit-runner`) |
| 38 ADP reference classification | 38/38 classified — 18 lean-ok; 20 known metric-drift remain blocked (not fabricated) |
| Permanent cross-report | 1 `CERTIFIED_MATCH` (Spice); 17 `CERTIFIED_HISTORICAL_REFERENCE`; 5 reconciliation; 15 blocked |
| Customer quality standard | PASS |
| PDF presentation (prior V4 pack + 4 cover regens) | PASS for cover title; portfolio certified PDFs retained from PR #145 pack |
| Historical preservation | Prior archive bytes restored; new cover PDFs are new version identities under `cover-title-regen-v1/` |

Historical references are **not** promoted to current-period MATCH.

---

## E. Deployment safety

| Gate | Result |
|---|---|
| `npm run ava:operational-acceptance` | PASS |
| Permanent runner regression | PASS |
| ADP↔AVA unit cert tests | PASS |
| Customer quality | PASS |
| Admin workflow | PASS |
| Airtable live persistence (bounded) | PASS (existing ADP tables; no Published Reports writes) |
| Secrets in PR | None (`.env` gitignored) |
| Auto-publish on deploy | No — draft/admin path only; publication gates still require founder + CERTIFIED_MATCH |
| Share-token contracts | Untouched |
| GDI / ADP methodology | Untouched by this PR’s AVA-only delta |

---

## F. Release recommendation

```
AVA_V4_CANONICAL_RELEASE_READY
```

### Authorized next steps (separate approval required)

1. Review & merge PR for `release/ava-v4-canonical-integration-v1`
2. Production deploy (does **not** auto-publish customer PDFs)
3. Optional: founder re-pin / remediate the 20 metric-drift hotels before joint ADP↔AVA customer delivery
4. Optional: close or supersede PR #145 after this PR merges

### Explicit non-claims

- Not merged
- Not deployed
- Not customer-published
- Published PDFs / share tokens not rotated
