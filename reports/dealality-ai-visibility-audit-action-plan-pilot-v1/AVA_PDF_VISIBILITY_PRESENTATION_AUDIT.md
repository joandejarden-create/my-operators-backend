# AVA PDF Visibility & Digital Authority Presentation Audit

**Contract:** `AVA_PDF_VISIBILITY_PRESENTATION_AUDIT_V1`  
**Generated:** 2026-11-05  
**Paid research:** $0  
**ADP measurements:** unchanged  
**Publication:** not authorized  
**Pilot results:** `AVA_PDF_VISIBILITY_PRESENTATION_PILOT_RESULTS.json`

---

## Executive verdict

**Root cause:** Digital Authority V2 findings **were present in payloads and partially rendered**, but customer presentation was weak: research jargon in executive copy, strategy codes shown as “why it matters,” inflated discoveryCoverage counts vs customer-facing sources, truncated/list-heavy layout, and platform statuses shown as raw registry codes. This is a **presentation and completeness** defect — not missing portfolio research.

**Pilot certification (3 hotels):** **PASS** after permanent renderer + CQA updates and staged PDF regeneration.

---

## Phase 1 — Field-to-PDF mapping

| Data field (payload) | Before (customer PDF) | After (customer PDF) | Gap closed? |
|---|---|---|---|
| `digitalAuthority.sources` VERIFIED_BACKLINK | Present as bullet list (≤4); destination URLs raw; strategy codes as relevance | Snapshot count + table (Referring website / Links to / Why it matters / Evidence), ≤6 | Yes |
| `digitalAuthority.sources` VERIFIED_EXTERNAL_MENTION | Present with internal “VERIFIED THIRD-PARTY REFERENCE” chip | Table: Source / Finding (“mentioned; no verified property hyperlink”) / Evidence | Yes |
| `discoveryCoverage.verifiedBacklinks` (incl. research inventory) | Shown as “Verified website links: 7” while customer sources had 4 | **Customer-facing source counts only**; partial-research disclosure | Yes |
| `opportunities` | Present but labeled with finding-class chips / priority scores | Commercial “Why it matters / What to do” without internal class chips | Yes |
| `competitorBacklinks` / `competitorGaps` | Present when non-empty; easy to miss | “Competitor visibility notes” with plain language | Partial (still evidence-limited) |
| `executiveFinding` | “Public V2 discovery verified N… excluding first-party brand-family…” | Commercial “What this means commercially” paragraph | Yes |
| `digitalContentReadiness.platformCoverageRegistry` | Table with `GOOGLE BUSINESS PROFILE` style labels | Human labels + “Identified — not field-assessed” | Yes |
| `platformCorrections` | Table existed; Issue column often empty/wrong field | Uses discrepancy/issue text | Yes |
| ADP metrics (consideration, scenario, peer) | Already on Executive Audit Summary | Unchanged | N/A (already correct) |
| Admin `researchInventory` | Intentionally not in PDF | Still Admin-only | Correct |

### Findings present in underlying data (pilot)

| Hotel | Verified BL (customer sources) | Mentions | Opps | Platforms registry |
|---|---:|---:|---:|---|
| Castillo Hotel Son Vida | 4 | 3 | 2 | Present (incl. GBP/OTA IDENTIFIED_NOT_ASSESSED) |
| AC Hotel A Coruña | 2 | 4 | 2 | Present + CVB assessed |
| Waterstone Resort & Marina | 0 | 1 | 2 | Present; Hilton brand ACCESS_RESTRICTED |

Note: Portfolio certification cited **56 backlinks / 142 mentions** across 38 hotels including research-inventory and coverage counters. Customer PDFs correctly show **customer-facing verified sources only**, with partial-research disclosure — not the Admin inventory total.

---

## Phase 2–3 — Corrections implemented (permanent)

### Renderer (`build-audit-pdf-html-v1.js`)

1. Renamed section to **Digital Authority & External Visibility**.
2. Added **External visibility snapshot** table (links vs mentions vs competitor examples + plain-language meaning).
3. Replaced jargon executive finding with commercial summary (and scrubbed stored payload copy).
4. Backlink **table** with commercial relevance (strategy codes suppressed).
5. Mentions clearly labeled as **references without a clear hotel link**.
6. Opportunities: Why it matters / What to do (no internal class chips).
7. Platform coverage: human platform + status labels; explicit note that “identified not assessed” ≠ incorrect.

### Customer language (`customer-facing-language-v1.js`)

- Block/translate: `Public V2 discovery`, `first-party brand-family`, strategy codes `A_DIRECT_*`, etc.

### CQA (`customer-quality-assurance-v1.js`)

New completeness defects:

- `MODULE_PDF_OMISSION` — DA/DCR module or significant BL/mentions/platform table missing from PDF HTML.
- `FINDING_COUNT_MISMATCH` — PDF snapshot backlink count ≠ customer-facing source count.
- Unassessed platforms must be labeled as not field-assessed.
- Fail HIGH/CRITICAL when significant customer-relevant findings are omitted.

Does **not** fail solely because a hotel has zero verified backlinks (must disclose accurately) or because a nonessential platform is unassessed (must label accurately).

---

## Phase 5 — Three-hotel pilot results

| Hotel | Evidence gate | CQA | Pages | BL hosts visible | Verdict |
|---|---|---|---:|---:|---|
| Castillo Hotel Son Vida | PASS | PASS | 17 | 4/4 | **PASS** |
| AC Hotel A Coruña | PASS | PASS | 16 | 2/2 | **PASS** |
| Waterstone Resort & Marina | PASS | PASS | 16 | 0/0 (disclosed) | **PASS** |

### Before → after (presentation)

| Check | Before | After |
|---|---|---|
| DA section commercially scannable | Weak / jargon | Snapshot + tables |
| BL vs mention distinction | Present but coded | Explicit headings + meaning |
| Strategy codes / V2 jargon | Leaked | Removed |
| Platform assessment clarity | Raw keys | Human labels + unassessed disclosure |
| Count reconciliation | Inflated vs customer sources | Matched |
| Prior PDFs | — | Preserved under `pdf-staged/pre-visibility-presentation/` |

---

## Recommended portfolio rollout

**Apply across all 38 hotels** via a controlled PDF-regeneration batch (no rediscovery):

1. Scrub `digitalAuthority.executiveFinding` / action `whatWeFound` with the same language rules.
2. Regenerate staged PDFs with the updated renderer.
3. Run evidence gate + CQA (module completeness).
4. Preserve prior PDF versions; do not change ADP baselines or founder flags.

Estimated effort: renderer already permanent; portfolio regen ≈ same cost as prior PDF-only passes (no paid research).

**Do not** re-run Exa/DA V2 discovery solely for this presentation fix.

---

## Authorization boundary

| Item | Status |
|---|---|
| Presentation / CQA / PDF regen (3 hotels) | Done |
| Paid research | Not performed |
| Deployment / publication | Not performed |
| ADP measurement changes | None |
| Automatic founder approval | Not granted |

---

## Remaining limitations

- GBP/OTA/maps remain largely **identified — not field-assessed** until authorized deeper listing audits.
- Competitor “gaps” remain thin where public HTML did not confirm peer outbound links — disclosed, not invented.
- Portfolio-wide PDF rollout not yet executed (pilot only).

*End of audit.*
