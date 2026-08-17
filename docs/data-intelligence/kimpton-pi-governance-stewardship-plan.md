# Kimpton Hotels — PI Governance Stewardship Plan

**Date:** 2026-07-06  
**Status:** Planning + controlled stewardship script (`npm run steward-kimpton-pi-pilot`, dry-run default). **Kimpton pilot completed** — prefer `npm run steward-partner-intelligence` for new packages.

> **Authority:** [partner-intelligence-profile-governance-runbook.md](./partner-intelligence-profile-governance-runbook.md)  
> **Audit source:** `reports/partner-intelligence-publish-readiness.{md,json}` (generated 2026-07-06T09:43:42Z)  
> **Publish dry-run:** `reports/partner-intelligence-profile-governance-publish.{md,json}` (Kimpton skipped — not eligible)

---

## 1. Current Kimpton Readiness Status

| Field | Value |
|-------|-------|
| Package key | `brand:recCKuXCmGvxHPfb3` |
| Brand name | Kimpton Hotels |
| Entity type | Brand |
| Target table | **Brand Setup - Brand Basics** |
| Brand record | **`recCKuXCmGvxHPfb3`** |
| Link method | `brand_link` — **resolved** |
| **Eligible** | **No** (blocked) |
| Approved facts | **0** |
| Published Explorer Fields rows | **0** |
| Target protected | **No** (audit) |
| Origin conflict | **No** (unlike Curio) |
| Publish dry-run (2026-07-06) | **Skipped** — `missing_proposed_governance` |

**Comparison to Arbor (completed):** Kimpton has a clean brand link and no mixed-origin conflict, but lacks **any approved facts** and **all four sources** are not approved for Explorer use. Stewardship is **source approval + selective fact review** — not linkage or conflict resolution.

---

## 2. Source Records Involved

| Source record ID | Audit blocker |
|------------------|---------------|
| `rec0wes92ieaTT5UU` | `approved_for_explorer_use_no` |
| `recEMuArkvbqpkrUU` | `approved_for_explorer_use_no` |
| `recjFAVo3M2CL9rBF` | `approved_for_explorer_use_no` |
| `recpXnHJUYhBIdCkt` | `approved_for_explorer_use_no` |

**Not flagged in audit:** `source_status_not_ready`, `source_stale`, `source_quality_low`, or `missing_entity_link` for these four IDs.

Source titles, origins, quality, and status values are **not in the audit JSON** — confirm in Airtable Source Library before approving.

---

## 3. Fact Records Involved

**48** Extracted Facts linked to this package (`factIds` in audit JSON).

**Approved / Edited facts:** **0** — triggers package-level `no_approved_facts`.

Full fact ID list: `reports/partner-intelligence-publish-readiness.json` → `blockedPackages` → Kimpton → `factIds` (48 records).

**Stewardship:** Do not bulk-approve all 48. After source review, approve **3–8 governance-relevant facts** with evidence (e.g. brand positioning, portfolio scale, parent-company relationship, regional presence) — minimum **1** for audit eligibility.

---

## 4. Current Blockers

| Blocker | Scope |
|---------|--------|
| `no_approved_facts` | Package — zero Approved/Edited facts |
| `approved_for_explorer_use_no` | All **4** linked sources |

**Not blocked (audit):** missing entity link, origin conflict, target protection, stale/Found/Captured source status (on these IDs).

---

## 5. Exact Manual Stewardship Actions

Execute in Airtable (human review). Order matters.

| # | Action | Table / field | When |
|---|--------|---------------|------|
| 1 | Open each of the **4** sources; confirm evidence, URL/file, and suitability for Explorer | Source Library | Before any approval |
| 2 | Confirm **Status** is not Stale/Rejected; quality **Medium** or **High** | Source Library | Before Explorer approval |
| 3 | Set **`Approved for Explorer Use`** = **Yes** on each reviewed source | Source Library | After per-source review |
| 4 | Identify **3–8** facts with clear **Evidence Text** and Directly Stated extraction | Extracted Facts | After sources approved |
| 5 | Set **`Human Review Status`** = **Approved** or **Edited** on those facts; set **Approved Value** where needed | Extracted Facts | After evidence read |
| 6 | **Do not** set Brand Basics **Company Validated** or **Validation Status = Company Validated** from PI work | Brand Setup - Brand Basics | Always |
| 7 | **Do not** set **External Display Status = Show Trust Label** on Brand Basics yet | Brand Setup - Brand Basics | Until re-audit shows eligible + publish dry-run approved |
| 8 | Re-run readiness audit | CLI | After steps 3–5 |

Optional later: Published Explorer Fields rows — Kimpton has **none** today; not required for profile-governance publish v1.

---

## 6. Kimpton-Specific Script — Worth Creating?

**Implemented:** `npm run steward-kimpton-pi-pilot` (dry-run default) → `reports/kimpton-pi-stewardship-pilot.{md,json}`.

Apply requires `--apply --approve-kimpton-stewardship` and explicit `--approve-fact-ids` (no bulk fact approval).

**Original assessment (still valid):** 4 sources and selective fact review suit a controlled script; facts are never auto-approved without explicit IDs.

---

## 7. Proposed Governance Values (if Eligible)

Audit **`proposed` is null** while blocked. Values below are **expectations only** — not written. After cleanup, **re-audit** will populate `proposed` and `expectedGovernance` in JSON.

**Conservative expectation** if primary Kimpton sources are **official brand/company materials** (website, brand deck, IHG-provided PDFs):

| Field | Expected value |
|-------|----------------|
| Validation Status | **Company Published** (if company-provided sources dominate) or **Source-Informed** (if public/third-party only) |
| Usage Permission | Platform Display Allowed |
| Source Region | **Global Reference** or **Regional** (confirm from source Region in PI) |
| Confidence Level | **Medium** (Brand Basics column — not Operator alias) |
| External Display Status | Show Trust Label (after steward sign-off) |
| Last Reviewed Date | From latest reviewed fact/source date in re-audit |
| Company Validated | **false** — do not set without attestation |
| Company Validation Date | **null** |

**Expected Explorer chip (after publish apply):** e.g. **Company Published** or **Source-Informed** · Last Reviewed · Region · Confidence: Medium — confirm via re-audit `proposed.expectedGovernance.displayLabel` / `displaySubtitle`.

---

## 8. What Must Not Be Changed

- **Company Validated** (Brand Basics)
- **Company Validation Date**
- Scoring / OAS / BAS snapshot fields
- Bulk approval of all 48 facts without evidence review
- Profile governance apply before package is **eligible** in readiness audit
- Curio linkage/conflict work (out of scope for Kimpton)
- APIs, UI, or schema (this plan is documentation only)

---

## 9. Recommended Next Commands (After Manual Cleanup)

```bash
# 0. Stewardship dry-run / report
npm run steward-kimpton-pi-pilot

# 1. Confirm eligibility
npm run audit-partner-intelligence-publish-readiness

# 2. Preview Setup field diff (only when Kimpton appears in eligiblePackages)
npm run publish-partner-intelligence-profile-governance -- --entity-type brand --target-rec-id recCKuXCmGvxHPfb3 --dry-run

# 3. Apply only after founder approval of dry-run report
npm run publish-partner-intelligence-profile-governance -- --apply --entity-type brand --target-rec-id recCKuXCmGvxHPfb3

# 4. Verify Brand Explorer trust chip
# GET /api/brand-library/brand?brandId=recCKuXCmGvxHPfb3
```

**Success criteria:** `eligiblePackages` includes `brand:recCKuXCmGvxHPfb3`; publish dry-run shows field diff (not `missing_proposed_governance`); post-apply readiness shows **`no_op`** or eligible with expected chip.

---

## Related

- [partner-intelligence-profile-governance-runbook.md](./partner-intelligence-profile-governance-runbook.md)
- [partner-intelligence-stewardship-fix-plan.md](./partner-intelligence-stewardship-fix-plan.md) — Kimpton section
- [partner-intelligence-profile-governance-publish-plan.md](./partner-intelligence-profile-governance-publish-plan.md)
- Arbor pilot (completed): [partner-intelligence-profile-governance-runbook.md#arbor-lodging-pilot-result](./partner-intelligence-profile-governance-runbook.md)
