# Partner Intelligence Stewardship Fix Plan

**Date:** 2026-07-06  
**Status:** Planning + **generalized stewardship assistant** (`npm run steward-partner-intelligence`, dry-run default). Arbor + Kimpton pilots completed — see [partner-intelligence-profile-governance-runbook.md](./partner-intelligence-profile-governance-runbook.md).

> **Preferred path:** `npm run steward-partner-intelligence -- --entity-type brand|operator --target-rec-id rec... --dry-run`  
> Pilot scripts `steward-arbor-pi-pilot` / `steward-kimpton-pi-pilot` remain as reference examples.  
> **Curio (`receQkxgjlezsc1xg`):** package-integrity cleanup required first — see [curio-pi-package-integrity-cleanup-plan.md](./curio-pi-package-integrity-cleanup-plan.md). **24 facts quarantined (2026-07-06).** Re-extract: `npm run curio-clean-reextract -- --dry-run` — extraction context fix: [curio-extraction-context-audit.md](./curio-extraction-context-audit.md). **Do not apply stewardship** across all 15 sources.

> **Authority:** [partner-intelligence-profile-governance-publish-plan.md](./partner-intelligence-profile-governance-publish-plan.md)  
> **Audit source:** `reports/partner-intelligence-publish-readiness.{md,json}` (generated 2026-07-06T09:02:35Z)

---

## Purpose

This plan translates **publish readiness audit blockers** into concrete **human review and stewardship actions** in Partner Intelligence and Setup tables. It does not publish profile governance or modify records. Stewards use it to make **one package publish-ready** before building `publish-partner-intelligence-profile-governance.mjs`.

---

## Current Audit Summary

| Metric | Count |
|--------|-------|
| Source records reviewed | 28 |
| Fact records reviewed | 475 |
| Published field records reviewed | 1 |
| Helena intake records reviewed | 0 |
| **Packages found** | **5** |
| **Eligible** | **0** |
| **Blocked** | **5** |
| Missing-link issues (source/package rows) | 4 |
| Target protected | 0 |
| Needs manual review | 1 |

### Top blockers (audit)

| Blocker | Packages affected |
|---------|-------------------|
| `no_approved_facts` | 4 (Kimpton, Curio linked, 2 unlinked Curio sources) |
| `approved_for_explorer_use_no` | All linked brand/operator packages + unlinked sources |
| `missing_entity_link` | 2 unlinked Curio source packages |
| `source_status_not_ready:Found` / `Captured` | Arbor (`Found` on one source); unlinked Curio sources (`Captured`) |
| `conflict:mixed_company_and_public_source_origins` | Curio Collection by Hilton |

### Blocked packages (from audit)

| Package | Entity | Record ID |
|---------|--------|-----------|
| Arbor Lodging (CALA) | Operator | `recF5Z87OAqFgndoq` |
| Curio Collection by Hilton | Brand | `receQkxgjlezsc1xg` |
| Kimpton Hotels | Brand | `recCKuXCmGvxHPfb3` |
| Curio Collection Fact Sheet May 2026 | Brand (unlinked) | — |
| 2025 Mexico Curio FDD | Brand (unlinked) | — |

---

## Priority Pilot Recommendation

**Clean Arbor Lodging (CALA) first** (`operator:recF5Z87OAqFgndoq`).

| Reason | Detail |
|--------|--------|
| Operator profile | Aligns with Operator Explorer + existing governance pilot path (Viento Sur validated end-to-end separately). |
| Entity link | **Resolved** — `operator_link` to Operator Setup - Master. |
| Facts | Large fact set (~280 linked facts per audit); **1 fact already Approved/Edited** (`approvedFactCount: 1`). |
| Published overlay | **1** Published Explorer Fields row linked (`recyXqZU26sKrPVhZ`). |
| Blockers | **Narrower than Curio/Kimpton** — no `no_approved_facts`, no origin conflict, no missing link. Primary gap: **Approved for Explorer Use = No** on most sources + one **Found** status source. |
| Existing docs | `docs/partner-intelligence-pilot-arbor-lodging.md` documents PI pilot entry points for this operator. |

Curio is the **linkage + conflict cleanup** reference. Kimpton is **lower priority** (brand-only, 48 facts, zero approved, no published rows).

---

## Arbor Lodging (CALA) Fix Plan

### Package identity (audit)

| Field | Value |
|-------|-------|
| Package key | `operator:recF5Z87OAqFgndoq` |
| Entity name | Arbor Lodging (CALA) |
| Entity type | Operator |
| Link method | `operator_link` |
| Target Operator Setup - Master | **`recF5Z87OAqFgndoq`** |

### Source Library records (audit `sourceIds`)

| Source record ID | In block list? | Audit blocker |
|------------------|----------------|---------------|
| `rec83yK5rIkE7aTWx` | Yes | `approved_for_explorer_use_no` |
| `recM4HuV9r5Gz35P7` | **No** | — (verify live fields — **Needs Verification**) |
| `recg3p1cVgwVmZ9ot` | Yes | `approved_for_explorer_use_no` |
| `recgadiUD9cdGmaqY` | Yes | `approved_for_explorer_use_no` |
| `reckn9Hgz1StOc4t1` | Yes | `approved_for_explorer_use_no` |
| `recwa89aO43SS9uey` | Yes | `approved_for_explorer_use_no` |
| `recyY5faXntjMFkZp` | Yes | `source_status_not_ready:Found`, `approved_for_explorer_use_no` |

Source titles, origins, and quality levels are **not in the audit report** — confirm in Airtable before approving.

### Extracted Facts (audit)

- **~280 fact IDs** linked to this package (see `reports/partner-intelligence-publish-readiness.json` → `blockedPackages` → Arbor `factIds`).
- **`approvedFactCount: 1`** — at least one fact is Approved or Edited; audit did **not** flag `no_approved_facts` for Arbor.

**Stewardship:** Identify the approved fact(s); confirm evidence and `Approved Value`. For profile-governance publish readiness, approve a **small governance-relevant subset** (e.g. company snapshot, regional presence, source-backed claims) — not bulk-approve all pending facts.

### Published Explorer Fields (audit)

- **1 published row:** `recyXqZU26sKrPVhZ`
- Publish status / Stale? / field name — **Needs Verification** in Airtable (not in audit JSON).

### Current blockers (audit)

1. `approved_for_explorer_use_no` on **5** sources (IDs above).
2. `source_status_not_ready:Found` on **`recyY5faXntjMFkZp`**.
3. Per-source gate failures propagate to package block (any failing source blocks the package today).

**Not blocked (audit):** missing entity link, `no_approved_facts`, origin conflict, target protection.

### Exact stewardship actions

| # | Action | Airtable table / field | When |
|---|--------|------------------------|------|
| 1 | Open each blocked source; confirm evidence and suitability for Explorer | Source Library | Before any approval |
| 2 | Set **`Approved for Explorer Use`** = **Yes** | Source Library | Only after human review of that source |
| 3 | Advance **`Status`** from **Found** → **Approved** or **Extracted** (per workflow) on `recyY5faXntjMFkZp` | Source Library | After review; not while still “Found” only |
| 4 | Confirm **`Source Quality`** is **Medium** or **High** | Source Library | Required for eligibility |
| 5 | Confirm **`Status`** ≠ **Stale** | Source Library | Required |
| 6 | Verify **`recM4HuV9r5Gz35P7`** — why not in block list; align other sources to same ready state | Source Library | Needs Verification |
| 7 | Keep **`approvedFactCount ≥ 1`**; approve 2–5 **representative** facts with evidence | Extracted Facts → **`Human Review Status`** = Approved or Edited | After evidence review |
| 8 | Confirm published row `recyXqZU26sKrPVhZ` if used for overlay | Published Explorer Fields → **`Publish Status`**, **`Stale?`** | Needs Verification |
| 9 | **Do not** set Setup **`Company Validated`** or **`Validation Status` = Company Validated** from public/PI work alone | Operator Setup - Master | Always |
| 10 | After PI cleanup, set profile governance manually or via future publish script dry-run | Operator Setup - Master P1 fields | Only after re-audit shows eligible |

### Human approval required

- **`Approved for Explorer Use = Yes`** on each source.
- **`Human Review Status`** on each fact used for governance proposal.
- **`External Display Status` = Show Trust Label** on Operator Master — only after steward signs off (same bar as governance pilot).
- **Never** Company Validated without direct company confirmation.

### Expected governance values after cleanup (proposed — not written)

Conservative expectation if primary sources are **public/website** (per `docs/partner-intelligence-pilot-arbor-lodging.md` public URL seeds):

| Field | Expected value |
|-------|----------------|
| Validation Status | **Source-Informed** (or **Company Published** if steward confirms official operator materials only) |
| Usage Permission | Platform Display Allowed |
| Source Region | **CALA-Specific** (if sources scoped to CALA) |
| Confidence Level | **Medium** (via **`Data Confidence Level`** on Operator Master) |
| External Display Status | Show Trust Label (after human review) |
| Company Validated | **false** — do not set without attestation |

Expected Explorer chip (if Show Trust Label + gates pass): e.g. **Source-Informed** · Last Reviewed · Region: CALA-specific · Confidence: Medium — confirm via re-audit `proposed.expectedGovernance`.

### First publish-script dry-run candidate?

**Yes — target Arbor after re-audit shows `eligible: true`.** Narrowest blocker set and resolved operator link make it the preferred first **`publish-partner-intelligence-profile-governance.mjs`** dry-run target.

---

## Curio Collection by Hilton Fix Plan

> **Updated investigation (2026-07-06):** [curio-pi-package-integrity-cleanup-plan.md](./curio-pi-package-integrity-cleanup-plan.md) — **package integrity cleanup, not normal stewardship apply.** Mexico FDD facts are Kimpton/IHG-contaminated; bulk apply is unsafe. **Next:** [curio-clean-reextraction-plan.md](./curio-clean-reextraction-plan.md) — re-extract from US FDD + fact sheet only.

### Linked package (audit)

| Field | Value |
|-------|-------|
| Package key | `brand:receQkxgjlezsc1xg` |
| Brand name | Curio Collection by Hilton |
| Brand Basics record | **`receQkxgjlezsc1xg`** |
| Sources | **15** (IDs in audit JSON) |
| Facts | **Large set** (~150+ IDs in audit) |
| Published | **0** |
| Approved facts | **0** |

### Current blockers

- `no_approved_facts`
- `approved_for_explorer_use_no` on **all 15** linked sources
- `conflict:mixed_company_and_public_source_origins` (also in Conflicts section)

### Unlinked source packages (audit)

| Source ID | Hint (missing link) |
|-----------|---------------------|
| `recA7VOMWhmTtiklj` | Curio Collection Fact Sheet May 2026 — **Captured**, no Brand link |
| `recW4sJlpnCdZAZJ0` | 2025 Mexico Curio FDD — **Captured**, no Brand link |

### Stewardship actions

| # | Action |
|---|--------|
| 1 | **Link** `recA7VOMWhmTtiklj` and `recW4sJlpnCdZAZJ0` to **Brand Setup - Brand Basics** for Curio (`receQkxgjlezsc1xg`) — or merge into linked package after review. |
| 2 | **Resolve origin conflict:** classify each source as **company-published** vs **public/third-party**; do not mix both into one profile-governance publish without steward decision. Options: (a) publish governance from **company-published sources only**, (b) use **Source-Informed** for public-only subset, (c) split packages by origin. |
| 3 | Set **`Approved for Explorer Use = Yes`** only on reviewed sources. |
| 4 | Advance **Captured** sources through review workflow before profile publish. |
| 5 | Approve facts **after** evidence review — start with a small set; do not bulk-approve ~150 facts. |
| 6 | **Do not** publish profile governance until conflict cleared and re-audit passes. |

**Needs Verification:** Which Curio Brand Basics record is canonical if multiple Hilton Curio rows exist; source-level `Source Origin` values in Airtable.

---

## Kimpton Hotels Fix Plan

> **Detailed assessment (2026-07-06):** [kimpton-pi-governance-stewardship-plan.md](./kimpton-pi-governance-stewardship-plan.md) — planning/dry-run; manual stewardship recommended (no Kimpton script yet).

### Package (audit)

| Field | Value |
|-------|-------|
| Package key | `brand:recCKuXCmGvxHPfb3` |
| Brand name | Kimpton Hotels |
| Brand Basics | **`recCKuXCmGvxHPfb3`** |
| Sources | **4** (`rec0wes92ieaTT5UU`, `recEMuArkvbqpkrUU`, `recjFAVo3M2CL9rBF`, `recpXnHJUYhBIdCkt`) |
| Facts | **48** linked |
| Approved facts | **0** |
| Published | **0** |

### Current blockers

- `no_approved_facts`
- `approved_for_explorer_use_no` on **all 4** sources

### Priority

**Lower** than Arbor and Curio. Entity link is good; work is mostly **source approval + fact review** at scale for a brand with many extracted facts and no approved facts yet.

### To become eligible later

1. Review and set **`Approved for Explorer Use = Yes`** on suitable sources.
2. Approve or edit **at least one** Extracted Fact (audit minimum); realistically several for brand profile governance.
3. Confirm source quality ≥ Medium, status not Stale.
4. Re-run readiness audit.

---

## Stewardship Action Checklist

Use per package (Arbor first):

- [ ] **Confirm target entity link** (Brand Basics or Operator Master `rec…`)
- [ ] **Confirm source status** — not Found/Captured-only; not Stale/Rejected
- [ ] **Confirm Approved for Explorer Use** = Yes on sources used for publish
- [ ] **Confirm Source Quality** = Medium or High
- [ ] **Confirm Stale?** = false (Published rows if applicable)
- [ ] **Review key extracted facts** — evidence quotes, extraction type
- [ ] **Set Human Review Status** to Approved or Edited on selected facts
- [ ] **Confirm Published Explorer Fields** status if overlay row exists (Arbor: `recyXqZU26sKrPVhZ`)
- [ ] **Confirm no target profile protection** (Company Validated, Do Not Use, etc.)
- [ ] **Confirm proposed governance values** in re-audit report (not invented here)
- [ ] **Re-run readiness audit**

---

## Manual Airtable Update Guidance

- These are **manual stewardship actions** by a human reviewer — not bulk automation.
- **Do not bulk-approve facts** without reading **Evidence Text** and source context.
- **Do not mark Company Validated** unless the company directly confirmed claims.
- **Do not set External Display Status = Show Trust Label** on Setup until review is complete and re-audit proposes eligible package.
- Use **Company Published** for official company website/PDF/brochure **after review**; **Source-Informed** for credible third-party/public sources; **AI-Assisted** only for reviewed AI interpretation.
- Record steward decisions in **Evidence Notes** / **Internal Notes** on Setup when profile governance is updated (later).
- PI fields: Source Library **`Notes`**, Facts **`Reviewer Notes`**, **`Reviewed At`** / **`Reviewed By`** when available.

---

## Definition of Publish-Ready

A package is **publish-ready** (per audit logic) when:

| Criterion | Met? |
|-----------|------|
| Entity link clear (Brand or Operator `rec…`) | Required |
| Source reviewed and not Stale | Required |
| Approved for Explorer Use = Yes | Required |
| ≥1 relevant fact Human Review Status = Approved or Edited | Required |
| Source quality / confidence Medium or High | Required |
| No unresolved conflicts (e.g. mixed origins) | Required |
| Target profile not protected | Required |
| Proposed governance values clear in audit | Required |
| Human reviewer identified or review date present | Required |

---

## Recommended Re-Audit Command

```bash
npm run audit-partner-intelligence-publish-readiness
```

Reports: `reports/partner-intelligence-publish-readiness.{md,json}`

---

## Recommended Next Step

1. **Kimpton Hotels** (`brand:recCKuXCmGvxHPfb3`) — next package per [runbook](./partner-intelligence-profile-governance-runbook.md) and [Kimpton plan](./kimpton-pi-governance-stewardship-plan.md): manual source approval + selective fact review (entity link OK).
2. Follow [partner-intelligence-profile-governance-runbook.md](./partner-intelligence-profile-governance-runbook.md) for the standard command sequence.
3. Defer Curio conflict/link cleanup until Kimpton path is proven (unless Curio is explicitly prioritized).

---

## Related

- [curio-clean-reextraction-plan.md](./curio-clean-reextraction-plan.md) — narrow company-material re-extract path (next step)
- [kimpton-pi-governance-stewardship-plan.md](./kimpton-pi-governance-stewardship-plan.md)
- [partner-intelligence-profile-governance-runbook.md](./partner-intelligence-profile-governance-runbook.md)
- [partner-intelligence-profile-governance-publish-plan.md](./partner-intelligence-profile-governance-publish-plan.md)
- [partner-intelligence-pilot-arbor-lodging.md](../partner-intelligence-pilot-arbor-lodging.md)
- `scripts/audit-partner-intelligence-publish-readiness.mjs`
- `scripts/steward-arbor-partner-intelligence-pilot.mjs` — Arbor CALA stewardship pilot (dry-run default)
- `reports/partner-intelligence-publish-readiness.md`
