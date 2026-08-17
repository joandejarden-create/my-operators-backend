# Operator Intelligence — Airtable Foundation + Wave 2 Founder Review

**Date:** 2026-08-04  
**Branch:** `app-shell-left-nav`  
**Stop point:** Founder review complete — My Deals remains **blocked / unwired**

---

## 1. Approved founder decisions

Recorded in `docs/architecture/decisions/operator-intelligence-airtable-wave-2-founder-approvals.md`:

- Group A existing-field population  
- Cenote geography normalization  
- Minimum claims / sources / comps / brand-relationship architecture  
- Publication methodology (deterministic classes)  
- Wave 2 research (Highgate, Atlantica, Driftwood, Santa Fe; Remington deferred)

**Not approved (confirmed untouched):** My Deals wiring, flag on, OAS / Brand Match v2 / intake changes, shortlist, pathway matrix, performance invent, outreach automation.

## 2. Airtable backup completed

- Path: `backups/operator-intelligence/2026-08-03T22-17-55-690Z/`  
- Manifest: `reports/operator-intelligence-airtable-backup-manifest.md`  
- Six calibration Master/Platform/Commercial (+ Case Studies) snapshots with checksums

## 3. Schema applied

| Operation | Result |
| --------- | ------ |
| `Operator Intelligence - Claims` table | **Created** |
| Case Studies `Why Comparable` | **Created** |
| Case Studies `Comparability Strength` | **Created** |
| Partner Intelligence - Source Library | Already present (reused) |
| Brand Relationships table | Reused (no duplicate) |

Details: `reports/operator-intelligence-schema-applied.md`

## 4. Records backfilled (apply)

8 Platform/Commercial field updates applied (see write plan):

- Arbor → Active Countries `[Mexico]` (US skipped — taxonomy)  
- GHL → countries + structures  
- Playa → countries + Full third-party management (Owner-Operated skipped — taxonomy)  
- Aimbridge → Mexico + Full third-party management  
- Cenote → Active Countries **normalized to `[Mexico]` only**  
- 9 Claims rows created  
- 14 Case Study comps created (deduped)

## 5. Records skipped

- HE Active Countries overwrite (already populated)  
- United States / Ecuador / Guatemala / Owner-Operated (unapproved taxonomy)  
- Hard-conflict / Internal Only claims not owner-facing  
- Remington Wave 2 inclusion  

Machine plan: `reports/operator-intelligence-approved-write-plan.json`

## 6. Conflicts encountered

- Calibration Cenote hard geo conflict → resolved by founder-approved normalize (not auto-published as broad list)  
- Wave 2: Argentina Active write rejected; Driftwood MX/DR Active write rejected (Strategic Interest only)

## 7. Cenote normalization result

**Active Countries = `[Mexico]` only.** Unsupported countries removed. Prior list retained in backup. Not asserted as Confirmed Absence for removed countries.

**Known limitation:** Overlay treats Mexico as Claimed Capability (empty Fit geography). Airtable Active Countries cannot express presence type — Airtable-only Ranking Ready can overstate vs overlay Conditional for Cenote on Mexico urban scenarios. Market Presence Type persistence recommended before pilot.

## 8. Calibration readiness after Airtable persistence

Representative urban new-build (Airtable + case-study hydrate): Arbor, HE, Playa, Aimbridge **Ranking Ready**; GHL Research Required; Cenote Airtable RR vs overlay Conditional (see §7).

Overlay project-specific Ranking Ready unchanged in spirit: Arbor, HE, Playa, Aimbridge, GHL (selected projects); Cenote Conditional.

## 9. Local-overlay versus Airtable consistency

- Geography/structures Group A writes applied successfully  
- Case Studies hydrate evidence for Airtable-backed path  
- Material consistency OK for 5/6; Cenote documented overstatement risk (§7)  
- Experience dimension parity still partly overlay-dependent (Commercial experience fields not fully backfilled this wave)  
- Rollback **not required**

## 10. Wave 2 cohort

**Selected:** Highgate · Atlantica · Driftwood · Grupo Hotelero Santa Fe  
**Deferred:** Remington → Wave 3  
Doc: `reports/operator-intelligence-wave-2-selection.md`

## 11–16. Sources, claims, publication

| Metric | Count |
| ------ | ----: |
| Sources reviewed (Wave 2) | 11 |
| Claims created (Wave 2) | 16 |
| Auto-Publish | 13 |
| Publish With Evidence Label | 1 |
| Internal Only | 1 |
| Other (e.g. Rejected never-infer handled) | 1 |

Exceptions: `reports/operator-intelligence-wave-2-exceptions.md`

## 17–19. Wave 2 readiness / scenarios / real deals

- **Highgate** Ranking Ready: urban new-build, luxury leisure resort, select-service conversion, turnaround, institutional reporting  
- **Santa Fe** Ranking Ready: urban new-build, conversion, lifestyle soft-brand, institutional  
- **Driftwood** Ranking Ready: turnaround  
- **Atlantica** Ranking Ready: none (Brazil geo vs Mexico-centric synthetic set)

Synthetic + real-deal reports under `reports/operator-intelligence-wave-2-*.md`

## 20. Real Deal B gap analysis

`reports/operator-intelligence-real-deal-b-gap-analysis.md`  
**Argentina** leisure/resort → **0 Ranking Ready** after Wave 2. Documented as specialized geography exception. Thresholds unchanged.

## 21. Active-universe readiness

`reports/operator-intelligence-active-universe-readiness.md` — calibration + Wave 2 overlays (10 operators).

## 22. Remaining highest-impact gaps

1. Argentina current operating presence  
2. Market Presence Type on Platform (Cenote / Strategic Interest)  
3. Brand approval verification (property-scoped)  
4. Commercial experience field persistence parity  
5. Project-specific fees / availability / team (never auto-publish)

## 23. Internal review routes

- `/internal/operator-intelligence.html` (claims / Wave 2 / universe)  
- `/internal/operator-fit-calibration.html`  
- `/internal/operator-fit-data-readiness.html`  

No owner-facing production route.

## 24–25. My Deals readiness gates

| Gate | Met? |
| ---- | ---- |
| ≥5 pipeline-relevant Ranking Ready for representative CALA projects | Partial (improved; still project-specific) |
| ≥2 real deals with ≥2 Ranking Ready | **No** (A=2 yes; B=0; C=5) |
| Deal B non-empty or documented exception | **Documented exception** |
| Airtable ↔ overlay material consistency | Partial (Cenote presence-type gap) |
| No completeness bias / evidence persists | Yes (controls held) |
| Founder QA of owner visualization | **No** |
| Shortlist architecture confirmed | **No** |
| Pilot flag rules approved | **No** |
| No unresolved high-impact conflicts | Cenote presence-type residual |

**My Deals remains blocked.** Recommendation: do not wire.

## 26. Recommended Wave 3

1. Remington Hospitality  
2. Argentina-capable regional operators (Deal B outreach)  
3. Persist Market Presence Type + Commercial experience fields  
4. Optional: brand-managed confirmation wave (Hilton/IHG/Marriott managed rows)

## 27. Exact founder decisions required before pilot wiring

1. Accept or remediate Cenote Active Countries vs Claimed Capability scoring behavior  
2. Approve Wave 2 Airtable backfill (or keep local-only)  
3. Confirm Deal B specialized exception language for any owner pilot  
4. Shortlist architecture decision (not ODR)  
5. Pilot feature-flag rules + owner visualization QA  
6. Explicit approval to enable `OPERATOR_FIT_ENGINE_V2` for pilot members only  

## Pre-existing OAS failures (out of scope)

2 failures in `test-operator-alignment-snapshot-page.mjs` — documented separately; not fixed.

## Confirmation of unchanged surfaces

Legacy OAS · Brand Match v2 · owner intake · `OPERATOR_FIT_ENGINE_V2` default **off** · My Deals **unwired**.
