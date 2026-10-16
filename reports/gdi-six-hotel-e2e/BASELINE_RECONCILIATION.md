# Baseline reconciliation — Renaissance & Waterstone (by opportunity ID)

**Generated:** 2026-10-16T07:48:28.210Z  
**Script:** `scripts/gdi-six-hotel-baseline-reconciliation-v1.mjs`  
**Artifacts:** `BASELINE_RECONCILIATION.csv`, `BASELINE_RECONCILIATION.json`  
**Mode:** Read-only. No reclassification, publication repair, or demotion applied.

## Verdict

All unique opportunity IDs reconcile: **29 / 29** Renaissance and **29 / 29** Waterstone. Airtable access succeeded; local files match Airtable ID sets (intersection 29; onlyLocal/onlyAirtable empty). “Internalish” is retired in favor of mutually exclusive primary dispositions that sum to total.

## Summary counts (Airtable = production authoritative)

| Hotel | Total unique | HOTEL_FACING eligible | Legacy/ready surface visible | Reviewed package facing |
|-------|-------------:|----------------------:|-----------------------------:|------------------------:|
| Renaissance (`recG66DQJKP2c0UNh`) | 29 | **2** | **8** | 0 |
| Waterstone (`recgMYovrrZDJMqzX`) | 29 | **3** | **9** | 0 |

### Primary disposition rollup (mutually exclusive; accounts for every ID)

**Renaissance**

| Primary disposition | Count |
|---------------------|------:|
| CUSTOMER_FACING_HOTEL_ELIGIBLE | 2 |
| LEGACY_OR_READY_SURFACE_VISIBLE | 6 |
| ENRICHMENT_QUEUE | 19 |
| NOT_A_PROSPECT | 2 |
| **Sum** | **29** |

**Waterstone**

| Primary disposition | Count |
|---------------------|------:|
| CUSTOMER_FACING_HOTEL_ELIGIBLE | 3 |
| LEGACY_OR_READY_SURFACE_VISIBLE | 6 |
| ENRICHMENT_QUEUE | 15 |
| NOT_A_PROSPECT | 5 |
| **Sum** | **29** |

Note: legacy/ready **surface** count (8 / 9) = HOTEL_FACING + LEGACY_OR_READY_SURFACE_VISIBLE. Hotel-facing rows are also surface-visible under `isCustomerFacingOpportunity`.

## Surfaces checked

| Surface | Status |
|---------|--------|
| Canonical / Airtable list | OK — fresh `listOpportunitiesForHotel` |
| Local filesystem export | OK — `data/group-demand-intelligence/hotels/<id>/opportunities.json` |
| Authenticated customer list | NOT_PROBED — needs Memberstack admin/customer session (separate ops check) |
| Share list | NOT_PROBED — needs legitimate share capability token |
| Reviewed publication basis | 0 rows with `REVIEWED_PACKAGE_V1`; Ren/Waterstone not on reviewed-hotel allowlist |

## Why historical ~8 facing ≠ current 2 / 3

| Cause | Evidence |
|-------|----------|
| **Different route / filter** | Integrity freeze `facingFilterCount` = **8** (Ren) / **9** (Waterstone) counted `filterVisible` ≈ `isCustomerFacingOpportunity` (legacy + ready + surface). Current “facing” in six-hotel prep = `evaluateProspectPublicationEligibility` → **HOTEL_FACING** only (2 / 3). |
| **Legacy vs reviewed eligibility** | Integrity freeze already recorded `hotelFacingElig` **2** / **3** — same as today. No evidence of bulk demotion on the hotel-facing axis between that freeze and this snapshot. |
| **Stale / paraphrased report** | Earlier six-hotel summary used “internalish” and paraphrased Waterstone as eight facing; durable comparator is `reports/gdi-use-case-qualification-integrity/release/baseline-freeze.json` (Waterstone filterVisible **9**). |
| **Missing publication metadata** | Reviewed-package path not enabled for these hotel IDs → reviewedVisible stays 0. |
| **Actual record change** | Not required to explain 8→2: filter definition change explains it. Legacy surface count still **8** (Ren) / **9** (Waterstone). |
| **Unresolved** | Auth/share live counts not probed this task — do not invent. |

### Facing IDs (hotel-eligible vs legacy-only surface)

Renaissance HOTEL_FACING (2): see CSV where `primaryDisposition=CUSTOMER_FACING_HOTEL_ELIGIBLE`.  
Renaissance historical filterVisible eight IDs (from integrity freeze):  
`gdi_opp_76th_annual_meeting_3`, `gdi_opp_namt_fall_conference_festival_of_new_musicals_0`, `gdi_opp_15th_annual_boutique_hotel_investment_conference_11`, `gdi_opp_sior_fall_event_7`, `gdi_opp_nyssba_annual_convention_education_expo_15`, `gdi_opp_forum_on_education_abroad_annual_conference_14`, `gdi_opp_spring_road_conference_2`, `gdi_opp_arias_u_s_fall_conference_4`.

Six of those remain LEGACY_OR_READY_SURFACE_VISIBLE (suitability/contact gaps such as `travel_need_evidence`); two are HOTEL_FACING.

## Proposed change set

**NONE_APPLIED.** No forced count, bulk restore, or silent demotion. If founder later wants legacy-surface rows promoted to hotel-facing, prepare a **separate** evidence-backed change set after suitability/contact-route gaps close — not in this preparation task.

## Per-record fields

CSV columns: `hotelId`, `opportunityId`, `title`, `researchOrigin`, `publicationBasis`, visibility flags, `qualificationResult`, `visibilityRuleVersion`, `currentFacingStatus`, `exclusionReason`, `evidenceTimestamp`, plus disposition and surface flags.
