# Record quality and duplication audit

**Method:** Live list of all rows in audited tables (capped 5000/table); identity duplicate scan; synthetic/provisional pattern scan; multi-hotel GDO vs FS.  
**Important:** Missing verification ≠ invalid. Pattern scan can over-count (e.g. word “provisional” inside opportunity payloads).

## Identity duplicates

| Table | Identity used | Duplicate identity keys | Interpretation |
|-------|---------------|------------------------:|----------------|
| Group Demand Opportunities | `opportunityId` | 0 | Healthy |
| Demand Generators / Programs / Signals | respective IDs | 0 | Healthy |
| Hotel Demand Generator Fit | Fit ID | 0 | Healthy |
| GDI Research Targets / Runs / Target Runs | respective IDs | 0 | Healthy |
| GDI Hotel Need Periods | Need Period ID | 0 | Healthy |
| GDI Need Period Opportunity Matches | **Need Period ID (wrong scan key)** | 20 “dupes” | **False primary-key assumption** — many matches share a Need Period ID by design. Upsert identity should be **Match Key**. |
| Decisions / Decision Events | decisionId / eventId | 0 | Healthy |

## Classification of notable populations

| Population | Classification | Evidence |
|------------|----------------|----------|
| Bethesda GDO (86) | Legitimate unverified / verified research mix (hotel research registry) | FS↔AT identity overlap complete |
| YOTEL GDO (108) vs FS (42) | Mixed + possible historical/superseded Airtable surplus | Count drift; needs founder decision on authority |
| Guadalajara FS (43) / GDO (0) | Research FS-only; Airtable not populated for opportunities | Live hotel filter |
| Waterstone GDI commercial profile | Synthetic fixture/test | Provenance `Synthetic Test`; SYNTHETIC_TEST accounts |
| Waterstone need periods (test/lifecycle) | Synthetic / provisional planning | Source `founder_test_scenario_v3`, Status Test Scenario / Withdrawn |
| Sheraton Guadalajara need periods | Provisional planning (explicitly not hotel-confirmed) | Source `PROVISIONAL_INTERNAL_PLANNING_SCENARIO_SHERATON_GDL_V1`, Status Test Scenario |
| Bethesda need periods with Source HOTEL_REPORTED / Active | Verified / hotel-reported production need windows | Sample `ndp_bethesda_marriott_2027_01` |
| Hotel Demand Generator Fit (~190 pattern hits) | Provisional planning / onboard seed | Fit Rationale `generic_seed …` |
| GDI Research Targets (~190 pattern hits) | Watchlist/monitoring seed | Reason Monitored `generic_seed …` |
| Demand Generators with onboard seed provenance | Watchlist / seed org graph | Research Reason `generic_onboard_seed:gdi_hotel_onboard_seed_v1` |
| Demand Generator Signals (23) | Historical / sparse signal set | Unchanged since prior audit |
| Hotel Seasonality & Need Periods (27) | Historical / legacy facility | Superseded for GDI authority |
| GDO “180 synthetic hits” | Mostly weak signal | Sample pattern hits dominated by nested “PROVISIONAL” language — **not** proof of synthetic GDOs |

## Missing required fields / orphans (patterns)

- Many older DG/Fit/Target rows have empty linked-record fields (Programs/Signals/GDO) — incomplete graph, not broken IDs.
- Research Target Runs (383) ≫ Research Runs (40) — expected many-to-one; orphan risk if Targets deleted (none deleted in audit).
- Opportunity Matches can exist without customer-facing need-date tile wiring (known from prior complete-model audit).

## Conflicting dates / geography

- Not exhaustively re-scored hotel-by-hotel in this pass.
- Multi-hotel count drift (YOTEL, Radisson, Guadalajara, Waterstone) is the primary integrity flag for opportunity identity consistency across FS vs Airtable.

## Contacts

- Contacts remain embedded on opportunities / payloads (prior audit). This pass did not scrape personal emails into reports.
- Unverified named contacts remain acceptable as research until hotel validation — do not auto-invalidate.
