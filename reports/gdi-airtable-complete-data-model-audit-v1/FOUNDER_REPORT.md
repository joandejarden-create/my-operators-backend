# GDI Airtable Complete Data Model Audit V1

**Read-only.** No schema/record changes. Bethesda V2 **not** republished.  
**Base:** `appa2cE7FTRmIbB32` · **Opportunities:** `tblRuReslJMwsfRQj`  
**Hotel:** Bethesda Marriott (`recLuxvwwxID7U2B8`)  
**Approved fingerprint:** `df8da65a172727e627a71c3a31bead5f3f70ee95b03b7245f7a0ea34bbb33c66`

## Verdict

| Gate | Result |
|------|--------|
| All 67 published IDs in Airtable research registry | **PASS** (86 Bethesda rows; 19 research-only) |
| Airtable-only reconstruction == approved V2 | **FAIL** — **67/67** opportunities have ≥1 compared-field or contract diff |
| Required customer fields fully Airtable-backed | **FAIL** |
| Template certification | **NOT CERTIFIED** |

Having every ID in Airtable proves registry membership. It does **not** prove that cards, drawers, commercial actions, icons, or need-date tiles can be regenerated from Airtable alone.

Customer surfaces for published hotels still correctly use the **immutable publication package**. This audit deliberately rebuilt from Airtable **without** package fallback to expose gaps.

## What actually powers each surface today

| Surface | Source |
|---------|--------|
| Auth/share opportunity cards & drawers (published) | Immutable publication package DTO |
| Hotel need-date status banner | Live `GDI Hotel Need Periods` (outside fingerprint) |
| Feedback / validation | FS `feedback.json` + Decisions/Events merge-at-read |
| Research / `?live=1` | Airtable opportunities (+ FS enrich) |
| SVG geometry / CSS | Application code (store semantic inputs, not paths) |

## Bethesda Airtable-only vs V2 — sharpest gaps

Compared without loading the package as fallback:

| Field | Match | Missing in Airtable value | Mismatch |
|-------|------:|--------------------------:|---------:|
| `id` / org / priority / whyNow / contact name+email | 67 | 0 | 0 |
| `salesActionKey` / `salesActionPillLabel` | 0 | **67** | 0 |
| `datePillDisplayLabel` | 0 | **67** | 0 |
| `displayTitle` | 33 | 33 | 1 |
| `whatIsHappening` | 27 | 35 | 5 |
| `needDateMatchV1` on opportunity DTO | 28 | 39 | 0 |
| `sharedTenSectionDrawer` | 60 | 7 | 0 |
| `opportunityType` | 47 | 0 | 20 |

**P0:** commercial-action badges and date pills are frozen in the publication DTO but absent from Airtable-backed reconstruction inputs.

## Schema inventory (GDI-relevant)

| Table | Total | Bethesda |
|-------|------:|---------:|
| Group Demand Opportunities | 962 | **86** |
| GDI Research Targets | 480 | 53 |
| GDI Research Runs | 40 | 5 |
| GDI Research Target Runs | 383 | 168 |
| Demand Generators / Programs | 95 / 94 | (global) |
| Hotel Demand Generator Fit | 276 | 20 |
| Demand Generator Signals | 23 | 23 |
| Private Event Venues / Signals / Hotel Venue Fit | 38 / 26 / 44 | Fit 11 |
| GDI Hotel Need Periods | 13 | 9 |
| GDI Need Period Change History | 33 | 19 |
| GDI Need Period Opportunity Matches | 119 | **92** match rows linked to published opp IDs |
| GDI Hotel Commercial Profiles | 2 | 1 |
| Decisions / Decision Events | 93 / 160 | 32 / 106 |
| Hotel Intelligence Evidence | 722 | (not hotelId-filtered in meta) |

Legacy facility tables (`Hotel Seasonality & Need Periods`, facility `Hotel Commercial Profiles`) must **not** override GDI need-date authority.

## `opportunityPayloadJson`

- Acts as the fidelity store for nested presentation (contacts, icons, CIB, sources).
- Typed columns cover identity, priority, dates, whyNow, recommendedAction, contact name/email/role, sourceSummary.
- Write compaction drops bulky research history — expected — but must **not** drop customer-required presentation keys (`salesAction*`, date pills, displayTitle, subtitle).
- Do not explode every JSON key into columns; add typed columns only for filter/search/workflow fields (see recommendation).

## Contacts

Embedded on opportunities today is adequate for registry identity. Gaps: professional title vs role overload; activity role / eligibility / source URL mostly JSON-only. Prefer typed subtitle/title columns before a full Contacts table. Surfe stays on-demand.

## Commercial recommendations

Facts (priority, whyNow, thesis) are largely Airtable-typed. **Derived commercial-action pills are publication/payload-bound** and missing from Airtable reconstruction — store keys + rule version + approved output for deterministic regen; never re-resolve independently in the customer renderer.

## Need-Date

Authority: **GDI Hotel Need Periods** + **GDI Need Period Opportunity Matches**. Hotel status is live at serve. Per-opportunity `needDateMatchV1` often absent on Airtable DTO even when match rows exist (92 rows for published IDs) — join/presentation wiring gap.

## Architecture target

**Airtable canonical research/commercial data → versioned deterministic presentation → immutable founder-approved publication (+ feedback).**

Do **not** replace publication with live Airtable customer reads.

## Certification

`GDI_FULL_E2E_HOTEL_TEMPLATE_V1` must fail while:

1. Required matrix fields are `LOCAL_ONLY_DEPENDENCY` / `MISSING_SOURCE`, or  
2. Airtable reconstruction parity vs approved package fails (unless waived).

ID coverage alone is insufficient.

## Artifacts

See `01`–`15` + `GDI_FIELD_TO_AIRTABLE_MATRIX.csv` in this directory.
