# GDI Wave 2 Discovery V3 — Failure Audit

**Marker:** `gdi_wave2_discovery_hygiene_v3_20260921`  
**Stack:** `4a2823fdfd67868d88fce694960260424fa5b728`  
**Source audit:** `data/group-demand-intelligence/evals/gdi-controlled-expansion-wave2-discovery-audit.json`

## Wave 2 engine TRUE → manual adjudication

| Hotel | Opportunity | Engine | Manual | Primary failure |
|---|---|---|---|---|
| JW SD | Expo Parks Dominicana 2026 | TRUE | FALSE | COMPETITOR_HOST_LOCKED |
| Caribe | AIJA Americas AWN Retreat | TRUE | FALSE | COMPETITOR_HOST_LOCKED |
| Caribe | ICR 2026 | TRUE | TRUE | — (overflow/housing) |
| Caribe | WEEF 2026 | TRUE | TRUE | — (overflow/housing) |
| Caribe | MATSUS-LATAM27 | TRUE | FUTURE/WATCH | DATE_UNCERTAIN (2027) |
| Westin | CAFA IV Conference | TRUE | FALSE | PAST_EVENT |
| Westin | SIBB 2026 | TRUE | TRUE | — (overflow/housing) |
| Westin | OGP Action Plan SPGG | TRUE | FALSE | NON_EVENT_CONTENT |

## Failure-class counts (invalid fixture set)

| Class | Count | Example |
|---|---:|---|
| COMPETITOR_HOST_LOCKED | 2 | Expo Parks host Marriott Piantini; AIJA Blue Apple fully placed |
| PAST_EVENT | 1 | CAFA IV Oct 2025 |
| NON_EVENT_CONTENT | 1 | Municipal OGP action plan |
| DATE_UNCERTAIN / FUTURE | 1 | MATSUS Feb 2027 |
| NO_OVERFLOW (host lock path) | 2 | Host-locked rows without overflow thesis |

## Root cause (systemic)

Wave 2 `autoManualActionable` promoted `PRIMARY_PURSUIT` → `TRUE_ACTIONABLE` when `venueSourcingStatus=UNKNOWN` and no open/RFP/overflow evidence. Venue-lock and event-identity gates were incomplete.

## Reusable rule mapping

| FAILURE | ROOT CAUSE | REUSABLE RULE | REGRESSION TEST |
|---|---|---|---|
| Auto-TRUE without sourcing | UNKNOWN treated as open PRIMARY | UNKNOWN ≠ OPEN; TRUE requires open sourcing **or** overflow/meeting-venue placement | `real_event_sourcing_unknown`, `sales_thesis_room_block_language_not_overflow` |
| Competitor host lock | Named host hotel ignored | Host-locked without overflow → WATCH/INVALID, never PRIMARY TRUE | `real_event_host_locked`, `host_locked_no_overflow` |
| Past-as-future | Past ISO date accepted as current | DATE_PAST → INVALID (or WATCH only with recurrence evidence) | `past_event`, `past_annual_next_cycle_confirmed` |
| Non-event plan | Governance document ingested as event | Semantic PLAN/PROGRAM/REPORT → INVALID without event markers | `strategic_plan_non_event`, `program_page_non_event` |
| Overflow without evidence | Thesis copy / engine VERIFIED_HOUSING label | Evidence-only overflow; sales thesis excluded; labels need backing evidence | `verified_housing_label_alone_not_strong_demand` |
| Valid overflow retained | Meeting venue + housing | Meeting venue rooms open + overflow evidence → OVERFLOW TRUE | `host_locked_overflow_evidence`, `valid_event_housing_evidence` |

## Fixtures

`data/group-demand-intelligence/evals/gdi-wave2-discovery-v3-invalid-fixtures.json`  
`data/group-demand-intelligence/evals/gdi-discovery-hygiene-v3-offline-fixtures.json`
