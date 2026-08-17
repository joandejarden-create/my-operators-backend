# Brand Explorer 39 — Live ADR Scenario Scrub

Generated: 2026-07-26T10:09:46.415Z
Version: brand-explorer-39-live-adr-scenario-scrub-v1
Apply performed: **true**
Write performed: **true**

## Executive summary

Targeted scrub of ADR in flagged `valueOwners.scenario.*` Body rows so the protected 39 Active/Live public-full baseline is live-clean again.

| Metric | Value |
| --- | --- |
| Flagged rows | 13 |
| Planned Body patches | 13 |
| Applied patches | 13 |
| Blocked | 0 |
| Company Validated writes | false |
| Source Library writes | false |
| Registry writes | false |
| Brand Status writes | false |
| Release field writes | false |
| Image writes | false |
| Wave 13 work | false |

## Patches

| Brand | Slot | Record ID | ADR Phrase | Applied |
| --- | --- | --- | --- | --- |
| AC Hotels by Marriott | `valueOwners.scenario.1` | `recOuvKY5QCAkPGI1` | ADR | yes |
| Canopy by Hilton | `valueOwners.scenario.1` | `recIGLioUN2WUel2o` | ADR | yes |
| City Express by Marriott | `valueOwners.scenario.1` | `reclBLmmOSSWtcQS4` | ADR | yes |
| Hotel Indigo | `valueOwners.scenario.1` | `recY6oHFUjaKw4Ibs` | ADR | yes |
| Kimpton Hotels | `valueOwners.scenario.1` | `recIQPqSxhrJYXr1t` | ADR | yes |
| Moxy Hotels | `valueOwners.scenario.1` | `recfpAQEnidwzTPOZ` | ADR | yes |
| Dazzler by Wyndham | `valueOwners.scenario.2` | `recYdFgUuRq7x7arw` | ADR | yes |
| Even Hotels | `valueOwners.scenario.2` | `rec4N8Qx3qUjJgp7L` | ADR | yes |
| Comfort Inn & Suites | `valueOwners.scenario.3` | `rec0Xzu7aXf1S1thM` | ADR | yes |
| Courtyard by Marriott | `valueOwners.scenario.3` | `recK09kuVsNwhcicQ` | ADR | yes |
| Everhome Suites | `valueOwners.scenario.3` | `recjqUhmrrWoXmd0E` | ADR | yes |
| Motto by Hilton | `valueOwners.scenario.1` | `rec2ZSeIZac9XhFXu` | ADR | yes |
| Motto by Hilton | `valueOwners.scenario.2` | `recXFCKeosfFJ9fZf` | ADR | yes |

## Ready statement

**protected_39_live_clean_wave13_may_resume**

## Post-apply validation

- Fresh PVQL public-full: PASS (39/39 `lockPass`)
- Quality audit: 39/39 `approve_for_baseline_freeze` → `ready_to_freeze_39_active_public_full_baseline`
- 39 baseline regression: PASS
- Evidence / OS / mandatory gates: PASS
- Wave 13 preflight: PASS (`protected_39_live_clean_wave13_may_resume`)

## Guardrails

- Only flagged scenario Body rows
- No CV / Source Library / Registry / Brand Status / release / image writes
- No Wave 13 source packs or content generation
- No Radisson Collection

