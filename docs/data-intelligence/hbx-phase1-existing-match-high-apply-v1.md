# HBX Phase 1 — Existing Match High Apply v1

**Status:** `production_census_hbx_phase1_existing_match_high_apply_v1_partial_schema_remaining`  
**Objective:** `hbx-phase1-existing-match-high-apply-v1`  
**Generated:** 2026-08-09T15:14:11.423Z  
**Airtable writes:** **341**

## Target
- Base: Deal Capture Platform
- Table: Hotel Property Census (`tbl9aY5ijiuIzzWam`)
- Match class: **existing_match_high only**
- Mode: field-completion-only · no inserts

## Summary
- existing_match_high reviewed: **607** (unique Census IDs: **470**)
- records updated (pass 1): **341**
- HBX Hotel Codes written (dedicated field): **0** (schema_missing)
- address field proposals: **271**
- website proposals: **1**
- PHONEHOTEL proposals: **306**
- city proposals: **8**
- country / category / chain / accommodation / license / lastUpdate writes: **0** (already present or schema_missing)
- conflicts held: **124**

## Schema missing
- `HBX Hotel Code`
- `HBX Chain Code`
- `HBX Category Code`
- `HBX Category Name`
- `HBX Accommodation Type`
- `HBX License / Registration Number`
- `HBX Last Update`
- `HBX Source Type`
- `HBX Content Review Status`
- `Hotelbeds Code`
- `HBX External ID`
- `Postal Code`
- `Phone Confidence`
- `Phone Source URL`
- `Phone Source Type`
- `Phone Review Status`
- `Phone Reviewed Date`
- `Phone Notes`

## Completion before → after (unique matched Census set)
| Field | Before | After (live) |
| --- | ---: | ---: |
| Address | 265 | 467 |
| Phone | 226 | 469 |
| Official Property URL | 466 | 467 |
| HBX External ID field | 0 | 0 |
| Notes with hbx_linkage | — | 272 |

## Fields held (license policy)
- coordinates, images, descriptions, facilities
- Rooms / Keys (HBX rooms[] is room-type catalog only)

## Confirmations
- No inserts: **true**
- No Rooms / Keys from HBX: **true**
- No coordinates/images/descriptions/facilities: **true**
- PHONEHOTEL only: **true**
- existing_match_high only: **true**
- Hotel Property Census only; Brand Explorer / Brand Setup / VIC: **0**
- Pass 2 idempotent after record-id dedupe fix

## Recommended next
- Create `HBX Hotel Code` (and optional Chain/Category/Accommodation/Last Update) on Census
- Keep inserts and license-gated content off until policy review
