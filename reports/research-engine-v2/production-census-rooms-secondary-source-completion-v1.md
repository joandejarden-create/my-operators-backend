# Production Census — Rooms Secondary Source Completion v1

**Status:** `production_census_rooms_secondary_source_completion_v1_partial_source_remaining`
**Objective:** `rooms-count-completion-v1`
**Generated:** 2026-08-07T19:37:16.139Z
**Table:** Hotel Property Census (`tbl9aY5ijiuIzzWam`)
**Base:** Deal Capture Platform
**Airtable writes:** true
**Inserts:** 0

## Policy

- Secondary hotel data: ON
- Secondary Rooms: ON (founder approved)
- Secondary Phone: OFF (not approved)
- Phone classification: `phone_secondary_source_policy_not_approved` where secondary would be required

## Rooms coverage

| Metric | Before | After |
| --- | ---: | ---: |
| With Rooms | 116 | 174 |
| Coverage % | 9.48 | 14.22 |
| Official High Rooms | 102 | 102 |
| Official High % (of filled) | 87.93 | 58.62 |
| Secondary Source Rooms | 0 | 58 |
| Secondary Source % | 0 | 33.33 |
| Steward-Verified Rooms | 0 | 0 |
| Rooms Missing | 1108 | 1050 |
| Rooms Conflict (Hold) | 0 | 0 |

## This run

- Records updated: **58**
- Rooms values written: **58**
- Official HTML rooms written: **0**
- Secondary (RNT) rooms written: **58**
- Conflicts held: **0**
- Phone written: **0** (must stay 0)
- Phone blocked by policy: **872**
- Fields written: ["Rooms / Keys","Rooms Confidence","Rooms Source URL","Rooms Source Type","Rooms Reviewed Date","Rooms Notes","Last Reviewed Date","Enrichment Status","Human Review Required"]

## Sources used by type (after)

- `trusted_secondary_source`: 58
- `(blank)`: 111
- `official_property_page`: 5

## Schema gaps

- **Rooms Evidence Tier** — not present on Hotel Property Census; encoded in `Rooms Notes` as `evidence_tier=…`.

## Next backlog

- Mexico / DR / Panama / Costa Rica rooms still need official parent adapters or approved country open-data matches
- Add Airtable field Rooms Evidence Tier (currently encoded in Rooms Notes)
- Phone remains blocked — founder has not approved secondary phone sources
- Choice property pages 403 / central phones / rooms=25 defaults remain rejected
- Expand RNT year coverage + fuzzy city aliases for more Colombia matches
- Marriott DAM factsheet batch for Marriott blanks (official High path)

## Confirmations

- Hotel Property Census only: yes
- No inserts: yes
- Brand Setup / Brand Explorer untouched: yes
- No owner/operator/date fields: yes
- No room inference / sitewide defaults: yes
- No phone secondary writes: yes
- No central reservation phone writes: yes
- Every room write has source URL / type / confidence / reviewed date / evidence_tier in Notes: yes
- Conflicts stewarded (not overwritten): yes
