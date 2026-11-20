# Phase 4 — Bethesda vs KGPV discrepancy checklist

**Audit date:** 2026-11-19  
**Bethesda preview:** `http://127.0.0.1:8091/hotel-explorer-preview.html?hotelExplorer=recLuxvwwxID7U2B8&previewMode=proposed`  
**KGPV reference:** `http://127.0.0.1:8091/hotel-intelligence-golden-demo.html?autopen=1&hotelExplorer=recUNycnMwOVFX0hc`  
(Integration `:8080` was down; KGPV served from the same HE `:8091` stack for UX comparison.)

## Preserve

- [x] Founder Preview Live / Proposed toggle
- [x] GET-only preview (`X-Dealality-Write: forbidden`)
- [x] Proposed ≠ persisted labeling
- [x] Shared Hotel Explorer components (no parallel mockup)

## Overview

| Issue | Bethesda (current) | KGPV (target feel) | Action |
|-------|--------------------|--------------------|--------|
| Description quality | Headline dumps `signals:` + snake_case tokens; body is thin “407 keys” line | Professional commercial prose | Reject low-quality headlines; write structured narrative from verified facts |
| Property Facts | Missing Year Built / Last Renovated | Include when verified; else clear unverified | Add rows; map HPC/HI fields; no invented years |
| Amenities | Raw tokens (`bethesda_md_location`), location/brand noise, crowded carousel, duplicates | Clean human labels + consistent icons | Prefer verified amenities; taxonomy filter; grid layout |
| Meeting summary | Present in facts (18,719 / 27 / Grand Ballroom) | Keep; deepen in new tab | Reuse provenance; add rooms tab |

## Ownership / Relationships / Contacts

| Issue | Bethesda | KGPV | Action |
|-------|----------|------|--------|
| Executive summary | Proposed text present but mixed with flat graph dump | Structured ownership story + hierarchy | Reuse workspace ownership report patterns; soften technical strings |
| Control status | Raw `OPERATOR_PRESENT_OWNER_UNRESOLVED` visible | Business-language status | Translate enums |
| Graph | PropCo/EO proposed labels OK in Relationships; Ownership tab still flattens | Named org hierarchy | Prefer KGPV structure chart; distinguish proposed vs validated |
| Relationship table | Risk of IDs / generic “Organization” | Human org names | Resolve IDs; readable types |
| Contacts | Proposed VM works; role/channel clarity uneven | Clear org vs personal channels | Keep CI design; tighten proposed badges / unresolved personal email |

## Guest Rooms & Meeting Space

| Issue | Current | Required |
|-------|---------|----------|
| Dedicated tab | Missing | Insert after Overview |
| Room mix | Header rooms=407 only | Single / double / suites when verified (ref: 171 / 236 / 8 — verify inclusion rules) |
| Meeting inventory | Aggregate only | Room table + capacities; no invented rows |
| Schema | Meeting space in overview enrichment / HPC aggregates | Audit reuse; propose relational structure if needed; STOP before schema apply |

## Sources

| Issue | Action |
|-------|--------|
| New facts must cite evidence | Link Built/Renovated, amenities, room/meeting specs into existing Sources / HI Graph Evidence model |
| No duplicate URLs | Dedupe by hotel+URL |

## Non-goals / gates

- No production Airtable apply without authorization
- No unapproved schema mutations
- No Bethesda-only hardcoding inside reusable renderers (hotel-agnostic rules + data-driven overlays OK)
