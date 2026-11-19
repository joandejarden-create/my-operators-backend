# HPC Correction Plan (READ-ONLY) — Curio & Ascend

**Status:** `READONLY_PLAN_ONLY` — **no production Airtable writes performed.**  
**Generated:** 2026-11-19T21:17:29.800Z  
**Live HPC configured:** true  
**Source:** Platform Airtable → Hotel Property Census → Current Brand matchers → Radar Status Open/Pipeline

## Governance

- Preserve existing HPC record IDs.
- Only verified corrections may be written after founder approval of the exact rows below.
- Do not invent room counts, property types, or geography.
- Unknown / missing classifications remain `RESEARCH_REQUIRED` until credible public evidence is attached.
- Brand Explorer, Hotel Explorer, Radar, ADP, and GDI all consume these same HPC records.

## Live footprint snapshot (pre-correction)

| Brand | Open hotels | Known open rooms | Props with known rooms | Props missing rooms | Pipeline | Region sum | Country sum | Archetype sum |
|-------|-------------|------------------|------------------------|---------------------|----------|------------|-------------|---------------|
| Curio | 20 | 653 | 5 | 15 | 0 | 20 | 20 | 20 |
| Ascend | 13 | n/a (all missing) | 0 | 13 | 0 | 13 | 13 | 13 |

### Customer-facing coverage copy (current)

**Curio:** Dealality has identified 20 affiliated properties in its hotel database. Room counts are available for 5 of these properties. This represents observed coverage rather than the brand's complete worldwide portfolio.

**Ascend:** Dealality has identified 13 affiliated properties in its hotel database. Room counts are not yet available for these properties. This represents observed coverage rather than the brand's complete worldwide portfolio.

### Region mapping note

Saint Lucia is now mapped to **CALA** in `mapCountryToFootprintRegion` (country was already present on the HPC record). No Airtable Country write required for that hotel for region display. Unclassified region count for Curio is now **0**.

## Deficiency totals

| Metric | Count |
|--------|------:|
| Qualifying HPC records (both brands) | 33 |
| Incomplete records (≥1 deficiency) | 29 |
| Field-level deficiency rows | 34 |
| Verified correction candidates ready to write | **0** (research evidence not yet attached) |
| Unresolved fields | 34 |

## Curio — required remediation targets

| Target | Live count | Notes |
|--------|----------:|-------|
| Missing Rooms / Keys | 15 | Do not treat as zero; Known Rooms = 653 from 5 properties |
| Unknown / blank Property Type | 3 | Must use controlled taxonomy; no generic filler |
| Unclassified region (derived) | 0 | After mapper fix should be 0 |

## Ascend — required remediation targets

| Target | Live count | Notes |
|--------|----------:|-------|
| Missing Rooms / Keys | 13 | All 13 open properties lack keys |
| Unknown / blank Property Type | 3 | |
| Unclassified region (derived) | 0 | |

## Proposed write template (empty until evidence attached)

For each approved row after research:

| HPC record ID | Hotel name | Field | Existing | Verified replacement | Evidence URLs | Research date | Confidence | Verification status |
|---------------|------------|-------|----------|----------------------|---------------|---------------|------------|---------------------|
| *(none approved yet — see JSON register for RESEARCH_REQUIRED rows)* | | | | | | | | |

Full record-level register: `hpc-deficiency-register-curio-ascend.json`

## Cross-product impact

Updating Rooms / Keys, Property Type, or Country on these HPC IDs will refresh Brand Explorer footprint summaries, Radar Open/Pipeline inventory, Hotel Explorer property cards, ADP/GDI consumers that read the same Hotel Property Census table. No Brand Explorer fixture duplication required after authorized writes.

## Explicit non-claims

- No production Airtable data was changed in this cycle.
- No paid research was executed.
- Curio and Ascend remain **NOT APPROVED**.
- Do not claim all records resolved until every required field has credible supporting evidence.
