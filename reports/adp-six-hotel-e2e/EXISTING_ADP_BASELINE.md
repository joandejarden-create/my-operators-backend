# Existing ADP baseline freeze (read-only)

**Machine snapshot:** `EXISTING_ADP_BASELINE.json`  
**Freeze time:** preparation run on `248a3098`  
**Rule:** Missing local files or failed API ≠ empty corpus. Airtable/share were not required to prove published filesystem baseline for Ren/Waterstone.

## Renaissance New York Times Square — `adp_renaissance_times_square`

| Field | Value |
|-------|-------|
| Status | **EXISTING_COMPLETED_RESEARCH** (CERTIFIED) |
| HPC / census link | `recG66DQJKP2c0UNh` (census-links linked; profile `censusRecordId` set) |
| Latest period | `adp_period_adp_renaissance_times_square_20261005130233_4ec990` |
| Published at | 2026-10-05T13:39:12.263Z |
| Contract | `ADP_MEASUREMENT_CONTRACT_V1` · product `ai_demand_positioning_v1` |
| Demand capture | **40.0%** (26/65 scenarios in published payload; 4 providers) |
| Lost demand | 39 scenarios |
| Competitive observed | 20 |
| Actions in payload | 2 |
| Prior periods on disk | also 2026-08-20, 2026-09-02 evidence/report pairs |
| Monthly reviews | archive `2026-09` v6–v8 · `2026-10` v1 |
| PDF backfills | present under monthly-review/pdf/backfill for RTS |
| External distribution hold | none on latest manifest |
| Customer edits / actions | Preserve any hotel-entered action-plan status via AI Admin — do not overwrite with placeholders on republish |

**Interpretation:** Not a first-time blank slate. Next authorized run is a **refresh/remeasure**, comparable only if query scope/providers/methodology align; label methodology drift explicitly. Do not fabricate trends from single new period alone.

## Waterstone Resort & Marina — `adp_waterstone_boca_raton`

| Field | Value |
|-------|-------|
| Status | **EXISTING_COMPLETED_RESEARCH** (CERTIFIED) + **PUBLICATION GATE** |
| HPC / census link | `recgMYovrrZDJMqzX` |
| Latest period | `adp_period_adp_waterstone_boca_raton_20260902231159_77334c` |
| Published at | 2026-09-10T11:04:32.682Z |
| Demand capture | **73.1%** (57/78) |
| Lost demand | 21 |
| Competitive observed | 33 |
| Actions | 3 |
| Monthly reviews | `2026-09` v6–v8 |
| External distribution hold | **HOLD** — allow existing URLs; **no** new external communications / new client-ready editions until forensic revalidation clears |

Research may be authorized separately from customer re-issue. Do not claim share/PDF “new delivery verified” while HOLD is active without founder override.

## Four Marriott properties (WASFP / WASPY / CLTCC / TPAAP)

| Status | **NO_PRIOR_ADP** |
|--------|------------------|
| Published dir | absent |
| Profile fixture | absent |
| Census link | absent |
| Partial/failed runs | none observed under proposed slugs |
| Stale reports | n/a |

Identity blocker is HPC VERIFIED_ABSENT (GDI V4). Not LOOKUP_BLOCKED.

## Surfaces not fully probed this freeze

| Surface | Status |
|---------|--------|
| Live Airtable ADP Live row | Not required for filesystem baseline; confirm on activation publish |
| Authenticated AI Admin UI | Separate ops check |
| Live share token open | Requires issue script + secret — not run |
| GDI opportunity tables | Intentionally not written / not used as ADP corpus |
