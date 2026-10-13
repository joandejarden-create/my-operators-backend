# Period Comparability

## Published baselines (pre-rerun)
| | Hilton | Renaissance |
|---|---|---|
| Period | adp_period_adp_hilton_times_square_20260927133046_20958b | adp_period_adp_renaissance_times_square_20260902235427_7d74ca |
| Date | 2026-09-27 | 2026-09-02 |
| Scenarios | 50 | 65 |
| Observations | 200 | 260 |
| FORMALLY COMPARABLE | **NO** | |

### Root cause of scenario count difference
1. **Published Hilton (50)** = NYC standard market pack only.
2. **Published Renaissance (65)** = standard + generic capability + **15 property-specific `prop_rts_*`** scenarios.
3. **Live Hilton universe now** = 65 (standard + generic_property_capability; **no** prop_rts).
4. **Live Renaissance universe now** = 80 (includes prop_rts exclusive set: 30).

**SCENARIO COUNT DIFFERENCE ROOT CAUSE:** actual scenario-set difference (Renaissance property-specific layer), **not** eligible-response filtering, rank filtering, or rendering confusion. Sept 27 Hilton report also omitted capability layer present in today's live builder (65 vs published 50) — monitoring-period / builder-version difference for Hilton alone.

## Live controlled rerun
- New Hilton period: adp_period_adp_hilton_times_square_20261005111601_190412
- New Renaissance control period: not created in this pack until Hilton QA passes.
- Shared comparable core: scenarios present in **both** live universes (exclude prop_rts for head-to-head).
- FORMALLY COMPARABLE (full universe): **NO**
- FORMALLY COMPARABLE (shared core excluding prop_rts): **YES** if both measured on same providers/window with entity_v2 aliases.

## Supersede note
Sept 27 Hilton remains historical baseline. Do not overwrite. Mark superseded for comparison-only after new certified period lands.
