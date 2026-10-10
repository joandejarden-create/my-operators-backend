# Regression Control Set

- **adp_hilton_times_square** (CONTROL_PRIMARY, urban_full_service_branded) — Identity alias + scenario mismatch + competitor source lessons
- **adp_renaissance_times_square** (CONTROL_PEER, urban_full_service_branded) — Matched-control peer; prop_rts_* scenario tracking
- **adp_bethesda_marriott** (CONTROL_BASELINE, suburban_upper_upscale) — Stable certified baseline regression
- **adp_spice_island_beach_resort** (PORTABILITY, resort) — Resort portability
- **adp_yotel_geneva_lake** (PORTABILITY, select_service_european) — Select-service + European
- **adp_w_rome** (PORTABILITY, european_lifestyle) — European lifestyle brand
- **adp_jw_marriott_santo_domingo** (PORTABILITY, cala) — CALA portability
- **adp_now_now_noho** (PORTABILITY, independent_non_major_chain) — Independent / non-major-chain portability

Test types: IDENTITY, SCENARIO_COUNT, SCENARIO_IDS, PROVIDER_COMPLETENESS, RAW_METRIC_RECOMPUTE, SOURCE_ATTRIBUTION, OWNED_DOMAIN, DENOMINATOR_GRAIN, REPORT_PERIOD_ID, COMPARABILITY

Assert contracts and internal consistency — do not assert historical metric values must never change.
