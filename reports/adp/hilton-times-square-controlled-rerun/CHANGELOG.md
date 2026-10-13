# Changelog — Hilton TS Controlled Rerun

## 2026-10-05
1. **Identity fix (proven bug):** `identityAliases` + `identityConfusableExclusions` on Hilton fixture; entity → `adp_hilton_times_square_entity_v2`.
2. **Renaissance parity aliases** added (no scenario definition changes).
3. **UI label fix:** “Top Source” → “Top Cited Source Across Monitored Responses”.
4. **Live Hilton period:** `adp_period_adp_hilton_times_square_20261005111601_190412` (65×4, certified). Force reparse for entity_v2 (parsePeriodObservations skips `parsed=true`).
5. **Sept27 preserved** historically; manifest latest = Oct5.
6. **Audit packs:** `reports/adp/hilton-times-square-controlled-rerun/`, `reports/gdi/hilton-times-square-current-process-rerun/`.
7. **ADP methodology changed?** NO. **Thresholds changed?** NO.
8. **Renaissance control live period:** not created (full-universe incomparable while `prop_rts_*` exclusive).
