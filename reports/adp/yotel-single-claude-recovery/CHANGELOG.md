# Changelog — YOTEL Single Claude Recovery

## Actions
- Identified single Claude timeout: `gen_adp_yotel_geneva_lake_18` / `obs_4d8d697a2caf`
- Exported `callProvider` for same-period recovery import (wiring fix; no methodology change)
- Retried **only** that Claude call via `recoverOneObservation`
- Canonical path: **NEW_CORRECTED_PERIOD**
- Original period SHA256 unchanged: **YES**
- New period: adp_period_adp_yotel_geneva_lake_20261005160004_d03a62
- Correction record: adp_corr_yotel_geneva_lake_muvfrelq
- Successor published: YES (manifest latest = adp_period_adp_yotel_geneva_lake_20261005160004_d03a62)
- Note: initial script publish used wrong `buildPublishedSnapshotBundle` arity; corrected publish applied in follow-up without additional provider calls

## Non-changes
- No full YOTEL rerun
- No other provider calls
- No scenario/prompt/identity/parser/threshold/methodology changes
- Hilton / Renaissance / Bethesda / Cambridge / Caribe / NOW NOW / W Rome untouched
