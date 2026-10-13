# Changelog — Cvent Phase 4 Remediation

## 2026-10-03

### Code
- Added `scripts/cvent-phase4-remediation-v1.mjs` — work queue, P0–P2 processing, mixed classification, dry-run/`--apply`.

### Airtable (Hotel Property Census · production)
1. **`recabSgALHHvys0In`** (`ind_choice_mx_mx092`)
   - Rooms / Keys: 110 (unchanged value)
   - Rooms Confidence: Medium → **High**
   - Rooms Source URL: Cvent venue → **https://www.am.com.mx/guanajuato/2019/07/03/llega-nuevo-hotel-irapuato-485467.html**
   - Rooms Source Type: → **trusted_secondary_source**
   - Notes for Steward: appended `cvent_phase4_remediation` audit trail
   - Old Cvent claim preserved in steward note

2. **`rec8OtmFD9eqKORYs`** (`ind_choice_mx_mx226`)
   - Rooms / Keys: **41 retained** (not zeroed; conflict 41 vs directories 36)
   - Rooms Confidence: Medium → **Low**
   - Rooms Source Type: → **steward_review**
   - Rooms Source URL: Cvent URL retained for provenance history
   - Notes for Steward: `REMOVE_FROM_CURRENT_CANONICAL_USE` + needsSourceReview + usedInScoring=false

### Not changed
- No record deletes
- No mass overwrites
- No ADP/GDI score recomputation
- No share tokens
- No Bethesda
- No published ADP package rewrites

### Reports
All artifacts under `reports/data-intelligence/cvent-sourcing-policy-v1/phase-4-remediation/`.
