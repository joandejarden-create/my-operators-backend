# Phone pipeline

## Root cause
No phone on freeze physical; blanket blocked_rights previously.

## Fix
- `normalizePhone` / `buildPhoneStaging`
- Claim-level selection: official eligible; SerpApi-only blocked

## Coverage on V3 freeze cohort
Phone staging nonblank: **0/150**
