# YOTEL Geneva Lake Forensic

## Root cause (before)
Profile used `displayName` without `name`, so it was **excluded from the global ADP property inventory** and failed portability as an incomplete identity contract (missing canonical name / aliases / owned-domain fields for certification).

## After deterministic fixes
- Normalized profile: `name`, `identityAliases`, `officialBrandDomain`, `ownedDomains`, `officialPropertyPageUrl`
- `customerDropdownVisible: false` preserved (prospect / unpublished baseline)
- Engine: **LEGACY_UNCERTIFIED** / bucket **LEGACY_ONLY**
- Period: `none — no official baseline period`

## Classification
**DETERMINISTIC_FIX_ONLY** for identity contract readiness; **FRESH_STANDARD_RUN** when pilot starts (no official period yet).

## Rerun required
**YES** for first official baseline (prospect onboarding) — not an emergency remediation of a broken live report.
