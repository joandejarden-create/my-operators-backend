# Changelog — Final Review Cleanup + YOTEL Baseline

## Fixes
- Cambridge: Independent/property-domain false positive removed
- Hotel Caribe: owned-source zero false positive (brand corporate ≠ owned rollup)
- NOW NOW: confirmed-real provider zeros → disclosure warnings
- Identity preflight outcome overwrite bug fixed
- YOTEL first official baseline path + profile completion

## Enforcement
- ADP_REQUIRE_CERTIFICATION_BEFORE_PUBLISH enabled in .env

## Non-changes
- Methodology / thresholds unchanged
- Legacy observation corpora not overwritten
- Metrics not forced
