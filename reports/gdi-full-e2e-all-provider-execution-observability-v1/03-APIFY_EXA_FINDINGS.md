# Apify + Exa integration findings

## Apify

- Actors are registered and selectively `enabled` in `apify-actor-registry-v1.js`.
- Full E2E orchestration prefers Apify for sports / social / preferred lodging / professional planners / local catering.
- Live execution requires `authorizeApifyLive` and `GDI_APIFY_LIVE=1`; default is mock/fail-closed.
- Observability records APIFY_* lifecycle states; HTTP success alone is not RESULTS_INGESTED.
- Historical YOTEL/Radisson/Bethesda: no Actor job IDs → APIFY_SKIPPED with explicit reason.

## Exa

- PR #333 segment titles preserved.
- `exa-live-discovery-provider-bridge-v1` records DISCOVERY calls when run context is active.
- Historical campaigns: aggregate Exa evidence only; per-segment UNKNOWN until future instrumented runs.
