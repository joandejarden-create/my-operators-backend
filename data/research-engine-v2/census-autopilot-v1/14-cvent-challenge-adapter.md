# Cvent Challenge Adapter (Quarantined)

## Role

**COVERAGE CHALLENGE SOURCE ONLY** — never production research evidence.

## Workflow

1. Load Cvent LATAM harvest URLs (retain harvest; do not delete).
2. Compare identity hints vs Verified Independent Census.
3. Emit:
   - `CVENT CANDIDATE NOT FOUND IN VERIFIED INDEPENDENT CENSUS` → **INDEPENDENT DISCOVERY CHALLENGE**
   - Identity overlap → bookkeeping only (no field copy)
4. Research using **non-Cvent** permitted sources (Lane A/B).
5. Track `cvent_candidate_id`, `independent_confirmation_status`.
6. Flags: `cvent_used_as_source = false`, `legacy_used_as_source = false`.

## Benchmark snapshot

- Mexico harvest: `C:\Dev\deal-capture-proxy\reports\cvent-venue-cache\country-results\harvest-cee8963bf9d6d715aa81.json`
- Hotel URLs total: **2948**
- Challenges emitted (capped): **400**
- Independent discovery challenges: **319**
- Identity overlap bookkeeping: **81**

Implementation: `lib/research-engine-v2/census-autopilot-v1/challenge-adapters.js`.
