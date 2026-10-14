# San Diego Parallel publication preview (dry-run)

**Hotel:** Wyndham San Diego Bayside (`recnPB3Q02NO8Ma1o`)  
**As-of:** 2026-10-14T21:10:28.328Z  
**Paid Parallel/Surfe calls this run:** 0

## Counts

| Metric | Value |
| --- | ---: |
| Fixture / replay candidates | 15 |
| After account+trip dedupe | 15 |
| Existing baseline matches | 0 |
| Proposed new (internal) | 15 |
| Customer-visible proposed | **0** |
| Confirmed open demand | **0** |
| Email or phone usable | 7 |
| Routing-only | 0 |
| Neither / quarantined | 8 |

## Baseline

- Readable: **true**
- Opportunity rows: 25
- Novelty policy: compare_against_baseline
- Auto-publication blocked: false

## Local recovery (Sol-Ark / SPARGO / Esri)

- **sol-ark**: local_channel_present_verify_variants_only (Surfe eligible=false, est. email credits=0)
- **spargo**: local_channel_present_verify_variants_only (Surfe eligible=false, est. email credits=0)
- **esri**: local_channel_present_verify_variants_only (Surfe eligible=false, est. email credits=0)

## Flags (this process)

```json
{
  "parallelEnabled": false,
  "killSwitch": false,
  "hotelAllowlist": [
    "recnPB3Q02NO8Ma1o"
  ],
  "surfeFallbackEnabled": false,
  "jevAdvisoryEnabled": false,
  "defaultOff": true,
  "envKeys": {
    "enabled": "GDI_PARALLEL_ENABLED",
    "hotels": "GDI_PARALLEL_HOTEL_IDS",
    "killSwitch": "GDI_PARALLEL_KILL_SWITCH",
    "surfe": "GDI_PARALLEL_SURFE_FALLBACK_ENABLED",
    "jev": "GDI_JEV_ADVISORY_ENABLED"
  }
}
```

## Next (explicit approval)

1. Review `san-diego-reconciliation.json` and card-level gaps.
2. Deploy code with `GDI_PARALLEL_ENABLED` still **OFF**.
3. Activate San Diego only after approval; do not publish customer cards without `--authorize-publish`.

Artifacts: `publication-preview.json`, `san-diego-reconciliation.json`, `discovery-replay/`, `targeted-contact-plan.json`.
