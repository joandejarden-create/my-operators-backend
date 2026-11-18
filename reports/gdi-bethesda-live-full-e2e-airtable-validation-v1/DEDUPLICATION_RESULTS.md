# Deduplication results

## Offline suite

| Metric | Value |
| --- | ---: |
| Candidates before | 4 |
| Unique after composite-key dedupe | 3 |
| In-run duplicates resolved | 1 |

Duplicates:
```json
[
  {
    "hotelId": "recLuxvwwxID7U2B8",
    "opportunityId": "disc_guidepoint_seed",
    "compositeKey": "recLuxvwwxID7U2B8::disc_guidepoint_seed",
    "segment": "projects_infrastructure",
    "reason": "DUPLICATE_COMPOSITE_KEY_IN_RUN"
  }
]
```

## Live target

- Zero duplicate paid deep research on identical organization/activity identities
- Cross-segment discovery may surface the same org once for deep
