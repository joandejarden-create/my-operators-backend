# Fourteen-segment functional certification

Generated: 2026-11-18T17:48:27.816Z

## Summary

| Status | Count |
| --- | ---: |
| VERIFIED_WORKING | 14 |
| PARTIALLY_VERIFIED | 0 |
| NOT_WORKING | 0 |

Disk executor artifacts: **ok**
Expected segments: **14** / actual **14**

Registry classifications remain `PARTIAL_EXECUTOR` by design (shared segment executor + specialized runners). Offline certification verifies discovery queries, Exa routing, mission upgrades, and contact prompt blocks — **not** fresh paid retrieval.

| Segment | Status | DiscoveryQs | Routing | Mission | ContactPrompt | ExecutorDisk |
| --- | --- | --- | --- | --- | --- | --- |
| associations_conferences_events | VERIFIED_WORKING | 10 | ASSOCIATION_EVENTS | true | 1710 | true |
| corporate_accounts_scaling | VERIFIED_WORKING | 4 | SCALING_COMPANIES | true | 1703 | true |
| corporate_meetings_incentives | VERIFIED_WORKING | 3 | ASSOCIATION_EVENTS | true | 1711 | true |
| government_public_sector | VERIFIED_WORKING | 8 | ASSOCIATION_EVENTS | true | 1689 | true |
| sports_entertainment | VERIFIED_WORKING | 2 | ASSOCIATION_EVENTS | true | 1662 | true |
| education_scientific | VERIFIED_WORKING | 4 | ASSOCIATION_EVENTS | true | 1674 | true |
| projects_infrastructure | VERIFIED_WORKING | 2 | SCALING_COMPANIES | true | 1671 | true |
| social_special_events | VERIFIED_WORKING | 2 | ASSOCIATION_EVENTS | true | 1655 | true |
| local_catering_day_meetings | VERIFIED_WORKING | 6 | ASSOCIATION_EVENTS | true | 1701 | true |
| corporate_celebrations_private_events | VERIFIED_WORKING | 3 | ASSOCIATION_EVENTS | true | 1682 | true |
| travel_trade_group_intermediaries | VERIFIED_WORKING | 5 | ASSOCIATION_EVENTS | true | 1664 | true |
| relocation_extended_stay | VERIFIED_WORKING | 3 | SCALING_COMPANIES | true | 1674 | true |
| preferred_lodging_partnerships | VERIFIED_WORKING | 3 | PREFERRED_LODGING | true | 1655 | true |
| professional_event_planners | VERIFIED_WORKING | 3 | ASSOCIATION_EVENTS | true | 1660 | true |

## Layer legend

- Discovery functioning = segment query builder returns ≥1 query.
- Deep research functioning = routing family + mission upgrade metadata present.
- Contact research functioning = universal contact prompt block generated.
- Commercial qualification = shared commercial spine (not segment-forked).
- End-to-end integration = executor artifact on disk + discovery queries.
