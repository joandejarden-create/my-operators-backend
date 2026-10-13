# Gating Audit — Why 9/10 Bases Did Not Produce Campaigns

## Findings (shared orchestration — no Westin fork)

1. **Prior e2e discovery budget** only routed HIGH bases (PUBLISHED_EVENT, PARTICIPANT_EXHIBITOR, INTL_ORG, RECURRING_CORPORATE, PHARMA) — other bases were wired in taxonomy but **not given equal SERP budget**.
2. **SERP never auto-admits campaigns** (shared AC/RAD/YOTEL rule) — signals stay SIGNAL_ONLY / WATCH_ONLY until a validated Watch exists.
3. **Campaign admission from Valid Future Watch only** — after IAA/stale Watch reconciliation, Valid Future Watch = 0, so few/new campaigns from expansion SERP.
4. **No YOTEL-specific campaign fork** blocking Munich — shared `buildGdiDiscoveryQueries` + DE lexicon used.
5. **Language** — German NATIVE + English control both executed this expansion (20 DE / 20 EN).
6. **Wrong-destination bleed** — Hannover IAA rejected at SERP classify + Watch reconcile (shared OUT_OF_MARKET improvement).

## Artificial gating fixed

| Issue | Fix |
|-------|-----|
| Unequal Base SERP budget | This run: all 10 Bases × NATIVE + ENGLISH_CONTROL (2 queries/lane) |
| isOutOfMarket missed Hannover | Shared far-city list + destinationStatus check |
| Invalid CFS=WATCH labels | Cleared when Valid Future Watch fails |

No Ready/Watch threshold changes.
