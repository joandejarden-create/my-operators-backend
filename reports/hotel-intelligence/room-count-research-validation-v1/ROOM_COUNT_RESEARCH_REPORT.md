# Room Count Research Engine

`ROOM_COUNT_RESEARCH_ENGINE_COMPLETE`

## Mission

Evidence-backed **TOTAL PROPERTY ROOM COUNT / KEYS** research for known Dealality hotels — without Hotelbeds LIVE and without building a crawler.

## Safety

```text
Airtable writes: 0
Census writes: 0
Brand Explorer writes: 0
Automatic merges: 0
Schema changes: 0
Secrets exposed: no
ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES=0
```

---

## Architecture

```text
Known hotel (dhl_ / name / city / country / brand / website)
        │
        ▼
hotel_room_count_research()
        │
        ├─ 1. Fetch official website (if present) — 1 page
        ├─ 2. Targeted SerpApi Google queries (capped 2–3) — snippets
        ├─ 3. Fetch ≤2–3 eligible follow-up URLs (official/brand/CVB/press)
        ├─ 4. Extract EXPLICIT multilingual phrases only
        ├─ 5. Score confidence + agreement / conflicts
        └─ 6. Stage evidence + quotes locally (no census write)
```

**Not a crawler.** Every run is hotel-scoped. OTA URLs are rejected as fetch targets.

### Modules

| Path | Role |
| --- | --- |
| `lib/hotel-intelligence/room-count-research/research.js` | Orchestrator |
| `lib/hotel-intelligence/room-count-research/extract.js` | EN/ES/PT/FR phrase extract + reuses production rooms extractor |
| `lib/hotel-intelligence/room-count-research/trust.js` | Source hierarchy |
| `lib/hotel-intelligence/room-count-research/confidence.js` | Transparent scoring + research_status |
| `lib/hotel-intelligence/room-count-research/queries.js` | Deterministic query builder |
| `lib/hotel-intelligence/room-count-research/fetch.js` | Single-page fetch helper |

Reuses: `production-census-rooms-keys-extractor.js` (false-positive guards, mixed-use holds, hotel/residences split).

---

## MCP Tool

`hotel_room_count_research`

**Inputs:** `hotel_id`, `hotel_name`, `city`, `country`, `brand`, `website`, `latitude`, `longitude`, optional caps

**Output:**

```text
candidate_room_count
confidence
supporting_sources
supporting_quotes
review_required
research_status
metrics
airtable_written: false
```

Statuses: `NOT_STARTED` · `SEARCHING` · `FOUND_SINGLE_SOURCE` · `FOUND_MULTI_SOURCE` · `CONFLICT` · `NO_EVIDENCE` · `MANUAL_REVIEW`

---

## Evidence Model

Every observation keeps:

```text
field: room_count
value: <integer>
source: room_count_research
source_category: Official Hotel | Official Brand | …
url
quote: short sentence only
language
method
observed_at
confidence
```

Conflicts are preserved — never auto-chosen.

---

## Confidence Model

```text
base(source_category)
+ agreement (≥2 / ≥3 sources)
+ explicit quote
+ official category bonus
± identity certainty
− weak / Hold extractor labels
```

Trust hierarchy (highest → lowest): Official Brand / Hotel / Owner → Operator → Tourism Authority → CVB / DMO → Press → Trusted Directory → News → Other.

Hotelbeds LIVE (when available later) is a **validator**, not the sole source — agreement should boost confidence.

---

## Controlled Validation

Frozen seed: `hotel-intelligence-cala-validation-v1`  
Sample: **100** missing-room hotels (72 HBX-linked priority + 28 fill)

| Metric | Count | % |
| --- | ---: | ---: |
| Resolved (any candidate) | 41 | 41% |
| High confidence (≥0.85) | 23 | 23% |
| Conflicts | 7 | 7% |
| No evidence | 59 | 59% |
| Manual review flagged | 84 | 84% |

### Performance

| Metric | Value |
| --- | ---: |
| Avg searches / hotel | 2.0 |
| Avg page fetches / hotel | 1.9 |
| Avg sources inspected | ~20 |
| Avg runtime / hotel | ~13.3 s |
| Total SerpApi searches | 200 |

---

## Scale Projection (extrapolation)

| Universe | High-confidence rooms (est.) | Manual review (est.) | Searches (est.) |
| --- | ---: | ---: | ---: |
| 1,000 hotels | ~230 | ~840 | ~2,000 |
| 5,765 missing rooms | ~1,326 | ~4,843 | ~11,530 |
| 10,000 hotels | ~2,300 | ~8,400 | ~20,000 |

Cost USD: **UNKNOWN** (SerpApi search units only).

Caveat: yield varies by brand presence and official-site availability; HBX-linked branded hotels may outperform independents.

---

## Recommended Production Workflow

1. Research only hotels missing Rooms/Keys with resolved `dhl_` identity  
2. Official website fetch first — skip SerpApi if High multi-source already  
3. Cap SerpApi Google searches at 2–3 per hotel; never crawl  
4. Stage quotes + evidence; human review for CONFLICT / confidence &lt; 0.85  
5. When Hotelbeds LIVE returns `roomsNumber`, use as agreement validator  
6. Auto-accept only under separate founder policy (not enabled)

---

## How to run

```bash
npm run test:hotel-intelligence-room-count-research
npm run hotel-intelligence:room-count-research-validation -- --limit 100
```

Artifacts: `reports/hotel-intelligence/room-count-research-validation-v1/`
