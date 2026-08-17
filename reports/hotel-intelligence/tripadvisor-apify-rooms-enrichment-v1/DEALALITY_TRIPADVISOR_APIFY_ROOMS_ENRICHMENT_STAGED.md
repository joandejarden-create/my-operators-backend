# Tripadvisor / Apify Room Enrichment — Staged Validation

`TRIPADVISOR_APIFY_ROOMS_ENRICHMENT_STAGED_COMPLETE`

**Mode:** READ ONLY — no Airtable / census writes  
**Module:** `tripadvisor-apify-rooms-enrichment-v1`  
**Actor:** [maxcopell/tripadvisor](https://apify.com/maxcopell/tripadvisor) via existing Apify MCP pool  
**Sample size:** 8 (4 known-room + 4 missing-room)

---

## Brief plan

1. Add Tripadvisor room enrichment library with safe URL resolution + strict match gates.  
2. Wire read-only provider into HI registry (`tripadvisor_apify`).  
3. Verification waterfall: official site → Hotelbeds → room-count research/SerpApi → extras.  
4. Stage local data model (never auto-write `Rooms / Keys`).  
5. Run small staged sample and stop for review.

## Risk check

| Risk | Mitigation |
| --- | --- |
| Destination free-text geo errors (Bogotá→Athens GA) | Banned query patterns; prefer Hotel_Review / Hotels-g / per-hotel Search?q= |
| Sister-brand collisions | Token mismatch + room-conflict gates |
| Auto-overwrite of trusted rooms | Authoritative path is compare-only |
| OTA/Tripadvisor as “proof” | TA excluded from independent confirmation set |
| Schema invention on Airtable | Local enrichment fields only; census map still uses `Rooms / Keys` |

## What changed

| Path | Role |
| --- | --- |
| `lib/hotel-intelligence/tripadvisor-rooms/*` | Match, query URLs, verify, enrich |
| `lib/hotel-intelligence/providers/tripadvisor-apify.js` | Provider (pool / MCP injection) |
| `lib/hotel-intelligence/map_hotel_intelligence_fields.js` | `MAP_PROVIDER_IDS.tripadvisor_apify` |
| `lib/hotel-intelligence/providers/registry.js` | Registry wiring |
| `scripts/tripadvisor-apify-rooms-enrichment-staged.mjs` | Staged runner |

## Staged results

| Metric | Count |
| --- | ---: |
| New room candidates (missing authoritative) | 4 |
| Primary-source verified | 0 |
| Multi-source verified | 0 |
| Single-source candidates | 4 |
| Conflicts | 2 |
| Unresolved | 0 |
| False-match rejections | 0 |
| Authoritative EXACT (compare only) | 2 |
| Authoritative NEAR (compare only) | 0 |
| Authoritative CONFLICT (compare only) | 2 |

### Cost

- Total Apify cost (attributed staged PPE @ SILVER): **$0.03**
- Cost per verified room count: **n/a** (0 primary/multi verified in this sample)
- Note: Actor results reused from benchmark pool; no new production census writes

### Row highlights

- **Comfort Inn & Suites Querétaro** — auth 70 vs TA 60 → `AUTHORITATIVE_COMPARE_CONFLICT` (no overwrite)
- **JW Marriott Hotel Bogota** — auth 239 vs TA 264 → `AUTHORITATIVE_COMPARE_CONFLICT`
- **Iberostar Waves Punta Cana** — 427 = 427 → `AUTHORITATIVE_COMPARE_EXACT`
- **Santarena Hotel** — 45 = 45 → `AUTHORITATIVE_COMPARE_EXACT`
- **Suryaa / Club Regina / HI Cartagena / HI Express SJ** — TA candidates 25 / 243 / 140 / 100 → `CANDIDATE_SINGLE_SOURCE` (official/SerpApi extract returned no agreeing independent explicit room phrases on this pass)

Verification status machine unit-checked with mocks: `VERIFIED_PRIMARY_SOURCE`, `VERIFIED_MULTI_SOURCE`, `CONFLICT_REVIEW_REQUIRED` all fire correctly when independent observations exist.

## Data model (local staged — provenance preserved)

```text
rooms_authoritative
rooms_candidate
rooms_source
rooms_source_url
rooms_verified_at
rooms_confidence
rooms_verification_status
+ tripadvisor { id, website, email, phone, hotelClass, amenities, webUrl }
```

Census authoritative field remains mapped as `MAP_CENSUS_FIELDS.roomCount` = **`Rooms / Keys`**.

## Change impact

**Medium** (new read path + provider registry entry). No write path enabled.

**Rollback:** remove registry entry / stop calling staged script; no Airtable migration performed.

## Manual QA checklist

- [ ] Confirm `ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES=0` during runs
- [ ] Spot-check CONFLICT rows are not written to census
- [ ] Confirm `assertNotBannedDestinationQuery("hotels in Bogotá")` fails
- [ ] Re-run: `node scripts/tripadvisor-apify-rooms-enrichment-staged.mjs --known=4 --missing=4`

## Regression risks

- Registry now lists `tripadvisor_apify` (disabled without pool/token)
- Sister-brand gate may over-reject some legitimate renames — monitor `FALSE_MATCH_REJECTED`
- Room-count research may return empty on JS-heavy brand pages → candidates stay single-source (expected)

## STOP

Staged validation complete. **No production writes.** Do not promote `rooms_candidate` to `Rooms / Keys` without independent verification.
