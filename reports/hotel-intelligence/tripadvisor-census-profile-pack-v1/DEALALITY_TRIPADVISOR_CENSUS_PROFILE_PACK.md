# Tripadvisor Census + Profile Pack V1

**Status:** `TRIPADVISOR_CENSUS_PROFILE_PACK_V1_COMPLETE`  
**Date:** 2026-08-12  
**Production safety:** `MANIFEST_ONLY` (no Airtable census writes executed)

Tripadvisor is treated as a **CORE OBSERVATIONAL HOTEL INTELLIGENCE SOURCE** — not universally authoritative. Architecture:

```text
TRIPADVISOR / APIFY
        ↓
RAW OBSERVATIONS + PROVENANCE
        ↓
NORMALIZED HOTEL FACTS
        ↓
        ├── HOTEL CENSUS (durable identity/profile facts — null-fill only)
        └── HOTEL INTELLIGENCE (Profile Pack — rankings, reputation, amenities, …)
```

---

## 1. Current census schema audit

Live Hotel Property Census (Airtable) inspected via schema fetch:

| Item | Value |
|------|-------|
| Table | Hotel Property Census (`tbl9aY5ijiuIzzWam`) |
| Field count | **146** |
| Artifact | `data/.../census-schema-fields.json` · `census-schema-audit-summary.json` |

Relevant existing fields (no Tripadvisor-specific duplicates):

- Identity / geo: Property Name, Address, City, State / Region, Country, Latitude, Longitude (+ coordinate provenance)
- Contact: Phone (+ Phone Source Type / URL / Confidence), Official Property URL
- Product: Rooms / Keys (+ Rooms Confidence / Source), Hotel Class / Segment, Property Type, Amenities - Structured Tags / Source Text
- Brand: Current Brand (Brand Resolver — do not overwrite from Tripadvisor)

**Not on live schema:** Email, Postal Code, Tripadvisor ID column.

---

## 2. Tripadvisor → census field map

Canonical map: `lib/hotel-intelligence/tripadvisor-profile/census-map.js` → report `field-map.json`.

| Classification | Examples |
|----------------|----------|
| EXISTING_CENSUS_FIELD | website→Official Property URL, phone, address, lat/lng, rooms, hotel class, property type, amenities tags |
| NEW_CENSUS_FIELD_RECOMMENDED | Email + Phone-parity provenance suite (**not applied**) |
| HOTEL_INTELLIGENCE_ONLY | rating, reviews, ranking*, histogram, category scores, priceLevel/Range, photoCount, Traveler Choice |
| PROVENANCE_ONLY | Tripadvisor `id` → external-id registry; `webUrl` as source URL |
| DO_NOT_STORE | name overwrite, country overwrite, brand inference, description/photos/offers/AI summary as census facts |

---

## 3. Recommended new census columns

| Column | Decision |
|--------|----------|
| Email (+ Confidence / Source Type / Source URL) | **Recommended** — Phone-parity; **not added** this phase |
| Postal Code | **Not recommended** unless product asks — low priority vs geo/contact |
| tripadvisor_id | **Do not add** — use `external-ids` registry (`tripadvisor_apify`) |

**NEW_CENSUS_FIELDS_ADDED: 0**

---

## 4. Fields explicitly NOT added to census

- rankingPosition / rankingDenominator / guest ranking percentile  
- rating / numberOfReviews / ratingHistogram / categoryReviewScores / reviewTags  
- priceRange / priceLevel / Traveler Choice / photoCount  
- historical snapshots / derived percentiles  
- `tripadvisor_*` parallel columns for website/phone/address  

These live in the **Profile Pack** + local evidence store.

---

## 5. BEFORE_COMPLETENESS_MATRIX

Census-wide (n = **15,485**):

| Field | Present | Missing | Completeness % |
|-------|---------|---------|----------------|
| Address | 9,802 | 5,683 | 63.3 |
| City | 14,838 | 647 | 95.8 |
| State / Region | 6,308 | 9,177 | 40.7 |
| Country | 15,485 | 0 | 100 |
| Latitude | 836 | 14,649 | **5.4** |
| Longitude | 836 | 14,649 | **5.4** |
| Rooms / Keys | 191 | 15,294 | **1.2** |
| Hotel Class / Segment | 0 | 15,485 | **0** |
| Official Property URL | 8,043 | 7,442 | 51.9 |
| Phone | 12,394 | 3,091 | 80.0 |
| Current Brand | 2,100 | 13,385 | 13.6 |
| Property Type | 362 | 15,123 | **2.3** |

**Gap ranking (owner / HI / match / TA recovery / confidence / cost):**

1. **Latitude / Longitude** — highest owner + match value; TA recovery excellent when matched; Tier A  
2. **Address** — high owner + match; Tier A  
3. **Official Property URL** — high owner; domain validation required; Tier A  
4. **Phone** — high owner; Tier A with provenance companions  
5. **Hotel Class / Segment** — HI + product positioning; methodology differs; Tier B  
6. **Property Type / Amenities** — HI value; option-mapping risk; Tier B  
7. **Rooms / Keys** — high owner value but **candidate-only** from Tripadvisor  
8. **Email** — conditional after schema; not owner contact  
9. **Brand** — do not fill from Tripadvisor  

Artifact: `before-completeness-matrix.json`

---

## 6. Field-level write policy

Implementation: `lib/hotel-intelligence/tripadvisor-profile/write-policy.js`

### Tier A — SAFE GAP FILL (null-only)

Approved when identity match passes gates, field factual, confidence HIGH, census empty:

- Official Property URL (reject aggregator/social hosts)  
- Phone (+ Phone Source Type / URL / Confidence)  
- Address (+ Address Confidence / Source URL)  
- Latitude / Longitude (+ coordinate provenance companions; apply also needs `ENABLE_COORDINATE_WRITES`)

**Never overwrite non-null.**

### Tier B — CONDITIONAL

- Hotel Class / Segment (retain attribution)  
- Property Type / City / State / Amenities tags  
- Email (HI-only until schema exists)

### Tier C — CANDIDATE ONLY

- Rooms / Keys — store as candidate/evidence; **never** auto-authoritative

### Hard blocks

- Existing non-null census value  
- Country overwrite  
- Brand inference from TA  
- Insufficient match confidence  
- Invalid URL / aggregator host / generic email  

---

## 7. Provenance architecture

Prefer existing local stores — **no** dozens of `tripadvisor_x_source` census columns.

| Mechanism | Role |
|-----------|------|
| `createExternalIdRegistry()` | `hotel_id` ↔ `tripadvisor_apify` property id |
| `createEvidenceStore()` | field-level observations (rating, reviews, guest ranking percentile, hotel class, …) |
| Census companion fields | Phone/Address/Coordinate Source + Confidence when Tier A applied |
| Apify usage ledger | `HOTEL_PROFILE` use case + `usageTotalUsd` |

Minimum evidence shape retained: hotel_id, field, value, provider, provider_property_id, source_url, retrieved_at, match_confidence, field_confidence.

**NEW_PROVENANCE_FIELDS_OR_TABLES:** none on Airtable; local external-ids + evidence + ledger (`HOTEL_PROFILE` use case added).

---

## 8. Profile Pack schema

`buildTripadvisorProfilePack()` in `lib/hotel-intelligence/tripadvisor-profile/profile-pack.js`

Includes:

- Tripadvisor identity (provider id, source URL, match confidence)  
- Guest reputation: rating, reviews, histogram, category scores  
- Competitive standing: ranking position/denominator + **guest_ranking_percentile**  
  `100 × (denominator − position + 1) / denominator`  
- Product: hotel class (+ attribution), amenities, Traveler Choice, rooms **candidate**  
- Directional price level/range (not ADR/RevPAR)  
- Contact observations (for census proposal evaluation — not HI display as owner contact)

Layer flag: `not_census_core: true`.

---

## 9. 100-hotel pilot methodology

| Parameter | Value |
|-----------|-------|
| Universe | Hotel Property Census |
| Pilot size | 100 CALA-diverse hotels (pinned in `pilot-hotels.json`) |
| Selection | Gap-score priority + country diversification |
| Geography | Mexico, DR, Colombia, Costa Rica, Panama, Caribbean, Central & South America (4 each for major markets) |
| Mix | Branded + independent; urban/resort/boutique/large/small names present |
| TA retrieve | 3 Apify `maxcopell/tripadvisor` full-detail batches (hotel-like search URLs) |
| Match | Strict `matchTripadvisorHotel` gates (49 **high** confidence) |
| Writes | Manifest only |

Limitation: gap-first selection includes many STR/Airbnb-style census shells → 51 remain `no_candidate` even after Apify pulls. Future pilots should prefer hotel-like name filters for higher match yield.

---

## 10. Proposed-write manifest / executed writes

| Metric | Count |
|--------|-------|
| PROPOSED_WRITES | 248 |
| TIER_A_PROPOSED | 199 |
| TIER_B_PROPOSED | 49 |
| EXECUTED_WRITES | **0** |
| BLOCKED_WRITES | 38 |

Tier A proposed by field: Latitude 49, Longitude 49, Address 42, Phone 36, Official Property URL 23.  
Tier B: Hotel Class / Segment 49.

Artifacts: `proposed-write-manifest.json`

**Apply gate (all required for any write):**

1. `ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES=1`  
2. `ENABLE_CENSUS_FIELD_ENRICHMENT=1`  
3. `CONFIRM_TRIPADVISOR_TIER_A_WRITES=1`  
4. Coordinate fills also need `ENABLE_COORDINATE_WRITES=1`  

Apply executor intentionally **not** auto-enabled in this script version even when flags are set — founder must approve manifest then enable apply in a follow-on task.

---

## 11. Rejected / conflicting observations

| Reason | Count |
|--------|-------|
| existing_non_null_blocked | 27 |
| aggregator_or_social_host | 11 |

Room candidates (compare/candidate path): 48 hotels with rooms candidate observations (see `room-candidates.json`). No authoritative rooms overwrites proposed.

---

## 12. Before vs after completeness

**Actual after = before** (no writes).  
Simulated if all Tier A proposals applied on the 100-hotel pilot (census-wide %):

| Field | Before % | After if Tier A applied % | Tier A proposed |
|-------|----------|---------------------------|-----------------|
| Address | 63.3 | 63.6 | 42 |
| Latitude | 5.4 | 5.7 | 49 |
| Longitude | 5.4 | 5.7 | 49 |
| Official Property URL | 51.9 | 52.1 | 23 |
| Phone | 80.0 | 80.3 | 36 |

Pilot-scale recovery is strong **per matched hotel**; census-wide % moves slowly until scaled.

Artifact: `completeness-improvement.json`, `after-completeness-matrix-simulated.json`

---

## 13. Profile Pack coverage (matched = 49)

| Metric | Coverage |
|--------|----------|
| RATING | 93.9% |
| REVIEW_COUNT | 100% |
| RANKING | 93.9% |
| RANK_DENOMINATOR | 93.9% |
| GUEST_RANK_PERCENTILE | 93.9% |
| AMENITY | 91.8% |
| CATEGORY_SCORE | 93.9% |
| PRICE_POSITION | 83.7% |
| HOTEL_CLASS | 100% |
| CONTACT | 100% |

Full-detail Tripadvisor is **consistent** on matched CALA hotels for HI Profile Packs.

---

## 14. Apify actual cost

Source: `GET /v2/actor-runs/{id}` → `usageTotalUsd` (ledger `HOTEL_PROFILE`).

| Run | USD | Items |
|-----|-----|-------|
| xDOqWY7cg7VWnKuSk | 0.0610 | 21 |
| wgkGCBsbft19wlhp7 | 0.0639 | 22 |
| fxcjTRVOkC6VdD1JN | 0.0494 | 17 |
| **TOTAL** | **0.1743** | 60 unique pool hotels |

| Metric | USD |
|--------|-----|
| COST_PER_HOTEL (100 pilot) | 0.001743 |
| COST_PER_SUCCESSFULLY_MATCHED_HOTEL | 0.003557 |
| COST_PER_PROFILE_PACK | 0.003557 |
| COST_PER_CENSUS_FIELD_RECOVERED (if Tier A applied) | 0.000876 |
| COST_PER_FIELD_WRITTEN | n/a (0 writes) |

---

## 15. Field-by-field scale recommendation

**Overall:** `SCALE_SELECTED_FIELDS`

| Field | Decision |
|-------|----------|
| Address | SCALE |
| Latitude / Longitude | SCALE |
| Official Property URL | SCALE |
| Phone | SCALE |
| State / Region, City | CONDITIONAL |
| Hotel Class / Segment | CONDITIONAL |
| Property Type / Amenities | CONDITIONAL |
| Email | CONDITIONAL_AFTER_SCHEMA |
| Rooms / Keys | CANDIDATE_ONLY |
| Current Brand | DO_NOT_SCALE_FROM_TRIPADVISOR |
| Country | DO_NOT_WRITE |
| Rating / rank / price / awards | HOTEL_INTELLIGENCE_ONLY |

See `field-scale-decisions.json`.

---

## 16. Recommended next step

1. **Founder review** of `proposed-write-manifest.json` Tier A rows for the 49 matched hotels.  
2. After approval: enable write flags + implement/apply **Tier A null-fill executor** only (still block overwrites).  
3. Improve pilot/scale selection: hotel-like name filter + expand Apify identity resolution for remaining unmatched census hotels.  
4. Persist Profile Packs into Hotel Intelligence storage for Comp Set / Market Alerts later — **do not** build those UIs yet.  
5. Do **not** promote Tripadvisor rooms to authoritative; keep candidate pipeline.  
6. Optional: add Email census columns with Phone-parity provenance before any email writes.

---

## Data contract snapshot

| Item | Value |
|------|-------|
| Airtable table | Hotel Property Census |
| Field map | `TA_FIELD_MAP` / `MAP_CENSUS_FIELDS` |
| Required for match | name + country; geo preferred |
| Profile Pack module | `tripadvisor-profile/profile-pack.js` |
| Write policy | `tripadvisor-profile/write-policy.js` |
| Pilot script | `scripts/tripadvisor-census-profile-pack-pilot.mjs` |

## Change impact

**High** (census write path designed) — but **EXECUTED_WRITES = 0**.  
Rollback: leave flags off; discard manifest; no Airtable changes to revert.

## Regression checklist

- [ ] Confirm no Airtable census mutations from this phase  
- [ ] Re-run `node scripts/test-apify-usage-tracking.mjs`  
- [ ] Review Tier A manifest for sister-property / country edge cases before apply  
- [ ] PVQL / Brand Explorer untouched  
- [ ] Rooms Confidence/Source not silently elevated  

## Modules / pages affected

- `lib/hotel-intelligence/tripadvisor-profile/*`  
- `lib/hotel-intelligence/apify-usage/constants.js` (`HOTEL_PROFILE`)  
- `scripts/tripadvisor-census-profile-pack-pilot.mjs`  
- Reports under `reports/hotel-intelligence/tripadvisor-census-profile-pack-v1/`  
- Local data under `data/hotel-intelligence/tripadvisor-census-profile-pack-v1/`  
- Local external-ids + evidence stores (no Brand Explorer / Operator Explorer)
