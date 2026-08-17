# DEALALITY Tripadvisor Hotel Profile & Market Intelligence (v1)

**Marker:** `TRIPADVISOR_HOTEL_PROFILE_INTELLIGENCE_V1_COMPLETE`  
**Production writes:** 0  
**Scope:** Architecture + field inventory + read-only owner/comp prototype. No productionization.

---

## 1. Executive summary

Tripadvisor via Apify is already returning a **full hotel observation pack** on detailed Actor pulls — not just `numberOfRooms`. Room count should remain a **candidate attribute** inside a broader **Tripadvisor hotel profile & market intelligence** layer.

**North star:** help owners decide about their hotel, competitors, positioning, brand/operator fit, CapEx, conversion/reflag — not “display every Actor field.”

**Recommended role:** `CORE_HOTEL_AND_MARKET_MONITORING_SOURCE` (candidate / observational), **not** authoritative census truth for rooms or rates.

**Key evidence from cached outputs:**

| Pool | What we see |
| --- | --- |
| Decision pool (n≈31 hotels, full detail) | ~100% rating, reviews, ranking, amenities, category scores, price level/range, contacts |
| CALA benchmark pool (n≈260) | Often **sparse** (rooms/identity strong; ranking/amenities/rating frequently absent) — scrape depth/config dependent |

Same Actor; **profile richness depends on hitting full hotel pages**, not destination scraps. Room-count Search URLs already unlock the rich pack when results resolve to Hotel_Review pages.

**Prototype (cached only):** Auramar Beach Resort vs 4 Albufeira comps → rating/reviews/rank percentile/category deltas/amenity-gap diagnostics work without new Actor spend.

---

## 2. Actual Tripadvisor schema inventory

Inventoried from:

- `data/hotel-intelligence/tripadvisor-apify-benchmark-v1/ta-pool.json`
- `data/hotel-intelligence/giata-tripadvisor-room-decision-v1/ta-decision-pool.json`

**~80 field paths** observed (top-level + nested). Presence is **pool-dependent**.

### Core hotel fields (decision-pool presence ≈ full)

`id`, `type`, `category`, `name`, `webUrl`, `website`, `address`, `addressObj.*`, `locationString`, `latitude`, `longitude`, `phone`, `email`, `hotelClass`, `hotelClassAttribution`, `numberOfRooms`, `numberOfReviews`, `rating`, `ratingHistogram.*`, `rankingPosition`, `rankingDenominator`, `rankingString`, `rankingSource`, `rawRanking`, `amenities[]`, `categoryReviewScores[]`, `subcategories[]`, `priceLevel`, `priceRange`, `photoCount`, `offers[]`, `roomTips[]`, `ancestorLocations[]`, `neighborhoodLocations[]`, `nearestMetroStations[]`, `localName`, `localAddress`, `localLangCode`, `image`, `description`, `checkInDate`, `checkOutDate`, `isNearbyResult`

### Observed but rare / restaurant leakage

`cuisines`, `dishes`, `dietaryRestrictions`, `establishmentTypes`, `features`, `mealTypes`, `hours`, `openNowText`, `menuWebUrl`, `orderOnline`, `isClosed`, `isLongClosed`, `isClaimedIcon`, `isClaimedText`

### Named in docs / schema but **0% in our caches**

`aiReviewsSummary`, `travelerChoiceAward`, `ownersTopReasons`, `photos` (array usually empty; `photoCount`/`image` present), `reviewTags` (usually empty arrays)

---

## 3. Field-by-field classification

| Field | Classification | Why |
| --- | --- | --- |
| `id` | STORE_NORMALIZED | Stable Tripadvisor property key for match/provenance |
| `type` / `category` | STORE_NORMALIZED | HOTEL gate; reject restaurant leakage |
| `name` / `localName` | STORE_NORMALIZED | Identity / match |
| `webUrl` | STORE_NORMALIZED | Provenance + deep link |
| `website` | STORE_NORMALIZED | Census identity / official-site routing (verify host) |
| `email` / `phone` | STORE_NORMALIZED | Hotel contact enrichment — **not owner contacts** |
| `address` / `addressObj.*` / `locationString` | STORE_NORMALIZED | Identity / geo |
| `latitude` / `longitude` | STORE_NORMALIZED | Match + map |
| `numberOfRooms` | STORE_NORMALIZED | Candidate rooms only; verification waterfall unchanged |
| `hotelClass` | STORE_NORMALIZED | Product positioning (note Giata attribution) |
| `hotelClassAttribution` | STORE_RAW_ONLY | Provenance of class; independence evidence |
| `numberOfReviews` | STORE_NORMALIZED | Reputation volume |
| `rating` | STORE_NORMALIZED | Overall guest score |
| `ratingHistogram.*` | STORE_NORMALIZED | Consistency / polarization diagnostics |
| `categoryReviewScores[]` | STORE_NORMALIZED | Category strengths/weaknesses |
| `rankingPosition` / `rankingDenominator` / `rankingString` | STORE_NORMALIZED | Competitive standing inputs |
| `rankingSource` / `rawRanking` | STORE_RAW_ONLY | Debug / lineage |
| `amenities[]` | STORE_NORMALIZED | Product positioning / gaps |
| `subcategories[]` | STORE_NORMALIZED | Property type signals |
| `priceLevel` / `priceRange` | STORE_NORMALIZED | Directional price tier only |
| `offers[]` | TRANSIENT | Stay-dated OTA quotes; volatile |
| `checkInDate` / `checkOutDate` | TRANSIENT | Quote window, not hotel facts |
| `photoCount` | STORE_NORMALIZED | Weak richness signal; snapshot candidate |
| `image` / `photos` | REQUIRES_RIGHTS_REVIEW | CDN media; do not republish |
| `description` | REQUIRES_RIGHTS_REVIEW / TRANSIENT_RESEARCH_INPUT | Expressive copy; don’t republish wholesale |
| `roomTips[].text` | REQUIRES_RIGHTS_REVIEW / TRANSIENT_RESEARCH_INPUT | User prose |
| `aiReviewsSummary` | REQUIRES_RIGHTS_REVIEW / TRANSIENT | AI expressive summary (when present) |
| `reviewTags` | DERIVE_FROM | Theme signals if populated; else ignore |
| `travelerChoiceAward` | STORE_NORMALIZED | Award flag when present |
| `ancestorLocations` / `neighborhoodLocations` | STORE_RAW_ONLY | Ranking geography context |
| `nearestMetroStations` | STORE_RAW_ONLY / IGNORE if empty | Sparse |
| `isNearbyResult` | IGNORE | Match quality filter only |
| Restaurant-only fields | IGNORE | Wrong entity type |
| `localLangCode` | STORE_RAW_ONLY | Locale hint |

**Counts (unique top-level + nested paths classified):** see STATUS block. Approximate: STORE_NORMALIZED ~35 · STORE_RAW_ONLY ~12 · DERIVE_FROM ~8 · TRANSIENT ~6 · IGNORE ~10 · RIGHTS_REVIEW ~6 (some fields dual-tagged).

---

## 4. Data rights / storage considerations

**Favor storing:** factual scalars/structures (IDs, ratings, counts, ranks, class, amenity labels, contacts, coords, price tier strings).

**Do not design around republication of:** descriptions, room-tip prose, AI summaries, photos/CDN images, large review text.

Those may be **TRANSIENT_RESEARCH_INPUT** → Dealality keeps **non-reconstructive** analytical outputs (e.g., theme flags, polarity shares) with lineage — subject to **legal/licensing review** before production retention policy is finalized.

**Flag for rights review:** `description`, `roomTips`, `aiReviewsSummary`, `image`/`photos`, any future full-review payloads.

No unsupported claims: Dealality does **not** assert Tripadvisor ToS allows wholesale republication.

---

## 5. Owner-value mapping

| Observation | Owner care | Decision influence | Raw useful? | Comp compare? | Derive? | Historical? |
| --- | --- | --- | --- | --- | --- | --- |
| Rating + reviews | Reputation vs peers | Pricing, service focus, brand standards | Yes | **Critical** | vs median, trajectory | Yes |
| Rank + denom | “Where am I in the market?” | Marketing, distribution, repositioning | Partial | **Critical** | `competitive_rank_percentile` | Yes |
| Category scores | Where guests praise/criticize | CapEx / ops priorities | Yes | Yes | deltas vs comps | Yes |
| Amenities | Product gaps/differentiators | CapEx options (not prescriptions) | Yes | **Critical** | gap/differentiator sets | Quarterly |
| Hotel class | Scale/positioning | Brand/operator fit | Yes | Yes | class vs comps | Rare |
| Rooms | Size / competitive scale | Feasibility, brand box | Candidate | Yes | verify first | Slow |
| Price level/range | Relative tier | Positioning questions | Directional | Yes | tier vs comps | Monthly |
| Website/phone/email | Research routing | Identity enrichment | Yes | No | host verification | On change |
| Description/tips/photos | Research flavor | — | Research-only | No | themes only | No store |

Organized categories: **A Profile · B Competitive standing · C Guest reputation · D Product/amenity · E Price context · F Contact/identity**.

---

## 6. Hotel Profile intelligence

Normalized profile pack (per matched hotel):

- Tripadvisor ID + webUrl  
- Class, subcategory, rooms (candidate)  
- Contacts + coords + website  
- Amenity set  
- Awards when present  

Fits existing Hotel Intelligence evidence store + census **identity enrichment** lanes — not silent overwrites of authoritative Rooms / Keys.

---

## 7. Competitive Standing intelligence

### `competitive_rank_percentile` (higher = stronger)

```
competitive_rank_percentile =
  100 × (rankingDenominator − rankingPosition + 1) / rankingDenominator
```

| Rank | Result |
| --- | --- |
| #1 of 100 | 100 |
| #7 of 105 | 94.3 |
| #20 of 34 | 44.1 |

**Limitation:** `rankingString` geography must be stored and compared (city vs neighborhood). Example: Barceló Puerto Vallarta `#1 of 2 hotels in Mismaloya` ≠ city-wide peer set.

Also retain raw `rankingPosition`, `rankingDenominator`, `rankingString`.

---

## 8. Guest Reputation intelligence

Store: rating, review count, histogram, category scores.

Derive (transparent):

- `share_4_5` / `share_1_2` from histogram  
- category deltas vs owner-selected comps  
- review volume vs comp median  

Avoid opaque “reputation score” when diagnostics suffice.

---

## 9. Product / Amenity intelligence

Amenity set comparison vs owner comps / destination peers:

- **AMENITY_GAPS** — missing where ≥70% of comps have it (diagnostic)  
- **AMENITY_DIFFERENTIATORS** — present where ≤30% of comps have it  

**Do not** auto-recommend building a spa because comps have one.

---

## 10. Price / positioning intelligence

`priceLevel` ($, $$, …) and `priceRange` ($min–$max) are **directional stay quotes** tied to `checkInDate`/`checkOutDate` and `offers[]` vendors.

**Use:** relative tier vs comps.  
**Do not:** treat as ADR/RevPAR.  
**Offers:** TRANSIENT — do not snapshot verbatim as hotel truth.

---

## 11. Comp-set comparison opportunities

Meaningful dimensions (when fields present):

rating · review volume · competitive_rank_percentile · hotel class · rooms (candidate) · amenity coverage · category scores · price level · awards  

**Less meaningful without shared geography:** raw rank position alone; priceRange across different stay dates.

Owner-selected comps > auto market peers for primary UI; peers as secondary context.

---

## 12. Historical snapshot strategy (**design only — not activated**)

| Field | Cadence | Why |
| --- | --- | --- |
| rating, numberOfReviews, ranking*, categoryReviewScores, ratingHistogram | **MONTHLY** | Moves enough to matter; alertable |
| priceLevel / priceRange | MONTHLY (optional) | Directional only; expect noise |
| amenities, hotelClass, travelerChoiceAward | **QUARTERLY** | Slower product/award change |
| photoCount | QUARTERLY | Weak signal |
| description / tips / photos | Do not snapshot for product store | Rights + bulk |

Cost driver = Actor refresh per hotel, not field count (rich pack arrives together).

---

## 13. Market Alert opportunities

| Signal | Owner relevance | Threshold (proposed) | Confidence | Cadence |
| --- | --- | --- | --- | --- |
| RANKING_IMPROVEMENT / DECLINE | Competitive standing | ≥10 percentile pts MoM **same rankingString geography** | High if denom stable | Monthly |
| RATING_IMPROVEMENT / DECLINE | Reputation | ≥0.2 bubbles MoM and ≥50 reviews | High | Monthly |
| REVIEW_VELOCITY_CHANGE | Demand/attention proxy | MoM review delta z-score vs own 6-mo | Medium | Monthly |
| CATEGORY_SCORE_CHANGE | Ops focus | ≥0.3 on a category | Medium | Monthly |
| NEW_AMENITY_DETECTED / REMOVED | Product change | Set diff on quarterly snap | Medium | Quarterly |
| PRICE_POSITION_CHANGE | Tier shift | priceLevel step change | Low–Med | Monthly |
| TRAVELER_CHOICE_STATUS_CHANGE | Award | flag flip | High | Quarterly |
| HOTEL_CLASS_CHANGE | Positioning | class change | Med (attribution) | Quarterly |
| CONTACT_OR_WEBSITE_CHANGE | Identity | host/phone/email change | High | On refresh |
| PRODUCT_POSITIONING_CHANGE | Composite amenity+class | quarterly rollup | Medium | Quarterly |

Avoid noisy alerts on offers[] or single-day priceRange.

---

## 14. Derived Dealality metrics

| Metric | Inputs | Logic | Owner value | Confidence | Limits |
| --- | --- | --- | --- | --- | --- |
| `competitive_rank_percentile` | pos, denom | see §7 | Market standing | High if geography clear | Geo mismatch |
| `rating_vs_comp_set` | rating, comps | hotel − median | Relative reputation | High | Comp selection |
| `ranking_vs_comp_set` | percentiles | hotel − median | Relative standing | Med–High | Shared geo |
| `review_volume_vs_comp_set` | reviews | hotel − median | Share of voice | High | Age of property |
| `category_score_vs_comp_set` | category scores | per-category delta | Focus areas | High | Sparse cats |
| `amenity_gap_flags` | amenities | ≥70% comps | Product questions | Med | Label noise |
| `amenity_differentiators` | amenities | ≤30% comps | Positioning | Med | Label noise |
| `histogram_polarization` | histogram | share_1_2 / share_4_5 | Consistency | Med | Small-N |
| `reputation_trajectory` | monthly snaps | Δ rating / Δ reviews | Trend | Needs history | — |
| `ranking_trajectory` | monthly snaps | Δ percentile | Trend | Needs history | Geo |
| `hotel_profile_completeness` | field presence | % priority fields | Data quality | High | — |

**Do not build now:** opaque composite “Tripadvisor power score.”

Implementation sketch lives in `lib/hotel-intelligence/tripadvisor-profile/`.

---

## 15. Proposed hotel-page information architecture

Fit into **existing Dealality property / Hotel Intelligence research surfaces** (not a bolted Tripadvisor dashboard). Suggested sections:

1. **Hotel Overview** — identity, class, rooms (candidate), contacts, website  
2. **Market & Competitive Position** — rank string, percentile, vs comps  
3. **Guest Reputation** — rating, volume, histogram, categories  
4. **Product & Amenities** — amenity chips + gap diagnostics  
5. **Comp Set Comparison** — owner-selected table  
6. **Market Position Trend** — when snapshots exist  
7. **Sources / Provenance** — Tripadvisor ID, retrieved_at, match confidence  

No major UI implementation in this phase.

---

## 16. Field-level confidence model

| Field | Confidence | Notes |
| --- | --- | --- |
| Hotel identity (matched) | HIGH | Existing match gates |
| Coordinates | HIGH | When present |
| Rating / reviews | HIGH | Platform core metrics |
| Ranking | HIGH | With geography caveat |
| Category scores | HIGH | When present |
| Amenities labels | MEDIUM–HIGH | Taxonomy noise |
| Hotel class | MEDIUM–HIGH | Often Giata-attributed |
| Room count | MEDIUM / CANDIDATE | Verification required |
| Website | HIGH/MEDIUM | Host verification |
| Email / phone | MEDIUM | Hotel ops contacts |
| Price level/range | DIRECTIONAL | Not ADR |
| Description / tips / photos | RESEARCH_ONLY | Rights |

---

## 17. Provenance model

Align with existing evidence store patterns:

```
hotel_id
provider = tripadvisor_apify
provider_property_id = Tripadvisor id
field
raw_value
normalized_value
retrieved_at
source_url = webUrl
confidence
match_confidence
```

Derived example:

```
competitive_rank_percentile
  ← rankingPosition, rankingDenominator
  ← tripadvisor_apify @ retrieved_at
```

Never present derived metrics as Tripadvisor-native fields.

---

## 18. Cost / value analysis

From Apify usage ledger + recent decision runs (Starter PPE ≈ **$0.0029**/result):

| Metric | Estimate |
| --- | --- |
| COST_PER_RETURNED_RECORD | ~$0.0029–$0.005 (tier-dependent) |
| COST_PER_HOTEL_PROFILE | ≈ **$0.003–$0.01** when one Search resolves to a full hotel page (same call returns rooms + rating + rank + amenities…) |
| COST_PER_USEFUL_FIELD | Near-zero **incremental** — rich fields arrive with the hotel page |
| COST_PER_COMP_SET (1+4) | ≈ **$0.015–$0.05** if each hotel fetched once |
| ESTIMATED_MONTHLY_SNAPSHOT_COST | `N_hotels × ~$0.003–0.01` (plus compute) |
| ESTIMATED_QUARTERLY_SNAPSHOT_COST | same × quarterly subset / amenity-heavy refresh |

**Conclusion:** We are **already paying for the profile pack** whenever we fetch hotels for room candidates — underusing non-room fields is the waste, not missing Actors.

No unnecessary Actor volume run for this report. Prototype used cache only.

---

## 19. Small owner-value prototype

See `OWNER_VALUE_PROTOTYPE.md` + `data/.../owner-comp-prototype.json`.

- **Subject:** Auramar Beach Resort (Albufeira)  
- **Comps:** Vila Gale Atlantico, Vila Gale Cerro Alagoa, Belver Boa Vista, Hotel Indigo Albufeira  
- Demonstrates position / reputation / amenity-gap diagnostics from **real cached data**

---

## 20. BUILD_NOW / BUILD_LATER / DO_NOT_BUILD

### BUILD_NOW
- Persist normalized factual Tripadvisor observations into Hotel Intelligence evidence (local / staged) with provenance  
- Expose rooms as one candidate field among profile pack  
- `competitive_rank_percentile` + ranking geography retention  
- Amenity gap/differentiator diagnostics for owner-selected comps  
- Field-level confidence tags  
- Continue Apify usage ledger with use case `HOTEL_IDENTITY` / `MARKET_ALERT` when monitoring starts  

### BUILD_LATER
- Monthly snapshot store + ranking/rating alerts  
- Comp-set UI section on property research page  
- Review velocity / trajectories  
- Destination peer sets (auto) secondary to owner comps  
- Traveler Choice when field populates  

### DO_NOT_BUILD
- Wholesale description/review/photo republication  
- Tripadvisor-as-ADR/RevPAR  
- Opaque composite “power scores”  
- Treating TA rooms as authoritative census writes  
- Confusing hotel email/phone with owner contacts  
- Activating full-census monitoring before staged N-hotel pilot  

---

## 21. Recommended next implementation step

**Staged “Tripadvisor Profile Pack” writer (local evidence only):** on each matched Actor hotel, upsert normalized factual fields + provenance into the Hotel Intelligence evidence store; compute `competitive_rank_percentile`; keep rooms on the existing verification waterfall. Pilot on a small CALA owner+comp set with full-detail Actor pulls — **no Airtable production writes**.

---

## Tripadvisor recommended role

**`CORE_HOTEL_AND_MARKET_MONITORING_SOURCE`**

Why: the Actor already supplies identity, reputation, competitive rank, product, and directional price context at near-zero incremental cost per hotel page; historical monitoring would unlock owner alerts. It remains **observational** — census authority and CapEx decisions still require Dealality verification / other sources.
