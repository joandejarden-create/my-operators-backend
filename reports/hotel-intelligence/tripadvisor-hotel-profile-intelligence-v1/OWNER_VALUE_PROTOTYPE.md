# Owner-value prototype (READ-ONLY)

**Subject (stand-in owner hotel):** Auramar Beach Resort  
**Tripadvisor ID:** 253383  
**Source:** cached `ta-decision-pool.json` (no new Actor runs)  
**Production writes:** 0  

> Illustrative comparison set from the same destination cluster (Albufeira).  
> Not an official Dealality owner comp set. Not investment advice.

## Hotel position

| Metric | Your hotel | Comp median |
| --- | ---: | ---: |
| Rating | 3.4 | 4.1 |
| Review volume | 4789 | 1136 |
| Competitive rank percentile (higher = stronger) | 66.7 | 68.95 |
| Rooms (TA candidate) | 287 | 152 |
| Hotel class | 3.0 | — |
| Price level (directional) | $$$$ | — |
| Price range (directional) | $108 - $488 | — |

**Ranking label:** #52 of 153 hotels in Albufeira  
**Formula:** `competitive_rank_percentile = 100 × (denom − position + 1) / denom`  
Example check: position 52, denom 153 → 66.7

## Guest reputation

Histogram shares (4–5 bubble / mid / low): 55.5% / 23.3% / 21.2%

### Category scores vs comps

| Category | Hotel | Comp median | Delta |
| --- | ---: | ---: | ---: |
| Location | 4.4 | 4.2 | 0.2 |
| Rooms | 3.3 | 3.9 | -0.6 |
| Value | 3.7 | 3.9 | -0.2 |
| Cleanliness | 3.7 | 4.15 | -0.45 |
| Service | 3.6 | 4.2 | -0.6 |
| Sleep Quality | 3.6 | 3.9000000000000004 | -0.3 |

## Competitive set (cached)

| Hotel | Rating | Reviews | Rank %ile | Class | Rooms | Amenities | Price |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| **Auramar Beach Resort** | 3.4 | 4789 | 66.7 | 3.0 | 287 | 58 | $$$$ |
| Vila Gale Atlantico | 4.2 | 1102 | 100 | 4.0 | 220 | 66 | $$$ |
| Hotel Vila Gale Cerro Alagoa | 4.2 | 2672 | 83 | 4.0 | 310 | 70 | $$$ |
| Belver Boa Vista Hotel & Spa | 3.3 | 1170 | 41.2 | 4.0 | 84 | 63 | $$ |
| Hotel Indigo Albufeira By IHG | 4 | 220 | 54.9 | 4.0 | 80 | 80 | $$$ |

## Product / amenity comparison

### Amenities missing vs majority of comps (≥70%)
- **fitness center** — missing; present in 4/4 comps (100%)
- **room service** — missing; present in 4/4 comps (100%)
- **spa** — missing; present in 4/4 comps (100%)
- **breakfast available** — missing; present in 4/4 comps (100%)
- **pool beach towels** — missing; present in 4/4 comps (100%)
- **steam room** — missing; present in 4/4 comps (100%)
- **wheelchair access** — missing; present in 3/4 comps (75%)
- **suites** — missing; present in 3/4 comps (75%)

### Relative differentiators (present here; uncommon in comps)
- **tennis court** — present here; only 0% of comps
- **refrigerator in room** — present here; only 0% of comps
- **free private parking nearby** — present here; only 0% of comps
- **gift shop** — present here; only 0% of comps
- **shops** — present here; only 0% of comps
- **swimup bar** — present here; only 0% of comps

## Key differences (supported)

- Reputation vs comps: rating 3.4 vs median 4.1; review volume 4789 vs 1136.
- Market standing: rank percentile 66.7 vs comps 68.95 (same destination cluster; still verify rankingString geography).
- Product: 14 majority-comp amenity gaps surfaced as diagnostics only.

## Potential questions for the owner

1. Is this the right **owner-selected** competitive set, or should neighborhood / brand / scale filters change?
2. Do the amenity gaps reflect intentional positioning or under-investment?
3. Does Tripadvisor ranking geography (`#52 of 153 hotels in Albufeira`) match how you define your market?
4. Should room count (287) be verified via official primary source before census use?

## Caveats

- Tripadvisor ranking geographies differ (city vs neighborhood); compare rankingString carefully.
- priceLevel / priceRange are directional OTA/stay quotes — not ADR or RevPAR.
- Amenity gaps are diagnostics, not CapEx recommendations.
- Room counts remain candidates until independently verified.
- Derived metrics are Dealality intelligence, not Tripadvisor-supplied scores.
