# Tripadvisor Apify Actor Benchmark v1

`TRIPADVISOR_APIFY_BENCHMARK_V1_COMPLETE`

**Actor:** [maxcopell/tripadvisor](https://apify.com/maxcopell/tripadvisor)  
**Mode:** READ ONLY — no Airtable / census / schema writes  
**Date:** 2026-08-12  
**Integration:** existing Apify MCP (`plugin-apify-apify`) — no additional MCP server installed

---

## Verdict (final question)

**Tripadvisor via Apify can materially reduce — but not eliminate — dependence on GIATA MHG Facts for room-count enrichment.**

| Dimension | Tripadvisor (Apify) | GIATA MHG (proposed) | Edge |
| --- | --- | --- | --- |
| Coverage | ~81% property match; ~79% usable room recovery on missing-room sample | Structured facts product; CALA production coverage not proven in this benchmark | Tripadvisor for opportunistic fill; GIATA for systematic identity+facts if entitled |
| Accuracy | 47% EXACT / 7% NEAR on known-room sample; **22% CONFLICT** | MHG Facts intended as curated inventory facts | GIATA for authoritative keys when entitled |
| Freshness | Scraped live listings; some stale/wrong `numberOfRooms` (e.g. mega-resort = 1) | Commercial feed freshness depends on GIATA SLA | Mixed |
| Matching reliability | Search URL + name/geo works; free-text destination queries **unsafe** (Bogotá → Athens GA) | Stable GIATA IDs / Multicodes | GIATA for identity join |
| Scalability | PPE ~\$0.00125–\$0.005/result; census-scale thousands of \$ | €200/mo + €150 setup, 24-mo (€4,950) | Tripadvisor cost wins at scale |
| Provenance | Tripadvisor public listing (consumer UGC + hotel claim) | Commercial hotel-guide facts | GIATA stronger for diligence |
| Maintainability | Actor scraping + matchers + anti-false-match gates | Contract + API adapter | Tripadvisor needs ongoing match QA |
| Cost | Benchmark ~\$0.80 SILVER incl. waste; ~\$0.01 per recovered room in this run | €4,950 / 24 mo commitment | Tripadvisor for low-commitment pilot |

**Recommendation:** Treat Tripadvisor as a **low-cost secondary / gap-fill enrichment source** with mandatory human or multi-source validation on CONFLICT and sister-brand matches. Do **not** auto-write `Rooms / Keys` from Tripadvisor alone. Keep GIATA MHG (if purchased) as the higher-trust facts path — or postpone GIATA until Tripadvisor+Hotelbeds/official sources fail to cover the remaining sparse census.

---

## Phase 1 — Actor verification

### Input (confirmed)

| Field | Role |
| --- | --- |
| `query` | Location/keyword search (geo resolution **unreliable** for ambiguous names) |
| `startUrls` | Preferred: `Search?q=…`, `Hotels-g…`, `Hotel_Review-…` |
| `maxItemsPerQuery` | Cap per URL/query |
| `includeHotels` / `includeRestaurants` / `includeAttractions` | Type filters (hotels-only used) |
| `includeNearbyResults` | Off for benchmark |
| `maxPhotosPerPlace` | 0 (photos are add-on \$\$) |
| language / currency | `en` / `USD` |

### Output vs expected schema

| Expected | Present? | Notes |
| --- | --- | --- |
| `id` | Yes | Tripadvisor place id |
| `name` | Yes | |
| `type` / `category` | Yes | e.g. `HOTEL` / `hotel` — also restaurants/attractions if enabled |
| `address` | Yes | plus `addressObj.*` |
| `latitude` / `longitude` | Yes | |
| `hotelClass` | Yes | string like `"5.0"` |
| `numberOfRooms` | Yes | **primary enrichment target**; sometimes null or absurd (`1` on 1,800-key resorts) |
| `description` | Yes | often null on newer listings |
| `website` | Yes | high coverage when matched |
| `email` | Yes | often present |
| `phone` | Yes | high coverage |

**Extras useful to Dealality:** `webUrl`, `rating`, `numberOfReviews`, `rankingString`, `amenities`, `priceLevel`/`priceRange`, `travelerChoiceAward`, `aiReviewsSummary` (optional add-on cost/latency), ancestor locations.

**Pricing (PPE):** Result FREE \$0.005 → DIAMOND \$0.00125; Actor start \$0.00005/GB; photo & leads add-ons not used.

### Smoke checks (manual CALA)

| Property | Tripadvisor `numberOfRooms` |
| --- | --- |
| JW Marriott Hotel Santo Domingo | 150 |
| Casas del XVI | 21 |
| Hotel Bantu by Faranda Boutique | 28 |
| Nayara Gardens | 50 |

---

## Phase 2 — Known-room benchmark (n=100)

**Sample:** high-confidence Dealality `Rooms / Keys` from CALA census (`samples.json` → `phase2_known`).  
**Limitation:** universe of trusted room counts is sparse (~191); sample is **branded-heavy** and concentrated in CO / DO / CR / MX / PA.

**Method:** per-hotel Tripadvisor `Search?q={name} {city} {country}` batches (20 URLs/run), hotels-only, `maxItemsPerQuery=3`, offline name+geo+country match. Free-text “hotels in Bogotá” rejected after wrong-geo failure.

### Results

| Metric | Value |
| --- | --- |
| Property match rate | **81%** (81/100) |
| EXACT room match | **47%** of sample (58% of matches) |
| NEAR_MATCH (±5 keys or ≤5%) | **7%** |
| CONFLICT | **22%** |
| MISSING rooms on match | **5** matches |
| NO_MATCH | **19%** |
| False-match risk (sister/truncated name + room conflict) | **4%** |

Notable CONFLICT examples (do **not** assume Tripadvisor is correct):

- JW Marriott Bogotá: Dealality 239 vs TA 264  
- Iberostar Waves Dominicana: 426 vs 490  
- Mercure Bogotá BH Zona Financiera: Dealality **1024** vs TA **63** (likely census anomaly and/or listing ambiguity)  
- Grand Velas Boutique Los Cabos matched Grand Velas Los Cabos (sister collision)

---

## Phase 3 — Missing-room test (n=100)

**Sample:** CALA census hotels without trusted room count; geographically broader (Caribbean + South America).

| Metric | Value |
| --- | --- |
| Property match rate | **81%** |
| Usable `numberOfRooms` recovered | **79%** (79/100) |
| Matched but unusable/missing rooms | 2 |
| NO_MATCH | 19 |

**No values written to census.**

---

## Phase 4 — Economic assessment

| # | Metric | Result |
| --- | --- | ---: |
| 1 | Property match rate (overall) | **81%** |
| 2 | `numberOfRooms` coverage among known matches | **97.5%** field present; usable after quality filter slightly lower |
| 3 | Exact accuracy (known sample) | **47%** |
| 4 | Near-match rate | **7%** |
| 5 | Conflict rate | **22%** |
| 6 | False-match rate | **4%** |
| 7 | Room recovery (missing sample) | **79%** |
| 8 | Website coverage (matches) | **98.8%** |
| 9 | Email coverage (matches) | **78.4%** |
| 10 | Phone coverage (matches) | **96.9%** |
| 11 | Total Apify cost (benchmark) | **~\$0.80** SILVER PPE incl. ~33 wasted wrong-geo rows (range ~\$0.40–\$1.60 by plan) |
| 12 | Cost per recovered room count | **~\$0.01** in this run (\$0.80 / 79) |

**Scaled illustration (not a commitment):** filling ~5,000 missing-room hotels at ~\$0.0025/result × ~1.5 results/hotel ≈ **~\$19** Actor PPE before engineering/QA — vs GIATA **€4,950 / 24 months**. Cost alone does not decide; CONFLICT rate and provenance do.

### Other materially useful fields

Website, email, phone, `hotelClass`, ratings/reviews, amenities, official `webUrl` for provenance links.

---

## Operational findings / risks

1. **Never trust free-text destination `query` alone** — “Bogotá Colombia” returned Athens/Winder, Georgia USA.  
2. Prefer **hotel Search URLs** or **Hotels-g** / **Hotel_Review** start URLs.  
3. Filter non-hotels (`type`/`category`); reject US when Dealality country is CALA.  
4. Treat `numberOfRooms <= 2` with huge review volume / description room claims as **unusable**.  
5. Sister-brand collisions require stricter match rules (exact name tokens, distance, brand code).  
6. Apify account **memory concurrency** capped mid-run (16 GB) — serialize large batches.

---

## Artifacts

| Path | Contents |
| --- | --- |
| `data/hotel-intelligence/tripadvisor-apify-benchmark-v1/samples.json` | Phase 2/3 census samples |
| `data/hotel-intelligence/tripadvisor-apify-benchmark-v1/datasets/` | Raw Actor downloads |
| `data/hotel-intelligence/tripadvisor-apify-benchmark-v1/ta-pool.json` | Deduped TA pool |
| `data/hotel-intelligence/tripadvisor-apify-benchmark-v1/phase2-known-results.json` | Row-level known matches |
| `data/hotel-intelligence/tripadvisor-apify-benchmark-v1/phase3-missing-results.json` | Row-level missing matches |
| `data/hotel-intelligence/tripadvisor-apify-benchmark-v1/metrics.json` | Metrics snapshot |
| `data/hotel-intelligence/tripadvisor-apify-benchmark-v1/cost-estimate.json` | PPE cost model |
| `reports/hotel-intelligence/tripadvisor-apify-benchmark-v1/metrics.json` | Report copy of metrics |
| `scripts/tripadvisor-apify-benchmark-sample.mjs` | Sample builder |
| `scripts/tripadvisor-apify-benchmark-match.mjs` | Offline matcher |

---

## Data contract snapshot

- **Airtable tables:** Hotel Property Census (read-only sampling)  
- **Fields read:** identity + `Rooms / Keys` + geo/country (via existing census sample script / mapping)  
- **Writes:** none  
- **External:** Tripadvisor via Apify Actor output fields listed in Phase 1  
- **UI output:** N/A (benchmark report only)

## Change impact

**Low** (read-only evaluation + report/scripts). No production enrichment path enabled.

## Regression / QA checklist

- [ ] Confirm no Airtable mutations during benchmark window  
- [ ] Spot-check 10 EXACT / 10 CONFLICT / 10 RECOVERED rows in result JSON  
- [ ] Re-run matcher after pool updates: `node scripts/tripadvisor-apify-benchmark-match.mjs`  
- [ ] Do not enable census writes without a separate validation+write gate task  

## STOP

Benchmark complete. No production writes performed.
