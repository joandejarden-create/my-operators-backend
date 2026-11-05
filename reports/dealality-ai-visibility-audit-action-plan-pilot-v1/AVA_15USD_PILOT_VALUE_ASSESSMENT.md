# AVA $15 Paid Pilot — Value Assessment

**Contract:** `AVA_15USD_PILOT_VALUE_ASSESSMENT_V1`  
**Generated:** 2026-11-05  
**Ledger:** `paid-pilot-15usd/PILOT_EXECUTION_LEDGER.json`  
**Scrapes:** disabled · **Ahrefs / Parallel paid:** not used

## Actual spend

| Bucket | Actual | Cap | Status |
|---|---:|---:|---|
| DataForSEO | **$0.376704** | $10.00 | Under |
| Context.dev | **23 credits** (~$0.069 worst-case overage) | $5.00 | Under |
| Combined (est.) | **~$0.446** | $15.00 | Under |
| First aborted run (path bug after DFS-01 charge) | ~+$0.024 possible | — | Account may show ~$0.40 DFS |

All calls stayed inside hard stops. Maps calls returned `cost=0` with empty items (no charge). TripAdvisor Context extracts returned HTTP 400 unbilled.

## Baseline vs after (customer-facing)

| Hotel | Baseline customer BL / mentions | After this turn | Pilot-driven customer BL adds |
|---|---|---|---|
| Waterstone | 0 / 1 | 0 / 1 | **0** verified BLs promoted (DFS inventory is noisy) |
| AC Hotel A Coruña | 2 / 4 | 2 / 4 | **0** (DFS RDs dominated by spam `.xyz`) |
| Castillo | 4 raw / 3 unique / 3 mentions | **6 unique / 3 mentions** | **+3 restored from Admin inventory** (not paid discovery); Visit Palma deduped |

## Incremental commercial findings

### Waterstone
| Signal | Result | Commercially useful? |
|---|---|---|
| DFS hotel summary | 1,018 backlinks / 341 referring domains | Index depth yes; **not** auto-verified customer BLs |
| Sample backlinks | seasprayboca, hotelcontact, tripguideusa, greathotelsinusa… | Mixed; need HTML quality filter before customer promotion |
| Competitor gaps (`hilton.com` exclude hotel) | 20 intersections, low relevance / legal / spam mix | **Weak** — peer target too broad |
| Maps / GBP | Empty results | **No** GBP correction from DFS |
| Context search | Found official Hilton Curio URL `hilton.com/en/hotels/bctbuqq-waterstone-resort-and-marina-boca-raton/` | **Yes** — brand URL identified |
| Context extract (brand locations fallback) | Returned wrong property (“Hilton Boca Raton Suites”) | Unusable as fact source |
| Context extract (TripAdvisor) | Failed 400 | No |
| New correction | Hilton brand URL confirmation + meetings align | **Yes** (1 actionable correction candidate) |

### AC Hotel A Coruña
| Signal | Result | Useful? |
|---|---|---|
| DFS hotel domain | 53 BL / 50 RD — heavy spam/UTM farms | Mostly **duplicate/unusable** |
| Brand Marriott summary | 15 BL / 12 RD | Contextual |
| Confirmed RD overlap | `infogal.es` present in DFS RDs | Reinforces existing customer BL |
| Competitor gaps vs `nh-hotels.com` | Spam-heavy intersections | **Weak** |
| Maps / GBP | Empty | No |
| Context search | Booking.com + Marriott + Google Travel entity URLs | Listing discovery **yes** |
| Context brand extract | AC Hotel A Coruña facts extracted | Partial — brand page reachable via Context |
| TripAdvisor extract | Failed | No |

### Castillo Hotel Son Vida
| Signal | Result | Useful? |
|---|---|---|
| DFS hotel domain | 671 BL / 452 RD; **mallorcacomercial.es** top RD | Validates inventory BLs |
| New candidate hosts | sampedrano.de, bestofmallorca.de, golfsiden.* | Candidates for future Exa/HTML verify — **not auto-promoted** |
| Competitor gaps vs arabellagolfmallorca | Includes sonvidagolf.com intersections | Moderate golf-destination gap leads |
| Maps / GBP | Empty | No |
| Context Marriott extract | Failed 400 | Brand still access-limited |
| Inventory restore (zero paid) | +3 customer BLs after re-verify | **Yes** — presentation completeness |

## Meaningful new actions (counted)

| # | Hotel | Action / correction | Source |
|---:|---|---|---|
| 1 | Waterstone | Confirm Hilton Curio property URL + reconcile meetings | CTX-01 + correction register |
| 2 | Castillo | Customer PDF now shows 6 unique verified website links (was 3 unique / 4 raw) | Inventory restore + dedupe |
| 3 | AC | Booking.com / Google Travel entity URLs noted for future platform assessment | CTX-04 search |

**Duplicate / low-quality:** Majority of DFS referring domains for AC; broad competitor intersections for Waterstone/AC; TripAdvisor extract failures.

## Cost per meaningful new action

Using **3** meaningful outcomes and combined est. **$0.45** (excl. possible aborted DFS-01):

**≈ $0.15 per meaningful outcome**

If only the Waterstone Hilton URL counts as paid-provider-attributable (Castillo restore was unpaid re-verify):

**≈ $0.45 per paid-attributable action** (1 action)

## Provider policy recommendation (ongoing)

| Provider | Policy |
|---|---|
| **Exa** | Remain **DEFAULT** for AVA discovery |
| **DataForSEO** | **OPTIONAL_DEEP_DIVE** only — useful for RD volume / validating known domains; **do not** auto-promote raw backlinks; require quality filter. Prefer hotel-owned domain summary + referring_domains over broad peer intersection |
| **Context.dev** | **CONDITIONAL** — search is high value for brand/listing URL discovery; extract succeeds unevenly on brand pages; TripAdvisor often fails without scrape (scrapes still deferred) |
| **Ahrefs** | Do **not** purchase |
| **Parallel paid** | Remain deferred |
| **Maps/GBP via DFS** | This account returned empty Maps items — investigate location/keyword packing before relying on it |

**Do not** wire paid providers into every AVA run. Keep `AVA_PAID_RESEARCH_AUTHORIZED` gate.
