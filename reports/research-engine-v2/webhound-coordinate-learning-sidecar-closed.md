# Webhound Coordinate Learning Sidecar — Closed

**Final status:** `production_census_coordinate_resolver_needs_code_improvement`  
**Generated:** 2026-08-05T14:15:00.000Z

## 1. Executive summary

Webhound Marriott coordinate **pattern research** is complete and capped. Learnings are converted into code-based Census resolvers. **Production writes from Webhound = 0.** Next Marriott/IHG coordinates must come from the code resolver (HQV signature harvest + retries), not a full Webhound Census run.

## 2. Webhound sidecar status

| Item | Value |
| --- | --- |
| Lifecycle | **Already complete** (`budget_complete`) |
| Stopped / capped | Cap honored — no new Webhound searches this task |
| Session | [bbaa85f9…](https://webhound.ai/session/bbaa85f9-3d19-4b05-a53c-4bc4e44fde02) |
| Title | Marriott Mexico hotel page coordinate extraction patterns |
| Budget | $4.86 |
| Final spend | **~$4.89** (slight budget_exceeded warning) |
| Page visits | 29 |
| Searches | 19 |
| Census records processed by Webhound | **0** (pattern research only) |
| Production / Airtable writes from Webhound | **0** |
| Coordinates applied from Webhound | **0** |

## 3. Webhound spend / scope

- Scope was **Marriott Mexico extraction patterns**, not the 666-record Census.
- ~29 page visits across sitemap, overview negatives, search bootstrap, robots, and secondary scrapfly docs.
- Remaining budget = $0 — do **not** add budget or restart for full-census coordinates.

## 4. Confirmation: Webhound did not write production

- No Airtable updates originated from Webhound output.
- First-pass Census coordinates (132) came from VIC freeze / Hilton+Choice directory claims in the prior apply lane — not this sidecar.
- This task performed **dry-run only** for the coordinate resolver (`exact_airtable_update_count_if_applied = 0`).

## 5. Learnings → code patterns

### Marriott (primary Webhound learning)

1. **Seed** Mexico sitemap → MARSHA via `/hotels/([A-Z0-9]{5})-`
2. **Prefer** GraphQL `phoenixShopHQVPropertyInfoCall` → `data.property.basicInformation.latitude/longitude`
3. **Overview HTML** usually has **no** JSON-LD / meta geo / map embeds — do not treat as primary
4. **Optional bootstrap:** harvest `__NEXT_DATA__.props.pageProps.operationSignatures` from a rendered search page; cache `graphql-operation-signature`
5. **Constraints:** Akamai; `/search/` disallowed in robots.txt — operational signature harvest, not bulk search scrape

**Code modules**

- `lib/research-engine-v2/marriott-hqv-coordinate-client.js`
- `lib/research-engine-v2/production-census-coordinate-extractor.js`
- `lib/research-engine-v2/production-census-coordinate-resolver.js`

### Hilton / Choice / IHG

Webhound was Marriott-only. Hilton/Choice patterns remain from first-pass VIC directory claims; IHG needs hoteldetail HTML/JSON lane (currently `official_page_blocked` in Node fetch sample).

### Sample property learning table

| Property | Brand | Official page | Coords on HTML | Suggested method | Code without Webhound? | Crawler rule? | Steward? |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Marriott Cancun (CUNMX) | Marriott | Yes | No | MARSHA → HQV GraphQL | Yes | Yes | If Akamai/signature fail |
| JW Marriott CDMX Polanco (MEXJW) | JW Marriott | Yes | No | MARSHA → HQV | Yes | Yes | If Akamai/signature fail |
| Mexico hotel sitemap | Directory | Yes | N/A (seed) | MARSHA enumeration | Yes | Yes | No |
| Cancun findHotels.mi | Search bootstrap | N/A | Via GraphQL batch | Signature harvest only | Yes | Bootstrap helper | Ops/robots |

## 12–14. Fields / BE / next step

- **Fields not touched:** owner, operator, developer, rooms, dates, Company Validated, Brand Verified, Recent Momentum, Brand Explorer.
- **Brand Explorer:** untouched.
- **Next step:** Harvest `MARRIOTT_GRAPHQL_OPERATION_SIGNATURE` via browser/XHR; retry `npm run research-engine-v2:production-census-coordinate-resolver -- --dry-run`. Do **not** restart Webhound for all Census records.
