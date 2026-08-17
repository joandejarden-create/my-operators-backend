# Production Census Coordinate Resolver

**Status:** `production_census_coordinate_resolver_needs_code_improvement`  
**Contract:** `production-census-coordinate-resolver-v1`  
**Generated:** 2026-08-05T14:15:44.434Z  
**Apply executed:** false (dry-run only)

## Executive summary

Code-based coordinate resolver replaces Webhound as the production path for Census pins. Webhound was capped/closed as a learning sidecar only.

| Metric | Value |
| --- | ---: |
| Census scanned | 666 |
| Already valid coordinates | 132 |
| Active missing | 293 |
| Proposed (dry-run) | 0 |
| Steward review | 25 |
| Pages fetched | 25 |
| Webhound production writes | 0 |

## Webhound sidecar

| Item | Value |
| --- | --- |
| Lifecycle | already_complete |
| Spend | $4.8906079999999985 / $4.86 |
| Page visits | 29 |
| Production writes from Webhound | **0** |
| Session | https://webhound.ai/session/bbaa85f9-3d19-4b05-a53c-4bc4e44fde02 |

Full learning table: `reports/research-engine-v2/webhound-coordinate-learning-sidecar-closed.md`.

## Resolver method

1. Census Official Property URL / Source URL  
2. Fetch official property or brand directory page  
3. Extract JSON-LD geo, family payloads (Marriott/Hilton/Choice/IHG), map embeds, official address  
4. Optional: geocode **official property name + street address only** (`--allow-official-address-geocode`)  
5. Validate ranges / reject 0,0 / city centroids / airports  
6. High/Medium → propose; Low/uncertain → steward  

### Crawler rules (code-reproducible)

```json
[
  {
    "family": "Marriott",
    "page_types": [
      "mexico hotel sitemap (MARSHA seed)",
      "GraphQL phoenixShopHQVPropertyInfoCall (preferred)",
      "overview HTML (usually negative for coords)"
    ],
    "patterns": [
      "data.property.basicInformation.latitude/longitude via HQV",
      "sitemap /hotels/([A-Z0-9]{5})- MARSHA",
      "__NEXT_DATA__.props.pageProps.operationSignatures[] → MARRIOTT_GRAPHQL_OPERATION_SIGNATURE",
      "optional JSON-LD geo (rarely present on Mexico overview)"
    ],
    "reproducible_without_webhound": true,
    "becomes_crawler_rule": true,
    "steward_if": "Akamai blocks HQV or signature missing",
    "learning_source": "webhound_sidecar_bbaa85f9",
    "env_required": [
      "MARRIOTT_GRAPHQL_OPERATION_SIGNATURE (optional but usually required)"
    ]
  },
  {
    "family": "Hilton",
    "page_types": [
      "hilton.com/en/hotels/{ctyhocn}-.../",
      "locations directory GraphQL"
    ],
    "patterns": [
      "localization.coordinate.latitude/longitude",
      "JSON-LD geo"
    ],
    "reproducible_without_webhound": true,
    "becomes_crawler_rule": true
  },
  {
    "family": "Choice",
    "page_types": [
      "choicehotels.com regional hotel cards",
      "property pages"
    ],
    "patterns": [
      "\"geoLocation\":{\"latitude\":n,\"longitude\":n}"
    ],
    "reproducible_without_webhound": true,
    "becomes_crawler_rule": true
  },
  {
    "family": "IHG",
    "page_types": [
      "ihg.com/.../hoteldetail"
    ],
    "patterns": [
      "hoteldetail latitude/longitude JSON",
      "JSON-LD geo",
      "map embed"
    ],
    "reproducible_without_webhound": true,
    "becomes_crawler_rule": true
  }
]
```

## First-pass validation

- Coordinates present: **132**
- Safe: **132**
- Needs review: **0**
- Shared-campus downgrade-later: **4**
- Public Map missing coords: **0**
- Zero-zero: **0**

No first-pass coordinates were modified in this task.

## Commands

```bash
npm run research-engine-v2:production-census-coordinate-resolver -- --dry-run
npm run research-engine-v2:production-census-coordinate-resolver -- --dry-run --fetch-limit=40 --families=Marriott,IHG
```

## Next step

Marriott/IHG official pages blocked (Akamai). Next code improvement: harvest GraphQL operation signature from a rendered Marriott search page (__NEXT_DATA__.props.pageProps.operationSignatures for phoenixShopHQVPropertyInfoCall), set MARRIOTT_GRAPHQL_OPERATION_SIGNATURE, retry HQV dry-run. Do not restart Webhound for full-census coordinates.
