# Hotel Census Secondary Source Decision Pack

**Status:** Evaluation only — **no secondary Census writes** until founder approval.  
**Related:** `ENABLE_SECONDARY_HOTEL_DATA_SOURCES=0` default.

## Why this pack exists

Choice (and other) official property pages are often bot-blocked (403). Wayback/HTML frequently yields:
- central reservation hotlines (must reject)
- sitewide rooms defaults such as 25 (must reject)

Phone and Rooms therefore need an explicit secondary-source policy before coverage can rise safely.

## Matrix

| Source | Phone | Rooms | Address | Website | Coords | Coverage | License | Founder approval |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Official parent/brand pages + directories | Primary when property-level | Primary when exact | Primary | Primary | When present | High for chained brands | Public pages; respect ToS | No |
| Official hotel websites at scale | Strong secondary after parent fail | Strong secondary when exact | — | — | — | Medium | Public | Yes |
| CoStar / STR licensed hotel database | — | — | — | — | — | High commercial | Licensed — never product-facing CoStar exposure | Yes |
| Google Places API | — | — | — | — | — | Very high | Storage restricted without Places ToS review | Yes |
| Data Appeal / licensed hospitality datasets | — | — | — | — | — | Medium–High | License-dependent | Yes |
| Geoapify / Foursquare / HERE / TomTom | — | — | — | — | — | Medium–High | Often no bulk store | Yes |
| Tourism boards / convention bureaus | — | — | — | — | — | Country-specific / sparse | Often open | Yes |
| Owner/developer websites | — | — | — | — | — | Low volume | Public | Yes |
| Hospitality trade publications | — | — | — | — | — | Sparse | Public | Yes |

## Recommended sequence

1. Keep secondary disabled.
2. Exhaust official parent adapters + hotel-site fetches linked from parent.
3. Founder picks **one** licensed secondary for phone/rooms gaps (not Google until legal review; not product-facing CoStar).
4. If approved, require Source URL / Type / Evidence Tier / Confidence / Reviewed Date on every write.

## Explicit non-goals until approval

- No Google Places Census writes  
- No OTA phones/rooms as SoT  
- No central reservation hotlines  
- No sitewide rooms defaults  
- No silent secondary writes  
