# StayingAPI Capability Map (from OpenAPI + docs)

## Base
- REST: `https://api.stayingapi.com/v1`
- Auth: Bearer `STAYINGAPI_KEY` (server-side only)
- Envelope: `{ data, meta }`; live may return 202 + jobId

## Endpoints used in benchmark
| Endpoint | Cost (docs) | Notes |
|----------|-------------|-------|
| GET /account | 0 | plan + credits |
| GET /search | 2+ (Airbnb 2/result; others 1/result; min 5/platform) | discovery |
| GET /listing/{platform}/{id} | 3 | full detail |
| GET /jobs/{id} | 0 | async poll |

## Property fields (unified schema)
id, platform, platformListingId, url, name, propertyType,
location { lat, lng, city, region, country, address },
starRating, guestRating, maxOccupancy, bedrooms, bathrooms,
amenities[], images[], host, price?, identity?

## STAYINGAPI_ROOMS_CAPABILITY
**NOT_SUPPORTED** — no total hotel room/key count. `bedrooms` / `maxOccupancy` must never map to Rooms / Keys.

## Platforms
airbnb | booking | vrbo | google
