# SerpApi Google Hotels — Capability Map (from official docs)

Sources: https://serpapi.com/google-hotels-api · https://serpapi.com/google-hotels-property-details · https://serpapi.com/account-api

## Endpoints used
| Call | Engine / URL | Cost (docs) | Purpose |
|------|--------------|-------------|---------|
| Search | `engine=google_hotels` + `q` + check-in/out | 1 search (cached free) | Discovery / ranking |
| Property details | `engine=google_hotels` + `property_token` | 1 search | Address, phone, amenities, GPS, class |
| Account | `GET /account.json` | Free | Quota / plan |

## Required search parameters
- `engine=google_hotels`
- `q` (search query)
- `check_in_date`, `check_out_date` (YYYY-MM-DD)
- `api_key` (server-side only)

## Optional / used
- `gl`, `hl`, `currency`, `adults`
- Filters: `hotel_class`, `amenities`, `brands`, `property_types`, etc. (not required for benchmark)

## property_token
- Returned on each search property card
- Passed back to same engine to retrieve property details (name, address, phone, prices, nearby places, amenities, …)
- Also available via Google Hotels Autocomplete (not required for this benchmark)

## Critical response shape (benchmark learning)
When `q` resolves to a **specific property**, SerpApi often returns **property-details fields at the response root** (`name`, `address`, `phone`, `property_token`, …) with **empty `properties[]`** (ads may still appear). Broad queries (e.g. "hotels in Cancun") return `properties[]` lists instead. The adapter must accept both shapes.

## Search result identifiers
- `property_token`
- `link` (Google Hotels / travel URL on search cards)
- `serpapi_property_details_link`
- `gps_coordinates.{latitude,longitude}`
- `name`, `type`, `hotel_class` / `extracted_hotel_class`
- `amenities[]`, `excluded_amenities[]`
- `overall_rating`, `reviews`, prices / rates
- `thumbnail` / `images`

## Property detail fields (documented)
- name, description, link (often official site), property_token, address, phone, phone_link
- gps_coordinates, check_in_time, check_out_time
- rate_per_night, total_rate, featured_prices[].rooms[] (**bookable room types, not total keys**)
- hotel_class, extracted_hotel_class, images[], ratings, amenities, excluded_amenities
- amenities_detailed, health_and_safety, sustainability
- essential_info (vacation rentals: e.g. "9 bedrooms" — **not hotel keys**)
- nearby_places, typical_price_range, other_reviews

## SERPAPI_ROOMS_CAPABILITY
**NOT_SUPPORTED** — no documented total hotel Rooms/Keys field. Do not map room-type arrays or VR bedrooms to Keys.

## Rate limits / cost
- Account API exposes `account_rate_limit_per_hour`, `plan_searches_left`, `total_searches_left`
- Typical billing: 1 search credit per successful non-cached request
