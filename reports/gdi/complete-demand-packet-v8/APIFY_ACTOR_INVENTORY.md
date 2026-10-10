# Apify Actor Inventory for GDI Packet V8

Status: **REPO_INVENTORY_ONLY**

No Eventbrite, LinkedIn Events, Meetup, or Google Maps Places actors are referenced in this repo. Do not invent actor IDs.

## Actors in repo

### `maxcopell~tripadvisor`
- GDI use: COMP_HOTEL_IDENTITY
- Used in V8: true
- Truth policy: SIGNAL_UNTIL_PAGE_VALIDATED

### `axlymxp~ihg-hotel-scraper`
- GDI use: BRAND_HOTEL_IDENTITY
- Used in V8: false
- Truth policy: SIGNAL_UNTIL_PAGE_VALIDATED

### `dataquarry~hotels-lodging`
- GDI use: OSM_HOTEL_POI_CANDIDATE_ONLY
- Used in V8: false
- Truth policy: CANDIDATE_ONLY_NOT_INVOKED


## Not in repo (do not invent)

- Eventbrite actor
- LinkedIn Events actor
- Meetup actor
- Google Maps Places actor
- Generic web-scraper actor ID for congress pages

## Recommended next

Add Store actors for Eventbrite/LinkedIn Events only after founder approval — map to organization+date+organizer pillars; keep SIGNAL until page validation.
