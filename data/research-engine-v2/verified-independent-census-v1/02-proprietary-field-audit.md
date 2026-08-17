# Proprietary / Legacy-Derived Field Audit

Inspected: `lib/hotel-census/fields.js`, `MAP_DIRECTORY_ENRICHMENT`, `DATA_DICTIONARY.md`, geography docs.

| Field | Classification | Notes |
|-------|----------------|-------|
| name, Affiliation, Parent Company, status, country, city, Website, Property ID, Brand Property Code, Address, Telephone, Open Date, Latitude/Longitude, Amenities (official) | **Safe factual reconstruction** | Directory / official page |
| rooms / keys | **Safe factual** when explicit on official sources | Never infer from room-type cards |
| Management Company / Owner | **Safe factual** only with explicit evidence; else escalate | Never infer operator from brand |
| Market / Submarket (STR-era labels) | **Legacy-only — do not migrate** as STR taxonomy | Product already prefers Dealality corridors |
| Dealality Market / corridor Submarket | **Dealality-derived replacement** | Use `country-configs` corridors |
| Chain Scale | **Dealality-derived replacement** or **External licensed** if STR scale | Do not copy STR Chain Scale blindly |
| Location (Urban/Resort/…) | **Dealality-derived** / steward | STR location-type vocabulary historically |
| STR Number / Chain ID / proprietary performance (ADR/RevPAR) | **Legacy-only — do not migrate** | Restricted / not independently licensable |
| Include in Brand Explorer / Data Confidence | **Dealality ops / first-party** | Governance fields |
| Images | **Image rights track** (separate) | Not factual census migration |

## Critical rule

Do **not** independently reconstruct hotel facts while continuing to rely on proprietary STR market/submarket/chain-scale taxonomies as if they were Dealality facts.
