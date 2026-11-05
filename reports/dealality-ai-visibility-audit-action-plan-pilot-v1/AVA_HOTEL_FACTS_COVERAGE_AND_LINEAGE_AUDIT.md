# AVA Hotel Facts Coverage and Lineage Audit

Generated: 2026-11-05T20:52:41.106Z

## Verdict

**CAUSE_A** — Hotel Facts Register stores material exceptions/conflicts/discrepancies — not a full property encyclopedia. Low row counts (Waterstone 4 / AC 2 / Castillo 2) do not mean only a handful of attributes were researched.

## Full E2E lineage (field-level)

1. **Researched:** property identity, brand pages, platform listings (seeds + discovery V3), DCR signals, digital authority, actions/findings.
2. **Verified:** exact listing URLs where identity matched; Maps place_ids via DataForSEO SERP; Travel entities retained separately.
3. **Stored:** payload JSON (`property`, `digitalContentReadiness`, `platformListingDiscoveryV3`, `actions`, `keyFindings`); seeds file; coverage matrix JSON per hotel.
4. **Analysis layer:** actions + keyFindings + DCR assessments; coverage matrix synthesizes commercial attributes without inventing facts.
5. **Customer report:** refined V4 surfaces Priority Findings, Platform Assessment, Website/Content, Digital Authority, Implementation Roadmap — not the full exception register.
6. **Excluded intentionally:** consistent verified facts (not exceptions); N/A attributes; access-restricted full OTA bodies; unconfirmed GBP ownership claims.

## Six-hotel coverage matrix summary

| Hotel | Register exceptions | FACT_VERIFIED | FACT_CONFLICTED | FACT_NOT_VERIFIED |
|---|---:|---:|---:|---:|
| Waterstone Resort & Marina | 4 | 2 | 0 | 3 |
| AC Hotel A Coruña | 2 | 4 | 0 | 0 |
| Castillo Hotel Son Vida, a Luxury Collection Hotel, Mallorca | 2 | 4 | 0 | 2 |
| Sheraton Mallorca Arabella Golf Hotel | 2 | 4 | 0 | 2 |
| Grand Fiesta Americana Chapultepec | 2 | 3 | 1 | 3 |
| Fiesta Americana Condesa Cancún All Inclusive | 1 | 3 | 0 | 7 |

## Example — Waterstone attribute flow

| Attribute | Status | Where stored | In register? | In V4 report? |
|---|---|---|---|---|
| Official name | PARTIAL / register row | `hotelFactsRegister` + property | Yes | Yes (identity) |
| Brand page (Hilton Curio) | FACT_VERIFIED | `platformListingDiscoveryV3` | No (consistent) | Yes (platform table) |
| Hotel website | FACT_VERIFIED | `property.website` | No (consistent) | Yes |
| Google Maps place | EXACT place_id | seeds + discovery V3 | No | Yes (Google row + links) |
| Amenity encyclopedia | often PARTIAL / NOT_VERIFIED | narrative/actions only | Only if conflict | Only if actionable |

Correction Register / actions remain the home for genuine defects — not every verified attribute.

## Machine-readable

Per-hotel: `AVA_HOTEL_FACTS_COVERAGE_MATRIX_<propertyId>.json` · Bundle: `AVA_FINAL_RESEARCH_V4_REFINEMENT_RESULTS.json`
