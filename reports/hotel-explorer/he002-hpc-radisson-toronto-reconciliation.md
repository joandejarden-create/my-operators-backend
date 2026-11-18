# HE-002 — HPC Open/Pipeline reconciliation (Radisson Toronto)

**Date:** 2026-11-18  
**Workspace:** `ws-hotel-explorer`  
**Scope:** Read-only Airtable + live `:8080` internal-preview API. No production writes.

## Totals

| Metric | Count |
| --- | ---: |
| HPC Radar Status Open (formula) | 8 |
| HPC Radar Status Pipeline | 0 |
| Missing Latitude/Longitude | 1 |
| Map-eligible (API `hotels`) | 7 |
| Merged into public census pool (`addedIds`) | 7 |
| Identity collisions vs Hotel Census | 0 |

## Founder cohort (8 Open)

| Property | HPC id | Stage |
| --- | --- | --- |
| Radisson Blu Toronto Downtown | `recH9h3McgMNFQEla` | **MISSING_COORDINATES** |
| Castillo Hotel Son Vida | `rec82D9zpB8fede1I` | MERGED_AS_INTERNAL_PREVIEW |
| Wyndham San Diego Bayside | `recnPB3Q02NO8Ma1o` | MERGED_AS_INTERNAL_PREVIEW |
| Sheraton Mallorca Arabella Golf Hotel | `recjhQdAUiSyxfCqE` | MERGED_AS_INTERNAL_PREVIEW |
| Nobu Hotel London Portman Square | `recC2kbCaS48jFjFF` | MERGED_AS_INTERNAL_PREVIEW |
| Waterstone Resort & Marina | `recgMYovrrZDJMqzX` | MERGED_AS_INTERNAL_PREVIEW |
| The Mayflower Hotel, Autograph Collection | `rec0eTEnCqlsGJJt9` | MERGED_AS_INTERNAL_PREVIEW |
| Bethesda Marriott | `recLuxvwwxID7U2B8` | MERGED_AS_INTERNAL_PREVIEW |

Eight Open in Airtable vs seven on the map is expected: status eligibility ≠ map eligibility.

## Radisson Blu Toronto Downtown — root cause

1. Airtable `Radar Status` = **Open** (included in dynamic formula).  
2. API formats the row but `Latitude` / `Longitude` fields are **absent** (`latField=null`, `lngField=null`).  
3. `classifyFormattedHotels` / `hotelHasMapCoordinates` exclude it from `mapEligible`.  
4. Client merge never receives it → not in `allHotels` / search / city markers.  
5. Not a dedupe collision (absent from Hotel Census public pool).  
6. ADP published manifest confirms same `censusRecordId` but stores **no** lat/lng to reuse.

**Present identity fields:** Address `249 Queens Quay West`, City `Toronto`, Country `Canada`, Postal `M5J 2N5`, Submarket `Harbourfront / Queens Quay`, official Choice URL.

**Not applied:** Third-party map sites list ~`43.6387, -79.3855`. Per HE rules these were **not** invented into the runtime or written to Airtable. Founder-approved census geocode apply is required.

## City / market marker audit (code)

- City aggregates key by `city|country` (Paris FR ≠ Paris US).  
- Marker number = `totalHotels` (all statuses in the filtered pool), with separate Open/Pipeline/Candidate tallies in the popup — not silently mixed into the badge.  
- Low-zoom density retention remains gated (`lowZoomNeedsDensity`).  
- Missing-coordinate hotels are reported in API `diagnostics.missingCoordinates` and surfaced in the Radar merge notice.

## Required founder action for Radisson map pin

Authorize a Hotel Property Census Latitude/Longitude write for `recH9h3McgMNFQEla` (geocode from Address + City, or verified coords). No HE-002 code path should special-case this hotel ID.
