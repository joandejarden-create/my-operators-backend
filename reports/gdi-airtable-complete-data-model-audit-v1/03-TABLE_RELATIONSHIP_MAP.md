# GDI Airtable relationship map

Base: `appa2cE7FTRmIbB32`

## Core opportunity spine

- **Group Demand Opportunities** (`tblRuReslJMwsfRQj`) ← canonical opportunity registry (`opportunityId` + `hotelId`)
- Links (typed columns): `peVenueId`, `peSignalId`, `hotelVenueFitId`, `decisionId`, discovery run IDs (optional columns)
- Nested fidelity: `opportunityPayloadJson` (compacted on write)

## Research coverage

- GDI Research Targets → Demand Generators / Programs / PE Venues (linked fields)
- GDI Research Runs → hotel-scoped runs
- GDI Research Target Runs → Target × Run execution grain

## Demand generator graph

- Demand Generators → Demand Programs → Demand Generator Signals
- Hotel Demand Generator Fit → hotel × generator

## Private events

- Private Event Venues → Hotel Venue Fit → Private Event Signals → Group Demand Opportunities

## Need dates

- GDI Hotel Commercial Profiles → GDI Hotel Need Periods → Change History
- GDI Need Period Opportunity Matches ↔ Opportunities + Need Periods
- Legacy: Hotel Seasonality & Need Periods / Hotel Commercial Profiles (facility) — do not treat as GDI authority when GDI tables populated

## Feedback / outcomes

- Decisions / Decision Events (Airtable) + FS `feedback.json` (merge-at-read)

## Customer publication (not Airtable)

- Bundled publication package CURRENT — immutable customer content master
