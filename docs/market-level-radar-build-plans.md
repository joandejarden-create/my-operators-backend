# Market-Level Radar Build Plans (Design Note)

## Why country-level build plans are not always enough

The CALA Radar Build Plans table tracks **one row per country**. That works well for:

- Island / compact countrywide builds (Puerto Rico, Aruba)
- Corridor-based countries with a single national rollout (Dominican Republic, Costa Rica, Jamaica)
- Early sequencing and executive visibility

It becomes strained for **large multi-market countries** where each city or resort corridor is a distinct build unit:

| Country | Why one row is insufficient |
|---------|----------------------------|
| **Mexico** | Cancún / Riviera Maya and Mexico City have different demand profiles, submarkets, targets, and verification passes. A flat “Mexico” dataset would break resort vs urban benchmarks. |
| **Brazil** | São Paulo and Rio are separate corporate/leisure markets; countrywide targets obscure pause-and-validate rules for travel infrastructure. |
| **Colombia** | Market-by-market first pass (Cartagena, Bogotá, Medellín, …) — country row is a rollup, not the active build unit. |
| **Peru** | Lima (urban/corporate) vs Cusco / Sacred Valley (heritage tourism) require split targets and submarkets. |
| **Chile** | Santiago-first vs future Patagonia/Atacama expansion. |
| **Argentina** | Buenos Aires vs Mendoza vs Patagonia — same pattern as Peru/Chile. |

### Current workaround (implemented)

Country configs store:

- `nextBuildMarket` — active market label (e.g. `Cancún / Riviera Maya`)
- `marketSubmarkets` — submarkets keyed by market
- `marketTargets` — first-pass / mature targets per market
- `buildApproachNotes` — explicit “do not flatten” guidance

Build plan generator resolves **active market submarkets and targets** from `nextBuildMarket` when `marketTargets` exists.

Fixture templates are **market-scoped** (e.g. `demand-anchors-mexico-cancun-riviera-maya-candidates.json`).

## Proposed future table: Radar Market Build Plans

Do **not** create in Airtable until explicitly requested. Proposed fields:

| Field | Type | Purpose |
|-------|------|---------|
| Country | Single line text | Parent country |
| Market | Single line text | e.g. Cancún / Riviera Maya |
| Region | Single line text | CALA region |
| Build Strategy | Single select | Same options as country plans |
| Priority Tier | Single select | Tier 1–3 |
| Recommended Build Sequence | Number | Order within country or CALA queue |
| Build Status | Single select | Same status model |
| Target Demand Anchors | Number | Market-scoped |
| Current Demand Anchors | Number | Live count |
| Target Travel Infrastructure | Number | Market-scoped |
| Current Travel Infrastructure | Number | Live count |
| Target Total Radar Points | Number | |
| Current Total Radar Points | Number | |
| Submarkets | Long text | Market submarket list |
| Primary Hotel Demand Profile | Single select | Dominant profile for market |
| Source Coverage % | Number | |
| Coordinate Coverage % | Number | |
| Data Confidence Mix | Long text (JSON) | |
| Last Build Date | Date | |
| Next Recommended Action | Long text | |
| Notes | Long text | |

Country row becomes **rollup / sequence anchor**; market rows become **execution units**.

## Recommendation

1. **Now:** Use country rows + `nextBuildMarket` + market-scoped fixture templates for Mexico, Brazil, Colombia, Peru, Chile.
2. **Later (when 2+ markets per country are actively building):** Add Radar Market Build Plans table and link Demand Anchors `Submarket` / `City` to market build status.
3. **Do not** merge Mexico markets into one import file or one flat candidate list.

## Google verification

Pre-import Google Places verification remains **fixture-only** (no Google fields on Demand Anchors or Travel Infrastructure). Market templates set `googlePreImportVerificationRecommended: true`.

## Governance

All imports require governance fields (`governanceRequired: true` on templates). This is independent of market-level vs country-level build plans.
