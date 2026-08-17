# Active Brand CALA — Wave 4 enrichment summary

**Generated:** 2026-07-24  
**Prior:** Waves 1–3  
**Scope:** Design Hotels Property ID · Curio/Tapestry Hotel Description · Choice HTML Amenities + Description  
**Rule:** Official sources only · fill-blank · Affiliation = Brand Setup `Brand Name`

## Headline results

| Brand / family | Metric | Before → After (wave3 → wave4) |
|----------------|--------|--------------------------------|
| **Design Hotels** | % Property ID | **85.3 → 97.1** (4 slug fills) |
| **Curio Collection by Hilton** | % Hotel Description | **71.8 → 79.5** (+3 GraphQL) |
| **Tapestry Collection by Hilton** | % Hotel Description | **58.8 → 70.6** (+2 GraphQL) |
| **Choice (Active CALA with saved HTML)** | Hotel Description | **large lift** — 85 meta fills + 3 amenity fills |

Coverage: `reports/active-brand-cala-enrichment-coverage-wave4-after.csv`

## Applied counts

| Action | Count |
|--------|------:|
| Design Hotels Property ID (designhotels.com Website slug) | **4** |
| Hilton GraphQL Hotel Description (Curio 3 + Tapestry 2) | **5** |
| Choice steward-HTML Hotel Description (meta, quality-filtered) | **85** |
| Choice steward-HTML Amenities | **3** |

## Details

### Design Hotels
- Script: `scripts/backfill-design-hotels-property-id.mjs`
- Property ID = path slug from official `designhotels.com/hotels/.../{slug}` Website
- Applied: Lo Sereno Casa de Playa, Hotelito by MUSA, Boca de Agua, Rabot Hotel
- Remaining blank PID(s): no designhotels.com Website (steward)
- Artifacts: `reports/design-hotels-property-id-backfill-plan.json`, `…-apply-log.json`

### Curio + Tapestry descriptions
- Script: `scripts/backfill-hilton-wave4-descriptions.mjs`
- Source: Hilton GraphQL via existing census Property ID (ctyhocn)
- Applied Wave-2 directory creates that still lacked description: Amare Cancun, Punta Sal, York Medellin, Chelsea Bogotá, Perla La Paz
- Remaining blank descriptions: mostly Pipeline / no ctyhocn → steward
- Artifacts: `reports/hilton-wave4-descriptions-plan.json`, `…-apply-log.json`

### Choice (Ascend + Comfort/Quality/Radisson family)
- Script: `scripts/backfill-choice-wave4-from-html.mjs`
- Source: steward-saved HTML under `reports/choice-amenity-html/` only (live fetch still Akamai-blocked)
- Hotel Description = `meta name="description"` when property-forward; rejects Choice Privileges / pure booking CTAs
- Amenities = existing `parseChoiceAmenitiesFromHtml` (only 3 still blank among HTML-indexed rows)
- Ascend example: Emotions Juan Dolio (DO012), Grand Hotel Guayaquil (EC002) descriptions applied where HTML present
- Artifacts: `reports/choice-wave4-from-html-plan.json`, `…-apply-log.json`

## Steward leftovers (no invent)

- Design Hotels rows without designhotels.com Website
- Curio/Tapestry Pipeline without Property ID
- Choice blanks without saved HTML (need steward page save or Wayback harvest)
- Trademark / Dazzler / Vignette / Kimpton / Indigo Website gaps remain Pipeline / catalog-absent (Waves 1–2)

## Change impact

**High** — ~97 Hotel Census updates (4 PID + 5 Hilton desc + 85 Choice desc/amenity).

**Rollback:** clear fields on apply-log record IDs for Design PID, Hilton wave4 descriptions, Choice wave4 HTML applies.

## Manual QA

- [ ] Design Hotels: Property ID equals designhotels.com hotel slug for the 4 applied
- [ ] Curio/Tapestry: Hilton GraphQL descriptions match property pages for Amare / Punta Sal / York / Chelsea / Perla
- [ ] Choice: sample meta descriptions are property-specific (not Choice Privileges CTA)
- [ ] No overwrite of previously filled Amenities / Hotel Description / Property ID
