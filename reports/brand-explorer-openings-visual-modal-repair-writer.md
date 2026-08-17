# Brand Explorer Openings Visual + Modal Repair Writer v25C-3D

- Generated: 2026-07-09T19:12:20.255Z
- Mode: **dry-run**
- Brand: **Tribute Portfolio** (`recCvV0PuZOi8c3hC`)
- v25C-3D exists: **yes**

## Summary

| Metric | Value |
|--------|-------|
| Rows would update | 0 |
| Rows would create | 0 |
| Root cause (images) | Marriott CDN URLs were stored as external URL attachments during v25C-3C but did not materialize into Airtable Image attachments; brand-library.js exposes imageUrl only from materialized attachments, so the UI renders empty hero images. |
| MARSHA removed from UI | yes |
| Consumer-site listing removed | yes |
| All cards will have images | yes |
| Modal fields complete | yes |
| Casa Nizuc future example only | yes |
| Loyalty untouched | yes |
| Momentum untouched | yes |
| Airtable modified | no |
| Company Validated untouched | yes |

## Image diagnosis by row

### Casa Nizuc, a Tribute Portfolio Resort

- Record: `recA64mOFrKJk91dg`
- Airtable Image count: **1**
- API imageUrl: `https://v5.airtableusercontent.com/v3/u/55/55/1783634400000/…`
- Root cause: **unknown_or_ok**
- Repair: **none**

### Crystal Cove, Barbados, a Tribute Portfolio All-Inclusive Resort

- Record: `recS8yiXhrw9GDLT9`
- Airtable Image count: **1**
- API imageUrl: `https://v5.airtableusercontent.com/v3/u/55/55/1783634400000/…`
- Root cause: **unknown_or_ok**
- Repair: **none**

### Hotel Rumbao, a Tribute Portfolio Hotel

- Record: `recVOx8mYrzfUnVFQ`
- Airtable Image count: **1**
- API imageUrl: `https://v5.airtableusercontent.com/v3/u/55/55/1783634400000/…`
- Root cause: **unknown_or_ok**
- Repair: **none**

### Humano, Lima, a Tribute Portfolio Hotel

- Record: `rec58S5gLVVHXiSNB`
- Airtable Image count: **1**
- API imageUrl: `https://v5.airtableusercontent.com/v3/u/55/55/1783634400000/…`
- Root cause: **unknown_or_ok**
- Repair: **none**

### Loma, Medellin, a Tribute Portfolio Hotel

- Record: `recKFr0DmBNxTBx4c`
- Airtable Image count: **1**
- API imageUrl: `https://v5.airtableusercontent.com/v3/u/55/55/1783634400000/…`
- Root cause: **unknown_or_ok**
- Repair: **none**

## Proposed cleaned card metadata

- **Casa Nizuc, a Tribute Portfolio Resort**: meta `Future Resort Example` → **Future Resort Example**; scenario `Riviera Maya Leisure Example` → **Riviera Maya Leisure Example**
- **Crystal Cove, Barbados, a Tribute Portfolio All-Inclusive Resort**: meta `Caribbean All-Inclusive Resort` → **Caribbean All-Inclusive Resort**; scenario `Barbados Resort Example` → **Barbados Resort Example**
- **Hotel Rumbao, a Tribute Portfolio Hotel**: meta `Historic City Lifestyle Hotel` → **Historic City Lifestyle Hotel**; scenario `Old San Juan Urban Example` → **Old San Juan Urban Example**
- **Humano, Lima, a Tribute Portfolio Hotel**: meta `Waterfront Urban Lifestyle Hotel` → **Waterfront Urban Lifestyle Hotel**; scenario `South America Urban Example` → **South America Urban Example**
- **Loma, Medellin, a Tribute Portfolio Hotel**: meta `Andean Urban Lifestyle Hotel` → **Andean Urban Lifestyle Hotel**; scenario `Andean Urban Example` → **Andean Urban Example**

## Proposed modal copy

### Casa Nizuc, a Tribute Portfolio Resort

- Property overview: Cancún-area resort listed on Marriott's Tribute Portfolio consumer site as a future example—leisure positioning on the Riviera Maya corridor.
- Why it is relevant: Useful when the owner story is an independent-character resort or boutique leisure asset seeking Marriott distribution and Bonvoy without a rigid full-service prototype.
- What it suggests: Shows how Tribute can frame a pre-opening leisure asset as a portfolio example—not a confirmed opening—for affiliation evaluation.
- Dealality takeaway: Treat published listing timing as illustrative metadata; validate ramp, fees, and PIP scope in the deal model before underwriting from this example.
- Tags: Resort, Mexico, CALA, Riviera Maya, Future example
- External link: https://www.marriott.com/en-us/hotels/cunan-casa-nizuc-a-tribute-portfolio-resort/overview/

### Crystal Cove, Barbados, a Tribute Portfolio All-Inclusive Resort

- Property overview: St. James, Barbados: all-inclusive resort operating under Tribute Portfolio on Marriott's official property pages—resort-scale leisure positioning in the Caribbean.
- Why it is relevant: Useful when owners evaluate all-inclusive or resort-scale leisure assets that need Marriott distribution while preserving independent resort character.
- What it suggests: Shows Tribute anchoring a Caribbean leisure asset with full resort programming—not a urban lifestyle or conversion-only play.
- Dealality takeaway: Compare all-inclusive operating complexity, seasonality, and fee stack against urban Tribute examples before using this as a performance proxy.
- Tags: Resort, All-Inclusive, Barbados, CALA, Caribbean
- External link: https://www.marriott.com/en-us/hotels/bgity-crystal-cove-barbados-a-tribute-portfolio-all-inclusive-resort/overview/

### Hotel Rumbao, a Tribute Portfolio Hotel

- Property overview: Old San Juan urban lifestyle hotel under Tribute Portfolio—design-forward positioning in a heritage city core within Marriott's CALA footprint.
- Why it is relevant: Illustrates Tribute for urban conversion or repositioning where owners want local character, Bonvoy participation, and Marriott commercial infrastructure.
- What it suggests: Reference for heritage or urban lifestyle assets in CALA gateway cities—not a resort or airport-capture play.
- Dealality takeaway: Validate ADR, operating complexity, and PIP scope for historic urban cores before modeling from this example.
- Tags: Urban, Puerto Rico, CALA, Old San Juan, Lifestyle
- External link: https://www.marriott.com/en-us/hotels/sjutx-hotel-rumbao-a-tribute-portfolio-hotel/overview/

### Humano, Lima, a Tribute Portfolio Hotel

- Property overview: Lima Malecón waterfront urban hotel under Tribute Portfolio—lifestyle positioning on Peru's Pacific coast within Marriott's South America map.
- Why it is relevant: Useful when owners compare urban lifestyle affiliation options in South America secondary and capital-city corridors.
- What it suggests: Shows Tribute as a waterfront urban lifestyle flag—not resort-scale or all-inclusive—within Marriott's commercial stack.
- Dealality takeaway: Model waterfront demand mix, seasonality, and operating complexity separately from Caribbean resort examples.
- Tags: Urban, Peru, South America, Waterfront, Lifestyle
- External link: https://www.marriott.com/en-us/hotels/limtx-humano-lima-a-tribute-portfolio-hotel/overview/

### Loma, Medellin, a Tribute Portfolio Hotel

- Property overview: Medellín urban lifestyle hotel under Tribute Portfolio—independent design sensibility in an Andean secondary city within Marriott's network.
- Why it is relevant: Reference for Andean urban lifestyle deals where owners want collection positioning without a rigid full-service box.
- What it suggests: Illustrates Tribute in a design-forward secondary city—not a resort, airport, or all-inclusive anchor.
- Dealality takeaway: Compare secondary-city demand drivers, fee economics, and ramp timing before using Medellín as a underwriting proxy.
- Tags: Urban, Colombia, South America, Andean, Lifestyle
- External link: https://www.marriott.com/en-us/hotels/mdetx-loma-medellin-a-tribute-portfolio-hotel/overview/

## Exact apply command

```bash
npm run brand-explorer-openings-visual-modal-repair-writer -- --brand tribute-portfolio --apply --approve-brand-explorer-v25C-3D-openings-visual-modal-repair --founder-reviewed-openings-ui-copy --approve-brand-explorer-v25C-3D-image-render-repair
```

## Does not do

- Create new footprint.openings rows (updates existing v25C-3C rows only)
- Modify loyalty or momentum rows
- Modify geographic footprint region rows
- Change Brand Basics or Company Validated
- Modify Brand Asset Registry records
- Write governance labels into presentation Body copy
- Imply Marriott validated anything
- Describe Casa Nizuc as a completed opening