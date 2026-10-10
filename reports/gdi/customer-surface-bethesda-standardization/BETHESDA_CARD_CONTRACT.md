# Bethesda GDI Opportunity Card Contract

## Canonical implementation
- Page: `public/group-demand-intelligence.html`
- Auth app: `public/js/group-demand-intelligence/app.js`
- Share app: `public/js/group-demand-intelligence/share-app.js`
- **Shared card:** `DealalityGdiUi.opportunityTileHtml` / `opportunityCardsGridHtml` in `dealality-gdi-ui.js`
- List DTO: `toOpportunityListDto` (`gdi_opportunity_list_v2`) in `opportunity-list-dto.js`
- CSS: `public/css/group-demand-intelligence.css` + Brand Explorer `brand-card` shell
- API: `GET /api/group-demand-intelligence/hotels/:hotelId/opportunities`
- Bethesda hotelId: `recLuxvwwxID7U2B8`

## Card fields (browse tile)
| Field | Source |
|---|---|
| Title | `displayTitle` || `title` (dates scrubbed) |
| Organization · segment | `organizationName` · segment/type label |
| Action pill | `bookingWindowStatus` (PURSUE / QUALIFY / WATCH / TOO EARLY) |
| Date pill | event start/end |
| Weekly delta pill | when NEW/UPDATED |
| Summary | `summaryWhat` (truncated) |
| Contact footer | named `primaryContact` **or** buyer entity + role + public contact path |
| CTA | View Details |

## Detail drawer
Full record via `GET .../opportunities/:id` — thesis, fit, sources, recommended action, evidence.

## Customer list eligibility
`filterCustomerFacingOpportunities` = surface-eligible AND (strict ready OR legacy stamp).
Future Watch maturity universe is larger; Bethesda does **not** list all valid Watch as cards.

## Demand Campaigns
Internal only (`/demand-campaigns` API + orchestration). **Not** on customer browse surface.
