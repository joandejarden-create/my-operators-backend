# GDI Executive Summary V2 — Metric Source Map

**Contract:** `gdi-executive-summary-v2`  
**Grains:** Research Activity · Qualified Inventory · Commercial Engagement

## Grain definitions

| Grain | Meaning | Must not conflate with |
|---|---|---|
| Research Activity | Discovery / deep / WHO-HOW / spend ledgers | STAGED qualified count |
| Qualified Inventory | STAGED or CURRENT package opportunity rows | Raw 161 discovery |
| Commercial Engagement | Pursuit + feedback records | Estimated room-night potential |

## Wyndham STAGED reconciliation (after content hydration)

| Metric (before V2) | Observed | Root cause | After V2 |
|---|---|---|---|
| Qualified Opportunities | 26 | Correct qualified grain | 26 |
| Unique Discovered | 26 | Mislabelled as discovery | Receipt uses “Qualified Delivered” |
| Evidence Sources | 0 | Package URLs not counted | 22 (derived) |
| Demand Segments | Unspecified 26 | Ignored `opportunityTypeFilterCategory` | Canonical taxonomy, 0 Unspecified |
| Verified Buyers / Entry | 0 | Required nested whoHow only | Named contacts + channels derived |
| Last Research | unavailable | Used Founder Preview as period | Customer label `STAGED research as of YYYY-MM-DD` (date from package timestamp or version); Founder Preview remains visibility context only |
| Priority Segment/Timing TBD | raw gaps | Wrong field paths | Canonical title/segment/timing |
| CONTACT_IDENTIFICATION_REQUIRED | raw enum | No customer label map | Proper Case labels |

## Audience permissions

| Perspective | Customer | Founder STAGED preview | Founder/Admin |
|---|---|---|---|
| Executive Overview | CURRENT only | Yes | Yes |
| Sales Performance | CURRENT only | Yes | Yes |
| Research Intelligence | **Never** | Yes (preview session) | Yes |

Server strips `researchIntelligence` unless founder/admin authorized (`stripResearchIntelligencePerspective`).

## Unavailable vs zero

- Missing Weekly Change history → **Unavailable** (not zero new / zero changed).
- Missing pursuit/feedback → engagement metrics Unavailable.
- Research spend / cost per FQO → Unavailable until ledger join.
- Raw discovery 161 → documented as separate grain; not shown as qualified count.
