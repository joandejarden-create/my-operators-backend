# GDI List View — Contact Columns & Actions Refinement

Date: 2026-11-27
Hotel: Wyndham San Diego Bayside (recnPB3Q02NO8Ma1o) STAGED 26
URL: http://localhost:8080/group-demand-intelligence.html?hotelId=recnPB3Q02NO8Ma1o&preview=staged
Cache: gdi-list-contact-actions-v1
Contract: DEALALITY_ADDITIVE_NON_REGRESSION_V1 (List presentation only)

## Validated on :8080

| Case | Observed |
|---|---|
| Name + verified title | Rebekah Bell / Corporate Marketing Specialist; Brittany Jolly / Conference Team Lead; Ann Marie Levra / Senior Director…; Kim Wang / Manager, Meeting Operations |
| Name without verified title | Kathryn Newstead, Brooke Wheeler, Kristine Coan → Title Not Verified |
| Email + phone | Kathryn / Ann Marie |
| Email only | Brittany Jolly, Rebekah Bell, Madeline Foresman (switchboard phone suppressed) |
| Phone only | Stephanie Pursglove (Email Not Available + direct phone) |
| Neither | Scaling rows without contact channels → Email/Phone Not Available |
| Switchboard/department phone | Madeline general_switchboard and Kristine department_phone → Phone Not Available |
| Eye icon | Opens opportunity drawer |
| Three-dot menu | Portal menu with authorized View Details; opens same drawer |
| Surfe | No auto call from list module; no Surfe references in list table JS |
| Pills | Sales Action / Contact Readiness plain text; Priority pills retained |
| Proper Case headers | Opportunity, Priority, Demand Type, Demand Window, Sales Action, Who to Contact, Contact Details, Actions |
| Tiles / inventory | Unchanged (26 STAGED; tiles path still cards) |

## Regression

- npm run test:gdi-list-view-contact-actions-v1 — PASS
- npm run test:gdi-sortable-commercial-list-view-v1 — PASS

## Screenshots

- reports/gdi-list-view-contact-actions-v1/01-list-contact-details.png
- reports/gdi-list-view-contact-actions-v1/02-more-menu-opens-drawer.png
- reports/gdi-list-view-contact-actions-v1/03-list-after-drawer.png

No paid research. No automatic enrichment. No unauthorized persistence. No publication changes.
