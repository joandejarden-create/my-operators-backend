# Contact data model review

## Current storage

Contacts are **embedded** on Group Demand Opportunities:

- Typed columns: `primaryContactName`, `primaryContactRole`, `primaryContactEmail`, `primaryContactPhone`, `contactQuality`, `primaryContactId`
- Rich object in `opportunityPayloadJson.primaryContact` / `tileContactPreview` / `cardContactSubtitle`

## Field authority (preserve)

- Card subtitle = frozen professional title / displayRole — not opportunityRole remapping
- Surfe remains on-demand; do not invent booking titles

## Adequacy

| Need | Status |
|------|--------|
| Name / email / phone | Typed columns + JSON — **adequate for registry** |
| Verified professional title | Mostly JSON (`primaryContact.title`) — **column weak** (role column overloaded) |
| Activity-specific role | JSON only |
| Buyer vs intermediary | JSON / eligibility objects |
| Source evidence URL | JSON only |
| Enrichment status | JSON / Surfe usage FS |

## Recommendation

- **P1:** Add typed columns: `primaryContactProfessionalTitle`, `primaryContactActivityRole`, `cardContactSubtitle`, `contactSourceUrl` without requiring a full Contacts table yet.
- **P2:** Normalized Contacts + Contact–Opportunity link table only if multi-hotel contact reuse / dedupe becomes operationally required.
- Do **not** create Contacts table in this audit; compare shows embedded model still primary consumer path.
