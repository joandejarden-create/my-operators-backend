# Brand Explorer Radisson Individuals Momentum Evidence-Source Correction v31M-R3

- Generated: 2026-07-10T23:57:36.250Z
- Brand: **Radisson Individuals by Choice**
- v31M-R3 exists: **yes**
- Mode: **apply**

## Tribute momentum evidence rules

Tribute Recent Momentum links to event-supporting sources (newsroom, PR wire, trade press, owner announcements). Property Marriott pages appear only when directly property-specific and no stronger announcement exists.

**Preferred hierarchy:**
- Official company press release / newsroom
- Official brand/development announcement
- Credible hospitality trade coverage
- Property page only as last resort

## Radisson momentum source audit

### Radisson Individuals Expands Across CALA
- Record: `rec0an5blfW4FtMfE`
- Source: https://media.choicehotels.com/Radisson-Individuals-press-kit (press_kit)
- Link label: *View Choice Hotels Press Kit*
- Follows Tribute rules: **yes** (event_supporting_evidence_source)

### Colombia Urban and Heritage Markets Add Individuals Properties
- Record: `recb0WzRRu6jrev4c`
- Source: https://www.choicehotels.com/colombia/cartagena/radisson-individuals-hotels/cb017 (property_listing)
- Link label: *View Cartagena Property Listing*
- Follows Tribute rules: **no** (property_listing_not_momentum_evidence)

### Panama Capital Corridor Extends Individuals Reach
- Record: `recpIgmBNBEMXVEda`
- Source: https://www.choicehotels.com/panama/panama-city/radisson-individuals-hotels/pa006 (property_listing)
- Link label: *View Panama City Property Listing*
- Follows Tribute rules: **no** (property_listing_not_momentum_evidence)

## Source URL before/after

- **Radisson Individuals Expands Across CALA**
  - Before: https://media.choicehotels.com/Radisson-Individuals-press-kit
  - After: https://hotelbusiness.com/radisson-individuals-debuts-in-latin-america/

- **Colombia Urban and Heritage Markets Add Individuals Properties**
  - Before: https://www.choicehotels.com/colombia/cartagena/radisson-individuals-hotels/cb017 _(property listing)_
  - After: https://www.hotelmanagement.net/development/radisson-opens-radisson-individuals-hotels-colombia-panama

- **Panama Capital Corridor Extends Individuals Reach**
  - Before: https://www.choicehotels.com/panama/panama-city/radisson-individuals-hotels/pa006 _(property listing)_
  - After: https://insights.ehotelier.com/properties/2022/03/11/radisson-individuals-debuts-in-latin-america-with-a-portfolio-signing-of-resort-hotels/

## Link label before/after

- `rec0an5blfW4FtMfE`: *View Choice Hotels Press Kit* → *View Hotel Business Article*
- `recb0WzRRu6jrev4c`: *View Cartagena Property Listing* → *View Hotel Management Article*
- `recpIgmBNBEMXVEda`: *View Panama City Property Listing* → *View Ehotelier Article*

## Frontend

- Label resolver changed: **yes**
- brand-explorer-momentum-link-label.js + brand-explorer-atelier-from-api.js — trade-publication and Choice development labels; property listings remain last-resort only.

## Governance

- Company Validated untouched: **yes**
- Airtable modified: **yes**

## Exact apply command

```bash
npm run brand-explorer-radisson-individuals-momentum-evidence-source-correction-writer -- --brand radisson-individuals-by-choice --apply --approve-brand-explorer-v31M-R3-momentum-evidence-source-correction --founder-reviewed-radisson-individuals-momentum-sources --confirm-no-company-validation-claim
```
