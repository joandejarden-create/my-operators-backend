# Brand Explorer Momentum Editorial + Link Repair Writer v25C-3E

- Generated: 2026-07-09T21:08:56.651Z
- Mode: **dry-run**
- Brand: **Tribute Portfolio** (`recCvV0PuZOi8c3hC`)
- v25C-3E exists: **yes**

## Summary

| Metric | Value |
|--------|-------|
| Momentum rows inspected | 6 |
| Rows would update | 0 |
| Rows would create | 0 |
| Announcement removed (non-PR) | yes |
| Internal/source-capture language removed | yes |
| Proper case enforced | yes |
| Casa Nizuc excluded from momentum | yes |
| Loyalty rows untouched | yes |
| Openings rows untouched | yes |
| Airtable modified | no |
| Company Validated untouched | yes |

## Frontend link-label root cause

- File: `public/js/brand-explorer-atelier-from-api.js`
- Function: `momentumAnnouncementLinkLabel`
- Issue: Hardcodes 'View {parentCompany} announcement' for any URL when publisher is Marriott International, Inc., even for property pages and Tribute consumer-site links.
- Repair: Classify URL as press/newsroom vs property vs consumer hub; return neutral labels (View property / View Tribute Portfolio site / View source) unless URL is an actual announcement.

## Title changes

| MARSHA | Current | Proposed |
|--------|---------|----------|
| LIMTX | Humano Lima Added To Tribute Portfolio Pipeline | Humano Lima Added To Tribute Portfolio Pipeline |
| MDETX | Loma Medellín Expands Tribute's Urban Lifestyle Presence | Loma Medellín Expands Tribute's Urban Lifestyle Presence |
| BGITY | Crystal Cove Adds Caribbean All-Inclusive Resort Example | Crystal Cove Adds Caribbean All-Inclusive Resort Example |
| SJUTX | Hotel Rumbao Strengthens Tribute's San Juan Presence | Hotel Rumbao Strengthens Tribute's San Juan Presence |
| BDOGP | Grand Hotel Preanger Adds Heritage-Led Asia Pacific Example | Grand Hotel Preanger Adds Heritage-Led Asia Pacific Example |
| MILNT | NEMI Milan Adds European Urban Lifestyle Example | NEMI Milan Adds European Urban Lifestyle Example |

## Link label repair

| MARSHA | Source type | Current label | Proposed label | PR found |
|--------|-------------|---------------|----------------|----------|
| LIMTX | Marriott Property Page | View Marriott International, Inc. announcement | View property | no |
| MDETX | Marriott Property Page | View Marriott International, Inc. announcement | View property | no |
| BGITY | Marriott Property Page | View Marriott International, Inc. announcement | View property | no |
| SJUTX | Marriott Property Page | View Marriott International, Inc. announcement | View property | no |
| BDOGP | Tribute Consumer Site | View Marriott International, Inc. announcement | View Tribute Portfolio site | no |
| MILNT | Tribute Consumer Site | View Marriott International, Inc. announcement | View Tribute Portfolio site | no |

## Exact apply command

```bash
npm run brand-explorer-momentum-editorial-link-repair-writer -- --brand tribute-portfolio --apply --approve-brand-explorer-v25C-3E-momentum-editorial-link-repair --founder-reviewed-momentum-ui-copy --confirm-no-false-announcement-links
```

## Does not do

- Create or delete momentum rows
- Change images or Sort Order
- Modify loyalty or openings rows
- Change Brand Basics or Company Validated
- Fabricate Marriott press-release URLs
- Imply Marriott validated anything
