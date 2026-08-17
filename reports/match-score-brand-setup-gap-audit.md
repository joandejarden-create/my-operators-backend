# Match Score Brand Setup Gap Audit

- **Version:** match-score-brand-setup-gap-audit-v1
- **Generated:** 2026-07-23T19:22:40.181Z
- **Universe:** Brand Basics Brand Status Active/Live
- **Active/Live count (live):** **24** (do not hardcode 26)
- **P1 complete (score-critical required):** 14 / 24
- **Avg score-critical required fill:** 87.5%
- **Brands with ≥40% null-drag weight missing:** 0

## Fill policy

- Allowed: A founder/brand knowledge; B existing Dealality docs/FDD/research
- Forbidden: inventing web/AI values; Explorer marketing as fees/geography; Global-priority defaults to juice scores
- Scoring: null soft factors are excluded from the denominator; still complete Brand Setup (min scored weight) for trustworthy published scores

## Active/Live brand list

- `ascend` — Ascend Hotel Collection (`reclkgOzvAcBheUSo`, status=Active)
- `autograph-collection` — Autograph Collection (`recEJCTDj1zrsjPM6`, status=Active)
- `bw-premier-collection` — BW Premier Collection (`recwXZ5gVZ8ZH8ekA`, status=Active)
- `bw-signature-collection` — BW Signature Collection (`recdeh1NsP4gjrv80`, status=Active)
- `comfort-inn-suites` — Comfort Inn & Suites (`recOzH5iAE1xEjyD0`, status=Active)
- `country-inn-suites` — Country Inn & Suites by Choice (`recaayt9u7YYg8h7Y`, status=Active)
- `curio-collection` — Curio Collection by Hilton (`receQkxgjlezsc1xg`, status=Active)
- `design-hotels` — Design Hotels (`rec02zPClpWUTCyXM`, status=Active)
- `everhome-suites` — Everhome Suites (`recqkkrsevi4r9ibj`, status=Active)
- `handwritten-collection` — Handwritten Collection (`rec7hTXwMRC81EPqz`, status=Active)
- `hotel-indigo` — Hotel Indigo (`recegXrqaPiSLGCIe`, status=Active)
- `kimpton` — Kimpton Hotels (`recCKuXCmGvxHPfb3`, status=Active)
- `mgallery-collection` — MGallery Collection (`recrWCD1LMqu864oU`, status=Active)
- `preferred-hotels-and-resorts` — Preferred Hotels & Resorts (`recwl5JOYxlChuCAr`, status=Active)
- `quality-inn` — Quality Inn (`recd8o4k1JddhkRWW`, status=Active)
- `radisson-blu` — Radisson Blu by Choice (`recWPEvxBQxVVzSq3`, status=Active)
- `radisson` — Radisson by Choice (`recywbx1YQSTCPqW1`, status=Active)
- `radisson-individuals-by-choice` — Radisson Individuals by Choice (`recRyvM8OmLlDj9G7`, status=Active)
- `radisson-red` — Radisson RED by Choice (`recmKqo7M7mLZgRqQ`, status=Active)
- `small-luxury-hotels-of-the-world` — Small Luxury Hotels of the World (`recjjSnY2opb8P4DG`, status=Active)
- `suburban-studios` — Suburban Studios (`reclcjg5Foa9Vs5TC`, status=Active)
- `tribute-portfolio` — Tribute Portfolio (`recCvV0PuZOi8c3hC`, status=Active)
- `vignette-collection` — Vignette Collection (`recDwzv86TWnz2gGB`, status=Active)
- `woodspring-suites` — WoodSpring Suites (`recsOd51NzRPYsMko`, status=Active)

## Per-brand score-critical fill

| Brand | Required % | Null-drag missing wt | Blank required keys | P1 done |
|-------|------------|----------------------|---------------------|---------|
| BW Premier Collection | 63.6% | 14% | roomCountRange, additionalAmenities, incentiveTypes, willingToNegotiateIncentives | no |
| BW Signature Collection | 63.6% | 14% | roomCountRange, additionalAmenities, incentiveTypes, willingToNegotiateIncentives | no |
| Design Hotels | 63.6% | 14% | roomCountRange, additionalAmenities, incentiveTypes, willingToNegotiateIncentives | no |
| Ascend Hotel Collection | 72.7% | 14% | additionalAmenities, incentiveTypes, willingToNegotiateIncentives | no |
| Comfort Inn & Suites | 72.7% | 14% | additionalAmenities, incentiveTypes, willingToNegotiateIncentives | no |
| Everhome Suites | 72.7% | 14% | additionalAmenities, incentiveTypes, willingToNegotiateIncentives | no |
| Quality Inn | 72.7% | 14% | additionalAmenities, incentiveTypes, willingToNegotiateIncentives | no |
| Radisson Individuals by Choice | 72.7% | 14% | additionalAmenities, incentiveTypes, willingToNegotiateIncentives | no |
| Suburban Studios | 72.7% | 14% | additionalAmenities, incentiveTypes, willingToNegotiateIncentives | no |
| WoodSpring Suites | 72.7% | 14% | additionalAmenities, incentiveTypes, willingToNegotiateIncentives | no |
| Autograph Collection | 100% | 0% | — | yes |
| Country Inn & Suites by Choice | 100% | 0% | — | yes |
| Curio Collection by Hilton | 100% | 0% | — | yes |
| Handwritten Collection | 100% | 0% | — | yes |
| Hotel Indigo | 100% | 0% | — | yes |
| Kimpton Hotels | 100% | 0% | — | yes |
| MGallery Collection | 100% | 0% | — | yes |
| Preferred Hotels & Resorts | 100% | 0% | — | yes |
| Radisson Blu by Choice | 100% | 0% | — | yes |
| Radisson by Choice | 100% | 0% | — | yes |
| Radisson RED by Choice | 100% | 0% | — | yes |
| Small Luxury Hotels of the World | 100% | 0% | — | yes |
| Tribute Portfolio | 100% | 0% | — | yes |
| Vignette Collection | 100% | 0% | — | yes |

## Next steps

1. Fill `reports/match-score-brand-setup-founder-worksheet.csv` from A/B sources.
2. `npm run apply-match-score-brand-setup-fills -- --dry-run`
3. Founder approve → `--apply`
4. Re-run `npm run audit-match-score-brand-setup-gaps`
5. `npm run refresh-deal-brand-cache-active-brands`
