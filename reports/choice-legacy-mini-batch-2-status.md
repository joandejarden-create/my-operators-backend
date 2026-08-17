# Choice Legacy Mini-Batch 2 — Status

Generated: 2026-07-06T23:04:35.974Z
Batch: **mini-batch-2**

## Summary

| Metric | Count |
|--------|------:|
| Brands | 4 |
| Ready for PDF registration | 4 |
| Ready for URL capture | 4 |
| Split-out recommended | 1 |
| Already Platform Ready | 0 |

## Batch apply commands (do not run until dry-run reviewed)

```bash
npm run choice-legacy-brand-source-package-batch -- --batch mini-batch-2 --apply --approve-choice-legacy-batch-source-register
npm run choice-legacy-batch-url-capture -- --batch mini-batch-2 --apply --approve-choice-legacy-batch-url-capture
npm run choice-legacy-batch-source-stewardship -- --batch mini-batch-2 --apply --approve-choice-legacy-batch-stewardship
npm run choice-legacy-batch-extract -- --batch mini-batch-2 --apply --approve-choice-legacy-batch-extract
npm run choice-legacy-batch-fact-stewardship -- --batch mini-batch-2 --apply --approve-choice-legacy-batch-fact-stewardship
npm run choice-legacy-batch-governance-publish -- --batch mini-batch-2 --apply --approve-choice-legacy-batch-governance-publish
```

## Brands

### Country Inn & Suites by Choice

- Record: `recaayt9u7YYg8h7Y`
- Explorer: **Active** · Profile: **Active — Evidence Package Needed**
- Governance: **—**
- Primary PDF: `Choice Hotels International/Country Inn & Suites/Country Inn & Suites Prototype Brochure.pdf`
- Consumer URL: https://www.choicehotels.com/country-inn-suites (**verified**)
- Press kit: — (**uncertain**)
- Development: https://www.choicehotelsdevelopment.com/our-brands/upper-midscale/country-inn-and-suites · JS-shell risk **high**
- Duplicate: none
- Ready — PDF reg: **true** · URL capture: **true** · stewardship: **false**
- Split out: **yes** (press_kit_uncertain_or_missing)
- Caveats:
  - Americas brand owned by Choice; global RHG Country Inn materials are separate reference only

### Radisson by Choice

- Record: `recywbx1YQSTCPqW1`
- Explorer: **Active** · Profile: **Active — Evidence Package Needed**
- Governance: **—**
- Primary PDF: `Choice Hotels International/Radisson/1. Brand Book - RD.pdf`
- Consumer URL: https://www.choicehotels.com/radisson (**verified**)
- Press kit: https://media.choicehotels.com/Radisson-press-kit (**verified**)
- Development: https://www.choicehotelsdevelopment.com/our-brands/upscale/radisson · JS-shell risk **high**
- Duplicate: none
- Ready — PDF reg: **true** · URL capture: **true** · stewardship: **false**
- Split out: **no**
- Caveats:
  - Americas Radisson owned by Choice; do not register RHG global radissonhotels.com facts on this Brand Basics row
  - Press kit includes explicit Americas vs RHG Belgium ownership split

### Radisson Individuals by Choice

- Record: `recRyvM8OmLlDj9G7`
- Explorer: **Active** · Profile: **Active — Evidence Package Needed**
- Governance: **—**
- Primary PDF: `Choice Hotels International/Radisson Individuals/RADIN_PitchDeck_PPT_New_Final.pdf`
- Consumer URL: https://www.choicehotels.com/radisson-individuals (**verified**)
- Press kit: https://media.choicehotels.com/Radisson-Individuals-press-kit (**verified**)
- Development: https://www.choicehotelsdevelopment.com/our-brands/upper-upscale/radisson-individuals · JS-shell risk **high**
- Duplicate: none
- Ready — PDF reg: **true** · URL capture: **true** · stewardship: **false**
- Split out: **no**
- Caveats:
  - Americas Radisson Individuals owned by Choice; exclude RHG global portfolio facts

### Radisson RED by Choice

- Record: `recmKqo7M7mLZgRqQ`
- Explorer: **Active** · Profile: **Active — Evidence Package Needed**
- Governance: **—**
- Primary PDF: `Choice Hotels International/Radisson RED/Upscale by Choice brand overview guide.pdf`
- Consumer URL: https://www.choicehotels.com/radisson-red (**verified**)
- Press kit: https://media.choicehotels.com/Radisson-Red-press-kit (**verified**)
- Development: https://www.choicehotelsdevelopment.com/our-brands/upscale/radisson-red · JS-shell risk **high**
- Duplicate: none
- Ready — PDF reg: **true** · URL capture: **true** · stewardship: **false**
- Split out: **no**
- Caveats:
  - Americas Radisson RED owned by Choice; RHG Enjoy It brochure is separate global reference (see save-radisson-red-choice-development-pdfs.mjs)
