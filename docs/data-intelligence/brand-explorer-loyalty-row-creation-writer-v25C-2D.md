# Brand Explorer Loyalty Row Creation Writer v25C-2D

- Generated: 2026-07-09T17:17:30.651Z
- Mode: **dry-run**
- Writer exists: **yes**
- Brand: **Tribute Portfolio** (`recCvV0PuZOi8c3hC`)
- Source package: `brand-explorer-loyalty-row-review-package.json`

## Summary

| Metric | Value |
|--------|-------|
| Rows would create | 0 |
| Rows update required | 0 |
| Rows matched (idempotent) | 10 |
| Duplicate rows found | 0 |
| loyalty.hero_title untouched | yes |
| KPI rows excluded | yes |
| Airtable modified | no |
| Company Validated untouched | yes |

## Governance labels (report metadata only)

- AI-assembled from approved source facts
- Founder-reviewed copy package
- Not company-validated
- Not Marriott-validated

## Approved facts verified

- `be.loyalty.earnMechanics`: **Approved**
- `be.loyalty.redeemMechanics`: **Approved**
- `be.loyalty.eliteTierLadder`: **Approved**
- `be.loyalty.memberRatesBenefit`: **Approved**
- `be.loyalty.programScaleStatement`: **Approved**

## Existing loyalty rows

- `loyalty.hero_title` `recDHR5pOBS4DRtlb` — Marriott Bonvoy — Loyalty at a Glance
- `loyalty.elite` `recFmNuUVVLhFkRZL` — Titanium Elite
- `loyalty.elite` `recH24c2vC7dG1snf` — Ambassador Elite
- `loyalty.implications.pnl` `recNza86lhiVTxdqg` — Owner implication (pnl): plan systems, staffing, and P&L treatment for Bonvoy participation—confirm with approved loyalt
- `loyalty.implications.ops` `recRN00OoWmeU3smz` — Owner implication (ops): plan systems, staffing, and P&L treatment for Bonvoy participation—confirm with approved loyalt
- `loyalty.elite` `recTpkAwUnxwGupiG` — Gold Elite
- `loyalty.earn` `recUN78aEOqWk7Z7h` — Earning & Member Rates
- `loyalty.elite` `recWUtOkDt4QwsQms` — Platinum Elite
- `loyalty.proof` `rech6Z3zevnu1utSd` — Member Rate Incentive
- `loyalty.elite` `reclCM2KxwVpBhYh6` — Silver Elite
- `loyalty.implications.systems` `reclajkWrHzPu2dRv` — Owner implication (systems): plan systems, staffing, and P&L treatment for Bonvoy participation—confirm with approved lo
- `loyalty.owner_lens` `recoF8nmTqEpW6mhF` — Model loyalty contribution net of program fees and channel mix; treat Bonvoy as demand support—not a substitute for loca
- `loyalty.elite` `recpM1TUkOROWrBO3` — Member
- `loyalty.redeem` `recqp3btZojuNbMBL` — Redeeming Through Bonvoy
- `loyalty.proof` `rectCr7cWrrYEVcFf` — Global Program Scale
- `loyalty.ecosystem` `recxFslJ231H8rYIb` — Bonvoy connects independent-character Tribute stays to Marriott's global loyalty ecosystem—supporting repeat and cross-s

## Exact apply command

```bash
npm run brand-explorer-loyalty-row-creation-writer -- --brand tribute-portfolio --apply --approve-brand-explorer-v25C-2D-loyalty-rows --founder-reviewed-loyalty-row-copy --approve-brand-explorer-v25C-2D-loyalty-row-create
```

## Does not do

- Modify loyalty.hero_title or existing matched rows
- Create loyalty.kpi.* rows
- Use pending, FDD, or internal-only facts
- Write governance labels into presentation Body copy
- Change images, Brand Basics, or Company Validated
- Imply Marriott validated anything
- Auto-update rows flagged update_required
