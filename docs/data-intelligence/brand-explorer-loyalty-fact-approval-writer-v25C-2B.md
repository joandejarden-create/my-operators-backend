# Brand Explorer Loyalty Fact Approval Writer v25C-2B

- Generated: 2026-07-09T15:03:41.950Z
- Mode: **dry-run**
- Writer exists: **yes**
- Brand: **Tribute Portfolio** (`recCvV0PuZOi8c3hC`)
- Marriott validation implied: **no**

## Summary

| Metric | Value |
|--------|-------|
| Eligible facts inspected | 5 |
| Facts safe to approve | 0 |
| Facts excluded | 0 |
| Missing eligible facts | 0 |
| Unsupported KPI facts in plan | no |
| Internal-only/FDD in plan | no |
| Non-loyalty facts leaked | 2 |
| Airtable modified | no |
| Presentation rows untouched | yes |
| Company Validated untouched | yes |

## Eligible fact keys

- `be.loyalty.earnMechanics`
- `be.loyalty.redeemMechanics`
- `be.loyalty.eliteTierLadder`
- `be.loyalty.memberRatesBenefit`
- `be.loyalty.programScaleStatement`

## Target future slots

- `loyalty.earn`
- `loyalty.redeem`
- `loyalty.elite`
- `loyalty.proof`

## Facts inspected

### be.loyalty.earnMechanics

- Record: `recmIY5Kteqv1BK5Q`
- Current status: **Approved**
- Assessment: **hold** · would approve: **no**
- Source safety: **safe** (approved_public_source:bonvoy_page)
- Exclusion reasons: already_approved_idempotent_skip
- Value: Earn and redeem points that take you everywhere you want to go.

### be.loyalty.redeemMechanics

- Record: `recbA0yIo4Xh3yuMq`
- Current status: **Approved**
- Assessment: **hold** · would approve: **no**
- Source safety: **safe** (approved_public_source:bonvoy_page)
- Exclusion reasons: already_approved_idempotent_skip
- Value: Earn and redeem points that take you everywhere you want to go.

### be.loyalty.eliteTierLadder

- Record: `recONga3XhpRyHpP0`
- Current status: **Approved**
- Assessment: **hold** · would approve: **no**
- Source safety: **safe** (approved_public_source:bonvoy_page)
- Exclusion reasons: already_approved_idempotent_skip
- Value: Member Silver Elite Gold Elite Platinum Elite Titanium Elite Ambassador Elite

### be.loyalty.memberRatesBenefit

- Record: `recnPbySxNiw0RKun`
- Current status: **Approved**
- Assessment: **hold** · would approve: **no**
- Source safety: **safe** (approved_public_source:bonvoy_page)
- Exclusion reasons: already_approved_idempotent_skip
- Value: Enjoy exclusive discounted rates for Marriott Bonvoy Members.

### be.loyalty.programScaleStatement

- Record: `recRtIx8ycmSQ1Z1E`
- Current status: **Approved**
- Assessment: **hold** · would approve: **no**
- Source safety: **safe** (approved_public_source:bonvoy_page)
- Exclusion reasons: already_approved_idempotent_skip
- Value: Rewards You at 7,000+ Hotels Worldwide.

## Non-loyalty pending facts (not in apply plan)

- `be.meta.fddDocumentVintage` (`rec6mXPxMqMQw2Um2`): standards_or_fdd_fact_excluded
- `be.standards.qualityAssuranceTheme` (`recYAnV9ScBGVw3CK`): standards_or_fdd_fact_excluded

## Exact apply command

```bash
npm run brand-explorer-loyalty-fact-approval-writer -- --brand tribute-portfolio --apply --approve-brand-explorer-v25C-2B-loyalty-facts --founder-reviewed-loyalty-facts
```

## Does not do

- Create or update Brand Explorer Presentation rows
- Change images, Sort Order, or Brand Basics content fields
- Set Company Validated or Company Validation Date
- Approve standards/FDD/internal-only facts or loyalty KPI counts
- Imply Marriott validated anything
- Approve facts outside the five eligible loyalty keys
