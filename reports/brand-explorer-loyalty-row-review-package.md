# Brand Explorer Loyalty Row Review Package v25C-2C

- Generated: 2026-07-09T15:02:28.659Z
- Mode: **dry-run**
- Package exists: **yes**
- Brand: **Tribute Portfolio** (`recCvV0PuZOi8c3hC`)
- Marriott validation implied: **no**

## Summary

| Metric | Value |
|--------|-------|
| Approved loyalty facts used | 5 |
| Target slots | 4 |
| Rows needing creation (v25C-2D) | 4 (10 presentation rows) |
| Rows needing update (v25C-2D) | 0 |
| KPI counts excluded | yes |
| Loyalty meets minimum after v25C-2D | yes |
| Airtable modified | no |
| Company Validated untouched | yes |

## Governance labels (all proposed copy)

- AI-assembled from approved source facts
- Pending founder review
- Not company-validated
- Not Marriott-validated

## Approved loyalty facts used

- `be.loyalty.eliteTierLadder` (`recONga3XhpRyHpP0`): Member Silver Elite Gold Elite Platinum Elite Titanium Elite Ambassador Elite
- `be.loyalty.programScaleStatement` (`recRtIx8ycmSQ1Z1E`): Rewards You at 7,000+ Hotels Worldwide.
- `be.loyalty.redeemMechanics` (`recbA0yIo4Xh3yuMq`): Earn and redeem points that take you everywhere you want to go.
- `be.loyalty.earnMechanics` (`recmIY5Kteqv1BK5Q`): Earn and redeem points that take you everywhere you want to go.
- `be.loyalty.memberRatesBenefit` (`recnPbySxNiw0RKun`): Enjoy exclusive discounted rates for Marriott Bonvoy Members.

## Existing loyalty rows

- `loyalty.hero_title` (`recDHR5pOBS4DRtlb`): Marriott Bonvoy — Loyalty at a Glance

## loyalty.earn

- Existing: **missing** (0 row(s))
- v25C-2D action: **create**
- Risk: **low**
- Founder review: AI-assembled from approved source facts; Pending founder review; Not company-validated; Not Marriott-validated
- Source facts: `be.loyalty.earnMechanics`, `be.loyalty.memberRatesBenefit`

### Proposed copy

**Title:** Earning & Member Rates

```
Marriott Bonvoy gives guests a familiar rewards path for earning points at participating hotels, with member-rate incentives that can support direct booking behavior.
```

### Proposed row payload(s)

```json
[
  {
    "table": "Brand Setup - Brand Explorer Presentation",
    "fields": {
      "Slot Key": "loyalty.earn",
      "Title": "Earning & Member Rates",
      "Body": "Marriott Bonvoy gives guests a familiar rewards path for earning points at participating hotels, with member-rate incentives that can support direct booking behavior.",
      "Brand": [
        "recCvV0PuZOi8c3hC"
      ],
      "Brand Name": "Tribute Portfolio",
      "Active": true,
      "Sort Order": 0
    }
  }
]
```

## loyalty.redeem

- Existing: **missing** (0 row(s))
- v25C-2D action: **create**
- Risk: **low**
- Founder review: AI-assembled from approved source facts; Pending founder review; Not company-validated; Not Marriott-validated
- Source facts: `be.loyalty.redeemMechanics`

### Proposed copy

**Title:** Redeeming Through Bonvoy

```
Bonvoy participation gives guests a redemption pathway through Marriott's loyalty ecosystem, adding a consumer-facing reason to consider the property within the broader Marriott network.
```

### Proposed row payload(s)

```json
[
  {
    "table": "Brand Setup - Brand Explorer Presentation",
    "fields": {
      "Slot Key": "loyalty.redeem",
      "Title": "Redeeming Through Bonvoy",
      "Body": "Bonvoy participation gives guests a redemption pathway through Marriott's loyalty ecosystem, adding a consumer-facing reason to consider the property within the broader Marriott network.",
      "Brand": [
        "recCvV0PuZOi8c3hC"
      ],
      "Brand Name": "Tribute Portfolio",
      "Active": true,
      "Sort Order": 0
    }
  }
]
```

## loyalty.elite

- Existing: **missing** (0 row(s))
- v25C-2D action: **create**
- Risk: **low**
- Founder review: AI-assembled from approved source facts; Pending founder review; Not company-validated; Not Marriott-validated
- Source facts: `be.loyalty.eliteTierLadder`

### Proposed copy

```
Member: Entry tier in the Marriott Bonvoy member ladder.

Silver Elite: Elite tier in the Marriott Bonvoy member ladder.

Gold Elite: Elite tier in the Marriott Bonvoy member ladder.

Platinum Elite: Elite tier in the Marriott Bonvoy member ladder.

Titanium Elite: Elite tier in the Marriott Bonvoy member ladder.

Ambassador Elite: Highest named elite tier in the Marriott Bonvoy member ladder.
```

### Proposed row payload(s)

```json
[
  {
    "table": "Brand Setup - Brand Explorer Presentation",
    "fields": {
      "Slot Key": "loyalty.elite",
      "Title": "Member",
      "Body": "Entry tier in the Marriott Bonvoy member ladder.",
      "Brand": [
        "recCvV0PuZOi8c3hC"
      ],
      "Brand Name": "Tribute Portfolio",
      "Active": true,
      "Sort Order": 0
    }
  },
  {
    "table": "Brand Setup - Brand Explorer Presentation",
    "fields": {
      "Slot Key": "loyalty.elite",
      "Title": "Silver Elite",
      "Body": "Elite tier in the Marriott Bonvoy member ladder.",
      "Brand": [
        "recCvV0PuZOi8c3hC"
      ],
      "Brand Name": "Tribute Portfolio",
      "Active": true,
      "Sort Order": 1
    }
  },
  {
    "table": "Brand Setup - Brand Explorer Presentation",
    "fields": {
      "Slot Key": "loyalty.elite",
      "Title": "Gold Elite",
      "Body": "Elite tier in the Marriott Bonvoy member ladder.",
      "Brand": [
        "recCvV0PuZOi8c3hC"
      ],
      "Brand Name": "Tribute Portfolio",
      "Active": true,
      "Sort Order": 2
    }
  },
  {
    "table": "Brand Setup - Brand Explorer Presentation",
    "fields": {
      "Slot Key": "loyalty.elite",
      "Title": "Platinum Elite",
      "Body": "Elite tier in the Marriott Bonvoy member ladder.",
      "Brand": [
        "recCvV0PuZOi8c3hC"
      ],
      "Brand Name": "Tribute Portfolio",
      "Active": true,
      "Sort Order": 3
    }
  },
  {
    "table": "Brand Setup - Brand Explorer Presentation",
    "fields": {
      "Slot Key": "loyalty.elite",
      "Title": "Titanium Elite",
      "Body": "Elite tier in the Marriott Bonvoy member ladder.",
      "Brand": [
        "recCvV0PuZOi8c3hC"
      ],
      "Brand Name": "Tribute Portfolio",
      "Active": true,
      "Sort Order": 4
    }
  },
  {
    "table": "Brand Setup - Brand Explorer Presentation",
    "fields": {
      "Slot Key": "loyalty.elite",
      "Title": "Ambassador Elite",
      "Body": "Highest named elite tier in the Marriott Bonvoy member ladder.",
      "Brand": [
        "recCvV0PuZOi8c3hC"
      ],
      "Brand Name": "Tribute Portfolio",
      "Active": true,
      "Sort Order": 5
    }
  }
]
```

## loyalty.proof

- Existing: **missing** (0 row(s))
- v25C-2D action: **create**
- Risk: **low**
- Founder review: AI-assembled from approved source facts; Pending founder review; Not company-validated; Not Marriott-validated
- Source facts: `be.loyalty.programScaleStatement`, `be.loyalty.memberRatesBenefit`

### Proposed copy

```
Global Program Scale: Marriott describes Bonvoy as a rewards platform spanning 7,000+ hotels worldwide; use current Bonvoy materials to confirm applicable participation and terms for a specific asset.

Member Rate Incentive: Marriott Bonvoy member rates create a consumer-facing booking incentive that may support direct-channel consideration where the property participates.
```

### Proposed row payload(s)

```json
[
  {
    "table": "Brand Setup - Brand Explorer Presentation",
    "fields": {
      "Slot Key": "loyalty.proof",
      "Title": "Global Program Scale",
      "Body": "Marriott describes Bonvoy as a rewards platform spanning 7,000+ hotels worldwide; use current Bonvoy materials to confirm applicable participation and terms for a specific asset.",
      "Brand": [
        "recCvV0PuZOi8c3hC"
      ],
      "Brand Name": "Tribute Portfolio",
      "Active": true,
      "Sort Order": 0
    }
  },
  {
    "table": "Brand Setup - Brand Explorer Presentation",
    "fields": {
      "Slot Key": "loyalty.proof",
      "Title": "Member Rate Incentive",
      "Body": "Marriott Bonvoy member rates create a consumer-facing booking incentive that may support direct-channel consideration where the property participates.",
      "Brand": [
        "recCvV0PuZOi8c3hC"
      ],
      "Brand Name": "Tribute Portfolio",
      "Active": true,
      "Sort Order": 1
    }
  }
]
```

## v25C-2D row plan

- Create: `loyalty.earn`, `loyalty.redeem`, `loyalty.elite`, `loyalty.proof`
- Update: —

## Exact next writer

```bash
npm run brand-explorer-loyalty-row-creation-writer -- --brand tribute-portfolio --dry-run
```

## Does not do

- Write Airtable or create/update Brand Explorer Presentation rows
- Change images, Sort Order, Brand Basics, or Company Validated
- Populate loyalty.kpi.* without verified KPI facts
- Use pending, FDD, or internal-only facts
- Imply Marriott validated anything
