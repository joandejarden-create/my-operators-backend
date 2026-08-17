# Operator Setup — Required Field Value Test (D.4C)

Policy: **operator-setup-no-optional-fields-v1**. If we would not spend the effort to obtain this answer for every operator, the field does not belong in the product.

## Rule

No OPTIONAL. Every field is exactly one of: RETAIN — REQUIRED | MOVE TO CLAIMS | PRESENTATION / WORKFLOW | DEPRECATE.

## Profile — RETAIN — REQUIRED

| Field | Why required |
| ----- | ------------ |
| company_name | Canonical operator identity |
| website | Official company URL |
| headquarters | Where the company is based |
| companySize | Approximate portfolio scale band |
| Brand Families Operated | Brand-family experience for Fit brand compatibility |
| Service Models Supported | Service-model experience for Fit segment/asset |
| propertyTypes | Hotel-type experience |
| additionalExperience | Urban/resort/conversion experience flags |
| chainScalesSupported | Chain-scale experience when evidenced; else controlled state |
| Soft Brand / Lifestyle Experience | Soft-brand depth with existing controlled options |
| companyDescription | Owner-facing who-is-this (1–3 factual sentences) |
| companyHistory | Founding/evolution/current form — researchable; controlled NPD when thin |
| differentiators | Evidence-backed differentiators or explicit no-diff state |

## Master (identity) — RETAIN — REQUIRED

| Field | Why |
| ----- | --- |
| Operator Parent Company | Parent / corporate context |
| Operating Model | How the company operates hotels |
| Management Availability | Third-party management availability |

## Platform — RETAIN — REQUIRED

| Field | Why required |
| ----- | ------------ |
| company_name | Row identity |
| Active Countries | Verified CALA taxonomy countries OR controlled no-presence state |
| Market Presence Type | Active / pipeline / no known presence posture |
| specificMarkets | Non-taxonomy geography notes or controlled empty-note state |
| Active Markets / Cities | CALA city/corridor mapping OR controlled no-mapping state |
| cap_profile_operational | Operating platform narrative — Writer v2 + controlled no-evidence |

## Removed from retained product

| Table | Field | Class | Why |
| ----- | ----- | ----- | --- |
| Profile | primaryServiceModel | DEPRECATE | Redundant with Service Models Supported; fails field-value test as separate required column |
| Profile | companyTagline | DEPRECATE | Low owner decision value; inventing taglines forbidden — do not require 36/36 marketing slogans |
| Platform | cap_profile_commercial | MOVE TO CLAIMS | No completed Writer v2 commercial contract; not justified as required Setup narrative before Fit |
| Platform | cap_profile_transition | MOVE TO CLAIMS | Already MOVE TO CLAIMS per D.2/D.5; not Setup narrative |

## Controlled states (replace blanks)

- `NPD`: Not publicly disclosed
- `NA`: Not applicable
- `RV`: Requires validation
- `NO_DIFF`: No sufficiently differentiated public evidence identified
- `NO_OPS`: No sufficiently specific public evidence identified
- `NO_CALA_COUNTRY`: No verified CALA operating presence
- `NO_CALA_MARKET`: No verified CALA market/city mapping
- `NO_MULTI`: No verified evidence
- `NO_MARKETS_NOTE`: No verified non-taxonomy market notes beyond Active Countries and Market Presence Type
