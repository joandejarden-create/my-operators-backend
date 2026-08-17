# Golden Census Priority Schema (V1.2)

Version: `census-autopilot-v1.2-golden-schema`

## Tracks

| Track | Role vs 95% target |
|-------|-------------------|
| **PRIORITY** | Determines ≥95% Priority Census Completeness |
| LIFECYCLE | Separate score — excluded from Priority denominator |
| OWNERSHIP / OPERATION | Separate score — opaque ownership must not fail Golden Census |
| IMAGE | Separate IMAGE COMPLETENESS — rights ≠ data completeness |
| GOVERNANCE / PROVENANCE | Separate — required for production eligibility |

## Priority groups

1. Hotel Identity & Geography (incl. Dealality Market/Submarket hierarchy)
2. Physical Profile (Rooms / Keys **REQUIRED**)
3. Amenities
4. F&B
5. Meetings & Groups
6. Dealality Classification
7. Content (source text + Dealality AI summary)

## Denominator

```
RAW PRIORITY COMPLETENESS =
  supported applicable Priority fields
  ÷ total applicable Priority fields
```

Excluded from denominator: NOT_APPLICABLE, OPTIONAL (non-bearing), Lifecycle, Ownership, Image, Governance.

Unknown applicable fields count as **incomplete**.

Material weighted completeness uses CRITICAL=4, HIGH=3, MEDIUM=2, LOW=1.

## Firewall

- Cvent / legacy: discovery challenge only — never production evidence
- Unknown preferred to unsupported
- Completeness ≠ production eligibility
