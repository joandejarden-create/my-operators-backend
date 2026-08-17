# Operator Setup — Profile & Positioning Final Product Schema

**Policy:** D.4E — If a business-data field remains in the live Profile table, it is **POPULATE** or **REMOVE**. No OPTIONAL. No completeness-by-narrowing.

**Question:** What does an owner need to know to understand who this operator is?

| Field | Storage | Class | Why |
| ----- | -------- | ----- | --- |
| company_name | profile | ACTIVE — REQUIRED | Canonical operator identity |
| website | profile | ACTIVE — REQUIRED | Official company URL |
| headquarters | profile | ACTIVE — REQUIRED | Where the company is based |
| companySize | profile | ACTIVE — REQUIRED | Approximate portfolio scale band |
| yearEstablished | profile | ACTIVE — REQUIRED | Operating-origin year (current company) |
| yearsInBusiness | profile | ACTIVE — DERIVED | `2026 − yearEstablished` (never free-typed) |
| brands | profile | ACTIVE — DERIVED | Brand Basics links from Brand Relationships ∪ Current Assignments |
| primaryServiceModel | profile | ACTIVE — REQUIRED | Portfolio service orientation (Explorer consumes) |
| managementPhilosophy | profile | ACTIVE — REQUIRED | How the company describes hotel operating approach |
| missionStatement | profile | ACTIVE — REQUIRED | Official mission / purpose / vision hierarchy |
| Brand Families Operated | profile | ACTIVE — REQUIRED | Brand-family experience |
| Service Models Supported | profile | ACTIVE — REQUIRED | Service-model experience |
| propertyTypes | profile | ACTIVE — REQUIRED | Hotel-type experience |
| additionalExperience | profile | ACTIVE — REQUIRED | Urban/resort/conversion flags |
| chainScalesSupported | profile | ACTIVE — REQUIRED | Chain-scale experience |
| Soft Brand / Lifestyle Experience | profile | ACTIVE — REQUIRED | Soft-brand depth |
| companyDescription | profile | ACTIVE — REQUIRED | Who-is-this (1–3 factual sentences) |
| companyHistory | profile | ACTIVE — REQUIRED | Founding/evolution/current form |
| differentiators | profile | ACTIVE — REQUIRED | Company-specific differentiation |
| Operator Parent Company | master | ACTIVE — REQUIRED | Parent / corporate context |
| Operating Model | master | ACTIVE — REQUIRED | How the company operates hotels |
| Management Availability | master | ACTIVE — REQUIRED | Third-party management availability |

## yearEstablished semantic

`yearEstablished = year the current operator/company traces its operating origin to`

Edge cases documented in `lib/partner-intelligence/operator-setup-years-registry.js` (e.g. Hotel Equities = company founding 1989 not CALA start; Auberge = management company 1998 not 1981 restaurant).

## REMOVE from active product (still physical until LEGACY/delete)

companyTagline, companyLogo, overview_*, brand_*_json, brand_signal_*, brand_narrative_*, ESG/sustainability selects, locationType*, capitalStatus, insuranceCoverage, crisisExperience, numberOfBrands, additionalBrands, figuresAsOf, businessContinuity, support24x7, readyForInvestorPublication, marketExpansionRampTimeMonths, brandedVsIndependentMix, brand_conversion_project_count, brand_soft_independent_narrative, emergencyResponse, carbonTracking, energyEfficiency, wasteReduction.

These must **not** appear in the founder working grid while blank.
