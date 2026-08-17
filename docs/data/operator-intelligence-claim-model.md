# Operator Intelligence — Structured Claim Model

**Date:** 2026-08-03  
**Principle:** Research findings exist as structured claims before profile fields.

---

## Required claim fields

| Field | Description |
| ----- | ----------- |
| Claim ID | Stable ID (`clm_…`) |
| Operator ID | Master `rec…` |
| Operator name | Display name |
| Claim category | identity / geography / structure / experience / brand / comparable / governance / performance / other |
| Claim subject | What is asserted about |
| Claim predicate | Relationship (e.g. `operates_in`, `supports_structure`) |
| Claim value | Raw value |
| Normalized value | Controlled-vocab / canonical |
| Geographic scope | Country/region/null |
| Brand scope | Brand/null |
| Property scope | Property/null |
| Hotel segment scope | Segment/null |
| Operating-structure scope | Structure/null |
| Effective date | When true |
| Expiration or review date | Staleness |
| Source ID | One or more `src_…` |
| Source type | Per source policy |
| Evidence class | Per Fit / governance enums |
| Verification status | Unverified / Verified / Rejected / Stale |
| Publication class | 1–4 |
| Confidence | Limited / Moderate / Strong (claim-level) |
| Conflict status | None / Soft / Hard |
| Scoring relevance | High / Medium / Low / None |
| Potential score impact | Eligibility / Alignment / Confidence / Coverage / None |
| Published destination | Target field or `internal` |
| Current published value | If any |
| Research date | ISO date |
| Researcher or process | Human / calibration-wave-1 |
| Notes | Free text |
| Limitations | Explicit limits |

---

## Field-state preservation

Every material fact must support: **Present · Absent · Unknown · Not Applicable · Invalid · Inferred**

Plus: evidence class, publication class, verification status, geographic/brand scope, date, limitations, scoring relevance.

---

## Supported relationships

- Multiple sources → one claim  
- One source → several distinct claims  
- Conflicting claims  
- Historical claims  
- Geographic / brand / property limitations  
- Stale evidence  
- Derived classifications  
- Human-reviewed exceptions  

Do not store all research only as unstructured notes.

---

## Local serialization

Calibration cohort JSON under `data/operator-intelligence/calibration-cohort/` mirrors this model (`claims.json`, `sources.json`, linked by IDs).
