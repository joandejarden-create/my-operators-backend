# Operator Explorer — Schema Duplication Analysis

**Date:** 2026-08-09  
**Principle:** Operator Explorer must not create a third SoT for the same fact.

| Concept | Existing Locations | Current Source of Truth | Problem | Recommended Future Source of Truth |
| ------- | ------------------ | ----------------------- | ------- | ---------------------------------- |
| Operator identity name | Master.`company_name`; Company Profile.`Company Name`; Companies.`company_name` | **Operator Setup - Master** for Explorer/Fit | Three company systems | Master for Explorer/Fit; Profile for platform users; Companies for outreach |
| Lifecycle / eligibility | Master.`submission_status`; Validation Status; Usage Permission; External Display Status | **submission_status** for universe | Overlapping permission/display/validation | Keep submission_status; add Record Purpose; use Validation/Usage/Display for governance only |
| Active countries | Platform.`Active Countries`; Market Presence.Country; geography JSON blobs; Claims geo | Split — Fit prefers Market Presence when present | Flat list hides presence type | **Market Presence** typed rows; Active Countries = derived summary |
| Market presence type | Platform flat multi; Intelligence Market Presence Type | Intelligence table (newer) | Dual taxonomy | Intelligence table; deprecate Platform flat for scoring |
| Brands operated | Profile.`brands`; Brand Families Operated; Brand Rel presentation; Case Study branded_independent; Claims | Profile.`brands` for list | Soft labels + presentation ≠ approval | Profile.brands display; **Brand Relationships (intel)** for typed edges; Assignments for evidence |
| Brand approval | Missing structured; inferred from brands/comps | None trustworthy | Over-inference risk | Explicit intel Brand Relationships only when evidenced |
| Conversion / reflag experience | Commercial field (0%); Case Study situation; Claims | Commercial field intended | Empty + narrative duplication | **Derive** from Assignments; keep Commercial as published summary |
| Resort / urban / mixed-use experience | Commercial/asset fields; Case Studies; marketing bf_* | Unclear | Marketing yes/no | Derive from Assignments + Claims Class 2 |
| Management structures | Commercial.`Management Structures Supported`; bf_selected_deal_structures; Deal SI fields | Commercial for operator capability; SI for deal need | bf_* legacy overlap | Commercial structured; SI deal; deprecate bf for scoring |
| Offered services / capabilities | Governance Offered Services; SI must-haves; Brand Rel execution rows | Governance structured | Table-stakes inflation | Governance for catalog; differentiators via evidenced Claims/Assignments |
| Case studies vs assignments | Case Studies only | Case Studies | Story ≠ inventory | Case Studies = published stories; Assignments = inventory |
| Fit scores on operator | ODR snapshot; Shortlist snapshot; never Master (good) | Shortlist/ODR workflow | Risk of writing scores to Master | **Keep off Master** |
| Company vs Operator Master | Company Profile vs Operator Setup | Different products | Confusion in onboarding | Document boundary; optional future link field only |
| Region vs country | Platform regions; Presence.Region; Case Study.region; Source Region on Master | Inconsistent | Soft region labels | Country-normalized Presence; region derived |
| Evidence sources | PI Source Library; Claims Source URLs; Presence Source URLs; Master Evidence Notes | PI Source Library preferred | URL-only duplicates | PI Source Library + claim links; URLs as secondary |

---

## Highest-priority de-duplications (future)

1. Active Countries ↔ Market Presence  
2. Brand Families / brands / Brand Rel presentation / future Brand Relationships intel  
3. Conversion Experience ↔ Assignments  
4. Case Studies ↔ Assignments  
5. Platform Market Presence Type ↔ Intelligence Market Presence Type  
