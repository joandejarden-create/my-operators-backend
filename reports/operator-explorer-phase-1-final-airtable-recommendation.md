# Phase 1 Final Airtable Recommendation (Evidence-Based)

**No writes in this phase.**

## CREATE TABLE

| Table | Why | Example | Volume | Explorer | Fit | Risk |
| ----- | --- | ------- | ------ | -------- | --- | ---- |
| Operator Intelligence - Assignments | Central SoT proven by 84 dry-run rows | Hampton St Thomas; Four Seasons México City | High | Selected Assignments | Comps/geo/dev | Medium |

## ADD FIELD

| Table | Field | Type | Purpose | Example |
| ----- | ----- | ---- | ------- | ------- |
| Master | Record Purpose | select | Production/Research/Test Fixture | Test Fixture on dummies |
| Master or Profile | Operating Model | select | Company form axis | Hybrid |
| Master or Profile | Management Availability | select | Owner engageability axis | Confirmed Direct Management |
| Claims | PI Source Library | link | Evidence spine | link src |
| Market Presence | City / Metro | text | Optional depth | Cancún |
| Market Presence | Verified Assignment Count | number | Optional | 3 |

## CREATE (typed) Brand Relationships intel table — **YES**

Presentation table remains. New intel table for Currently Operates + Brand Managed Capability.

## REUSE EXISTING

Master, Claims, Market Presence, PI Source Library, Case Studies (stories), Shortlist/ODR workflow.

## NORMALIZE LATER

Claims selects; Case Study situation/branded_independent; Shortlist Candidate Type cleanup.

## DERIVE LATER

Active Countries summary; conversion/resort experience flags; brand family lists.

## DEPRECATE LATER

Platform flat Market Presence Type for scoring; bf_* score weight.

## DO NOT ADD

- Project approval on Master  
- Fit scores on Master  
- Per-brand Operator Masters  
- Duplicate MxM/HMS Masters  
- Approval Status as default Brand Rel field  
- Full state/province required on Assignments v1  

## Explicit verdicts

| Object | Verdict |
| ------ | ------- |
| Assignments table | **Required — YES** |
| Typed Brand Relationships | **Required — YES** |
| Market Presence | **Sufficient + minor additions** |
| Claims | **Sufficient + minor additions** |
