# Operator Explorer — Taxonomy Audit

**Date:** 2026-08-09  
**Sources:** Live Airtable options + `docs/operator-alignment-airtable-options-audit.md` + bindings  
**Rule:** Do not modify options in this phase.

---

| Taxonomy | Current options (summary) | Used by code? | Used in scoring? | Used in UI? | Duplicated? | Issues | Recommend |
| -------- | ------------------------- | ------------- | ---------------- | ----------- | ----------- | ------ | --------- |
| Master.`submission_status` | Draft, Submitted, In Review, Approved, Archived, Active, Research Stage | Yes | Eligibility | Yes | Overlaps Validation Status / External Display | Mixes workflow + Explorer lifecycle; no explicit Test Only | **KEEP + NORMALIZE** — add Test Only or Record Purpose; avoid third parallel status |
| Master.`Validation Status` | Company Validated … Do Not Use | Yes (sparse) | Soft | Trust footnote | Brand parallel | Underpopulated | KEEP |
| Master.`Usage Permission` | Internal Only … Do Not Use | Yes (sparse) | Soft | Soft | Overlaps External Display | Underpopulated | KEEP + clarify vs display |
| Master.`External Display Status` | Show/Hide Trust … Do Not Display | Yes (sparse) | No | Explorer | Overlaps Usage Permission | Underpopulated | KEEP |
| Master.`Data Confidence Level` | Verified … Medium | Display | Soft | Footnote | — | Options slightly inconsistent naming | KEEP + NORMALIZE |
| Master.`Source Type` | multi incl. Imported sample data | Display | Soft | Soft | — | Useful for dummy detection | KEEP |
| Market Presence Type (table) | Current Managed Property … Unknown | Yes Fit geo | **Yes** | Explorer future | Flat Platform.`Market Presence Type` | Good controlled vocab | **KEEP** as SoT |
| Platform.`Market Presence Type` | Active operations; Prior; Pipeline; Target; None; Unknown | Legacy Fit path | Legacy | Partial | Yes vs intelligence table | Dual taxonomy | DEPRECATE LATER for scoring |
| Platform.`Active Countries` | Live multi options | Yes | Yes | Yes | Presence table countries | Flat summary vs typed presence | KEEP as derived/hybrid |
| Profile.`chainScalesSupported` | Chain scale vocab | Yes | Yes | Yes | Soft brand families | Strong coverage | KEEP |
| Profile.`Service Models Supported` | multi | Yes | Medium | Yes | Offered Services overlap | Naming drift | KEEP + NORMALIZE |
| Profile.`Brand Families Operated` | soft | Soft | Low–Med | Yes | Profile.brands | Soft labels | NORMALIZE / DERIVE |
| Commercial.`Management Structures Supported` | Full third-party; Brand-managed; Franchise support; … | Yes | **High** | Partial | Deal SI Preferred Management Structure | Must stay aligned with deal path | KEEP |
| Deal SI management / operating / brand agreement | Structured (options audit Exact) | Yes | **High** | Deal UI | Commercial structures | Preserve; do not silently rename | KEEP |
| Commercial conversion / new-build experience | select/text | Intended | High if used | Partial | Assignments | 0% conversion populated | KEEP; DERIVE later |
| Governance.`Offered Services` | Structured services | Yes | High but generic risk | Yes | SI must-have services | Table-stakes inflation | KEEP; scoring policy caution |
| Case Studies.`situation` | Dozens of narrative tags | Explorer | No numeric | Yes | Development experience flags | **Too granular / polluted** | KEEP + NORMALIZE (split story vs type) |
| Case Studies.`branded_independent` | Branded/Independent **plus brand names** | Explorer | Soft | Yes | Brand Relationships | **Inconsistent / polluted** | REPLACE options carefully later |
| Case Studies.`Comparability Strength` | High / Moderate / Limited | Fit comps | Intended | Soft | — | Clean | KEEP |
| Brand Relationships.`section` | Brand Snapshot … Brand Signal | Explorer writer | No | Yes | — | Presentation enum | KEEP |
| Shortlist.`Candidate Type` / `Shortlist Status` | Workflow enums | Fit shortlist | Workflow | Internal | — | Clean | KEEP |
| Claims categories / statuses | Mostly **free text** today | Intel | Evidence | Internal | Should be selects | Too free-form | KEEP + NORMALIZE to selects |
| Company Profile company type / operating model | Platform onboarding enums | Company profile | No Fit | Platform | Parallel to Operator Setup | Different product surface | KEEP separate |

---

## Cross-cutting findings

1. **Too many lifecycle-ish statuses** on Master without explicit Test Fixture purpose.  
2. **Case Study selects are the worst pollution** and block assignment derivation.  
3. **Market Presence Type** has a good intelligence taxonomy and a weaker flat Platform twin.  
4. **Claims** should move key enums from free text → selects (non-destructive add).  
5. Do **not** invent overlapping “Operator Explorer Status” if `submission_status` can be extended carefully — prefer **Record Purpose** + existing status.
