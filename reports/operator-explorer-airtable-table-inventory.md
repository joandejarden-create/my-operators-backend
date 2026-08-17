# Operator Explorer — Airtable Table Inventory

**Mode:** Read-only live meta audit  
**Base:** `appvtnDurnMSjINP6` (`AIRTABLE_BASE_ID`)  
**Generated:** 2026-08-09T21:09:52.359Z  
**Branch/Commit:** app-shell-left-nav / 3c88c0b

| Table | Airtable ID | Purpose | Primary Entity | Operator Relevant? | Production Used? | Explorer Relevant? | Fit Relevant? | Workflow Only? | Fields |
| ----- | ----------- | ------- | -------------- | ------------------ | ---------------- | ------------------ | ------------- | -------------- | -----: |
| Company Profile | `tblItyfH6MlOnMKZ9` | Platform commercial company onboarding (multi-role) | Company (platform) | partial | true | false | false | true | 41 |
| Companies | `tblDg1SWTCbkJdufh` | Outreach CRM companies | Company (outreach) | partial | outreach | false | false | true | 15 |
| Contacts | `tbl2Flfkd3H88jHVG` | People contacts | Contact | partial | true | false | false | true | 18 |
| Operator Setup - Master | `tbl4YPJ3XhnYLHLsD` | Canonical operator/company record for Explorer + Fit | Operator | true | true | true | true | false | 44 |
| Operator Setup - Profile & Positioning | `tblNHCPoYI7Dz23wK` | Scale, brands, service models, positioning | Operator child | true | true | true | true | false | 68 |
| Operator Setup - Platform & Markets | `tblJ9uTS6vzCZ5248` | Geography, markets, platform JSON | Operator child | true | true | true | true | false | 131 |
| Operator Setup - Governance, Delivery & Diligence | `tblygTlNhFqyhU1Wu` | Services, reporting, RM/F&B capability | Operator child | true | true | true | true | false | 81 |
| Operator Setup - Commercial Fit & Terms | `tblkWAQHmTZRQGZES` | Structures, openings, commercial/bf_* | Operator child | true | true | true | true | false | 165 |
| Operator Setup - Leadership Platform | `tblhsiVJkVU6H7GVT` | Leadership tab platform rows | Presentation | true | true | true | false | false | 8 |
| Operator Setup - Case Studies | `tblAh1X0KDK8SeYK0` | Property proof / comparables (sparse structured) | Assignment-like | true | true | true | narrative-only | false | 13 |
| Operator Setup - Explorer Materials | `tbllzayIeyvlRzHdz` | Media/presentation assets | Presentation | true | true | true | false | false | 22 |
| Operator Setup - Leadership Team Members | `tbl8jX7BoOcwUIEOd` | Named leadership people | People | true | true | true | false | false | 15 |
| Operator Setup - Diligence QA | `tbl5kF0qGg7X6FtCr` | Diligence QA checklist child | Workflow | true | partial | false | false | true | 5 |
| Operator Setup - Engagement & Reporting | `tblBwfXeyeZ5mQd3T` | Owner engagement reporting rows | Presentation | true | true | true | false | false | 8 |
| Operator Setup - Operating Platform | `tbl3ARHs2Gl187YDc` | Operating platform explorer rows | Presentation | true | true | true | false | false | 9 |
| Operator Setup - Brand Relationships | `tblWU8UDz2pVh3ss4` | Explorer Brand tab presentation rows (NOT normalized brand-approval graph) | Presentation | true | true | true | false | false | 8 |
| Operator Setup - Infrastructure Platform | `tblWOPwYIIlnNBZ1w` | Infrastructure explorer rows | Presentation | true | true | true | false | false | 8 |
| Operator Deal Requests | `tbl0nNYd97yhGj7I9` | Deal↔operator outreach junction + stored alignment | Deal workflow | true | true | false | snapshot | true | 20 |
| Partner Intelligence - Source Library | `tbl8iR7AtkUe0uctp` | Shared evidence sources (brand+operator) | Source | true | true | true | evidence | false | 34 |
| Partner Intelligence - Extracted Facts | `tbl4j5MAQtPL45BmF` | Extracted facts before publish | Fact | true | true | pipeline | false | false | 29 |
| Partner Intelligence - Published Explorer Fields | `tblGGYom9YM7d1OeS` | Published explorer field registry | Publish | true | true | true | false | false | 21 |
| Partner Intelligence - Helena Outreach Intake | `tblKh5kF6Bqj5LWFO` | Helena outreach intake | Outreach | true | partial | false | false | true | 23 |
| Partner Intelligence - Brand Asset Registry | `tblwNaf9DZt8Lth4t` | Brand image/asset registry (brand-primary) | Asset | false | true | false | false | false | 37 |
| Operator Intelligence - Claims | `tblZE18CKPISe1Dcs` | Structured claim spine for research auditability | Claim | true | pilot | evidence | evidence | false | 21 |
| Operator Intelligence - Market Presence | `tblrFqjMNGzxzbZnu` | Normalized country presence types | Presence | true | pilot | true | true | false | 15 |
| Operator Fit - Shortlist | `tbl4D5DCK7oPFhi98` | Internal pilot shortlist + immutable snapshot | Fit workflow | true | internal-pilot | false | true | true | 27 |

## Notes

- Legacy `3rd Party Operator - *` tables were **not present** in live base meta (96 tables). Treat as retired or moved; some code maps may still reference legacy names.
- Canonical Operator Master for Explorer/Fit is **Operator Setup - Master**, not `Companies` or `Company Profile`.
- `Operator Setup - Brand Relationships` is a **presentation row store**, not a normalized brand-approval graph.
- Machine dump: `reports/operator-explorer-architecture-live-schema-dump.json` (886 fields across 26 tables).
- Volume snapshot: Claims=28, Market Presence=42, Case Studies=58, Brand Relationships rows=73, Shortlist=10, Masters=36.
