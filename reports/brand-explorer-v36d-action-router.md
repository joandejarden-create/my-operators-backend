# Brand Explorer v36D Action Router

## Batch action table

| Brand | State | Action | Safe to apply now | Founder review | Generic fix % | Brand config | Code patch |
|-------|-------|--------|-------------------|----------------|---------------|--------------|------------|
| Design Hotels | draft_applied_with_defects | **remediation_apply** | no | yes | 20% | yes | no |
| Small Luxury Hotels of the World | draft_not_applied | **apply_draft** | yes | yes | 67% | yes | yes |
| Tribute Portfolio | draft_applied_with_defects | **remediation_apply** | no | yes | 85% | yes | yes |
| WoodSpring Suites | draft_applied_with_defects | **remediation_apply** | no | yes | 84% | yes | yes |
| Everhome Suites | draft_applied_with_defects | **investigate_exception** | no | yes | 95% | yes | no |

## Apply gate

- Default: **dry-run** — no Airtable writes
- Future apply modes: `--apply-draft`, `--apply-remediation`, `--apply-approved` with confirm flags
- Never mutates Company Validated
- Never auto-approves active profile

Generated: 2026-07-15T18:13:58.981Z
Mode: **dry-run**
Airtable writes: **false**

## Routing match vs expected

- `design-hotels`: expected `remediation_apply` → MATCH
- `small-luxury-hotels-of-the-world`: expected `apply_draft` → MATCH
- `tribute-portfolio`: expected `remediation_apply` → MATCH
- `woodspring-suites`: expected `remediation_apply` → MATCH
- `everhome-suites`: expected `investigate_exception` → MATCH
