# Dealality Intelligence Profile Workflow

Generated: 2026-07-06T16:13:07.595Z
Workflow: **v1**
Entity: brand — **Radisson Blu by Choice**
Target record: `recWPEvxBQxVVzSq3`

## Current stage

**Stage 1** — Source discovery (`source_discovery`)

Blockers:

- no_linked_sources

## Package snapshot

- Sources (linked): 0
- Approved for Explorer Use: 0
- Facts (linked): 0
- Approved facts: 0
- Pending facts: 0
- Completeness tier: **empty**
- Governance change class: **—**
- Publish eligible: **no**

## Next recommended commands

```bash
npm run partner-reference:search -- --operator "Radisson Blu by Choice"
```

```bash
npm run partner-reference:init-folder -- --company "Radisson Blu by Choice" --dry-run
```

## Notes

- Verify official URLs before capture; avoid third-party first-pass unless scoped.

## Safety (v1)

- Does not write: Company Validated
- Does not write: Company Validation Date
- Does not write: BAS / OAS / OCS scoring fields
- Does not write: Deal Readiness outputs
- Does not write: Explorer UI
- Does not write: Airtable schema
- Company Validated write path blocked in publish layer: **yes**
- Apply orchestration in workflow CLI: **disabled — use explicit scripts**
