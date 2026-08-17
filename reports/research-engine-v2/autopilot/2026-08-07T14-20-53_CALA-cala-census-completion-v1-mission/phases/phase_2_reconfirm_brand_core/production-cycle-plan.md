# Production Cycle Plan

- Mode: production-cycle
- Region: CALA
- Scope: official-parent-inventory
- Batch size: 100
- Max passes: 3
- Writes enabled: true
- Census before: 1205
- Queue order: brand_normalization → parent_company_normalization → core_identity_quality → core_identity_source_lookup → key_field_completion
- Per-bundle ChatGPT approval: **false** (founder CLI is approval)
