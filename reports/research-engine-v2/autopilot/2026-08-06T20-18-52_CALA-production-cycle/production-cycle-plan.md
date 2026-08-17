# Production Cycle Plan

- Mode: production-cycle
- Region: CALA
- Scope: active-brand-setup
- Batch size: 100
- Max passes: 3
- Writes enabled: true
- Census before: 907
- Queue order: core_identity_source_lookup → core_identity_quality → key_field_completion
- Per-bundle ChatGPT approval: **false** (founder CLI is approval)
