# Operator Alignment — Phase 5B Schema Plan

**Status:** Implemented — see [operator-alignment-phase-5b-schema-implementation.md](./operator-alignment-phase-5b-schema-implementation.md)

This file is the planning index for Phase 5B-1 / 5B-2. Detailed field priorities remain in [operator-alignment-recommended-airtable-fields.md](./operator-alignment-recommended-airtable-fields.md).

**Execution order used:**

1. Export schema backup (`reports/operator-alignment-5b-schema-backup-*.json`)
2. Create missing columns via Metadata API
3. Append operator writer bindings + regenerate build sheet
4. Update `deal-setup-fields.js` and prefill maps
5. Inject Deal Intake / Operator Setup UI (non-breaking, optional fields)
6. Validate with `scripts/validate-operator-alignment-phase-5b.mjs`
