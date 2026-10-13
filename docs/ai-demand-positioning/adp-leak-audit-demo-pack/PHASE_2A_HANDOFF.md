# AI Demand Leak Audit — Phase 2A Handoff

**Date:** 2026-09-07  
**Mode:** Generation workflow (template locked)

## What is done

1. **Template lock** — 3-page Cover / Executive Diagnostic / Action+Conversion (`TEMPLATE_LOCK_V1.md`)
2. **ADP Lite research mode** — `leak_audit_lite` · 15×4=60 · partial OK at 15×3
3. **Full vs Lite comparison** — avg score 88; safe for free prospect use with caution language (`reports/ai-demand-positioning/leak-audit-comparison/`)
4. **Phase 2A generation stack**
   - Schema: `lib/ai-demand-positioning/leak-audit/airtable-schema.js`
   - Client: `airtable-client.js` (Leak Audit base only)
   - Repository: `leak-audit-repository.js` (filesystem live/ + optional Airtable dual-write)
   - Single-property + portfolio generators
   - Copy-from-ADP CLI with `--mode` / `--scope` / `--output` / `--dry-run`
   - Admin routes under `/admin/adp-leak-audits/*` wired to repository
   - Share tokens: `/adp-leak-audit/share/:shareToken`

## Defaults

* `researchMode = leak_audit_lite`
* `prioritySource = inferred_from_results`
* Cover meta: `PROVIDERS 4 · SCENARIOS 15 · OBSERVATIONS 60 · ACTION ITEMS 3`
* Warm optional: `20 × 4` (manual / qualified only)

## Env

```bash
# Optional — without these, filesystem fallback is used
AIRTABLE_LEAK_AUDIT_BASE_ID=
AIRTABLE_LEAK_AUDIT_API_KEY=
AIRTABLE_LEAK_AUDIT_TABLE_PREFIX=
# LEAK_AUDIT_ROOT=   # test isolation
```

## Explicitly not in Phase 2A

* Public LinkedIn landing
* Automated outbound email
* Full paid ADP setup creation
* Full monthly monitoring automation
* Uncontrolled live provider runs at scale

## Next priorities (after 2A)

1. Provision physical Airtable Leak Audit base from schema map
2. Founder-approved live provider runner behind admin gate
3. Promote-to-paid-ADP workflow (explicit confirmation; still separate)

## QA entry

See `AIRTABLE_GENERATION_QA.md` and comparison summary under `reports/ai-demand-positioning/leak-audit-comparison/comparison-summary.md`.
