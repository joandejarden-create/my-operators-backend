# Dealality permanent Airtable persistence gate

**Contract:** `DEALALITY_AIRTABLE_PERSISTENCE_GATE_V1`  
**Effective:** 2026-11-19 (founder directive)  
**Applies to:** All Dealality streams — ADP, GDI, Hotel Explorer, Brand Explorer, Census/HI, Integration Controller reviews  

## Non-negotiable

Every development phase involving **durable business intelligence** must explicitly define and validate its **canonical persistence architecture**.

**Do not classify a feature as FULL E2E COMPLETE** based solely on:

- Working UI
- Successful API responses
- Generated PDFs
- Local JSON files / runtime caches
- Untracked checkout artifacts
- Passing unit tests alone

Local demonstration without proven persistence is at most **PROTOTYPE**.

## Required persistence design (every durable capability)

Before claiming persistence readiness, document and validate:

1. **Verified canonical Airtable base / table / record ownership** (env role + table name/id where known).
2. **Existing schema reuse** before proposing new tables or fields.
3. **Stable identifiers** for hotel (`HPC rec…` / governed aliases), organization, opportunity, and research runs.
4. **Governed, idempotent persistence** (dry-run → apply; dedupe keys; no silent overwrite of validated data).
5. **Retrieval** across process restarts and authorized environments (not only the creating checkout).
6. **Source provenance** and **historical version retention** (append/correct; do not erase prior belief).
7. **Cross-product references without unnecessary duplication** (one canonical owner; others link/reference).
8. **Tests** using isolated fixtures or explicitly authorized test records — never production customer mutation without founder auth.
9. **Clear split:** Airtable business records vs durable raw-evidence storage (FS archives / approved volumes with hash pointers).
10. **Explicit founder/IC approval** before schema changes or production Airtable writes.

Existing durable evidence files and immutable research archives may remain in approved storage when their identities, provenance, hashes, locations, and lifecycle references are persisted appropriately.

## Cross-workstream completion classifications

These classifications apply across streams. They **do not replace** product-specific lifecycle states (e.g. AVA publication statuses, GDI research run states). Use them in PR closeouts and Integration Controller review.

| Status | Meaning |
|---|---|
| **PROTOTYPE** | Demonstrated locally; persistence not proven. |
| **PERSISTENCE_DESIGNED** | Schema, ownership, and write/read contracts documented; no production write claimed. |
| **PERSISTENCE_VALIDATED** | Approved persistence and retrieval verified across restart/authorized environment. |
| **FULL_E2E_VALIDATED** | Research → validation → persistence → retrieval → analysis → UI → history → applicable authorization gates tested. |
| **CUSTOMER_PUBLISHED** | Independently authorized and verified for customer distribution. |

**Never claim FULL_E2E_VALIDATED when the information exists only in a local checkout or temporary runtime cache.**

Related product gates remain binding where applicable:

- Radar authenticated acceptance: `RADAR_AUTHENTICATED_APP_ACCEPTANCE_GATE.md`
- PR merge/home sync: `PR_COMPLETION_AND_WORKSPACE_OWNERSHIP.md`
- Schema authority: matching `docs/*-airtable-fields.md` + `.env.example`

## Mandatory PR closeout (durable intelligence)

Every PR involving durable intelligence must report:

1. **Airtable source-of-truth mapping** — base role, table(s), key fields/IDs, record ownership.
2. **Persistence and retrieval evidence** — write path, read path, restart/environment proof (or explicit PROTOTYPE / PERSISTENCE_DESIGNED only).
3. **History / provenance verification** — prior values retained or N/A with reason.
4. **Cross-workstream identity checks** — hotel/org/opportunity/research IDs and no unsafe duplication.
5. **Remaining persistence limitations** — honest gaps (e.g. FS-only raw blobs, gated history writes).
6. **Explicit E2E completion classification** — one of the statuses above (plus product-specific lifecycle if any).

Integration Controller must refuse to treat a PR as FULL_E2E_VALIDATED / customer-ready when closeout omits persistence evidence or classifies above proven level.

## ADP 2.0 application note

The 2026-11-19 ADP 2.0 capability audit classified measurement cores as operational while action completion, longitudinal AT history, AI-ready packages, and GDI→scenario links remain below FULL_E2E_VALIDATED until Airtable (or approved durable) persistence and retrieval are designed and validated under this gate.

## Authorization boundary

This gate **does not** itself authorize Airtable writes, schema creation/modification, paid research, customer publication, or production deployment. Those require separate founder authorization and dry-run discipline.

## Related

- `AI_BUILD_PROTOCOL.md`
- `CURSOR_IMPLEMENTATION_PROTOCOL.md`
- `PR_COMPLETION_AND_WORKSPACE_OWNERSHIP.md`
- `PRODUCT_CHAT_STARTUP_PROMPTS.md`
- `.cursor/rules/dealality-airtable-persistence-gate.mdc`
- `../data-intelligence/INTELLIGENCE_GOVERNANCE.md`
- `../dealality-pr-validation-matrix.md`
