# Storage authority and reconciliation contract

**Version:** `gdi-multi-hotel-airtable-reconciliation-v2`  
**Phase:** Read-only reconciliation — **no production writes**

## Principle

Filesystem and Airtable are **not mirrors**. Each store has a role. Synchronization is explicit, eligibility-gated, and founder-authorized per write set.

## Authority by data type

| Data type | Authoritative when | Notes |
|-----------|-------------------|--------|
| Research execution evidence | Filesystem execution ledger + hotel research stores | Operational SoT for runs, drafts, and in-progress research |
| Global organization / program / signal intelligence | Airtable (after authorized write) | Demand Generators, Programs, Signals, Private Event Venues |
| Hotel-specific opportunities | Filesystem until qualified sync; Airtable GDO after authorized sync | Reconcile by canonical opportunity ID before treating either as complete |
| Customer-visible inventory | Founder-approved publication packages (`CURRENT`) | Never auto-promote from Airtable or filesystem |
| Hotel feedback & decisions | Airtable Decisions / Decision Events (+ authorized feedback) | **Never** copy into global reusable intelligence |

## Reconciliation rules

1. **Join key:** canonical opportunity ID per hotel (`gdi_opp_*`, `rx_d_*`, etc.) — not org name or title alone.
2. **Shared demand:** same global DG/program may legitimately map to **different** hotel GDO IDs; cross-hotel duplicate GDO IDs are invalid.
3. **Decisions:** CREATE, UPDATE, UNCHANGED, DUPLICATE_LINKED, DEFERRED, CONFLICT, REJECTED — planner is fail-closed; zero CREATE candidates in current dry-run.
4. **Airtable-only historical (non-YOTEL):** preserve; do not silent-import or delete. **YOTEL exception (founder-authorized policy):** obsolete GDO inventory is scheduled for controlled delete-and-replace; backup for recovery only — see `../gdi-yotel-replacement-other-hotel-reconciliation-v1/SUPERSEDED_YOTEL_POLICY.md`.
5. **Filesystem-only:** research continuation until commercial eligibility + founder allowlist.
6. **Conflicts:** block automated sync until human/founder resolution.

## Non-goals (this phase)

- Force equal record counts across stores
- Bulk GDO create for research bags (YOTEL, Guadalajara FS-only, Radisson FS-only)
- Schema changes or new Airtable columns
- Publication or paid research

## Contract artifacts

- Machine manifest: `MULTI_HOTEL_DRY_RUN_SYNC_MANIFEST.json`
- Code: `lib/group-demand-intelligence/workflow/gdi-multi-hotel-airtable-reconciliation-v2.js`
- Prior table-purpose gate: PR #362 D1–D8 (still binding)