# Remaining blockers to production Airtable activation

1. **Founder authorization of the exact Bethesda 4-op write set** (`PRODUCTION_PILOT_AUTHORIZATION_REQUEST.md`) — still required; CD-8 keeps apply fail-closed.
2. **Adapter apply gate** still returns `AIRTABLE_PRODUCTION_WRITES_NOT_AUTHORIZED` even when flags are present (intentional until separate pilot approval).
3. **YOTEL / Radisson / Guadalajara ID reconciliations** remain FS-ahead / namespace-split — no bulk import authorized.
4. **Deferred Bethesda GDOs** (3 WATCHLIST rows) stay out of the first pilot.
5. **Linked-record resolution** for Demand Generator / Program / Signal requires existing Airtable record ids after DG creates — text ids write first; link mirrors follow on subsequent dry-run/apply once records exist.
6. **No customer publication mutation** in this phase.
7. **No schema migration** — if any required field is ever missing live, stop and report (verification script must PASS).

Until items 1–2 are explicitly approved in a later founder turn, **do not apply Bethesda operations**.
