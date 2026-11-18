# Implementation plan (post–founder approval only)

**Do not execute production writes from this audit PR.**

## After founder approves table-purpose / target-state matrix

1. **Reuse PR #356 foundation**  
   - Contract + dry-run planner + read-back outcomes  
   - Keep `AIRTABLE_PRODUCTION_WRITE_GUARD` until explicit write-set auth

2. **Apply contract corrections (code, still fail-closed)**  
   - Match Key identity for Matches table  
   - Explicit Full E2E deny-list: need-date / GDI commercial / Decisions / facility HI / ADP measurement  
   - Research Targets policy per D3  
   - Provenance preservation for `generic_seed` rows

3. **Authorized write capability**  
   - Idempotent upserts by stable IDs  
   - Linked-record integrity (DG before Program/Signal/Fit/GDO as needed)  
   - Safe retry + outcome ledger (CREATED/UPDATED/UNCHANGED/DUPLICATE_LINKED/DEFERRED/FAILED)  
   - Read-back certification before success

4. **Bethesda selective pilot**  
   - Re-run dry-run on processed-20  
   - Publish exact write manifest (IDs + tables)  
   - Separate founder authorization of that manifest  
   - Apply + read-back  
   - Confirm publication CURRENT pointers unchanged  
   - Confirm synthetic/provisional need periods untouched

5. **Multi-hotel regression (read-only + dry-run)**  
   - Bethesda, Guadalajara, San Diego, Castillo, YOTEL, Radisson, Waterstone  
   - Assert no cross-hotel opportunity linkage  
   - Assert deny-list tables not planned

6. **Observability**  
   - Persist dry-run/apply receipts under `reports/`  
   - Link Full E2E checkpoint `airtablePersistence*` to FS ledger

## Explicit non-goals until further approval

- Schema migrations / field deletions  
- Table merges  
- Deleting synthetic records  
- Backfill of 165 Bethesda raw Exa candidates  
- Automatic CURRENT publication updates  
- Paid research

## Sequencing note

Audit merge ≠ activation. Activation requires:

1. Founder approval of `FOUNDER_DECISIONS_REQUIRED.md`  
2. Implementation PR for routing/guards  
3. Separate authorization of the precise production write set
