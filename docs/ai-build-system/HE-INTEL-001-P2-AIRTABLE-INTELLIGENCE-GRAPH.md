# HE-INTEL-001 P2 — Airtable Intelligence Graph

**Contract:** `HOTEL_INTELLIGENCE_AIRTABLE_GRAPH_V1` (`1.0.1-p2-hpc-base`)  
**Canonical base:** `appCCUsuGsE1ifoLk` (same as Hotel Property Census `tbl9aY5ijiuIzzWam`)  
**Forbidden base for HI graph:** `appa2cE7FTRmIbB32`  
**Paid providers:** disabled ($0)  
**Bulk research data writes:** disabled in P2  

## Tables (HPC base)

| Table | Purpose |
| --- | --- |
| HI Organizations | Canonical orgs (`dle_…`) |
| HI Hotel–Org Relationships | HPC × org × role (linked to Census) |
| HI Org Contacts | Person × org affiliation (`dlp_…`) |
| HI Contact Channels | Email/phone with verification class |
| HI Ownership Research Runs | Run ledger / resume (linked to Census) |
| HI Graph Evidence | Field-level graph evidence on HPC base |

Live IDs: `reports/hotel-explorer/HE-INTEL-001-P2-live-schema-ids-v1.json` and `lib/hotel-intelligence/research-engine-v1/airtable-graph-table-ids.generated.json`.

## Direct HPC linkage (mandatory)

Native Airtable linked-record fields on **HI Hotel–Org Relationships**:

| Field | Type | Target |
| --- | --- | --- |
| **HPC Hotel** | `multipleRecordLinks` | Hotel Property Census `tbl9aY5ijiuIzzWam` |
| **HPC Hotel ID** | `singleLineText` | Canonical HPC record id (text twin; preserved) |
| **Organization** | `multipleRecordLinks` | HI Organizations |

Cardinality: **exactly one** HPC hotel per relationship — enforced by application validation (`airtable-graph-referential-integrity.js`), not Airtable alone.

Reciprocal on Census (Airtable-generated; rename-only, no duplicates on rerun):

- **HI Organization Relationships** → HI Hotel–Org Relationships

Referential integrity before create/update:

1. Resolve canonical HPC record in `appCCUsuGsE1ifoLk`
2. Verify hotel exists
3. Resolve canonical HI Organization
4. Confirm org exists or approved creation
5. Write relationship with both native links
6. Read back both linked-record references
7. Confirm stored **HPC Hotel ID** matches linked HPC record
8. Reject/hold orphan, ambiguous, or mismatched rows

Synthetic pilot demo (not verified research):

```bash
npm run demo:he-intel-001-p2-hpc-link-synthetic -- --dry-run
npm run demo:he-intel-001-p2-hpc-link-synthetic -- --apply
```

Evidence: `reports/hotel-explorer/HE-INTEL-001-P2-hpc-link-synthetic-demo-v1.json` (Wyndham / Renaissance / Westin → one synthetic HEI org; second apply is UPDATE-only).

## Evidence architecture

- **HI Graph Evidence** lives on the HPC base so ownership/org/person/channel/run evidence stays with Census-linked identity.
- **ADP `Hotel Intelligence Evidence` (`tblHxS8x0niDEGVsh` on `appa2cE7FTRmIbB32`)** is preserved — do not delete or mutate without separate founder approval.

## Base gate

`assertCanonicalGraphBase` / `resolveGraphBaseId` refuse the forbidden ADP/intelligence base. Schema ensure and future writers must verify `appCCUsuGsE1ifoLk` before writes.

## Migration

```bash
npm run ensure:he-intel-001-p2-airtable-graph-schema -- --dry-run
npm run ensure:he-intel-001-p2-airtable-graph-schema -- --apply
```

Misplaced empty tables on the wrong base (if still present) require **manual Airtable UI deletion** — Meta API returns `404 NOT_FOUND` for `DELETE /tables/{id}`.

```bash
node scripts/delete-he-intel-001-p2-misplaced-graph-tables.mjs --dry-run
# apply only after HPC tables verified; API deletion may still fail → manual UI
```

## Tests

```bash
npm run test:he-intel-001-p2-airtable-graph-v1
```
