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
