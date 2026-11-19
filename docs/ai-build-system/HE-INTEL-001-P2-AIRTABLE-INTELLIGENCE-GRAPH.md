# HE-INTEL-001 P2 — Airtable Intelligence Graph

**Contract:** `HOTEL_INTELLIGENCE_AIRTABLE_GRAPH_V1` (`1.0.0-p2`)  
**Base:** `appa2cE7FTRmIbB32` (canonical intelligence / ADP)  
**Engine:** extends `HOTEL_INTELLIGENCE_RESEARCH_ENGINE_V1`  
**Paid providers:** disabled ($0)  
**Bulk research data writes:** disabled in P2  

## Tables

| Table | Purpose | Primary key field |
| --- | --- | --- |
| HI Organizations | Canonical orgs (`dle_…`) | Organization ID |
| HI Hotel–Org Relationships | HPC × org × role | Relationship ID |
| HI Org Contacts | Person × org affiliation (`dlp_…`) | Person ID |
| HI Contact Channels | Email/phone with verification class | Channel ID |
| HI Ownership Research Runs | Run ledger / resume | Run ID |

**Reuse:** `Hotel Intelligence Evidence` (`tblHxS8x0niDEGVsh`) — extend Entity Type choices for graph entities.

## Publication governance

Layers are separate: discovery → evidence validation → canonical approval → Airtable persistence → internal HE publication → customer publication.

Airtable row creation defaults to `INTERNAL_ONLY` / `CUSTOMER_BLOCKED`. Never auto-publishes to customers. HPC Owner/Operator and Radar Status are **not** written by this graph.

## Migration

```bash
npm run ensure:he-intel-001-p2-airtable-graph-schema -- --dry-run
npm run ensure:he-intel-001-p2-airtable-graph-schema -- --apply
```

Non-destructive: create tables/fields/links only; never delete or replace existing fields. Unexpected name variants → STOP.

## Upsert planner

`planGraphUpserts` + synthetic `createSyntheticGraphStore`:

- Deterministic dedupe keys (no name-only merge)
- Stronger-evidence-wins; equal-confidence conflicts held for review
- Dry-run diffs with zero writes
- Apply requires founder auth + synthetic/test allow (bulk production writes off)
- Read-after-write reconciliation
- Interrupt/resume safety

## Tests

```bash
npm run test:he-intel-001-p2-airtable-graph-v1
```

## Explicit non-goals (P2)

- No paid Exa / Parallel / Surfe / PDL  
- No bulk pilot ownership research apply  
- No claim that live ownership/contact research is complete  
- No customer publication  
- P3 = research-method cost/value benchmark  

## Rollback / recovery

- Schema: leave tables in place (non-destructive). Empty unused tables manually only with founder auth.  
- Data: synthetic store is test-only. Production data apply remains gated off.  
- Resume: research runs carry case/run IDs + checkpoint refs; upsert planner is idempotent by dedupe key.
