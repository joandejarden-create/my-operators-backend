# SEC EDGAR V2.3 — Permanent Discovery Inventory Delivery

**PR:** #492  
**HEAD (pre-push):** pending commit  
**Paid research spend:** $0  
**Customer publication:** BLOCKED  
**Merge / deploy:** NOT performed  

## Schema additions (exact)

| Table | Airtable ID | Action |
|---|---|---|
| HI Hotel Discovery Inventory | `tblx77Nmh56h4xa9z` | CREATED |
| HI Discovery Findings | `tblGjTWNm8G4UOF1e` | CREATED |

- Base: `appCCUsuGsE1ifoLk` (canonical HPC)
- HPC core columns: **unchanged**
- HI Graph Evidence schema: **unchanged**
- Diff artifacts: `SCHEMA_DIFF.json`, `SCHEMA_APPLY.json`

### Graph Evidence reuse decision

**DO_NOT_REUSE_FOR_UNMATCHED** — HI Graph Evidence is HPC-linked Sources SoT; unmatched discovery must not pollute customer evidence readers. New `HI Discovery Findings` holds pre-match intelligence.

## Schema read-back

Post-apply meta read-back confirmed both tables + field sets present. Generated registry:  
`lib/hotel-intelligence/sec-edgar/discovery-inventory-table-ids.generated.json`

## Migration

| Cohort | Input | Unique | Imported | Notes |
|---|---|---|---|---|
| A VERIFIED_PROPERTY | 18 | 18 | 18 | Lifecycle `IDENTITY_VERIFIED` |
| B PROBABLE_PROPERTY | 71 | 71 | 71 | Lifecycle `DISCOVERY_PERSISTED` / RESEARCH_ONLY |
| Findings | 10 | 10 | 10 | Discrete rows (not JSON blobs) |
| Duplicates prevented | — | — | 0 | Already unique by propertyId |
| Read-backs | — | — | **99/99 OK** | |

Retained in filesystem archive (not imported as properties):

- 1,124 AMBIGUOUS
- 21 MULTIPLE_PROPERTIES
- 378 rejected artifacts (generic + extraction)

## Post-import quality quarantine

6 imported rows had legal-suffix city bleed (`Ltd. …`, `L.P. …`). Demoted to `PROBABLE_PROPERTY` + `QUARANTINED` so VERIFIED precision is not silently weakened.

**Post-quarantine Airtable standing:** 89 inventory rows; ~12 remain VERIFIED_PROPERTY; findings 10.

## HPC reconciliation

| Metric | Result |
|---|---|
| HPC matches linked | 0 (none in migration cohort had hpcRecordId) |
| HPC fields enriched | **0** (no `--apply` enrichment; dry-run plan only) |
| Customer publication | BLOCKED |

## Remaining evidence-quality gaps

1. 1,124 ambiguous candidates need address / alias / brand / room-count evidence  
2. City/legal-suffix bleed in older schedule parsers (gate now blocks VERIFIED promotion)  
3. Property-level franchise expiration years scarce in recent DiamondRock 10-K portfolio tables  
4. Hotel-specific EX-10 content filtering still needed (employment/credit exhibits common)  
5. EX-21 SPE → property ownership linking not yet automated  

## Commands / tests

```bash
npm run test:sec-edgar-discovery-inventory-v23
npm run test:sec-edgar-identity-quality-v2
```

## Governance

- HE 5.0 dirty work preserved (not in this commit)
- No merge, deploy, or customer-facing publish
- Airtable `Publication State` = CUSTOMER_BLOCKED on all migrated rows
