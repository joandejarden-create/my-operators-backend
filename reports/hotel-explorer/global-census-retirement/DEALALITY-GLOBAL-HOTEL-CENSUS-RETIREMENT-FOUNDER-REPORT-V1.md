# Dealality — Global Hotel Census Retirement Founder Report v1

**Date:** 2026-11-20  
**Stream workspace:** `C:\Dev\dealality\ws-hotel-explorer`  
**PR:** #463 (bounded wave; Integration Controller for merge + cross-stream follow-ons)  
**Canonical HPC:** `appCCUsuGsE1ifoLk` / `tbl9aY5ijiuIzzWam`  
**Legacy Hotel Census:** `tblgj2qEwxjTcg6q0` — **not deleted**

## Authorization honored

- No production Airtable writes  
- No schema changes  
- No production deploy  
- No table deletion  
- No paid research  

## Certification statuses

| Status | Result |
| --- | --- |
| `DEPENDENCY_INVENTORY_COMPLETE` | **YES** — see inventory JSON/MD |
| `HPC_DATA_RECONCILED` | **PARTIAL** — fixture dry-run matrix only (Bethesda / Papagayo); full-table matrix pending |
| `SHARED_RESOLVER_VALIDATED` | **YES** — `dealality-hpc-identity-resolver-v1.js` |
| `PRODUCT_MIGRATIONS_COMPLETE` | **NO** — runtime Scout / Brand Presence / operators / BE footprint / brand-fit migrated; enrichment CLIs + linked-record apply remain |
| `LEGACY_RUNTIME_ACCESS_ZERO` | **PARTIAL** — migrated product paths no longer call Census; CLIs / migration tools still can with opt-in |
| `AIRTABLE_LINKS_MIGRATED` | **NO** — dry-run only; founder apply required later |
| `GLOBAL_RETIREMENT_READY` | **NO** |

## What migrated this wave (HPC runtime)

| Product / surface | Change |
| --- | --- |
| Scout cache | `lib/scout/census-read-cache.js` reads HPC + adapts fields |
| Scout market-coverage / map / signals | Source metadata → HPC |
| Brand Presence list/search | `api/brand-presence.js` → HPC Open\|Pipeline |
| Brand Presence by-id | Already HPC-first ownership (HE-QL-041) |
| Operators by brand/region | `api/operators-by-brand-region.js` → HPC |
| Brand fit analyzer | Hotel query → HPC |
| Brand Explorer censusSummary | `aggregate-presence-summary.js` → HPC Current Brand |
| Shared resolver | `lib/hotel-census/dealality-hpc-identity-resolver-v1.js` |
| Dealality-wide guard | `DEALALITY_LEGACY_HOTEL_CENSUS_MIGRATION_READ` (+ HE alias) |

## Shared resolver assessment

Reuses ADP/GDI alias map + `HPC_LIVE_IDENTITY_COLLISION_V1` + HPC fetch. Does **not** create a parallel DB. Default path never calls legacy Census.

## Secrets Papagayo

Remains **HPC CREATE** candidate with historical provenance `recyX1wLsHBJYRPw7` — not a runtime canonical id (unchanged from HE-QL-041).

## Remaining blockers (IC coordination)

1. **Enrichment / import CLIs** (`scripts/apply-*`, `backfill-*`, `sync-*-census-*`) still target Hotel Census — retarget to HPC Autopilot / shared upsert.  
2. **Full-table reconciliation matrix** (every legacy id → HPC match / CREATE / ambiguous).  
3. **HI linked-record migration** dry-run → founder-approved apply.  
4. **Before/after population comparison** for Scout & Brand Explorer denominators (Census Open vs HPC Radar Status).  
5. **ADP/GDI** opportunity/hotel alias validation without rewriting sales-readiness rules.  
6. **Browser validation pack** on live Radar / Scout / BE / HE after server restart.

## Integration Controller plan

1. Merge #463 wave after CI (preserve unrelated dirty work in HE workspace).  
2. Open follow-on IC task: CLI retarget + full reconciliation + link migration dry-run.  
3. Separate founder auth for any Airtable apply.  
4. Only then evaluate `GLOBAL_RETIREMENT_READY` and table deletion.

## Final recommendation

**Do not delete Hotel Census.** Product runtime for Scout, Brand Presence, operators-by-brand, brand-fit, and BE footprint summaries now targets HPC. Global retirement remains blocked until CLIs, linked records, and full reconciliation clear.
