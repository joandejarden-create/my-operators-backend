# HE 5.0 — Legacy Hotel Census Independence Audit (v1)

**Date:** 2026-11-20  
**Stream:** Hotel Explorer (`ws-hotel-explorer`)  
**PR:** #463  
**Canonical HPC:** `appCCUsuGsE1ifoLk` / `tbl9aY5ijiuIzzWam`  
**Legacy table (do not delete):** `Hotel Census` / `tblgj2qEwxjTcg6q0`  
**Production Airtable apply:** NOT authorized  
**Paid research:** not used

## Certification statuses

| Status | Result |
| --- | --- |
| `DEPENDENCIES_FOUND` | **YES** — inventory below |
| `MIGRATION_REQUIRED` | **YES** — cross-product + brand-presence list/search still on Census |
| `HE_LEGACY_INDEPENDENT` | **YES** — HE 5.0 research / identity / dry-run / property ownership (HPC hotels) |
| `CROSS_PRODUCT_RETIREMENT_BLOCKED` | **YES** — Scout, Brand Explorer metrics, operators-by-brand-region, census enrichment CLIs |
| `GLOBAL_RETIREMENT_READY` | **NO** |

**Final assessment:** Hotel Census must **not** be deleted yet. Hotel Explorer production-intended HE 5.0 / HPC identity paths can run with zero legacy-table requests. Global deletion awaits cross-product migration owned with Integration Controller.

---

## 1. Hard access guard

| Item | Location |
| --- | --- |
| Guard module | `lib/hotel-intelligence/hpc-population/legacy-hotel-census-access-guard-v1.js` |
| Table ID | `tblgj2qEwxjTcg6q0` |
| Opt-in | `HE_LEGACY_HOTEL_CENSUS_MIGRATION_READ=1` + `allowLegacyHotelCensusRead` (GET only) |
| Error code | `LEGACY_HOTEL_CENSUS_ACCESS_BLOCKED` |
| Instrumented log | `getLegacyCensusAccessLog` / `summarizeLegacyCensusIndependence` |
| fetch wrapper | `guardFetch` |

Wired into:

- `hotel-census-identity-search-v1.js` (migration-only)
- `mullen-pilot-identity-resolution-v1.js` (skips census unless opt-in)
- `property-census-identity-v1.js` (HPC-first; census probe only with opt-in)

---

## 2. HE runtime dependency inventory (executable)

| File | Classification | Notes / replacement |
| --- | --- | --- |
| `hpc-population/legacy-hotel-census-access-guard-v1.js` | LOOKUP/FALLBACK (block) | New guard |
| `hpc-population/hotel-census-identity-search-v1.js` | IMPORT/MIGRATION (GET only when opted in) | Blocked by default |
| `hpc-population/mullen-pilot-identity-resolution-v1.js` | LOOKUP → HPC-only | Uses `hpc-live-identity-collision-v1` |
| `hpc-population/mullen-pilot-cohort-v1.js` | HISTORICAL/INACTIVE outcome enum | `MATCHED_HOTEL_CENSUS` retained for migration tooling |
| `property-census-identity-v1.js` | LOOKUP → HPC-first | Census only with migration flag |
| `he-5-0-full-e2e/*` | READ/UPSERT dry-run on HPC | Papagayo = CREATE; no census table destination |
| `api/brand-presence.js` `getBrandPresenceHotelById` | READ via ownership resolver | HPC hotels: zero census probe |
| `api/brand-presence.js` list/search (`F.hotels.table`) | READ · **still Census** | **MIGRATION_REQUIRED** for full HE product search |
| `api/hotel-explorer-submarket-snapshot.js` | READ HPC | Independent |
| `lib/.../he-hpc-submarket-snapshot-v1.js` | READ HPC | Independent |
| `public/js/hotel-detail-panel.js` | LOOKUP/FALLBACK Scout copy | Enrichment language; Submarket SoT = HPC |
| `tmp/he50-identity-conflict-probe-v1.mjs` | HISTORICAL/INACTIVE | Local probe; not HE runtime |

### HE 5.0 confirmed operations (after this change)

| Workflow | Legacy GET | POST | PATCH/PUT | DELETE | Fallback |
| --- | --- | ---: | ---: | ---: | --- |
| Mullen identity (default) | 0 | 0 | 0 | 0 | none |
| HE 5.0 supervisor / dry-run | 0 | 0 | 0 | 0 | none |
| Property ownership (HPC id) | 0 | 0 | 0 | 0 | none |
| Papagayo plan | provenance only | — | — | — | HPC CREATE |

---

## 3. Cross-product consumers (blocks global retirement)

| Product / surface | File(s) | Class | Owner |
| --- | --- | --- | --- |
| Scout market coverage | `api/scout-market-coverage.js` | READ | Scout / Radar · IC |
| Scout opportunity signals | `api/scout-opportunity-signals.js` | READ | Scout · IC |
| Brand presence list/geo | `api/brand-presence.js` | READ | HE + Radar shared · IC |
| Brand fit analyzer | `api/brand-fit-analyzer.js` | READ | HE/Brand · IC |
| Brand library census metrics | `api/brand-library.js` | READ | Brand Explorer · IC |
| Operators by brand/region | `api/operators-by-brand-region.js` | READ | Operator Explorer · IC |
| Operator census footprint | `api/operator-census-footprint.js` | READ | Operator Explorer · IC |
| Brand presence summary | `api/brand-presence-summary.js` | READ | BE/HE · IC |
| Match score server | `api/match-score-server.js` | LOOKUP | Match · IC |
| Hotel census field map | `lib/hotel-census/fields.js` | RELATIONSHIP | Shared · IC |
| Census enrichment CLIs | `scripts/sync-hilton-census-*.mjs`, `run-*-census-*-backfill.mjs`, etc. | UPDATE/IMPORT | Census Autopilot · IC |
| BE SoT tests | `scripts/test-be-airtable-integration-sot-v1.mjs` | TEST ONLY | Brand Explorer |
| Gate1 KGPV provenance | `scripts/gate1-he-tabs-1-5-*.mjs` | HISTORICAL + live Census for KGPV | HE demos · IC |
| Radar internal preview | `radar-internal-preview-hotels-v1.js` | LOOKUP/FALLBACK comments | Radar · IC |

**Migration plan (cross-product):** each consumer moves to HPC (`tbl9aY5ijiuIzzWam`) or an approved derived rollup; Integration Controller sequences shared API changes; HE does not mutate Scout/BE write paths in this PR beyond documenting blockers.

---

## 4. Secrets Papagayo migration plan

| Item | Value |
| --- | --- |
| Legacy census id | `recyX1wLsHBJYRPw7` (**provenance only**) |
| Prior plan | `STEWARD_PROMOTE_FROM_CENSUS` |
| New plan | `CREATE_AFTER_FOUNDER_APPROVAL` → HPC `CREATE` `M-NEW-secrets_papagayo` |
| Identity outcome | `NEW_HPC_CANDIDATE` |
| Rooms | 202 HIGH (Hyatt Inclusive) |
| Duplicate check | Live HPC collision must remain absent at apply time |
| Runtime canonical id | **none until founder-approved CREATE** |
| Do not | Call legacy table; use census id as HE hotel id; invent second hotel |

---

## 5. HE 5.0 dry-run updates

- Papagayo mutation: `CREATE` on HPC with `legacyHotelCensusProvenanceId`
- `legacyCensusRuntimeDependency: false`
- Reconciliation op `M-NEW-secrets_papagayo` (was promote)
- Scorecard: `matchedCensus: 0` for pilot packets

---

## 6. Instrumented zero-access proof

```bash
npm run test:he-legacy-hotel-census-independence-v1
npm run test:he-5-0-full-e2e-pilot-v1
npm run test:he-property-census-identity-v1
```

Asserts: blocked GET/POST/PATCH/PUT/DELETE; `searchHotelCensusIdentityCandidates` fails closed; dry-run has no promote-from-census; supervisor `matchedCensus === 0`; Mullen path `censusSkipped` + empty census ids; Bethesda ownership without census probe.

---

## 7. Quality gate

- `HE-QL-041` / `LEGACY_HOTEL_CENSUS_INDEPENDENCE_GATE`
- Docs: `HOTEL_EXPLORER_QUALITY_STANDARD.md` §9c
- Registry: `HOTEL_EXPLORER_FOUNDER_QUALITY_LESSONS_REGISTRY.json` v1.26

---

## 8. Browser validation note

Founder Preview for **Bethesda** (`recLuxvwwxID7U2B8`) and other **HPC** ids uses ownership resolver → HPC only (no census probe). Full product **search/list** via brand-presence still hits Hotel Census until that shared API is migrated — recorded under `MIGRATION_REQUIRED` / `CROSS_PRODUCT_RETIREMENT_BLOCKED`. Authenticated Radar acceptance not claimed PASS solely from this audit.

---

## 9. Safety checklist

- [x] No deletion of Hotel Census table  
- [x] No production Airtable apply  
- [x] No production deploy  
- [x] No paid research  
- [x] Unrelated dirty work preserved  
- [x] HE research path independent of legacy table  
- [ ] Global retirement — **blocked**
