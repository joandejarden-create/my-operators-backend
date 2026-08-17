# Production Census Adapter Wave 2

**Status:** `production_census_adapter_wave_2_partial_steward_remaining`

Accor, Wyndham, and Preferred Autopilot discovery adapters are wired. Production-cycle applied **150 inserts** and **113 updates** to Hotel Property Census (`tbl9aY5ijiuIzzWam`). **14** insert candidates remain stewarded (Marriott missing city).

## Production target (guard)

| | |
| --- | --- |
| Base | Deal Capture Platform |
| Table | Hotel Property Census |
| Table ID | `tbl9aY5ijiuIzzWam` |
| Brand Setup / Brand Explorer | read-only |
| VIC | evidence / dedupe only |
| Old / legacy Census | blocked |
| Webhound | not invoked (learning only) |

## Adapters built

| Parent | Module | Source |
| --- | --- | --- |
| Accor | `census-autopilot-accor-cala-discovery-adapter.js` | Continent browse + Catalog API hydrate |
| Wyndham | `census-autopilot-wyndham-cala-discovery-adapter.js` | Property sitemaps + JSON-LD `addressCountry` |
| Preferred | `census-autopilot-preferred-directory-discovery-adapter.js` | Official `/directory` `__NEXT_DATA__` |

**Parent inference (read-only):** `census-autopilot-parent-inference.js` — slug→parent for Autopilot routing when Brand Setup Parent Company is blank. Does not write Brand Setup.

**Deferred:** BWH / SLH / Bunkhouse.

## Controlled smokes

| Scope | Discovered | Existing matches | New candidates | Writes |
| --- | ---: | ---: | ---: | --- |
| Preferred | 61 | — | 61 | no |
| Accor | 77 | — | 77 | no |
| Wyndham | 80 | — | 80 | no |
| Active Brand Setup (all) | 561 | 397 | 164 | no |

New candidates by family (full controlled): Hilton 5 · Marriott 27 · Accor 49 · Wyndham 22 · Preferred 61.

## Production-cycle

**Run:** `reports/research-engine-v2/autopilot/2026-08-06T11-34-37_CALA-production-cycle`

| Metric | Value |
| --- | ---: |
| Records before | 757 |
| Records after | 907 |
| Inserts applied | 150 |
| Updates applied | 113 |
| Steward cases | 42 |
| Steward inserts remaining | 14 |
| Runtime | ~19.8 min |

**Inserts by family:** Hilton 5 · Marriott 13 · Accor 49 · Wyndham 22 · Preferred 61

**Update fields:** Address, Asset Context, Market / Submarket, Property Type

**Steward:** Marriott `missing_or_unknown_city` — not auto-inserted.

## Countries searched (priority)

Mexico · Dominican Republic · Costa Rica · Colombia · Panama

## Blocked patterns

- OTAs; Accor short continent 403 paths; Wyndham path-keyword CALA guesses; Preferred collections as brands; owner/operator/dates; Recent Momentum; Company Validated / Brand Verified; fuzzy auto-insert

## Next adapter recommendation

Defer BWH (403 property pages) / SLH / Bunkhouse. Next value: Accor Brazil depth beyond priority five, and resolve Marriott missing-city steward queue.

## Validation

- `npm run test:census-autopilot` — pass (7 parents × 5 countries = 35 supported)
- Production-cycle checkpoints present under run folder
- Brand Setup / Brand Explorer / VIC / old Census — not written

## Change impact

**High** — Hotel Property Census inserts + enrichment updates.

**Rollback:** soft-delete or filter new records by Discovery Date / identity keys `ind_accor_*`, `ind_wyndham_*`, `ind_preferred_*` from this cycle; disable Wave 2 families in region coverage if needed.
