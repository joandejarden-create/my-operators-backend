# Airtable Canonicalization + Duplicate Audit V1 — Founder Report

Generated: 2026-09-29  
Branch: `deploy/gdi-pe-v1-7-customer-closure`  
Audit base of work: `appa2cE7FTRmIbB32` (platform) · HPC `appCCUsuGsE1ifoLk` · forbidden MVP `appvtnDurnMSjINP6`

Artifacts:
- `reports/airtable-canonicalization-v1/AUDIT.json`
- `reports/airtable-canonicalization-v1/META_TABLES.json`
- `reports/airtable-canonicalization-v1/WRITE_PATHS.json`
- `reports/airtable-canonicalization-v1/CLEAN_RECONSTRUCTION.json`
- `scripts/test-airtable-canonicalization-v1.mjs` (PASS)

**No tables deleted. No rows deleted. No rows deactivated** (none required).

---

## A. TABLE INVENTORY (keyword / expected)

| Table | ID | Base | Count (sampled) | Canonical? | Reads | Writes |
|---|---|---|---|---|---|---|
| Hotel ADP Attributes | `tblMA6v0HAmsY9ImW` | `appa2cE7…` | Bethesda+AC+Spice scoped | **YES** | HI/ADP profile builders | `syncHotelAdpAttributesToAirtable` |
| Hotel Commercial Profiles | `tblBYJtKj6oi4J0Ow` | platform | per-hotel | YES | HI loaders | `applyHotelIntelligencePacket` |
| Hotel Event Spaces | `tblDwW2jpbKBBtpko` | platform | per-hotel | YES | HI | HI apply |
| Hotel Demand Nodes | `tbl5xEOk7Hnr5Iebq` | platform | per-hotel | YES | HI | HI apply |
| Hotel Seasonality & Need Periods | `tbllCSH31bxsXM3C0` | platform | per-hotel | YES | HI | HI apply |
| Hotel Intelligence Evidence | `tblHxS8x0niDEGVsh` | platform | per-hotel | YES | HI | HI apply |
| Hotel Demand Generator Fit | `tblqamXbIk9n6gXfV` | platform | AC 7 / Spice 7 / Beth 20 | YES | GDI | `upsertHotelGeneratorFit` |
| GDI Research Targets | `tblVyuEf5vjooWDKX` | platform | AC 14 / Spice 14 / Beth 53 | YES | GDI | `upsertResearchTarget` |
| GDI Research Runs | `tblzNMUIo2T6onaKH` | platform | AC 2 / Spice 1 / Beth 5 | YES | GDI | `upsertResearchRun` |
| GDI Research Target Runs | `tblSA0cFVplNfWMjp` | platform | present | YES | GDI | `upsertTargetRun` |
| **Group Demand Opportunities** | `tblRuReslJMwsfRQj` | platform | (hotel-scoped via Hotel ID) | YES | GDI UI/API | opportunity store |
| Demand Generators / Programs / Signals | expected IDs match | platform | org-global | YES | GDI | DG stores |
| Private Event Venues / Hotel Venue Fit / Signals | expected IDs match | platform | PE graph | YES | GDI PE | PE stores |
| Decisions / Decision Events | expected IDs match | platform | decision layer | YES | outcomes | decision store |
| Hotel Property Census | `tbl9aY5ijiuIzzWam` | **HPC** `appCCUsu…` | census | YES (HPC) | identity | census stewardship |
| Demand Anchors / Centers / Categories | HPC base | HPC | census ontology | **DIFFERENT_CONCEPT** | census | census (not HI) |

Platform base table count: **29**. HPC: **22**. Legacy MVP readable: **102** (no HI/GDI attribute twin found by name scan).

All expected **Hotel Intelligence** table IDs: **MATCH**.  
Expected GDI IDs: **MATCH** except founder label “GDI Opportunities” → live name **Group Demand Opportunities** (same ID `tblRuReslJMwsfRQj`).

---

## B. POSSIBLE DUPLICATE ATTRIBUTE TABLES

| Table | ID | Purpose | Classification |
|---|---|---|---|
| Hotel ADP Attributes | `tblMA6v0HAmsY9ImW` | Derived ADP consumption attributes | **CANONICAL** |

**No second attribute table** on platform.  
Legacy MVP: no Hotel ADP Attributes / Hotel Attributes twin found in interesting-name scan.

---

## C. HOTEL ADP ATTRIBUTE RECORD AUDIT

| Hotel | Total | Active | Duplicate Active | Legacy | Clean |
|---|---|---|---|---|---|
| Bethesda Marriott | 91 | 46 | **0** | 0 active-key dupes; 45 inactive = supersession history | **YES** |
| AC Hotel A Coruña | 39 | 39 | **0** | 0 | **YES** |
| Spice Island Beach Resort | 47 | 47 | **0** | 0 | **YES** |

Notes:
- Dedupe key = `HPC::category::name::version` (`adp_attr_v1`).
- Bethesda inactive rows are **intentional** leftovers from idempotent sync (deactivate when attribute set changes) — **not** duplicate active keys.
- `legacyV1Keys` heuristic in raw audit counted `adp_attr_v1` (current version) — **false positive**; ignore.

---

## D. GDI FIT / TARGET DUPLICATION

| Hotel | Fits Reported | Unique Fits | Targets Reported | Unique Targets |
|---|---|---|---|---|
| AC Coruña | 7 | **7** | 14 | **14** |
| Spice Island | 7 | **7** | 14 | **14** |
| Bethesda | 20 | 20 | 53 | 53 |

AC/Spice “7 / 14” are **unique records**, not doubled rows.

---

## E. WRITE PATHS

| Logical Object | Writer | Current/Legacy | Upsert Safe? |
|---|---|---|---|
| Hotel ADP Attributes | `syncHotelAdpAttributesToAirtable` | CURRENT | YES |
| HI tables | `applyHotelIntelligencePacket` | CURRENT | YES |
| Hotel Demand Generator Fit | `upsertHotelGeneratorFit` | CURRENT | YES |
| Research Targets / Runs | research-coverage airtable-stores | CURRENT | YES |
| Opportunities | opportunity-persistence → Group Demand Opportunities | CURRENT | YES |
| HPC census | ALT base writers | CURRENT (HPC) | YES |

**DOUBLE_WRITE_RISK:** multiple scripts call the **same** upsert — not two schemas. Classification: **IDEMPOTENT_UPSERT** / **CURRENT_WRITE**.

---

## F. WRONG BASE

WRONG-BASE WRITES FOUND (HI/GDI/ADP Attributes): **0**

LEGACY BASE WRITERS STILL ACTIVE for HI/GDI/ADP: **0**  
(Guards: `assertNotLegacyMvpCanonicalBase` on GDI/HI/opportunity/decision stores. Product `AIRTABLE_BASE_ID` may still be MVP for unrelated CRM — fail-closed for intelligence.)

HPC writes intentionally → `appCCUsuGsE1ifoLk`.  
HI / ADP Attributes / GDI → `appa2cE7FTRmIbB32`.

---

## G. REMEDIATION

| Action | Count |
|---|---|
| TABLES DELETED | **0** |
| ROWS DELETED | **0** |
| ROWS DEACTIVATED | **0** (none needed) |
| WRITE PATHS DISABLED | **0** |
| READ PATHS REMOVED | **0** |

Recommendation only (not executed): keep Bethesda inactive attribute history for audit; optional future archive filter in UI.

---

## H. CLEAN RECONSTRUCTION

| Hotel | Result |
|---|---|
| Bethesda | **PASS** |
| AC | **PASS** |
| Spice | **PASS** |

All: HPC bind · HI commercial from Airtable · ADP attrs no multi-active key · GDI config/fits/targets · ADP Live published.

---

## I. DIRECT ANSWERS

1. Is Hotel ADP Attributes present more than once? **NO** (one table).
2. If yes, which is canonical? **N/A — only `tblMA6v0HAmsY9ImW`.**
3. Duplicate active table or legacy table? **Neither for attributes.** Naming alias only for Opportunities.
4. Duplicate active attribute records? **NO** for Bethesda/AC/Spice.
5. AC’s 7 fits unique? **YES.**
6. AC’s 14 targets unique? **YES.**
7. Spice’s 7 fits unique? **YES.**
8. Spice’s 14 targets unique? **YES.**
9. Is any code writing twice? **Same upsert called from multiple entrypoints — safe idempotent, not dual schema.**
10. Still writing to legacy base for HI/GDI/ADP? **NO.**
11. Can one duplicate source safely be retired? **No duplicate source to retire.** Optional: document Opportunities display name; optional UI hide inactive ADP attrs.
12. Bethesda/AC/Spice clean after remediation? **YES** (no remediation required).

---

## J. FINAL VERDICT

**AIRTABLE CANONICALIZATION PASSES — NO ACTIVE DUPLICATION**

---

## WHAT FOUNDER SEES IN AIRTABLE

| Layer | Base | Where |
|---|---|---|
| HPC | `appCCUsuGsE1ifoLk` | Hotel Property Census |
| Hotel Intelligence | `appa2cE7FTRmIbB32` | Commercial Profiles, Event Spaces, Demand Nodes, Seasonality, Evidence |
| ADP Attributes | `appa2cE7FTRmIbB32` | **Hotel ADP Attributes** (only one) |
| GDI | `appa2cE7FTRmIbB32` | Fits, Targets, Runs, Generators, **Group Demand Opportunities** |

### Looks duplicative but is different
- **Hotel Demand Nodes** ≠ **Hotel Demand Generator Fit**
- **Hotel Demand Nodes** ≠ HPC **Demand Anchors/Centers**
- **Hotel ADP Attributes** ≠ Commercial Profile columns
- Docs **“GDI Opportunities”** = Airtable **“Group Demand Opportunities”** (same ID)

### Bethesda “lots of attribute rows”
46 active + 45 inactive = sync history, not double-active keys. Filter `Active? = checked` for current set.

---

STOP. No new GDI discovery. No new hotel onboarding this pass.
