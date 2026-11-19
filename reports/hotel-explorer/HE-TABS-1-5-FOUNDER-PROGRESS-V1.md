# HE Tabs 1–5 — Founder Progress (Airtable-first)

**Branch:** `feat/he-tabs-1-5-airtable-first-e2e-v1`  
**Paid spend to date:** **$0.00** (authorized ceiling $25 unused — reuse first)

## Identity resolution

| URL param | Resolved hotel | Notes |
|---|---|---|
| `hotelExplorer=recUNycnMwOVFX0hc` | **Krystal Grand Puerto Vallarta** | Golden-demo reference — **not** Bethesda |
| Bethesda Marriott | `recLuxvwwxID7U2B8` | Repeatability / pilot target |

## Tabs in scope (1–5 of 11)

1. Hotel Overview · 2. Ownership Intelligence · 3. Contact Intelligence · 4. Organization & Portfolio · 5. Relationships & Structure  

Tabs 7–9 (Area / Demand / Access) reuse HPC/Radar — not researched here.

## Phase 1 baseline (API inventory)

See `HE-TABS-1-5-FIELD-INVENTORY-V1.md` / `.json` (63 fields × 2 hotels).

| Hotel | Populated / 63 | Fixture-only UI | HI graph rels (before) |
|---|---:|---:|---:|
| KGPV | 37 | 37 | 0 |
| Bethesda | 11 | 11* | 0 |

\*Bethesda “fixture-only” metric overstated Hotel Overview gaps caused by **field-name alias bug** (below).

## Implemented (this cycle)

### 1. Brand-presence Census↔HPC field aliases — **IMPLEMENTED**

Hotel Census rows using HPC-style names (`Rooms / Keys`, `Official Property URL`, `Phone`, `Operator / Management Company`) now map into HE Overview DTOs.

**Before (Bethesda API):** rooms=0, website/phone/management=null  
**After (local HE :8091):** rooms=407, website/phone/management populated from Airtable  

Test: `npm run test:brand-presence-census-field-aliases-v1`

### 2. HI graph persistence — Bethesda — **PERSISTED (INTERNAL_ONLY)**

| Entity | Action |
|---|---|
| Marriott International, Inc. | CREATE org `recStGNNlkZIMgmwK` |
| Operator relationship | CREATE `recxH9JFPubxVBPG9` |
| Brand relationship | CREATE `rec4Bpr6tIcVA2fce` |
| Economic owner | **NOT claimed** |

Report: `HE-TABS-1-5-BETHESDA-HI-GRAPH-PERSIST-V1.json`

### 3. KGPV → HI graph — **BLOCKED (identity)**

`recUNycnMwOVFX0hc` is **Hotel Census–native** (`tblgj2qEwxjTcg6q0`).  
HI `HPC Hotel` link targets **Hotel Property Census** (`tbl9aY5ijiuIzzWam`).  
Airtable rejects the link (`ROW_TABLE_DOES_NOT_MATCH_LINKED_TABLE`).  
GSF orgs may exist from partial migrate; **relationships held** until Census↔HPC identity unification (no duplicate hotel created).

### 4. Airtable graph reader + ownership overlay — **IMPLEMENTED**

- `lib/.../he-airtable-graph-reader-v1.js`
- `GET /api/he-airtable-graph/hotel/:recordId`
- Golden-demo ownership endpoint overlays HI graph for out-of-cohort hotels (`AIRTABLE_GRAPH_PARTIAL`)

### 5. Bethesda ownership research — **STAGED (not current claim)**

Free web evidence (2013): Northview Hotel Group + Oaktree affiliate acquisition.  
**Current economic owner = `NOT_VERIFIED`** (stale for current assertion).  
Staged: `HE-TABS-1-5-BETHESDA-OWNERSHIP-RESEARCH-STAGED-V1.json`  
Paid spend: $0

## Persistence vs UI

| Content | UI before | Canonical Airtable now |
|---|---|---|
| Bethesda rooms/website/phone/operator | Missing via API bug | Already in HPC/Census — **API fix exposes them** |
| Bethesda Operator/Brand graph | Empty | **Persisted** HI Organizations + Relationships |
| Bethesda Economic owner | Empty | Still unresolved (staged historical lead) |
| KGPV ownership tabs | Rich fixtures | **Not** HI-graph linked (identity blocker) |
| Contacts | KGPV showcase JSON | Not migrated this pass |

## Visual validation (`:8091`)

| Check | Result |
|---|---|
| Bethesda Hotel Overview | **PASS** — 407 rooms, Upper Upscale, Marriott Hotels, Operator Marriott International, website/phone links |
| Bethesda Ownership (Airtable graph) | **PASS** — Operator from HI graph; Economic owner Not verified; 2 graph relationships; no fabricated owner |
| KGPV Ownership (fixtures) | **PASS** earlier on `:8080` (rich cohort) |
| Tabs 6–9 regression | Not redesigned; Radar embed path unchanged |
| Integration `:8080` | **SKIP_INTEGRATION_DIRTY** — missing ADP module in dirty primary checkout |

## Before → after (Bethesda tabs 1–5 field inventory)

| Metric | Before | After |
|---|---:|---:|
| Populated / 63 | 11 | **21** |
| HI graph relationships | 0 | **2** (Operator + Brand) |
| Overview rooms/website/phone | Missing via API | **Populated from Airtable** |
| Paid spend | — | **$0** |

## Remaining gaps (priority)

1. Unify KGPV identity onto HPC (or retarget HI link) — schema/governance decision.  
2. Confirm **current** Bethesda economic owner (SDAT / filings) — may use paid ≤$25 if free sources fail.  
3. Migrate KGPV contacts showcase → HI Org Contacts / Channels when linkable.  
4. Organization/Relationships UI richness for Bethesda beyond operator/brand rows.  
5. Repeatable refresh workflow packaging + PR merge.

## Milestone legend (honest)

| Milestone | Status |
|---|---|
| Implemented (code) | Partial — aliases, reader, overlay, scripts |
| Researched | Partial — Bethesda historical ownership staged |
| Verified (current) | Operator/brand from HPC; economic owner not verified |
| Persisted (Airtable) | Bethesda operator/brand graph yes; KGPV rels blocked |
| Visually validated | KGPV ownership yes; Bethesda post-fix pending :8091 |
| Merged | **Not yet** |
| Deployed | **Not** |
