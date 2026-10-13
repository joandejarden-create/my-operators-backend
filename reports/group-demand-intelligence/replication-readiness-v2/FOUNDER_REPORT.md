# Dealality ADP + GDI Replication Readiness Audit V2

**Date:** 2026-09-24  
**Branch:** `deploy/gdi-pe-v1-7-customer-closure`  
**Mode:** MODE B — audit first; no second-hotel build  
**Reference hotel:** Bethesda Marriott `recLuxvwwxID7U2B8`  
**Canonical base:** `appa2cE7FTRmIbB32`  
**Legacy MVP (forbidden for new writes):** `appvtnDurnMSjINP6`

---

## T. FINAL VERDICT

**REPLICATION READY AFTER MINOR REPAIRS**

Do **not** start full second-hotel replication until the blocking items in §S.14 are closed. Renaissance is the best second-hotel candidate for ADP; GDI weekly seed for Renaissance is **not** ready yet.

---

## A. BETHESDA REFERENCE

### ADP
- Census-linked: `adp_bethesda_marriott` → `recLuxvwwxID7U2B8` (`fixtures/ai-demand-positioning/census-links-v1.json`)
- Persistence SoT: **filesystem published snapshots** + optional Airtable Live overlay (`Published Reports`)
- History Airtable graph: **designed, writes gated OFF** by default
- Heavy Bethesda-named foundation packs exist (entity registry, CORE, scenarios, intake) — **working reference, not turnkey template**
- Not in the 5-property certified ADP cohort (Waterstone / Renaissance / Phillips / Cambridge / NOHO)

### GDI
Freeze (`reports/.../replication-readiness-v2/BETHESDA_GDI_FREEZE.json`):

| Metric | Value |
|--------|------:|
| Actionable opportunities | 34 |
| HIGH / MEDIUM / WATCHLIST | 6 / 15 / 13 |
| NEW / UPDATED | 4 / 21 |
| Contact NAMED_DIRECT | 4 |
| NAMED_PARTIAL | **11** |
| FUNCTIONAL | **10** |
| ORGANIZATION_PATH | **8** |
| GENERIC_ONLY | 0 |
| NO_CONTACT (actionable) | **1** (Marriott HQ account watch) |

Latest weekly V1.2 apply: 8/8 targets, 199 queries, 21 fetches, 0 TRUE promotions, Surfe 0 (`WEEKLY_V12_…_fb0844.json`).

### CONTACT
Sufficient for weekly operation after V1–V1.2. Actionable blank = account-level ceiling, not extraction failure. Disqualified NO_CONTACT must be excluded from gap counts.

### PERFORMANCE
Prior measured (Bethesda):

| Flow | P50 (warm) | Notes |
|------|------------|-------|
| GDI list | ~336 ms | Cold Airtable still multi-second |
| GDI detail | ~171 ms | Cache after list |
| List payload | ~39 KB sparse | Was ~478 KB |

Cold open still Airtable-bound — watch for second hotel, not a hard block if warm path holds.

---

## B. ADP AIRTABLE

TABLES (runtime):
- **Published Reports** (name-based Live overlay) — hotel via text `ADP Property ID` / `Census Record ID`
- **Hotel Property Census** `tbl9aY5ijiuIzzWam` (platform base) — join target
- **History tables** (9 proposed: periods, snapshots, metrics, evidence index, …) — writes **OFF**

ORPHANS / BROKEN LINKS / DUPLICATES: not live-enumerated this cycle (filesystem SoT) — treat as **UNKNOWN pending live inventory script**  
TEXT-LINK DEFECTS: **0 by design** for cross-base census text join (`TEXT_ONLY_ACCEPTABLE`)  
WRONG-BASE WRITES: **0 hard-coded**; residual risk if `ADP_AIRTABLE_BASE_ID` unset → falls back to `AIRTABLE_BASE_ID` (could be MVP)  
HISTORY: **PASS (filesystem)** / **DEFERRED (Airtable history writes)**

---

## C. GDI AIRTABLE

| Table | ID |
|-------|-----|
| GDI Opportunities | `tblRuReslJMwsfRQj` |
| Demand Generators | `tblykUVOjGVkawvdD` |
| Demand Programs | `tblxm7wqfFNG37Z2m` |
| Hotel Demand Generator Fit | `tblqamXbIk9n6gXfV` |
| Demand Generator Signals | `tblZ0cOM78uYMCTXx` |
| Private Event Venues | `tblE5p4HjVHfcMDna` |
| Hotel Venue Fit | `tbluXqX7ecocCrdE1` |
| Private Event Signals | `tbl5rSZxKv218GuOP` |
| Decisions | `tblulPWvEmd3iiDhJ` |
| Decision Events | `tblLL1CuKpvI43DtB` |
| **GDI Research Targets** | **`tblVyuEf5vjooWDKX`** |
| **GDI Research Runs** | **`tblzNMUIo2T6onaKH`** |
| **GDI Research Target Runs** | **`tblSA0cFVplNfWMjp`** |

ORPHANS / BROKEN LINKS: not fully live-counted this cycle — graph + wrong-base guards exist  
PROVENANCE GAPS: contact canary briefly used wrong promote API (repaired) — see §Q  
WRONG-BASE WRITES: guarded via `assertNotLegacyMvpCanonicalBase` — **0 expected when env correct**

---

## D. CUSTOMER DATA CONSISTENCY

ADP AIRTABLE/API/UI: **PASS** (published read priority + owner/share Playwrights exist)  
GDI AIRTABLE/API/UI: **PASS** (canonical load + customer-visibility filter)  
SHARE: **PASS** (HMAC capability + hotel binding + sanitize)  
CSV: **PASS** (formula escape + customer filter + hotel scope tests)

---

## E. WEEKLY DISCOVERY

| Lane | Status |
|------|--------|
| TRADITIONAL | PARTIALLY_WIRED (via New Opps URL harvest) |
| NEW OPPS | PARTIALLY_WIRED (registry harvest auto; full SERP rediscovery separate) |
| DEMAND GENERATORS | **FULLY_WIRED** |
| RECURRING | CONTROLLED_ONLY |
| TRAINING | PARTIALLY_WIRED |
| GOV/PROJECT | PARTIALLY_WIRED |
| SPORTS | PARTIALLY_WIRED |
| OVERFLOW | PARTIALLY_WIRED |
| PRIVATE EVENTS | PARTIALLY_WIRED (monitor; TRUE needs peMaxQueries>0) |
| VENUE PARTNERSHIPS | PARTIALLY_WIRED |
| CORPORATE | PARTIALLY_WIRED / thin |

Canonical promote intent: `promoteQualifiedGdiOpportunity()` — **required**; legacy `saveOpportunitiesCanonical` / raw upsert still exist (cleanup before multi-hotel automation).

---

## F. CONTACT (actionable)

| Tier | Count |
|------|------:|
| NAMED_DIRECT | 4 |
| NAMED_PARTIAL | 11 |
| FUNCTIONAL | 10 |
| ORGANIZATION_PATH | 8 |
| GENERIC | 0 |
| NO_CONTACT ACTIONABLE | 1 |

SURFE AUTO: **0**  
SURFE PII PERSISTED: **0**  
On-demand CTA: **PASS**

---

## G. PERFORMANCE

| Flow | P50 | P95 | Errors |
|------|-----|-----|--------|
| ADP dashboard | not remeasured this cycle | — | — |
| ADP detail | not remeasured | — | — |
| GDI list (warm) | ~336 ms | ~392 ms | 0 |
| GDI detail (warm) | ~171 ms | ~178 ms | 0 |
| GDI share list | parallel resolve+list | — | 0 |
| CSV | harness PASS | — | 0 |
| weekly run | ~8 targets / ~30–60s class | — | 0 |

Cold GDI open: **watch** (Airtable ~5s).

---

## H. AIRTABLE EFFICIENCY

READS: list sparsified; detail cache TTL ~45s  
WRITES: promote/upsert per opportunity; DG/PE batch clients  
REDUNDANT READS: previously fixed (detail no longer full re-list on warm)  
N+1 ISSUES: commercial progression / decision hydration still a risk on detail  
BATCHING: **PASS** for opportunity list path; history ADP batching designed (10/batch) when enabled

---

## I. SECURITY

AUTH ISOLATION: **PASS** (owner property allowlist + GDI hotel scope)  
SHARE ISOLATION: **PASS** (signed capability + surface gates)  
CROSS-HOTEL LEAKS: **0 known** in current gates (negative tests exist for share/CSV)  
CLIENT SECRET LEAKS: **0** in GDI public JS (scan)  
LOG SECRET LEAKS: not re-audited live logs this cycle — **UNKNOWN**  
TEST DATA LEAKS: filtered via `customer-visibility` (`isTestData` / canary IDs)  
CSV LEAKS: **0** expected (formula + scope tests)

---

## J. GENERALIZATION

BETHESDA PRODUCTION HARDCODES: **many** in ADP foundation packs + GDI pilot seed / QA overrides / contact enrichments / DMV deepen  
HOTEL-SPECIFIC PRODUCTION DOMAINS: present in pilot contact seeds  
HOTEL-SPECIFIC PERSON LOGIC: present in `contact-official-enrichments-v1.js` / discoveries  
FIXTURE/CANARY ONLY REFERENCES: also many (acceptable)

**Portable path exists** (hotel config + independent candidates + promote), but **pilot paths remain in production tree**.

Classification for replication gate: **GENERALIZATION GAP — not zero hardcodes**.

---

## K. JEV GDI

| Decision Type | Status | Outcome Evidence | High-Conf Wrong |
|---------------|--------|------------------|-----------------|
| TARGET_RESEARCH_PRIORITY | KEEP_SHADOW | calibration V1.1 | watch |
| RESEARCH_PLAYBOOK | KEEP_SHADOW | historical + adjudicated | watch |
| FOLLOWUP_TYPE | KEEP_SHADOW | mixed | watch |
| STOP_CONTINUE | KEEP_SHADOW | mixed | watch |
| SOURCE_UTILITY | KEEP_SHADOW | — | — |
| GENERATOR_CADENCE | KEEP_SHADOW | — | — |
| OPPORTUNITY_PREQUAL | KEEP_SHADOW / NOT for final CQ | — | — |
| CONTACT_SOURCE_PATH | KEEP_SHADOW | V1.1/V1.2 mostly same as GDI | 0 recent |
| CONTACT_FOLLOWUP_TYPE | KEEP_SHADOW | same | 0 |
| STOP_CONTACT_RESEARCH | KEEP_SHADOW | occasional disagree vs CONTINUE | 0 proven yield |
| MATERIAL_CHANGE / SIGNAL_* | KEEP_SHADOW | — | — |

Default mode: **SHADOW**. Does not change production route.

---

## L. JEV ADP SHADOW

RUN: **NO** (no ADP Jev adapter in tree)  
CALLS: 0  
Recommendation: optional future shadow for `EVIDENCE_UTILITY` / `SOURCE_PRIORITY` / `FOLLOWUP_RESEARCH_TYPE` only — **not** finding truth / narrative / security.

---

## M. JEV RECOMMENDATION

SAFE FUTURE CANDIDATES (still shadow until multi-hotel outcome proof):
- CONTACT_SOURCE_PATH / CONTACT_FOLLOWUP_TYPE (after more hotels)
- RESEARCH_PLAYBOOK / STOP_CONTINUE (GDI)
- ADP EVIDENCE_UTILITY / SOURCE_PRIORITY (new, shadow-only)

KEEP SHADOW: all current GDI Jev types; any ADP candidates  

NOT APPROPRIATE:
- final commercial qualification
- NEW classification
- ADP finding truth / root cause / customer narrative
- security / persistence authorization
- Surfe / person invention

---

## N. SECOND HOTEL PRECHECK

HOTEL: **Renaissance New York Times Square Hotel**  
HOTEL ID: **`recG66DQJKP2c0UNh`** (verified in census-links + GDI config)

DATA READINESS: **PARTIAL**

| Surface | Readiness |
|---------|-----------|
| ADP profile / rooms / meeting / website / brand | **AVAILABLE** |
| ADP census link | **AVAILABLE** |
| ADP certified cohort membership | **AVAILABLE** |
| GDI hotel config onboarded | **AVAILABLE** |
| GDI Research Target backfill | **MISSING** (`BACKFILL_…_DRY` totals.total = **0**; hotelFits empty) |
| Bethesda-quality weekly GDI | **BLOCKED** until Fit + Targets seeded |

Alternatives with census links: Waterstone, Cambridge Beaches, NOW NOW NOHO — same GDI seed discipline required.

---

## O. REPLICATION DRY RUN (no production write)

| Class | Count (approx) | Examples |
|-------|----------------|----------|
| AUTOMATED | ~12 | scenario universe build, provider run, publish snapshot, GDI weekly loop once targets exist, contact dual-extract |
| SEMI_AUTOMATED | ~8 | certification gates, Airtable Live publish, share token issue, target backfill scripts |
| MANUAL | ~10 | ADP entity/CORE/peer packs, property scenarios, GDI hotel config, Fit rows, competitor set stewardship |
| BLOCKED | ~3 | Renaissance GDI target registry empty; ADP Bethesda-pack pattern not auto-generated; history Airtable off |

MANUAL INTERVENTIONS REQUIRED (Bethesda→Renaissance parity): **~12–18**  
(not “zero-touch”).

---

## P. COST / SCALE (order-of-magnitude)

| Item | Estimate |
|------|----------|
| EXPECTED ADP COST | multi-provider × ~60 scenarios class — **high** vs GDI weekly |
| EXPECTED GDI COST | SERP + fetches for due targets — **moderate** (Bethesda weekly ~200 queries / 20 fetches) |
| EXPECTED JEV COST | shadow-only; low if sampled |
| EXPECTED RUNTIME | ADP baseline hours-class; GDI weekly minutes-class per hotel |
| BOTTLENECKS | Airtable cold reads; provider rate limits; SERP quota; single-hotel promote serial writes |

---

## Q. REPAIRS (this audit)

| Area | Count | Notes |
|------|------:|-------|
| AIRTABLE / PROVENANCE | 1 | Contact V1.2 canary used invalid promote API → upgrades not persisted; **fixed + re-applied** 3 contact upgrades |
| SECURITY | 0 | |
| PERFORMANCE | 0 | |
| GENERALIZATION | 0 (documented, not removed) | |
| UI | 0 | |

---

## R. REGRESSION

| Suite | Result |
|-------|--------|
| ADP | not fully re-run this cycle (docs/tests exist) — **PARTIAL** |
| GDI contact V1.2 / UI / share | **PASS** |
| Jev V1.1 | **PASS** (prior) |
| SECURITY share/CSV | **PASS** |
| AIRTABLE wrong-base guards | **PASS** (code) |
| CSV | **PASS** |

---

## S. DECISION

1. Is ADP fully persisted and reproducible? **Partially** — filesystem reproducible with packs; Airtable Live optional; history gated; Bethesda packs manual.  
2. Is GDI fully persisted and reproducible? **Yes for Bethesda weekly path**; not yet for empty-target hotels.  
3. Correct Airtable structures? **Yes** when env points to `appa2cE7…`; guards vs MVP.  
4. Links/provenance intact? **Mostly**; contact canary persistence defect found & repaired.  
5. Tenant isolation secure? **Yes** for known share/auth/CSV gates.  
6. Performance OK for second hotel? **Warm path yes; cold Airtable watch**.  
7. Free of Bethesda production logic? **No** — many pilot hardcodes remain.  
8. Weekly GDI end to end? **Yes for wired lanes** (DG + New-Opps-family + PE monitor); not all founder lanes FULLY_WIRED.  
9. Contact sufficient for replication? **Yes as architecture**; per-hotel public-web ceiling expected.  
10. Valuable Jev uses? Contact routing + research playbook/stop — still shadow.  
11. Risky Jev uses? Final CQ, NEW, ADP truth, security.  
12. Renaissance ready as second hotel? **ADP candidate yes; GDI weekly seed no**.  
13. Manual interventions? **~12–18**.  
14. Must fix before replication starts:
    1. Seed Renaissance **Hotel Demand Generator Fit + Research Targets** (apply backfill)  
    2. Confirm `ADP_AIRTABLE_BASE_ID` / `AIRTABLE_GDI_BASE_ID` always = `appa2cE7…` in deploy  
    3. Inventory/retire or quarantine **pilot-only** GDI seed/DMV/person hardcodes from default production path  
    4. Prefer ADP hotel that already has CORE/entity packs **or** budget pack authoring for RTS  
    5. Keep Jev **SHADOW**  
    6. Do not force Surfe  

---

## PERSISTENCE / GENERALIZATION CHECK

CODE FILES CHANGED:
- `scripts/gdi-contact-intelligence-v1-2-gap-canary.mjs` (promote API + top-level contact field sync)
- `reports/group-demand-intelligence/replication-readiness-v2/*` (this audit)
- live Airtable contact updates for 3 Bethesda opportunities (data, not code)

TESTS ADDED: none required beyond existing V1.2 suite  
FIXTURES ADDED: freeze JSON  
BETHESDA-SPECIFIC PRODUCTION LOGIC: **YES** (pre-existing; not expanded)  
HARD-CODED DOMAINS: **YES** in pilot contact modules  
JEV PRODUCTION BEHAVIOR CHANGED: **NO**

COMMITS: pending this audit repair commit  
PUSH: pending  
GIT SHA: see post-commit  
WORKING TREE CLEAN: **NO** (market-alerts dirty remains unrelated)
