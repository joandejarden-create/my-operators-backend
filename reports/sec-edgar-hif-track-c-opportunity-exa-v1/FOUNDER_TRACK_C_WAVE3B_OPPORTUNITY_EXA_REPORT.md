# Track C Wave 3b — Opportunity-First HPC + Exa Benchmark V1

**Publication:** CUSTOMER_BLOCKED · **Branch:** `feat/hif-v6-track-c-wave3b-opportunity-exa`  
**Exa spend:** **$0.121 / $10.00 hard cap** · **Agent:** not used (MANUAL_ESCALATION_ONLY)

## 1. Baseline commercial inventory (IC-qualified)

| Class | Hotels |
|-------|--------|
| First-100 open BCT | Lakeville Inn · Kimpton Shorebreak · Courtyard Denver Downtown |
| First-100 open MCT | Hilton Trinidad · Four Seasons Bengaluru Embassy ONE |
| Preferred / not open competitive | Legacy Hotel Green Bay (IC corrected) |
| Emerging / adjacent / award-watch | Gideon Putnam · Hotel do Mel (project) · Oromana · Honey Creek · Marquis Chicago · Aena sites |

Sources: Track A CB2/CB3 (PR #541/#544 lineage); Track B contractual signals as P2/P3 only.

## 2–6. HPC identity resolution

| Outcome | Detail |
|---------|--------|
| Distinct investigated | **78** (15 P0/P1 + SEC near-term/unresolved + 50 control) |
| Pre-existing match | Hilton Trinidad → `recFNfTvcph0fYQnm` |
| New HPC creates | **5** (Shorebreak, Courtyard Denver Downtown, Embassy ONE, Legacy, Marquis Chicago) |
| Schema validation failures | 3 (Oromana wrong Exa hit; Silver Spring North; Denver Stapleton) |
| US HPC | 89 → **93** |
| MX/BR | unchanged (CALA preserved) |
| Still missing P0 create | **Lakeville Inn** (independent URL gate) |

**Opportunity→HPC linked after this wave (P0):** Trinidad (existing) + Shorebreak + Denver Downtown + Embassy ONE = **4/5**. Lakeville remains the gap.

## 7. Hotel Explorer handoff

See `HOTEL_EXPLORER_HANDOFF.json` + `POST_CREATE_HPC_LINKS.json`.  
Track C does **not** modify the HE worktree. All items CUSTOMER_BLOCKED. HE profile status UNKNOWN pending HE stream.

## 8–10. Exa vs free-source benchmark

| Method | Investigated | Official URL recoveries | Spend |
|--------|-------------:|------------------------:|------:|
| Free-source | 78 | 5 brand-directory | $0 |
| Exa-assisted | 25 | 22 | **$0.121** |
| Hybrid (recommended) | 78 | free-first then Exa gaps | $0.121 |

- Cost/verified-assist ≈ **$0.005–0.006** per Lite call (reserve accounting).  
- **Exa is not verification** — Oromana returned a wrong Marriott property (create correctly failed schema/identity). Always verify underlying URL.  
- **Recommendation:** Hybrid free-first; Exa Lite only for P0–P3 gaps; no Agent; no unrestricted bulk.

## 11–12. Airtable

Inventory: 3 creates · 9 no-ops · 0 conflicts · 5 staged  
HPC: 5 creates · 3 failures · 6 inventory↔HPC enrichments  
Idempotency: discoveryPropertyId find-before-create

## 13. Scalable methodology

Opportunity-linked hotels first → free brand directories → Exa Lite for residual identity gaps within hard caps → authorized HPC create → HE handoff. Full US/UK/CA census remains enabled but not the immediate objective (`CENSUS_EXPANSION_ROADMAP.json`).

## 14. Founder gate recommendation (no schema change)

Accept **verified independent official hotel website** (e.g. gideonputnam.com) and optionally corroborating DMO listing (Lakeville) as Official Property URL for independents — without weakening brand-directory rules for branded hotels.

## Tests / IC

- Reuses `test:hif-track-c-universe-expansion`  
- CI precheck before PR  
- Integration Controller owns merge  
- PR #548 (census scale) remains separate OPEN; this is successor Wave 3b
