# Design Hotels Full GDI E2E Commercial Research V2 — Founder Report

**Authorization used:** GO — $0 preflight, evidence reuse, planning, local UI validation  
**Paid spend this cycle:** **$0.00**  
**Status:** `STOP — AWAITING NEW PAID BUDGET AUTHORIZATION`  
**Date:** 2026-11-23  
**Volume standard (amendment):** minimum **25** commercially credible / hotel (preferred **30–40**); portfolio min **75** (preferred **90–120**)

---

## Executive verdict

1. Local Research visibility for the three hotels is restored on **`:8097`** (not `:8080`).
2. Authentic saved research inventories already exist at **82 / 50 / 56** unique post-dedupe candidates — far above the old MaxCandidates=4 deep cap.
3. Those counts are **saved Research inventory**, not certified commercially credible opportunities and **not** Sales Ready.
4. Reaching the **25–40 commercially credible** standard requires a **new** paid Full E2E authorization. Historical **$12** pilot spend must **not** be reused.
5. This report stops at the paid-budget gate with a concrete proposal.

---

## Phase 1 — Integration baseline & which UI is live

| PR | State | Merge SHA / CI |
|---|---|---|
| **#574** Design Hotels live research | OPEN / MERGEABLE | CI green — **awaiting IC merge** |
| **#575** UI contact + architecture | **MERGED** | `aecc3e3146522cdf5da98e341ed0501eb7267348` |
| **#576** Universal cohort + dedupe V2 | OPEN / MERGEABLE | CI green after rebase onto #575 — **awaiting IC merge** |
| **#578** Local Research visibility materialize | OPEN | Local inventory + configs for Design Hotels |

| Port | Serves Design Hotels? | Role |
|---|---|---|
| **:8097** | **YES** (25 hotels incl. Condesa/LaLuna/Memmo) | Authorized founder Research review server (visibility WT) |
| :8080 | NO (22 hotels, DH absent, counts 0) | Other WT — do not use for DH review |
| :8091 | N/A for this cycle | Preserved |

Dirty workspaces preserved (`ws-gdi` and others untouched for merge).

### Source-of-truth Research inventory BEFORE further paid work

| Hotel | HPC | Saved Research inventory | Raw eligible (pilot) | After V2 dedupe | Deep-selected (pilot) | Sales Ready |
|---|---|---:|---:|---:|---:|---:|
| Condesa DF | `rece0NdV9FCnaFlVS` | **82** | 87 | 82 | 4 | 0 |
| LaLuna | `recaokE8psIbuVeuH` | **50** | 66 | 50 | 4 | 0 |
| Memmo Príncipe Real | `rec1t35e3sPerzxYu` | **56** | 61 | 56 | 4 | 0 |

These are Research-mode records (`?live=1` → `FOUNDER_RESEARCH_INVENTORY`). Missing STAGED/CURRENT packages is **expected** and not treated as missing research.

**Review URL:**  
http://127.0.0.1:8097/group-demand-intelligence.html?inventory=research&live=1&founderPreview=1

---

## Hotel-by-hotel scorecard (current cycle = $0)

### Condesa DF — `rece0NdV9FCnaFlVS`

| Metric | Value |
|---|---:|
| 1. Raw discoveries (prior pilot) | 87 |
| 2. Deduplicated eligible / unique | 82 |
| 3. Candidates screened (prior skip for cap) | 83 skipped under MaxCandidates=4 |
| 4. Deep-researched | 4 |
| 5. Commercially credible **certified** now | **4** progressed deep / research-continuation (not yet re-qualified to 25+) |
| 6. Sales Ready | 0 |
| 7. Active Research / Research Continuation | inventory 82 / deep continuation 4 |
| 8. Competitive | 0 |
| 9. Verified named decision-makers | 0 in saved inventory |
| 10. Verified emails | 0 |
| 11. Research cost this cycle | **$0** (historical pilot ~$1.20) |
| 12. Evidence improvements this cycle | $0 reuse/planning only |
| 13. Duplicates suppressed | 5 |
| 14. Recommended commercial actions | Ranked deep pool of **32** (min) / **46** (preferred) from existing inventory + gap discovery for corporate/procurement priorities |
| 15. Actual saved Research inventory (UI) | **82** |
| 16. Local browser verification | PASS — All/Hotel 82, Competitive 0 |
| 17. Remaining gaps | Deep+qualify ≥25; named contacts; leisure/partnership balance |
| 18. Customer publication readiness | **NOT READY** |

### LaLuna — `recaokE8psIbuVeuH`

| Metric | Value |
|---|---:|
| Raw discoveries | 66 |
| Unique after dedupe | 50 |
| Screened (prior cap skips) | 62 |
| Deep-researched | 4 selected / 2 marked in materialize join |
| Commercially credible certified now | **≤4** |
| Sales Ready | 0 |
| Research continuation / inventory | 4 / **50** |
| Competitive | 0 |
| Named / emails | 0 / 0 |
| Cost this cycle | **$0** (historical ~$1.00) |
| Duplicates suppressed | 16 |
| Recommended actions | Deep **34–48** priority-fit rows; extra gap discovery for DMC / wedding / boutique leisure (avoid convention-hotel profile) |
| UI Research count | **50** — PASS |
| Publication readiness | **NOT READY** |

### Memmo Príncipe Real — `rec1t35e3sPerzxYu`

| Metric | Value |
|---|---:|
| Raw discoveries | 61 |
| Unique after dedupe | 56 |
| Screened (prior cap skips) | 57 |
| Deep-researched | 4 |
| Commercially credible certified now | **≤4** |
| Sales Ready | 0 |
| Research continuation / inventory | 4 / **56** |
| Competitive | 0 |
| Named / emails | 0 / 0 |
| Cost this cycle | **$0** (historical ~$1.00) |
| Duplicates suppressed | 5 |
| Recommended actions | Deep **33–47** boutique/executive/small-meeting rows; do not promote large-congress overflow without lodging mechanism |
| UI Research count | **56** — PASS |
| Publication readiness | **NOT READY** |

### Portfolio totals (current)

| | |
|---|---:|
| Saved Research inventory | **188** |
| Deep-researched (pilot) | **12** |
| Commercially credible certified to volume standard | **≪ 75** (honest shortfall vs min 75) |
| Sales Ready | **0** |
| Spend this cycle | **$0** |

---

## Why the old pilot is insufficient (and what to reuse)

- MaxCandidates **4** caused dozens of high-scoring eligible candidates to be skipped (`below_cap_or_outranked`).
- Discovery inventory is already large enough that **most of the path to 25–40 is deep research + qualification on existing IDs**, not net-new discovery.
- Reuse: prior Exa discovery cells, seed URLs, evidence objects, V2 dedupe decisions, hotel configs — **$0**.
- Gaps: under-deepened priority pools; thin named-contact coverage; LaLuna leisure/DMC depth; Memmo VIP/small-group lodging mechanism checks.

Orchestration remains canonical:  
`Invoke-GdiPortfolioProcessing.ps1` → `CohortPreflight` / `CohortExecute` (no hotel-specific runners).  
`$0` CohortPreflight for the three hotels: **ok: true, spentUsd: 0**.

---

## Phase 8 — NEW paid budget proposal (authorization required)

**Do not reuse the historical $12 authorization.**

Pricing assumptions (Exa Agent fixed effort, quote as-of 2026-10-24):

- Discovery / deep **medium**: **$0.10** / run  
- Deep **high** (optional quality): **$0.50** / run  
- Findymail / Surfe: **OFF**  
- Assumed deep→commercially-credible yield: **~70%** (so ~36 investigates ≈ 25 credible; ~50 investigates ≈ 35 credible)

### Recommended authorization (medium deep — primary ask)

| Cap | Amount |
|---|---:|
| **Per-hotel ceiling** | **$18** |
| **Aggregate ceiling** | **$54** |
| Authorization id (suggested) | `FOUNDER_GDI_DH_FULL_E2E_COMMERCIAL_V2_USD_54` |

Covers per hotel (approx):

- Gap discovery: ~8–14 medium runs  
- Deep investigate: ~32–48 existing ranked candidates (no MaxCandidates 4)  
- Contingency + NO_IMPROVEMENT stop-loss  
- Public-first contact research: **$0** paid enrichment

### Alternates (from `BUDGET_PROPOSAL_V2.json`)

| Path | Approx aggregate |
|---|---:|
| Minimum path (medium, ~25 credible aim) | **~$18.50** |
| Preferred path (medium, ~30–40 aim) | **~$26.90** |
| Preferred + high deep quality | **~$83.30** |

**Recommended ask `$54`** provides headroom above the preferred medium path without committing to high-effort deep by default.

### Stop-loss

- Hard ceiling reached  
- `NO_IMPROVEMENT` on 3 consecutive deep calls for same opportunity family  
- Provider error budget / duplicate live-call block  
- Founder revoke  
- Cost per **new** commercially credible opportunity > **$2.50**

### Cost-per-credible target

**≤ $1.50** average after reuse (preferred).

### Execute command (ONLY after founder GO on new caps)

```powershell
.\scripts\Invoke-GdiPortfolioProcessing.ps1 `
  -Mode CohortExecute `
  -HotelsFile reports\gdi-design-hotels-full-e2e-commercial-research-v2\design-hotels-full-e2e-cohort.txt `
  -BudgetUSD 54 `
  -PerHotelBudgetUSD 18 `
  -AuthorizePaidResearch `
  -AllowLivePilot
```

Artifacts:

- `BUDGET_PROPOSAL_V2.json`
- `RANKED_DEEP_POOLS_V2.json`
- `PERSISTENCE_WRITE_SET_DRY_V2.json` (188 rows, **dry-run only**)
- `design-hotels-full-e2e-cohort.txt`

---

## Persistence & publication

- Dry write-set prepared for research-inventory upsert (**WOULD_UPSERT** only).  
- **No** Airtable production writes.  
- **No** STAGED/CURRENT customer packages.  
- Separate authorization still required for persistence and publication.

---

## Acceptance vs this gate

| Criterion | Status |
|---|---|
| Open Condesa / LaLuna / Memmo in local Research UI with authentic inventories | **PASS on :8097** (82 / 50 / 56) |
| Process standardized / repeatable (Cohort*) | **PASS** (preflight $0) |
| ≥25 commercially credible **certified** per hotel | **NOT YET** — requires paid deep+qualify under new budget |
| Sales Ready not forced | **PASS** (still 0) |

---

## Remaining blockers

1. Founder **new** paid budget GO (`$54` / `$18` recommended)  
2. Integration Controller merge of #574 → #576 (primary dirty; GDI chat does not merge)  
3. Optional: point founder `:8080` at visibility materialize or continue using `:8097`  
4. After paid run: qualification pass, contact coverage, persistence auth, then publication auth  

---

## Governance confirmation

No new paid spend · No Findymail/Surfe · No Airtable production writes · No customer publication · No production deploy · Dirty worktrees preserved · IC owns merge  

**STOP for founder review at the paid-budget authorization gate.**
