# GDI Repeatable Processing V2 — Founder Report

**Date:** 2026-11-23  
**Authorization:** FOUNDER GO — $0 improvement  
**Baseline:** Design Hotels Commercial Certification V1 (PR #574)  
**Worktree:** `C:\Users\joand\AppData\Local\Temp\dealality-gdi-repeatable-processing-v2-20261123`  
**Branch:** `feat/gdi-repeatable-processing-v2`  
**Paid spend this task:** **$0**  
**Airtable writes:** none  
**Customer publication:** none  

---

## 1. PR integration status

| PR | Title | Status on `origin/main` |
|---|---|---|
| **#574** | Design Hotels live research + commercial certification | **OPEN** — not merged |
| **#575** | Customer UI inventory / contacts regression | **OPEN** — not merged |

Integration Controller owns merges. This V2 branch was cut from `origin/main` and merge-committed both #574 and #575 tips so work builds on certified pilot artifacts + UI contact fixes without claiming main merge.

Dirty worktrees preserved (`ws-gdi` contact-intel dirty; Integration checkout dirty). All new work in isolated temp worktree.

---

## 2. Universal runner readiness

**READY for HotelsFile-driven cohorts.**

| Path | Role |
|---|---|
| `CohortPreflight \| CohortExecute \| CohortReplay` | **Canonical universal path** — dynamic hotels |
| `DesignHotelsLive*` | Frozen historical Condesa/LaLuna/Memmo regression only |
| `FullE2E` / PaidPilot | Unchanged certified engines; cohort reuses `executeHotelPaidPilot` |

- Supports 1 / 3 / 5 / arbitrary authorized hotel IDs via `-HotelsFile`, `-Hotels`, or `-HotelId`
- No new hotel-specific research runner
- Dual ledgers, Exa-only, no Surfe/Findymail, no Airtable apply, no publication

Preflight smoke on Design Hotels cohort file: `ok: true`, `spentUsd: 0`, `executeReady: false` until `GDI_EXA_AGENT_LIVE=1` (expected).

---

## 3. Automatic configuration results

Module: `gdi-hotel-config-proposal-v2`  
Validation vs frozen Condesa / LaLuna / Memmo configs (`skipHpc: true`): **PASS**

| Hotel | Proposal OK | Hard blockers | Rooms match | Territory |
|---|---|---|---|---|
| Condesa DF | yes | none | 40 = 40 | Mexico City |
| LaLuna | yes | none | 16 = 16 | Grenada |
| Memmo | yes | none | 41 = 41 | Lisbon |

Unsupported fields (e.g. meeting space on villa/leisure) marked **UNRESOLVED** — not invented. No schema changes.

---

## 4. Cross-segment deduplication results

Module: `gdi-cross-segment-opportunity-dedupe-v2` (pre-deep + selection safety net)

| Hotel | Eligible before | After dedupe | Removed |
|---|---:|---:|---:|
| Condesa | 87 | 82 | 5 |
| LaLuna | 66 | 50 | 16 |
| Memmo | 61 | 56 | 5 |
| **Total** | **214** | **188** | **26** |

### Observed pilot cases

| Case | Before | After |
|---|---|---|
| **CARPHA** `disc_a6def3588729` + `disc_023923963f08` | both eligible / both historically deep-selected | **canonical only** — peer suppressed with segment associations preserved |
| **Advantage** `disc_9cd2c082c05d` + `disc_51073448c775` | both eligible / both historically deep-selected | **canonical `disc_9cd2c082c05d` only** |

Root cause fixed: fair segment floor previously selected the same program once per segment. V2 clusters by soft org + year + title family before deep spend.

---

## 5. Contact research improvements

| Finding | Detail |
|---|---|
| Why LaLuna had 0 named stakeholders | Deep slots spent on association/destination events with thin public organizer pages; Condesa/Memmo wins were **corporate expansion** paths (Salesforce / TDCX) with public named leaders |
| Policy V2 | Retain verified **name + role + employer + source** without personal email; reject gap labels; Sales Ready fail-closed |
| Paid enrichment | **Not activated** (Surfe / Findymail HOLD) |
| UI coordination | Aligns with PR #575 — do not invent contacts or overwrite verified customer names |

Next paid deep on leisure hotels should prioritize DMC / destination-event coordinators / housing bureau contacts per `gdi-public-contact-research-policy-v2`.

---

## 6. Hotel-fit ranking improvements

`scoreHotelCapabilityFit` uses rooms / meetings / classification:

| Example | Effect |
|---|---|
| Condesa (urban, 7 meeting rooms) | Corporate / meetings demand boosted |
| LaLuna (leisure destination, 16 villas) | Destination / sailing / incentive boosted |
| Memmo (41-key boutique) | Large congress (ESICM) demoted without overflow evidence; TDCX / Advantage retained |

Historical Memmo deep included ESICM + duplicate Advantage. V2 re-rank on frozen eligible set: **ESICM out of top-4**, Advantage duplicate suppressed, TDCX retained. Hotel / Competitive classifications unchanged (pilot remained Hotel / OPEN).

---

## 7. Revised persistence write-set

Artifact: `reports/gdi-repeatable-processing-v2/PERSISTENCE_WRITE_SET_DRY_V2.json`  
**Dry only — no Airtable apply.**

| Class | Treatment |
|---|---|
| Strong evidence-backed (e.g. Salesforce) | Internal preserve; customer-eligible **candidate only** |
| Incomplete research continuations | Internal preserve as exploratory |
| Duplicates | Suppress `disc_023923963f08`, `disc_51073448c775` (evidence retained on canonical) |
| Sales Ready | **0** — integrity preserved |
| Customer publication | **None** without separate auth |

---

## 8. Three-hotel zero-paid regression results

| Check | Result |
|---|---|
| Paid spend | **$0** |
| CARPHA deduped | **true** |
| Advantage deduped | **true** |
| Config validation | **true** |
| Sales Ready | **0** (unchanged — not fabricated) |
| Historical research findings | Not rewritten; selection/ranking/dedupe only |
| Stable IDs | Canonical IDs preserved; peers linked via `suppressedPeerIds` |

Unit tests: `node scripts/test-gdi-repeatable-processing-v2.mjs` → **ok**

---

## 9. Exact standard PowerShell command

```powershell
.\scripts\Invoke-GdiPortfolioProcessing.ps1 `
  -Mode CohortPreflight `
  -HotelsFile .\reports\gdi-repeatable-processing-v2\design-hotels-pilot-cohort-hotels.txt `
  -BudgetUSD 12 `
  -PerHotelBudgetUSD 4 `
  -MaxCandidates 4
```

Execute (after founder GO only):

```powershell
$env:GDI_EXA_AGENT_LIVE = '1'
.\scripts\Invoke-GdiPortfolioProcessing.ps1 `
  -Mode CohortExecute `
  -HotelsFile .\path\to\next-cohort-hotels.txt `
  -BudgetUSD <approved> `
  -PerHotelBudgetUSD <approved> `
  -AllowLivePilot `
  -AuthorizePaidResearch `
  -FounderSpendConfirmation FOUNDER_GDI_UNIVERSAL_COHORT_PAID `
  -MaxCandidates 4
```

SOP: `docs/data-intelligence/gdi-universal-operating-procedure-v2.md`

---

## 10. Remaining blockers to processing the next cohort

1. **PR #574 / #575 / this V2 PR** must be merged by Integration Controller (or stacked merges) before treating `main` as source of truth.  
2. **Verified HPC ID** + GDI config (existing or auto-proposed with hard fields resolved).  
3. **Founder-approved budget** + spend phrase.  
4. **Commercial quality still LIMITED** — Sales Ready 0; contact coverage weak on leisure/association deeps until public-contact brief is used in live runs.  
5. **No Airtable persistence apply** until separate write-set authorization.

---

## 11. Recommended next validation steps

1. IC merge #574 → #575 → this V2 PR (or equivalent stack).  
2. Post-merge sync `ws/gdi`.  
3. CohortPreflight on the **next** authorized hotel (single HPC) at $0.  
4. Founder review of proposed config unresolved fields.  
5. Small paid execute only after GO (do not expand portfolio in this PR).  
6. Optional: zero-paid contact pass over saved Exa evidence for LaLuna Sailing Week / CTLPA organizer pages.

---

## 12. Commercial readiness verdict

### **LIMITED_PILOT_ONLY → REPEATABLE_PROCESS_READY (ops) / COMMERCIAL_PARTIAL**

| Dimension | Verdict |
|---|---|
| Ops repeatability (HPC + budget + PowerShell) | **READY** |
| Cross-segment dedupe before deep | **READY** (demonstrated on frozen pilot) |
| Auto config proposal | **READY** (validated vs 3 pilot configs) |
| Contact intelligence | **IMPROVED POLICY** — live gain pending next $0/public or authorized deep |
| Sales Ready conversion | **NOT READY** (honest 0) |
| Customer publication | **NOT AUTHORIZED** |
| Scale to next cohort without new hotel-specific code | **YES** — blocked only by merge + budget + HPC verification |

**STOP for founder review before any additional spending or portfolio expansion.**

---

## Final status

`AWAITING_INTEGRATION_CONTROLLER` (PR open; GDI does not merge)
