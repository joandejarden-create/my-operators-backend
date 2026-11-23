# GDI Design Hotels — Repeatable Operating Runbook V1

**Contract:** `gdi-design-hotels-operating-runbook-v1`  
**Canonical PowerShell entry:** `scripts/Invoke-GdiPortfolioProcessing.ps1`  
**Canonical engine:** Full E2E via Design Hotels Live modes → `scripts/gdi-design-hotels-live-research-pilot-v1.mjs` → `executeHotelPaidPilot` → `runCanonicalFullE2eResearch`  
**Certification verdict (2026-11-23):** `LIMITED_PILOT_ONLY` — see `reports/gdi-design-hotels-commercial-certification-v1/FOUNDER_REPORT.md`

This runbook is the operating standard for Design Hotels research. It does **not** invent a second pipeline.

---

## 1. Select a hotel

1. Confirm the property is in Hotel Property Census with a stable HPC `rec…` ID.
2. Confirm Design Hotels (or intended) affiliation on the HPC row.
3. Confirm market / city / country are populated (legacy HPC `name` / `city` / `country` / `Affiliation` are accepted by profile resolution).
4. Prefer properties with a usable commercial demand profile (rooms, meeting space, urban vs leisure).
5. Do **not** invent HPC IDs.

---

## 2. Required HPC / GDI profile fields

| Field | Required | Notes |
|---|---|---|
| HPC record ID | Yes | Canonical hotelId |
| Display name | Yes | From HPC or config |
| City / Country | Yes | Market relevance |
| Affiliation | Yes | Design Hotels verification |
| Rooms / meeting capacity | Strongly preferred | Fit scoring |
| GDI hotel config JSON | **Yes for live research** | `config/group-demand-intelligence/hotels/<hotelId>.json` |

Without a GDI hotel config, live discovery fail-closes on `HOTEL_PROFILE_UNRESOLVED` (observed on first $0 attempt).

**Config template (ops, not new engineering):** copy an existing Design Hotels config (e.g. Condesa / LaLuna / Memmo), replace `hotelId`, geography, rooms, `commercialPriorities.targetSegments`, and `demandTerritory`.

---

## 3. Prepare a hotel cohort

1. Seed one config JSON per hotel under `config/group-demand-intelligence/hotels/`.
2. Set priority segments to match the property demand profile (do not force equal spend across all 14 categories).
3. Confirm no existing production inventory overwrite risk — review local `data/group-demand-intelligence/hotels/<id>/` if present.
4. Clear blocked fourteen-segment durable state only when intentionally re-opening research (preserve evidence otherwise).
5. **Cohort size:** start with **3 hotels** (certified pilot size). Max recommended controlled scale before next certification: **5**.

### Current Design Hotels Live mode constraint

`DesignHotelsLivePreflight|Execute|Replay` currently execute the **fixed pilot cohort** (Condesa, LaLuna, Memmo) defined in `gdi-design-hotels-live-research-pilot-v1.js`.

To process **additional** Design Hotels without changing that locked pilot:

- Seed configs, then use portfolio `FullE2E` / PaidPilot paths with explicit founder GO and dual budget flags, **or**
- Request a focused engineering change to accept `-Hotels` / `-HotelsFile` into Design Hotels Live modes (not done in this certification).

Do **not** create a new hotel-specific runner.

---

## 4. Choose a research budget

| Rule | Value |
|---|---|
| Provider | Exa Agent only (unless founder expands) |
| Contact enrichment | Off (no Surfe / Findymail / PDL / FullEnrich) |
| Per-hotel ceiling (pilot pattern) | ≤ $4 |
| Aggregate ceiling (3-hotel pattern) | ≤ $12 |
| Do not force full spend | Yes — stop when yield plateaus |
| Paid auth ≠ persistence ≠ publication | Three independent GOs |

Pilot actual: **$3.20 / $12**.

---

## 5. Exact PowerShell commands

### Preflight ($0)

```powershell
.\scripts\Invoke-GdiPortfolioProcessing.ps1 `
  -Mode DesignHotelsLivePreflight `
  -BudgetUSD 0
```

Confirm: configs resolve, dual caps OK, publication/Airtable apply false.

### Execute paid (founder GO required)

```powershell
.\scripts\Invoke-GdiPortfolioProcessing.ps1 `
  -Mode DesignHotelsLiveExecute `
  -BudgetUSD 12 `
  -PerHotelBudgetUSD 4 `
  -AuthorizePaidResearch `
  -AllowLivePilot `
  -FounderSpendConfirmation FOUNDER_GDI_DESIGN_HOTELS_LIVE_RESEARCH_12
```

### Zero-paid replay

```powershell
.\scripts\Invoke-GdiPortfolioProcessing.ps1 `
  -Mode DesignHotelsLiveReplay `
  -BudgetUSD 0
```

Expect: `additionalSpendUsd=0`, stable opportunity IDs, no production overwrite.

### Safe preview for arbitrary hotels (non–Design Hotels Live)

```powershell
.\scripts\Invoke-GdiPortfolioProcessing.ps1 `
  -Mode Preview `
  -Hotels "recAAA,recBBB,recCCC"
```

---

## 6. Verify research completion

Review under `reports/gdi-design-hotels-live-research-pilot-v1/`:

| Artifact | Check |
|---|---|
| `EXECUTION_SUMMARY.json` | `ok`, spend ≤ caps, `airtableApply=false` |
| Per-hotel `canonical-full-e2e-summary.json` | `FULL_E2E_EXECUTION_COMPLETE` |
| Durable ledgers | `data/.../paid-budget-ledgers/FOUNDER_GDI_DH_LIVE_V1_*` |
| `HOTEL_INTELLIGENCE_PACKAGES.json` | Deep-selected opportunities |
| `ZERO_PAID_REPLAY.json` | `$0` additional |
| `research-inventory-persistence-ledger.json` | `dryRun=true`, writes=0 |

Exit code 0 ≠ Sales Ready. Read classifications.

---

## 7. Review commercial quality (mandatory)

For each promising opportunity, answer:

1. Real future demand signal?
2. Room-night / meeting mechanism?
3. Hotel market relevant?
4. Competitor commitment verified? (Unknown ≠ Competitive)
5. Timing actionable?
6. Who should sales contact?
7. Independently verified?
8. Specific commercial action?
9. What remains incomplete?
10. Truly Sales Ready? (fail closed)

Promote nothing that fails 1–3. Do not invent emails.

---

## 8. Preserve findings

- Keep run reports + ledgers in git (certification / evidence PRs).
- Local `opportunity-research-state/` is research continuity — do not delete.
- Near-duplicates (same org/program, different segment path): keep one canonical ID before persistence.

---

## 9. Authorize persistence (separate GO)

Destination: **GDI Hotel Research Opportunities** (`tblbkDj0vhQLfrssV`).

Requires **all** of:

1. Founder authorize research persistence  
2. Exact write-set authorization (see `PERSISTENCE_WRITE_SET_DRY.json`)  
3. Env unlock `GDI_RESEARCH_INVENTORY_PRODUCTION_WRITES`

Paid research authorization does **not** unlock persistence. Customer publication remains independently blocked.

Dry write-set for the pilot:  
`reports/gdi-design-hotels-commercial-certification-v1/PERSISTENCE_WRITE_SET_DRY.json`

---

## 10. Rerun a hotel later

1. Prefer `DesignHotelsLiveReplay` / zero-paid replay first (saved evidence).  
2. For incremental paid research: new founder GO + remaining ledger headroom (retry must not double-charge).  
3. Reconcile: `NEW_UNIQUE` / `UPDATED` / `UNCHANGED` / `RESEARCH_CONTINUATION` / `WATCHLIST_RESEARCH` / `COMPETITIVE` / `COMMERCIALLY_INELIGIBLE`.  
4. Do not overwrite production inventory without persistence GO.

---

## 11. Errors and incomplete research

| Symptom | Action |
|---|---|
| `HOTEL_PROFILE_UNRESOLVED` | Seed GDI hotel config; clear blocked state if intentional |
| Budget refuse / dual-cap fail | Lower plan or stop; do not bypass ledgers |
| Thin deep (`investigatedUrlCount=0`) | Record `NO_IMPROVEMENT`; do not treat as Sales Ready |
| Near-duplicate IDs | Prefer one programKey; exclude extras from write-set |
| Partial hotel failure | Resume/replay; durable ledger prevents double spend |
| Temptation to publish | Stop — publication is a separate founder GO |

---

## 12. What this runbook does **not** authorize

- Additional paid spend beyond an explicit founder GO  
- Airtable live writes  
- Customer publication  
- Weekly automation  
- Production deployment  
- New per-hotel Cursor engineering runners  

**STOP** for founder review before the next operational expansion.
