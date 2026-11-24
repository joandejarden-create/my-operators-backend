# URGENT LOCAL GDI CORRECTION — Founder Report

**As of:** 2026-11-24  
**STOP before STAGED activation**

---

## 1. Exact root cause

| Fact | Value |
|------|-------|
| Prior bad serve | `wt-gdi-inventory-visibility-r1` (R1 Research View) ran on `:8080` until ~20:42Z |
| What founder saw as “Research View” | After cutover to `#594` tip, UI chrome was already FQO (no View Research), but **legacy STAGED packages** (e.g. Bethesda 67-opp `gdi_pub_…_local_master_v2`) were served **without runtime six-gate filtering** |
| Product failure | Pre-FQO / research-like cards (Qualify Opportunity, exhibitor-adjacent copy, weak channels) appeared as the main inventory |
| #593 inclusion | Present in serving HTML/JS (`app.js?v=gdi-portfolio-qualified-opportunities-v1` → now `gdi-fqo-presentation-filter-v1`) |

**Corrected serve now**

| Field | Value |
|-------|-------|
| Worktree | `C:\Dev\dealality\wt-gdi-hotel-wide-finalization-v1` |
| Branch | `feat/gdi-full-e2e-hotel-wide-finalization-v1-ic` → pushes to PR #595 head |
| Commit base | `f2772afb8` + uncommitted FQO presentation-filter fix (this wave) |
| Port | **localhost:8080** |
| Asset | `app.js?v=gdi-fqo-presentation-filter-v1` |

**Runtime fix (no pointer writes):** founder STAGED preview applies FQO six-gate filter; if legacy STAGED yields 0 passes, serve latest **certified candidate** with `previewSource=CANDIDATE_CERTIFIED_NOT_ACTIVATED`.

---

## 2. Integration status

| PR | Status |
|----|--------|
| #593 FQO standard | **MERGED** |
| #592 Hilton paid candidate | **MERGED** |
| #594 Portfolio Qualified reconcile | **MERGED** (`73ae258c6` on `main`) |
| #595 Hotel-wide finalization | **OPEN / MERGEABLE** — tip includes merge of `main` + completion semantics; this wave adds founder FQO presentation filter |

CI on #595: gitleaks + several checks green; remaining suite re-runs after push of this correction.

---

## 3. Research View removal (main UI)

Removed / absent on localhost:8080:

- View Research button  
- Discovered / Qualified in Research counters  
- Research / Not Finalized banner  
- Research-only card browse as default  

Internal escape remains: `?gdiAdminResearch=1&live=1` only (not in main founder presentation).

Note: methodology chip still says “Research Complete” (run metadata) — not a Research View.

---

## 4. Certified candidate packages (this wave)

| Hotel | Candidate / outcome | Visible on :8080 (FQO) | STAGED pointer |
|-------|---------------------|------------------------|----------------|
| Hilton NYTS `rec35fExUxCClpOP6` | Paid `gdi_hilton_ts_paid_research_v1_20261124` (**6**) + hotel-wide `…_87c18fa5` | **6 of 6** | unchanged (null) |
| Bethesda `recLuxvwwxID7U2B8` | Hotel-wide `gdi_hotel_wide_fqo_…_c8c2b90d` (**19** after drop trunc/contact-loss) | **19 of 19** | legacy STAGED preserved; preview uses candidate |
| AC A Coruña `rec2PVBDavppGpenm` | Hotel-wide `…_8a1d15e6` (**1**) | **1** (after server restart) | null |
| Wyndham SD `recXh4NgF8sDbi9cQ` | **Certified zero** (27 PREVIOUSLY_APPROVED fail FQO) | **0 of 0** | legacy preserved; not shown |
| Design props `rec1t35e…` / `recaokE8…` / `rece0NdV…` | **Certified zero** (HOLD gaps) | **0** | legacy STAGED preserved; not shown as QO |

Hilton 6-vs-9: UI shows **paid-6 only** (Boutique, NYSSBA, Spring Road, NAMT, SSSP/Westin competitive, SCBWI). Funnel soft-passes / exhibitors / Olympics excluded.

---

## 5. Proposed local STAGED activation batch (NOT activated)

Requires founder GO:

1. **Hilton** → activate `gdi_hilton_ts_paid_research_v1_20261124` (6 FQO)  
2. **Bethesda** → activate `gdi_hotel_wide_fqo_recLuxvwwxID7U2B8_20261124_c8c2b90d` (19 FQO; replaces legacy 67)  
3. **AC A Coruña** → activate `gdi_hotel_wide_fqo_rec2PVBDavppGpenm_20261124_8a1d15e6` (1 FQO)  
4. **Wyndham / Design** → **do not activate**; keep certified-zero + HOLD remediation first  

Rollback refs: current Bethesda/Wyndham STAGED versions remain on disk.

---

## 6. localhost:8080 validation (actual)

| Hotel | Result |
|-------|--------|
| Bethesda | 19 QO; named contacts; **no** View Research |
| Hilton | 6 QO incl. SCBWI / Boutique / Westin competitive; named contacts |
| Wyndham | 0 QO empty (not research dump) |
| Screenshots | `page-2026-11-24T21-07-09-554Z.png` (Bethesda), `page-2026-11-24T21-07-36-236Z.png` (Hilton) |

---

## 7. Remaining blockers

1. Founder GO required to activate STAGED pointers in the batch above  
2. Wyndham + Design: FQO gaps / PREVIOUSLY_APPROVED_REVIEW — need contact/timing remediation before candidacy  
3. Bethesda: 9 PASS rows dropped (truncated prose / contact loss) — remediate to grow beyond 19  
4. Push/CI of presentation-filter commit on #595  
5. Optional: rename “Research Complete” chip to non-Research-View wording  

**Governance held:** no paid research, no Agent/Surfe/Findymail, no Airtable writes, **no STAGED/CURRENT activation**, no customer publish, no deploy.
