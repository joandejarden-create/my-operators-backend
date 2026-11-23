# GDI Design Hotels Operational Pilot V1.2 — Founder Report

**Date:** 2026-11-23  
**PR:** [#571](https://github.com/joandejarden-create/my-operators-backend/pull/571) (update)  
**Status:** Final cohort certified for $0 preparation — **STOP** before paid research / opportunity Airtable / publication

---

## 1. Exact Memmo HPC identity resolution

| Field | Value |
|---|---|
| HPC ID | **`rec1t35e3sPerzxYu`** |
| Property Name | Memmo Príncipe Real |
| Current Brand | Design Hotels |
| MARSHA | **LISDM** (official Marriott; identity key `memmo_pt_lisbon_principe_real_lisdm`) |
| City / Country | Lisbon / Portugal |
| Market / Submarket | Lisbon / Príncipe Real |
| Rooms | **41** |
| Official URLs | Design Hotels + Marriott LISDM overview |

## 2. Existing versus newly enrolled

**NEW_RECORD_CREATED** after conclusive absence.

- Authorized full-table scan: **19,580** HPC records
- Portugal / Lisbon / Memmo / LISDM hits before create: **0**
- Pre-write dedup: clean
- Post-create idempotency: **single record** `rec1t35e3sPerzxYu`
- Legacy STR-style field patch (`name` / `Affiliation` / `Property ID`): not writable via this API schema (unknown field names on write) — canonical Property Name / Current Brand / Rooms path used (same as YOTEL/AC steward enrollments)

## 3. Source evidence and duplicate checks

Evidence pack: `reports/gdi-design-hotels-operational-pilot-v1/memmo-hpc-enrollment-v1/`

- `phase1-portugal-scan.json` — full scan absence
- `EVIDENCE_PACK.json` — official DH + Marriott sources
- `WRITE_PREVIEW.json` — dry-run payload
- `PREWRITE_DEDUP.json` / `ENROLLMENT_RESULT.json` — create + readback

No duplicate HPC created on re-dedup.

## 4. Final three-hotel cohort

| # | Hotel | HPC | Posture |
|---|---|---|---|
| 1 | Condesa DF Hotel | `rece0NdV9FCnaFlVS` | Urban / meetings |
| 2 | LaLuna Boutique Hotels & Villas | `recaokE8psIbuVeuH` | Leisure / destination |
| 3 | **Memmo Príncipe Real** | **`rec1t35e3sPerzxYu`** | European boutique / limited meetings |

Antigua `recGL1epYhEle5SDM` **removed from this pilot cohort only** — HPC / research **not** deleted.

`design-hotels-pilot-cohort-hotels.txt` contains exactly the three IDs above.

## 5. Updated hotel preflight

3/3 `NEW_HOTEL`, `readyForDryRun=true`, affiliation Design Hotels, Memmo rooms=41 / LISDM / BOUTIQUE_LIMITED_MEETINGS.

## 6–7. $0 PowerShell Preview / Rehearsal

```powershell
.\scripts\Invoke-GdiPortfolioProcessing.ps1 -Mode Preview -HotelsFile reports\gdi-design-hotels-operational-pilot-v1\design-hotels-pilot-cohort-hotels.txt
.\scripts\Invoke-GdiPortfolioProcessing.ps1 -Mode Rehearsal -HotelsFile reports\gdi-design-hotels-operational-pilot-v1\design-hotels-pilot-cohort-hotels.txt
```

| Mode | Run ID | Result |
|---|---|---|
| Preview | `gdi_portfolio_20261123130136_c84d5e` | 3/3, `$0`, `LIMITED_PILOT_ONLY` |
| Rehearsal | `gdi_portfolio_20261123130137_1611d3` | 3/3, `$0`, `LIMITED_PILOT_ONLY` |

Preflight tests: **5/5 PASS**. Evidence: `rehearsal-summary.json`.

## 8. Configuration gaps

| Gap | Notes |
|---|---|
| Coordinates | Address-only; permanent geocode deferred |
| Legacy STR fields on Memmo | Not written (API schema); canonical fields present |
| Local GDI opportunity artifacts | None — expected NEW_HOTEL |
| #564/#566/#567/#570 | Still OPEN — prefer merge before paid |

## 9. PR and integration status

| PR | Status |
|---|---|
| **#571** Design Hotels pilot (V1.2) | OPEN — this update |
| #564 / #566 / #567 / #570 | OPEN |
| #562 / #224 | MERGED |

Suggested merge order before paid FullE2E: **#564 → #566 → #567 → #570 → #571** (or #571 can merge independently for $0 prep; paid should wait for #564+ controls).

## 10. Remaining blockers before paid FullE2E

1. Founder budget ceiling authorization (`-BudgetUSD`, suggested ≤$60 portfolio)
2. Prefer IC merge of paid-pilot / conversion / evidence / CMI PRs
3. Separate GO for GDI research-inventory Airtable apply
4. Separate GO for customer publication
5. No Surfe / paid contact enrichment in this plan

**Paid research plan remains prepared-not-executed** (`paid-research-plan-v1-1.json` — update Hotel 3 HPC to `rec1t35e3sPerzxYu`).

---

**STOP** before paid GDI discovery, research inventory persistence, customer publication, or broader scaling.

**Engine readiness:** $0 Preview/Rehearsal on Mexico / Grenada / Portugal — **YES**. Paid — **NO** until founder budget GO.
