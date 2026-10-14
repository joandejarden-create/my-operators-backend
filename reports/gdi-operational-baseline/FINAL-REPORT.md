# GDI operational baseline — final report

**As of:** 2026-10-14  
**Committed build (local proof):** `f911ff9ae4b5d7d0c83c0e94e0400f1a74271bfd` on `gdi/parallel-provider-san-diego`  
**Deployed build:** **not deployed** — Railway CLI unauthorized (`railway login` required). Pre-deploy gates PASS; `railway up` failed before upload.

---

## 1. Committed build

| Item | Value |
|------|--------|
| Branch | `gdi/parallel-provider-san-diego` |
| Commit | `f911ff9a` — *feat(gdi): finish operational baseline consolidation with geo quarantine* |
| Prior tip | `9acf5dc1` (Parallel provider integration) |
| What landed | Parallel live transport + identity lock, geo quarantine before fit, PE fixture lock, programs→occasion, draft review package, Beth/May UI/API, rematerialized wrong-market evidence, budget probe reconciliation |

Unrelated ADP / census / decision-outcomes working-tree noise was **not** included in this commit.

---

## 2. Hotel identity correction

### Root cause (failed run `trun_567f3335d0c140ad8e87120211c6f61f`)

- Prompt initially lacked full hotel identity lock; Parallel substituted **Bethesda Marriott** for opaque `hotelId`.
- Reused research gaps / prior Bethesda context primed DMV results.
- Mapper ran **hotel-fit before geographic quarantine**, so wrong-market rows scored `FITS_THIS_HOTEL`.
- Job/budget paths were polluted when `GDI_DATA_ROOT` pointed at drawer-completeness sandbox.

### Fixes now in `f911ff9a`

- Canonical hotel id, name, location, territory, rooms, meeting profile in Parallel prompt.
- Gap text sanitized (Bethesda/DMV lines stripped for San Diego).
- Geographic gate quarantines wrong-market **before** fit / card generation.
- Hotel identity recorded on job result meta; corrected live uses new job segment `bounded-live-v1-idlock` (does **not** resubmit contaminated `bounded-live-v1`).
- Regression test: `node scripts/test-gdi-geographic-relevance-gate-v1.mjs` (preserves rematerialized evidence; 5+ Bethesda rows quarantine; **0** counted as SD prospects).

### Program drafts (12)

| Disposition | Count |
|-------------|------:|
| Kept as SD internal queue | 10 |
| Quarantined thematic contamination (Clinical / Military medical education) | 2 |

SITE Global program draft still carries SD territory stamp — treat as **unresolved geo** until cycle city is verified (Parallel live previously placed SITE Global in Vancouver). Not counted as a delivered SD prospect.

---

## 3. Budget reconciliation (both run IDs)

| Run | Role | Ledger |
|-----|------|--------|
| `trun_567f3335d0c140ad8e87120211c6f61f` | Contaminated research (preserved) | spent **$1.00** estimated |
| `trun_567f3335d0c140ad9269500c06dc2b38` | Connectivity probe | reconciled **$0.50** estimated (was missing; **not treated as free**) |
| `trun_567f3335d0c140ad8fc1b7a929f468b3` | Corrected identity-lock live | spent **$0.50** (`actualBilledUsd: 0.5` from provider) |

| Cap | Estimated spent | Remaining |
|-----|----------------:|----------:|
| **$2.00** | **$2.00** | **$0.00** |

Actual Parallel invoice total still unknown beyond provider payload on the third run. Ledger was **not** reset; no fresh $2 allowance created.

---

## 4. Corrected live targeting results

Provider run: `trun_567f3335d0c140ad8fc1b7a929f468b3`  
Geo gate: **8 accepted / 0 quarantined** (all San Diego / downtown waterfront / Gaslamp).

Examples (internal drafts only — **not published**):

| Occasion | Location | Thesis posture |
|----------|----------|----------------|
| TCT 2026 | San Diego Convention Center | Bayfront fit; onPeak housing channel; Wyndham block unconfirmed |
| WEST 2027 | San Diego | Convention Center catchment; referral possible |
| NCCN Annual 2027 | San Diego | Host Hilton Bayfront; Wyndham = nearby alternative |
| AEFP 2027 | Downtown / Gaslamp | Official block elsewhere; overflow unconfirmed |

All 8 produced **COMPLETE_DRAFT_CARD_INTERNAL** with sources + thesis. Gaps remaining: named email/phone (housing URL qualifies as intermediary route under shared policy), internal reviewer decision, hotel sales validation after any future publication.

---

## 5. Private Events (hotel-specific)

| Item | Result |
|------|--------|
| Bethesda fixtures | **Not used** (`fixtureFallback.used: false`) |
| Shared Airtable PE read | 11 venues + 6 signals — **all Bethesda/DMV** |
| Disposition | **WRONG_MARKET_QUARANTINED** for San Diego (reuse ≠ local research) |
| Hotel×venue fits for SD | **0** |
| Confirmed SD PE partnership prospects this pass | **0** |
| Hotel-specific queue framed | Convention Center / Embarcadero / Gaslamp venue classes using SD Bayside attributes — **unresolved research queue**; paid venue discovery blocked by $0 remaining Parallel budget |

Artifact: `san-diego-post-live-drafts/pe-hotel-specific-assessment.json`

---

## 6. Beth / May title cleanup + Airtable

| Hotel | Updates | Facing | Drawer pass |
|-------|--------:|-------:|------------:|
| Bethesda | 14 | 13 | 13 |
| Mayflower | 15 | 11 | 11 |

- Via canonical importer (`--adapter airtable --apply`), backups under `title-cleanup-apply/backups/`.
- Airtable read-back titles now concise (e.g. `AHIMA Advocacy Summit 2027`, `NDSS Advocacy Conference 2027`) — verification notes no longer in titles.
- Feedback / actions / outcomes / unmatched rows preserved.

**Local API→Airtable read-back:** executed (`title-cleanup-apply/airtable-title-readback.json`).  
**Production browser proof of new UI assets:** blocked pending Railway deploy.

---

## 7. Deploy

```
railway whoami  → Unauthorized. Please login with `railway login`
RAILWAY_TOKEN   → unset
deploy record   → reports/deployments/2026-10-14T22-30-35-054Z-production.json
status          → RAILWAY_UP_FAILED (gates PASS; upload never started)
```

### Exact login step (then re-run deploy)

1. In this repo shell (interactive): `railway login`
2. Complete browser auth when prompted.
3. Confirm: `railway whoami` and `railway status`
4. Re-run: `npm run deploy:production`
5. After upload, verify production `/group-demand-intelligence.html` for Bethesda + Mayflower lists/drawers (Priority/Exploratory vs Action WATCH, clean titles, thesis/sources/contacts, null scores, stable IDs/feedback). Unrelated hotels unchanged.

Do **not** print credentials or bypass auth.

---

## 8. Repeatability verdict

**Not yet end-to-end repeatable for prospect delivery.**

What worked: corrected live targeting produced evidence-backed, hotel-specific **internal** draft cards through the shared workflow (modest credible yield).

What blocks calling the process E2E-complete:

1. Production UI/API deploy (Railway login).
2. San Diego **publication** intentionally not authorized.
3. Shared PE corpus is Bethesda-only — no confirmed local venue partnership cards.
4. Named contact recovery still thin (housing URLs only).
5. Human reviewer + hotel sales validation still required before any publish.
6. Parallel budget at estimated **$0** remaining under the original $2 cap.

Wrong-market results from the first live run remain preserved evidence and are **not** San Diego prospects.
