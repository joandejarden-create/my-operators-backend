# GDI Portfolio Inventory Visibility Recovery — R1 Certification

**Date:** 2026-11-24  
**Authorization:** FOUNDER GO — R1 APPROVED ($0)  
**Branch:** `feat/gdi-inventory-visibility-r1`  
**Worktree:** `C:\Dev\dealality\wt-gdi-inventory-visibility-r1`  
**Serving:** `http://localhost:8080` (this worktree)

---

## 1. Root-cause correction summary

After STAGED became the default founder inventory, missing-STAGED hotels rendered a **generic empty state** (“No finalized opportunities…”) that:

1. Omitted that research inventory still existed.
2. Removed the founder **VIEW RESEARCH** recovery control.
3. Collapsed lifecycle conditions into one empty screen.

STAGED fail-closed (HTTP 409 `STAGED_PUBLICATION_PREVIEW_UNAVAILABLE`) was correct and is retained. R1 restores founder navigation and lifecycle clarity **without** fabricating STAGED packages, relaxing qualification, or substituting research as finalized inventory.

Shell localhost default had drifted to Research; R1 restores **STAGED** as the default in both `public/app.js` and GDI `app.js`.

---

## 2. Exact code changes

| Area | Change |
|---|---|
| `public/js/group-demand-intelligence/app.js` | Lifecycle states; founder copy + counts/timestamps; VIEW RESEARCH (`?inventory=research&live=1`); research banner + return to STAGED; load-error vs empty separation |
| `public/app.js` / `public/app.html` | Localhost default → STAGED; cache-bust |
| `public/css/group-demand-intelligence.css` | Inventory counts + research-mode banner styles |
| `api/group-demand-intelligence.js` | `RESEARCH_INVENTORY_FORBIDDEN` for authenticated non-admin `?live=1` |
| `lib/.../gdi-founder-preview-availability-v1.js` | Research counts + VIEW_RESEARCH recovery action |
| `scripts/test-gdi-inventory-visibility-r1.mjs` | Permanent 25-hotel portfolio regression |
| CI | Wired into `gdi-regression-recovery-baseline.yml` |

**Not changed:** STAGED/CURRENT pointers, Airtable, paid research, customer publication, qualification gates.

---

## 3. Before / after screenshots

| File | Evidence |
|---|---|
| `screenshots/01-hilton-missing-staged-founder-empty.png` | Hilton STAGED empty → founder copy + 45/44/0 + VIEW RESEARCH |
| `screenshots/02-hilton-research-mode.png` | Hilton Research → RESEARCH / NOT FINALIZED + Showing 44 of 44 |
| `screenshots/05-cambridge-missing-staged.png` | Cambridge 8/8/0 + VIEW RESEARCH |
| `screenshots/06-ac-coruna-missing-staged.png` | AC Coruña missing-STAGED founder state |
| `screenshots/04-bethesda-staged.png` | Bethesda STAGED preserved — Showing 67 of 67 |

---

## 4. Portfolio-wide count matrix

Machine-generated: `PORTFOLIO_COUNT_MATRIX.json` (25 hotels).

### Seven no-STAGED (API + store)

| Hotel | Discovered | Qualified in research | STAGED HTTP | Finalized STAGED |
|---|---:|---:|---:|---:|
| Hilton New York Times Square | 45 | 44 | 409 | 0 |
| AC Hotel A Coruña | 41 | 5 | 409 | 0 |
| Cambridge Beaches | 8 | 8 | 409 | 0 |
| Castillo Hotel Son Vida | 20 | 20 | 409 | 0 |
| NOW NOW NOHO | 8 | 8 | 409 | 0 |
| Sheraton Mallorca Arabella | 30 | 30 | 409 | 0 |
| The Westin Grand München | 26 | 21 | 409 | 0 |

### Protected STAGED

| Hotel | STAGED count | HTTP |
|---|---:|---:|
| Condesa DF | 4 | 200 |
| LaLuna | 3 | 200 |
| Memmo Príncipe Real | 11 | 200 |
| Bethesda Marriott | 67 | 200 |

---

## 5. Founder Research navigation proof

- Hilton STAGED empty → **VIEW RESEARCH** sets `inventory=research&live=1` and preserves `hotelId=rec35fExUxCClpOP6`.
- Research view shows **RESEARCH / NOT FINALIZED**, counts 45/44/0, **Showing 44 of 44**, and **View Finalized STAGED**.
- API: `listScope=FOUNDER_RESEARCH_INVENTORY`, count 44 (does not reinterpret as finalized customer inventory).

---

## 6. Customer authorization proof

| Control | Result |
|---|---|
| UI `canAccessFounderResearchInventory()` | VIEW RESEARCH / research banner only for localhost founder host or admin flag |
| API `?live=1` | Authenticated non-admin → HTTP 403 `RESEARCH_INVENTORY_FORBIDDEN` |
| Customer empty (non-founder) | Neutral “No finalized…” without research CTA |
| Share / CURRENT paths | Unchanged; no auto research substitute |

---

## 7. STAGED preservation results

- Condesa 4 / LaLuna 3 / Memmo 11 / Bethesda 67 — pointers and package counts unchanged.
- No `stageForProduction` / pointer writes in this change set.
- CURRENT untouched.

---

## 8. Permanent CI results (local)

```
npm run test:gdi-inventory-visibility-r1                 → PASS (12/12)
npm run test:gdi-missing-inventory-empty-state-v1        → PASS
npm run test:gdi-founder-preview-missing-staged-nav-v1   → PASS
npm run test:gdi-buyer-who-e2e-recovery-v1               → PASS
npm run test:gdi-best-suggested-contact-restore-v1       → PASS
npm run test:gdi-customer-ui-regression-inventory-contacts-v1 → PASS
npm run test:gdi-regression-recovery-v1                  → PASS
```

Registered in `.github/workflows/gdi-regression-recovery-baseline.yml`.

---

## 9. PR and Integration Controller status

- PR: opened for Integration Controller merge (URL in chat closeout).
- Product chat does **not** merge.
- Status after PR open: `AWAITING_INTEGRATION_CONTROLLER`

---

## 10. Remaining visibility defects

1. **Selectable GDI catalog** in this worktree currently lists **22** hotels — Condesa / LaLuna / Memmo are absent from the dropdown even though STAGED packages resolve by direct `hotelId` API (browser deep-link may warn / fail closed rather than remapping). Not introduced as a publication change; track as follow-up catalog registration.
2. Browser PASS is founder/local pilot-read on localhost — not a production Memberstack customer session recording (API + UI gates covered in tests).

---

## Governance checklist

- [x] $0 — no Exa/Agent/Airtable/paid calls  
- [x] No STAGED/CURRENT pointer changes  
- [x] No customer publication / production deploy  
- [x] Dirty worktrees preserved elsewhere  
- [x] STOP after founder review of corrected UI + certification  

### Browser acceptance

| Hotel | Result |
|---|---|
| Hilton New York Times Square | **PASS** (empty + research) |
| Cambridge Beaches | **PASS** (empty + VIEW RESEARCH) |
| AC Hotel A Coruña | **PASS** (empty + VIEW RESEARCH) |
| Bethesda | **PASS** (67 STAGED) |
| Condesa / LaLuna / Memmo | **API PASS** (4/3/11); UI catalog gap noted above |
