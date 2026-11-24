# P0 INCIDENT — GDI Portfolio Inventory Regression (Zero Opportunities)

**Status:** DIAGNOSED · **STOP before implementation** (founder review required)  
**Contract:** `gdi-portfolio-inventory-regression-incident-v1`  
**Audit mode:** `$0` only · no Exa/Agent · no Findymail/Surfe · no Airtable writes · no STAGED/CURRENT flips · no publish · no deploy  
**Served app:** `http://localhost:8080`  
**Evidence:** `reports/gdi-portfolio-inventory-regression-incident-v1/PORTFOLIO_MATRIX.json`  
**Date:** 2026-11-24

---

## Executive verdict

Previously researched GDI hotels are showing **zero opportunities because localhost now defaults to the STAGED finalized inventory view**, and those hotels have **no STAGED publication package**.

**Opportunity data was not deleted.** Research remains in the hotel opportunity store and is recoverable via `?live=1` / `?inventory=research`.

This is primarily a **lifecycle / data-source selection / founder UI default** defect — not lost inventory, not a contact-gate rewrite of historical rows, and not a hotel-ID mix-up for the reported case.

---

## 1. Complete portfolio count matrix (before vs now)

Portfolio size: **25** hotels in the GDI dropdown.

| Hotel | Store (research) | Default API / localhost STAGED UI | `?live=1` research | STAGED pkg | CURRENT pkg |
|---|---:|---:|---:|---|---|
| AC Hotel A Coruña | 41 | **0 / empty** | 5 | none | none |
| Bethesda Marriott | 86 | 67 (STAGED) | 88 | 67 | present |
| Cambridge Beaches | 8 | **0 / empty** | 8 | none | none |
| Castillo Hotel Son Vida | 20 | **0 / empty** | 20 | none | none |
| Condesa DF | 122 | 4 (STAGED) | 122 | 4 | — |
| Falls Church Marriott Fairview | 0 | 0 | 0 | none | none |
| **Hilton New York Times Square** | **45** | **0 / empty** | **44** | **none** | **none** |
| LaLuna Boutique | 89 | 3 (STAGED) | 89 | 3 | — |
| Memmo Príncipe Real | 96 | 11 (STAGED) | 96 | 11 | — |
| Nobu Hotel London Portman Sq | 65 | 34 (store customer-facing) | 65 | none | none |
| NOW NOW NOHO | 8 | **0 / empty** | 8 | none | none |
| Radisson Blu Toronto Downtown | 36 | 66 (STAGED) | 36 | 66 | — |
| Radisson Hotel Santo Domingo | 29 | 1 | 16 | none | none |
| Renaissance NYTS | 72 | 43 | 70 | none | none |
| Sheraton Guadalajara Expo | 43 | 38 (STAGED) | 43 | 38 | — |
| Sheraton Mallorca Arabella | 30 | **0 / empty** | 30 | none | none |
| Spice Island Beach Resort | 7 | 7 | 7 | none | none |
| The Mayflower Hotel | 65 | 37 | 65 | none | none |
| The Westin Grand München | 26 | **0 / empty** | 21 | none | none |
| The Westin Tampa Waterside | 49 | 6 | 49 | none | none |
| W Rome | 13 | 2 | 13 | none | none |
| Waterstone Resort & Marina | 55 | 28 | 55 | none | none |
| Wyndham Alltra Punta Cana | 27 | 27 (STAGED) | 27 | 27 | — |
| Wyndham San Diego Bayside | 82 | 47 | 82 | none | none |
| YOTEL Geneva Lake | n/a (pkg) | 42 (STAGED) | n/a | 42 | — |

**Summary**

| Metric | Count |
|---|---:|
| Hotels in dropdown | 25 |
| Hotels with research store > 0 | 23 |
| Hotels with default/STAGED UI > 0 | 17 |
| Hotels with research via `?live=1` > 0 | 23 |
| Hotels with research but zero STAGED UI | **7** |

Full machine matrix: `PORTFOLIO_MATRIX.json`.

---

## 2. Hotels affected (zero UI with existing research)

1. **Hilton New York Times Square** — `rec35fExUxCClpOP6` (reported)  
2. AC Hotel A Coruña — `rec2PVBDavppGpenm`  
3. Cambridge Beaches Resort & Spa — `recIwaP1etgx2g9nA`  
4. Castillo Hotel Son Vida — `rec82D9zpB8fede1I`  
5. NOW NOW NOHO — `recGkME49yYuxQl0u`  
6. Sheraton Mallorca Arabella Golf Hotel — `recjhQdAUiSyxfCqE`  
7. The Westin Grand München — `recFaxTEFF9ILHWC9`

Design Hotels with STAGED packages (Condesa / LaLuna / Memmo) are **not** in this zero set — they correctly show STAGED counts (4 / 3 / 11).

---

## 3. Root cause for `rec35fExUxCClpOP6`

**Identity:** Hilton New York Times Square (census / HPC `rec35fExUxCClpOP6`).

### Reproduced

URL: `http://localhost:8080/group-demand-intelligence.html?hotelId=rec35fExUxCClpOP6&preview=staged`

Observed UI:

- All filters `(0)`
- “Showing 0 of 0 Opportunities”
- Empty copy: **“No finalized opportunities are available for this property.”**
- Last Updated: **Sep 27, 2026**
- Status: **Research Complete**

### API chain

1. Localhost founder mode defaults to **`staged`** (`resolveFounderInventoryMode` → `isLocalFounderHost()` → `"staged"`).
2. UI requests:  
   `/api/group-demand-intelligence/hotels/rec35fExUxCClpOP6/opportunities?publicationPackage=1&surface=customer&preview=staged`
3. API returns **HTTP 409** `STAGED_PUBLICATION_PREVIEW_UNAVAILABLE`  
   - `hasStagedPublication: false`  
   - `hasCurrentPublication: false`  
   - recovery actions: `BACK_TO_HOTELS`, `CHOOSE_ANOTHER_HOTEL` only (no in-UI Research escape hatch in empty state).
4. UI maps 409 → empty inventory message (not an error toast).
5. Hotel **summary** still reports research metadata (`opportunitiesDiscovered: 45`, `qualifiedCount: 44`, `runStatus: COMPLETED`, `lastResearchAt: 2026-09-27…`) — hence “Research Complete” + September date beside an empty opportunity list.

### Proof data is not lost

| Path | Result |
|---|---|
| Default / STAGED preview | 0 / 409 |
| `?live=1` | **44** opportunities · `FOUNDER_RESEARCH_INVENTORY` · store still **45** |
| Hotel summary | discovered **45**, qualified **44** |
| Bundled `STAGED.json` / `CURRENT.json` | **absent** |

Secondary note: bare store + customer-facing filter (no package) also yields **0** for Hilton — Hilton was never a customer-published inventory. The founder-visible regression is the STAGED-default empty state, not deletion of the store.

Export CSV under staged preview returns a **header-only 200** rather than the same 409 fail-closed signal (secondary inconsistency).

---

## 4. Root causes for other zero-opportunity hotels

All 7 affected hotels share the **same primary mechanism**:

1. Localhost defaults to STAGED finalized view (PR #579 workflow restore).  
2. No hotel `STAGED.json` (and no CURRENT).  
3. API fail-closed 409 → UI empty “No finalized opportunities…”.  
4. Research store still populated (`?live=1` recovers it).

Secondary / hotel-specific layers (do **not** assume identical):

| Hotel | Additional notes |
|---|---|
| Hilton TS | Customer-facing readiness historically expect **0** published; research 45 never staged |
| AC A Coruña | Store 41 → live research only 5 after salesperson/customer filters; still non-zero research |
| Cambridge / Castillo / NOHO / Sheraton Mallorca / Westin München | Store equals or nearly equals live research; zero only under STAGED default |

Hotels that still show opportunities either:

- have a resolvable **STAGED** package, or  
- fall through to store rows that still pass **customer-facing** eligibility (e.g. Nobu, Renaissance, Mayflower).

PRs #583–#588 (contact restore / WHO / Agent) did **not** delete Hilton store rows. They are not the primary zero-UI cause for these seven hotels.

---

## 5. Exact first bad commit / configuration

| Item | Value |
|---|---|
| **First intentional bad (for this symptom)** | Merge **PR #579** · `367bf7a1468cfb89b28d104cff4eb1513eec8e8f` (2026-11-23) |
| Feature commit inside PR | `fd7353348` — “restore localhost STAGED default for founder review” |
| Prior opposite default | `1570d69a5` — “Restore localhost Research Preview without requiring STAGED packages” (2026-11-18) |
| Amplifying UX change | PR **#583** (`eab775f79`) — cleaned STAGED empty state; empty copy no longer offers **View Research** in the normal founder empty panel |

PR #579 deliberately restored Full E2E staging discipline (STAGED-first localhost). The incident is the **portfolio-wide consequence** for hotels that have research but were never staged — combined with empty-state UX that no longer surfaces Research as an escape hatch.

---

## 6. Was any opportunity data actually lost?

**No evidence of deleted research inventory for the affected set.**

- Hilton store still holds **45** opportunities.  
- `?live=1` returns **44**.  
- Summary still reports discovery/qualification counts.  
- Design Hotels STAGED packages still resolve.  
- Bethesda STAGED 67 still resolves.

What changed is **which inventory surface localhost serves by default**, not erasure of historical research files.

---

## 7. Which inventories remain recoverable

| Inventory | Recoverable? | How |
|---|---|---|
| RESEARCH (hotel opportunity store) | **Yes** | `?inventory=research` / `?live=1` |
| QUALIFIED research subset | Partially | Present in store/summary; not all customer-facing |
| FINALIZED STAGED | Only where packages exist | Condesa/LaLuna/Memmo/Bethesda/etc. |
| CURRENT published | Only where CURRENT pointer exists | Unchanged; not flipped in this audit |
| Sales Ready | Independent | Unchanged; not used to invent rows |

---

## 8. Proposed smallest safe correction (DO NOT IMPLEMENT YET)

Founder must choose policy. Recommended smallest safe path:

### Option R1 — Restore explicit Research access without weakening STAGED (preferred)

1. Keep localhost default = **STAGED** for hotels that have STAGED (Full E2E contract preserved).  
2. When STAGED is missing, empty state must clearly distinguish:
   - “No **finalized STAGED** package”  
   - vs “No research exists”  
3. Restore an explicit **View Research** control (founder/admin only) that switches to `?inventory=research&live=1` — **no silent fallback**, no auto-stage, no publish.  
4. Show research counts from hotel summary beside the empty STAGED message (e.g. “45 researched · 0 staged”).  
5. Fix export.csv to fail-closed consistently on missing STAGED preview (or label CSV as empty staged export).

### Option R2 — Default localhost to Research when no STAGED exists

Conditional default: STAGED if pointer exists, else Research.  
Risk: reintroduces research-as-primary habit for unfinished hotels. Only if founder accepts.

### Explicitly rejected for this incident

- Fabricating STAGED packages from raw research to make counters positive  
- Weakening customer-facing / contact / Sales Ready gates  
- Silent STAGED pointer flips to older snapshots without identity/version authorization  
- Paid re-research to recreate visible rows  
- Treating contact-quality recovery (#589) as the cause of this zero UI

---

## 9. Regression tests required (plan only)

Permanent portfolio inventory regression suite should fail when:

1. An approved STAGED/CURRENT hotel inventory unexpectedly returns empty.  
2. A valid STAGED pointer cannot resolve its package.  
3. Hotel ID A returns Hotel B opportunities.  
4. Server silently falls back to `[]` after a data-source error (prefer 409/503 with code).  
5. A new contact rule retroactively suppresses previously approved STAGED/CURRENT inventory without an authorized lifecycle transition.  
6. Missing STAGED is reported as “no research” while store/summary show discoveries > 0.  
7. Filter chip counts disagree with returned inventory length.  
8. Stale research timestamp is presented as current without provenance (summary vs publication).

Keep existing security, contact verification, and publication gates intact.

---

## 10. PR and integration plan (after founder GO)

1. Open a focused incident-fix PR (separate from Agent quality recovery #589).  
2. Implement only the chosen Option R1/R2 + regression tests.  
3. Local suites + `dealality:ci-precheck`.  
4. Browser acceptance on Hilton + one Design Hotel with STAGED + one unaffected customer-facing hotel.  
5. Integration Controller merges; no STAGED/CURRENT flips in that PR unless separately authorized.

**Do not merge opportunistic contact/Agent work into this incident fix.**

---

## 11. Browser and API acceptance plan

### API

- Hilton `preview=staged` → 409 `STAGED_PUBLICATION_PREVIEW_UNAVAILABLE`  
- Hilton `live=1` → count ≈ 44, storeIdentity 45  
- Hilton summary → discovered/qualified > 0 while staged unavailable  
- Condesa `preview=staged` → count 4 package  
- Affected hotels matrix matches `PORTFOLIO_MATRIX.json`

### Browser (`localhost:8080`)

- Hilton STAGED default: empty finalized message + Research Complete + Sep 27 date (current defect baseline)  
- After fix: empty STAGED state must not claim “no research”; Research view must show recovered rows  
- Condesa STAGED still shows 4  
- No accidental publish / CURRENT change

---

## Lifecycle separation (confirmed needed)

| Layer | Meaning | Hilton today |
|---|---|---|
| RESEARCH | Discovered / under investigation | **45 in store** |
| QUALIFIED | Commercially relevant with evidence | Summary qualified **44**; not customer-published |
| FINALIZED STAGED | Validated founder/customer review package | **Missing** |
| CURRENT | Authorized published inventory | **Missing** |
| SALES_READY | Independent sales-action bar | Not implied by research presence |

**Absent STAGED must not be presented as absent demand.**  
**Restoring research visibility must not silently publish.**

---

## Governance confirmation

- $0 audit complete  
- No new Exa/Agent/Findymail/Surfe  
- No Airtable writes  
- No STAGED/CURRENT pointer flips  
- No customer publication / production deploy  
- Dirty worktrees and historical packages preserved  
- Contact architecture not replaced  

---

## STOP

**No implementation in this turn.**

Await founder choice: **R1** (recommended) vs **R2**, then authorize a focused recovery PR.

**Final status:** `BLOCKED — AWAITING_FOUNDER_RECOVERY_POLICY`
