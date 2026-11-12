# Bethesda GDI — Hotel Sales Commercial Acceptance

**Hotel:** Bethesda Marriott (`recLuxvwwxID7U2B8`)  
**Date:** 2026-11-12  
**GitHub main before work:** `fd887c16e633e4cd3486a294ffe8c7d020f2a0e3`  
**Contract:** DOS/DOSM review of the **actual** primary commercial list (not the raw 88)

## Lock verification

| Check | Result |
|-------|--------|
| Canonical store rows | **59** |
| Historical / union IDs | **29 / 88 preserved** |
| Five Scaling Companies | **present** |
| Store SHA before remediation | `0c9a235bd47e6a19139089ec43ff44c01954b9edb36e8bc93ae1ef9fce036962` |
| Store SHA after remediation | `79be23c4ff344679381c6aa48cbe42fc189d69fce5eac4aa02f5e025bab26b26` |
| Forbidden commercial leaks | **0** |
| Detail resolution | **67/67** (post-remediation commercial list) |

## Commercial count note (68 → 67)

PR #196/#197 reported **68** commercial cards. Independent hotel-sales review found **ACCP Annual Meeting 2026** (`gdi_opp_accp_annual_meeting_20261001`) hosted in **Salt Lake City** (`OUT_OF_MARKET_HOST_RESEARCH_GATED`) still stamped `HIGH_PRIORITY` / commercial. It was moved to **ARCHIVE_DISQUALIFIED** and added to the forbidden commercial ID list.  
**Legitimate updated commercial count: 67.** All 88 underlying IDs remain intact.

## Sales usability scorecard (67 displayed)

| Verdict | Count | Meaning for hotel sales |
|---------|------:|-------------------------|
| ACTIONABLE_NOW | **3** | Enough verified info to begin appropriate outreach |
| ACCOUNT_DEVELOPMENT | **11** | Credible account / recurring demand — develop proactively |
| QUALIFY_FIRST | **7** | Promising; resolve host/housing/fit before pitching |
| MONITOR | **46** | Future-cycle, overflow, partnership, or housing-not-open |
| REMOVE_FROM_COMMERCIAL | **0** | (ACCP already removed from list) |

Machine-readable 67-row scorecard: `01-commercial-sales-scorecard.json`.

## Revenue types (Guest Rooms / Local Catering / Both)

| Type | Count |
|------|------:|
| GUEST_ROOMS | 44 |
| LOCAL_CATERING | 7 |
| BOTH | 16 |
| UNCONFIRMED | **0** |

No fabricated icons. Competitor-host F&B was not assumed available to Bethesda Marriott.

## Contact coverage (displayed commercial)

| Bucket | Count |
|--------|------:|
| A — named + direct email | 38 |
| B — named + org route | 28 |
| C — named, no route | 1 |
| D–F | 0 on commercial list |

## Engine vs hotel-sales reconciliation (focus set)

| Opportunity | Engine eligibility | Persisted product maturity | Hotel-sales usability | Proposed product action |
|-------------|--------------------|----------------------------|-----------------------|-------------------------|
| Loudoun College Showcase 2027 | SALES_READY_ELIGIBLE | not persisted | **MONITOR / QUALIFY Beltway** | **Do not promote Sales Ready** — Loudoun primary; Beltway only if HBC accepts |
| ACTS Translational Science 2027 | ACCOUNT_DEVELOPMENT_ELIGIBLE | not persisted | ACCOUNT_DEVELOPMENT | Stage AD stamp only with founder approval |
| NADO WashCon 2028 | SALES_READY_ELIGIBLE | not persisted | **QUALIFY_FIRST** | **Do not promote Sales Ready** — Arlington / hotel TBA |
| NADO WashCon 2027 overflow | ACCOUNT_DEVELOPMENT_ELIGIBLE | not persisted | MONITOR | Keep overflow watch |
| Five Scaling Companies | ACCOUNT_DEVELOPMENT_ELIGIBLE | not persisted | ACCOUNT_DEVELOPMENT | Stage AD stamps in one approval package |

Full staged maturity package: `03-maturity-promotion-approval-package.json` (**33** candidates — **none auto-applied**).

**Founder decision required (single package):**
1. Reject Sales Ready persistence for Loudoun + NADO 2028 (recommended).
2. Optionally persist Account Development stamps for the 31 engine-AD-eligible rows (or a subset of the top prospects below).
3. No customer publication in this exercise.

## Top prospects for Bethesda Marriott sales (15)

1. **Woman's Club of Bethesda** — Diane Robertson (rentals@…) — Preferred lodging partnership — **ACTIONABLE_NOW** — Guest Rooms  
2. **Bethesda Premier Cup** — Brad Roos — Local tournament housing — **ACTIONABLE_NOW** — Guest Rooms  
3. **Potomac Memorial 2027** — Kathy Hauschild / HBC housing list — **ACTIONABLE_NOW** — Guest Rooms  
4. **ACTS TS27** — Adam Joyce — DC translational science housing — ACCOUNT_DEVELOPMENT — Guest Rooms  
5. **AHIMA Advocacy Summit 2027** — Sabrina Bracken — DC advocacy housing — ACCOUNT_DEVELOPMENT — Guest Rooms  
6. **ACC Legislative Conference 2027** — Meg Novak — DC legislative housing — ACCOUNT_DEVELOPMENT — Guest Rooms  
7. **NICE 2027** — Karen Wetzel — NIST/cyber education — ACCOUNT_DEVELOPMENT — Both  
8. **AMWA 2027 Annual** — Kelly McMahon — Medical association — ACCOUNT_DEVELOPMENT — Both  
9. **NAR GAD Institute 2027** — Jami Sims — Arlington leadership — ACCOUNT_DEVELOPMENT — Both  
10. **AFCEA Health IT Summit 2027** — Andrea Snader — Overflow vs Bethesda North — ACCOUNT_DEVELOPMENT — Guest Rooms  
11. **CMSS Spring 2027** — Shaniece Rigans — Small leadership meeting — ACCOUNT_DEVELOPMENT — Guest Rooms  
12. **Matthews** — Jeff Perkins — Scaling corporate — ACCOUNT_DEVELOPMENT — Guest Rooms  
13. **IMC Construction** — Dan Baker — Scaling corporate — ACCOUNT_DEVELOPMENT — Guest Rooms  
14. **BioHub Maryland** — Alexandra Haarich — Scaling corporate — ACCOUNT_DEVELOPMENT — Guest Rooms  
15. **Miles & Stockbridge / REGENXBIO** — named partners — Scaling corporate — ACCOUNT_DEVELOPMENT — Guest Rooms  

Exact next actions: `02-top-prospects-and-next-actions.json`.

## Corrections actually persisted

1. **ACCP 2026** → ARCHIVE_DISQUALIFIED (Salt Lake City out of market); forbidden on commercial list.  
2. **RIC** `summaryWhat` placeholder replaced with Rockville HQ / lodging-adjacent overflow copy.  
3. **2027 DDAA** placeholder summary + monitor fit note grounded.  
4. **Potomac Memorial** recommended action → HBC / tournament housing-list inclusion (not “do not pursue as venue”).  
5. **Loudoun Showcase** recommended action → Beltway qualify-first with HBC.  
6. Surface disposition: `OUT_OF_MARKET_HOST_RESEARCH_GATED` + outside-host evidence → archive.  
7. Permanent gate: `npm run test:gdi-bethesda-68-hotel-sales-commercial-acceptance-v1`.

## Remaining commercially significant gaps

- Many MONITOR rows still need future housing/host confirmation (Crystal City / Bethesda North competitor hosts).  
- Engine Sales Ready (2) is **looser** than hotel-sales ACTIONABLE_NOW — tighten SR eligibility permanently (destination confirmed + hotel fit not thin).  
- Generic capacity boilerplate remains on some `summaryWhyHotel` fields (non-blocking).  
- No paid research run; several housing partners still need public deadline confirmation closer to event.

## Tests / API results

- `test:gdi-bethesda-68-hotel-sales-commercial-acceptance-v1` — PASS  
- `test:gdi-commercial-surface-disposition-v1` — PASS (includes ACCP)  
- `test:gdi-list-to-detail-integrity-v1` — **67/67 detail** PASS  
- `test:gdi-bethesda-commercial-surface-route-fail-closed-v1` — PASS  

Authenticated browser verification: **not claimed** in this report (API + in-process list/detail gates completed).

## Safety confirmation

- No paid research spend  
- No customer publication  
- No unauthorized maturity promotion  
- No destructive git operations  
- 88 IDs / 59+29 / five Scaling Companies preserved  
- Evidence history and share registries untouched except exact-field patches above  

## Artifacts

- `00-acceptance-summary.json`  
- `01-commercial-sales-scorecard.json` (full 67-row DOS scorecard)  
- `02-top-prospects-and-next-actions.json`  
- `03-maturity-promotion-approval-package.json`  
- `04-remediation-candidates.json`  
- `05-commercial-id-list.json`  
- `06-acceptance-remediation-applied.json`  
