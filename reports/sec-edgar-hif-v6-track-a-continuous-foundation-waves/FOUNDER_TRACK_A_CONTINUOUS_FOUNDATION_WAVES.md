# HIF Track A — Continuous Foundation Waves (A1–A4)

**As of:** 2026-11-21  
**Publication:** CUSTOMER_BLOCKED  
**Research spend:** **$0**  
**Branch:** `feat/hif-v6-track-a-continuous-foundation-waves`  
**Baseline HEAD at start:** `1f6af0677` (= `origin/main` tip including PR #540)  
**E2E classification:** **PERSISTENCE_VALIDATED** (when Airtable apply + read-back succeed) — not FULL_E2E / not CUSTOMER_PUBLISHED

---

## 1. Distinct hotels investigated

**28** distinct hotels / sites / portfolios investigated across waves A1–A4 (US, CA, MX, BR, GB, TT, PT, ES, SG, MY, OM incidental).

Priority countries covered without restricting eligibility: **US, CA, MX, BR, GB**.

---

## 2. New qualified opportunities by deal type

| Deal type | New this wave | Notes |
|-----------|--------------:|-------|
| **BCT (principal)** | 0 net-new | Lakeville carried (still open) |
| **MCT (principal)** | **1** | **Hilton Trinidad & Conference Centre** |
| Concession (adjacent) | 1 refresh | Gideon Putnam — proposals due **2027-01-14** |
| Hotel development (adjacent) | 1 refresh | Pueblo downtown — due **2027-01-11** |
| Transaction (expanded) | 1 | Magdalena Grand sale EOI (not first-100) |
| Operator tender watch | 1 | George Hotel Stranraer — submissions closed; award watch |

---

## 3. First-100 BCT/MCT progress

| Metric | Count |
|--------|------:|
| Qualified open **BCT** | **1** (Lakeville) |
| Qualified open **MCT** | **1** (Hilton Trinidad) |
| **First-100 progress** | **2 / 100** (was 1 after CB-1) |
| Commercially relevant | **2** |
| Commercially accessible | **2** |
| Outreach-ready | **0** |

---

## 4. Expanded opportunity pipeline

- Gideon Putnam concession (US) — open RFP  
- Pueblo downtown hotel development (US) — open RFP requiring brand + operator  
- Magdalena Grand (TT) — sale EOI (sale-only; preserve, do not inflate first-100)  
- George Hotel Stranraer (GB) — award-pending adjacent operator  
- Dorset 61-property MCT — award-pending (carried)  
- Casa Mett (SG) — pre-opening operator adjacent (carried)

---

## 5. Commercial relevance vs accessibility

| Dimension | Count |
|-----------|------:|
| VERIFIED / EVIDENCE-SUPPORTED relevant (first-100) | 2 |
| ACCESSIBLE_VERIFIED / ACCESSIBLE_PROBABLE (first-100) | 2 |
| ACCESS_ROUTE_RESEARCH_REQUIRED (watch) | 1 (Mont-Royal) |
| ACCESS_CLOSED (award watch) | 2 (Dorset, George) |
| Outreach-ready | 0 |

Unknown accessibility was **not** used as automatic rejection (Mont-Royal kept as watch).

---

## 6. Country × Timing × Deal Type × Freshness

See `MATRIX_COUNTRY_TIMING_DEALTYPE_FRESHNESS.json`.

| Country | Open principal | Gap |
|---------|----------------|-----|
| US | Lakeville BCT + adjacent Gideon/Pueblo | Private franchise windows still thin |
| TT | Hilton Trinidad MCT | New CALA principal this wave |
| CA | 0 principal | Mont-Royal / Saint John deflags without brand-search evidence |
| MX | 0 | UNDER_RESEARCHED (awarded conversions only) |
| BR | 0 | UNDER_RESEARCHED (macro conversion window) |
| GB | 0 open for new entrants | Dorset/George award watch |
| ES/PT | 0 | Completed internal rebrands |

---

## 7. Public vs private discovery yield

| Pathway | First-100 | Adjacent / expanded |
|---------|----------:|--------------------:|
| Private (owner-led) | 1 (Lakeville) | Mont-Royal watch |
| Public (tender/EOI) | 1 (Hilton Trinidad) | Gideon, Pueblo, Magdalena, George, Dorset |

Private owner-led pathways remain scarce but high quality; public pathways dominate volume.

---

## 8. Hotel-level opportunity dossiers (principal)

### Lakeville Inn — Lakeville, MN, US — **PRIORITY A BCT · first-100** (carried)

- Franchise termination + owner-stated brand search by end-2026  
- Pathway: OWNER_LED_PRIVATE_DECISION  
- Accessibility: ACCESSIBLE_PROBABLE · contact not validated  
- Airtable: expected **NOOP** (already persisted CB-1)

### Hilton Trinidad & Conference Centre — Port of Spain, TT — **PRIORITY A MCT · first-100** (new)

- **Agreement type:** short-term Hotel Management Agreement (6 months + optional 6)  
- **Opportunity type:** MCT (longer-term operator talks) — related unresolved instrument = HotelTT acquisition EOI (do not force sale instrument into MCT)  
- **Trigger:** State SPA → HotelTT ownership; temporary Hilton HMA; Government EOI for buyers; minister confirmed operator talks will follow  
- **Timing:** 0–6 months (HMA window / EOI responses due ~2027-01-29)  
- **Accessibility:** ACCESSIBLE_PROBABLE via public procurement / eTecK  
- **Sources:** TTT News 2026-09-04; Guardian 2026-10-25 / 2026-10-31; Express 2026-10-18

---

## 9. Airtable creates / updates / no-ops / conflicts

Ledger files: `PERSISTENCE_LEDGER_DRY_RUN.json` · `PERSISTENCE_LEDGER_FIRST_APPLY.json` · `PERSISTENCE_LEDGER_IDEMPOTENT.json`

| Action | Result |
|--------|--------|
| Destinations | Discovery Inventory + Discovery Findings only |
| Creates | **10** (5 inventory + 5 findings) |
| No-ops (first apply) | **2** (Lakeville inventory `recPXlLWkd63mLK5h` + finding `recjL2vx3ExNlReF1`) |
| Idempotent retry | **12** no-ops / 0 creates |
| Updates | 0 |
| Conflicts | 0 |
| HPC creates | **0** |
| Schema changes | **0** |
| Publication State | CUSTOMER_BLOCKED |

Created inventory IDs: Hilton Trinidad `rec0f9E8genYc3tI2` · Magdalena `recAFqwZkl8gkVfwf` · Gideon `rec9AlrXhmBeLj6U3` · Pueblo `recyuthsKuooZ8ndS` · George `recmK0e6HuNZtyBzQ`  

Created finding IDs: Trinidad MCT `recDRyNUriKMAl58C` · Magdalena `reciSG6VNdeY3Xnu4` · Gideon `recNI18m37OxJjTIK` · Pueblo `rectAlkLNFS928wpB` · George `recpBeIvHOKlpB7CB`

---

## 10. Cost, tests, branch, PR

| Item | Value |
|------|-------|
| Cost | **$0** |
| Tests | `npm run test:hif-track-a-continuous-foundation` · `npm run test:hif-foundation-persistence` · `npm run test:hif-v7-commercial-dimensions` |
| Branch | `feat/hif-v6-track-a-continuous-foundation-waves` |
| PR | Open for Integration Controller — **do not merge from Track A** |

### Rejected / completed (honest non-claims)

Hotel Pattee (awarded Embergrove), Saint John Harbourfront (independent strategy), Onyria→Kimpton Cascais (completed), Aimbridge Real Inn→voco (awarded), JW Marriott Reston→Davidson (awarded), Hotel Hartness in-house, Hotel Pommier (prior completed).

### IC handoff

1. Mandatory CI + gitleaks  
2. Integration Controller merges only  
3. After merge: `npm run dealality:post-merge-sync` for affected homes when safe  
4. Next Track A: deepen MX/BR named-hotel private MCT/BCT; monitor Trinidad EOI/HMA; Mont-Royal brand-intent probe; Dorset/George awards

**CUSTOMER_BLOCKED** · No paid enrichment · No schema invention
