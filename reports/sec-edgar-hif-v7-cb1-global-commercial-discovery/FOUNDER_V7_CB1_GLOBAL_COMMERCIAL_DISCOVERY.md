# HIF V7 CB-1 — Global Commercial Development Opportunity Discovery

**As of:** 2026-11-21  
**Publication:** CUSTOMER_BLOCKED  
**Research spend:** **$0**  
**Branch:** `feat/hif-v6-track-a-v7-cb1-global-commercial-discovery`

## Permanent policy encoded

A **public RFP is not required** for commercial qualification.

Independent dimensions:

| Dimension | Purpose |
|-----------|---------|
| COMMERCIAL_RELEVANCE | Real potential brand/management decision? |
| COMMERCIAL_TIMING | When might the decision occur? |
| COMMERCIAL_ACCESSIBILITY | Plausible engagement route? |

Unknown owner/contact → `ACCESS_ROUTE_RESEARCH_REQUIRED` — **never** auto `ACCESS_CLOSED`.

Module: `lib/hotel-intelligence/sec-edgar/hif-v7-commercial-dimensions-v7.js`  
Gate: `npm run test:hif-v7-commercial-dimensions` (12 permanent regressions)

---

## Commercial results

| Metric | Count |
|--------|------:|
| Qualified open **BCT** (first-100) | **1** |
| Qualified open **MCT** (first-100) | **0** |
| Priority A (principal) | **1** (Lakeville Inn) |
| Priority A adjacent | **3** (Gideon, Pueblo, Casa Mett, Aena BR*) |
| Priority B | **3** (Chicago Marquis, Dorset award-pending, Kenricia) |
| Private pathway | **2** (Lakeville, Casa Mett) |
| Public pathway | **4+** |
| Commercially relevant | **1** |
| Commercially accessible | **1** |
| Outreach-ready | **0** |
| **First-100 progress** | **1 / 100** (was 0 after V7 validation) |

\*Aena counted as one adjacent development program (4 airport sites).

---

## Opportunity dossiers (qualified / Priority A)

### 1. Lakeville Inn — Lakeville, MN, US — **PRIORITY A BCT · first-100**

- **Current brand/operator:** Independent (ex-Hampton Inn); Elevate Hospitality / JAMP Hotels (new owner Mar 2026)
- **Trigger:** Hilton franchise termination Jan 2026 + ownership change; owner states goal of **new brand agreement by end of 2026**
- **Pathway:** OWNER_LED_PRIVATE_DECISION (no RFP)
- **Relevance:** VERIFIED_RELEVANT · **Timing:** 0–6 months · **Accessibility:** ACCESSIBLE_PROBABLE
- **Engage:** Direct owner (Jaymin Patel / JAMP) — contact not validated → not outreach-ready
- **Next:** Brand-development outreach; confirm brand still uncommitted
- **Sources:** Star Tribune 2026-04-09; Lakeville Chamber 2026-04-13

### 2. Casa Mett — Singapore — **PRIORITY A adjacent (pre-opening operator)**

- Owner Royal Group seeking replacement operator after Sunset exit; open target **Q2 2027**
- **Not** first-100 (not an existing-hotel BCT/MCT)
- Pathway: OWNER_LED_PRIVATE_DECISION

### 3–4. Gideon Putnam (concession) + Pueblo downtown (development)

- Preserved open adjacent Priority A from V7 validation — outside first-100 BCT/MCT numerator

### Dorset Council 61-property management tender (GB) — **PRIORITY B MCT**

- Public MCT portfolio; submissions closed 2026-08-05; award notice not yet published
- ACCESS_CLOSED for new entrants; commercially relevant for monitoring

---

## Coverage & research economics

- **Hotels investigated:** 18 distinct  
- **Countries:** US, CA, MX, BR, GB (+ SG, ZA incidental)  
- **Cost:** $0  
- **False positives / completed:** Hotel Pommier (IA), Hilton Durban→Royal Majestic, Hotel Cala/Curio Tampa, HALL Arts reflag (awarded), Raleigh Studios housing RFP  
- **MX / CA:** UNDER_RESEARCHED (macro conversion signals ≠ hotel-specific opportunities)  
- **Productivity:** Private owner-led pathway produced the only first-100 BCT; public RFP scan mostly closed/adjacent

Country × Timing × Accessibility: `MATRIX_COUNTRY_TIMING_ACCESSIBILITY.json`

---

## Engineering

| Item | Status |
|------|--------|
| `hif-v7-commercial-dimensions-v7.js` | Added |
| Procurement helper | Updated for no-RFP private pathways |
| Tests | commercial-dimensions (12) + procurement + V7 current-backward |
| Reports | `reports/sec-edgar-hif-v7-cb1-global-commercial-discovery/` |

---

## IC handoff

1. Open PR from this branch → mandatory CI + gitleaks  
2. Integration Controller merges only  
3. Preserve dirty HE/ADP/GDI/BE/Track worktrees — no post-merge apply until safe  
4. Next Track A task: **CB-1b** deepen US private franchise nonrenewals + ES/PT MX/BR existing-hotel operator searches + GB award monitoring (Dorset)

**CUSTOMER_BLOCKED** · No paid enrichment · SEC continuous engine unchanged
