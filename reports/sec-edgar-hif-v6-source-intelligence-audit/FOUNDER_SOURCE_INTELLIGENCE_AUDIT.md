# HIF V6 — Source Intelligence & Discovery Pattern Audit

**As of:** 2026-11-21  
**Publication:** CUSTOMER_BLOCKED  
**Research cost:** $0  
**Name-free discovery validated:** YES

## Executive result

Hotel-name-first and country-keyword strategies were producing diminishing returns because they targeted the wrong **document semantics**.

The audit identified a repeatable, **name-free** CMBS discovery path:

1. Search EDGAR for the column header  
   **`Expiration/Termination of Related License`** (424B2 / 424H)  
2. Download the annex using **depositor/issuer CIK** (not the 1539497 filing-agent CIK)  
3. Extract property rows with a plaintext parser that survives `<BR>`-split headers and nested tables  

**Held-out validation (10 docs, known-success URLs excluded):**

| Metric | Result |
|--------|-------:|
| Docs retrieved OK | 10 |
| Net-new hotels | **129** |
| Active dated (toward 1,000) | **89** |
| Historical (preserved, not milestone) | 40 |
| Duplicates vs prior inventory | 165 |
| False-positive / noisy names | 33 |
| Cost per incremental toward-1000 | **$0** |

Examples (previously unknown to the name-first queue, dates independently extracted):

- Hilton Garden Inn - Baltimore White Marsh — franchise/license exp **2032-06-30**
- Courtyard by Marriott - Wall at Monmouth Shores — **2032-05-15**
- Sheraton - Orlando North — **2037-06-16**
- Holiday Inn Express Jacksonville Beach — **2032-07-12**
- Fairfield Inn & Suites Baltimore - White Marsh — **2033-04-30**

---

## 1) What distinguishes successful vs unsuccessful sources

### Successful (Wave 2 gold standard)
- CMBS **424B2 annex** HTML (`n*_424b2-x*.htm` / `n*_x*-424b2.htm`)
- Property schedule with column:  
  **Expiration/Termination of Related License/ Franchise Agreement, Operating Agreement or Management Agreement**
- Multiple hotels per document (schedule density)
- Hotels discovered **without** prior hotel-name query

### Unsuccessful / false-positive families
| Pattern | Why it fails |
|---------|--------------|
| `Franchise Expiration Reserve / Trigger` | Cash-management language, not a date column |
| Narrative `franchise expiration date` (key money / FF&E sweep) | Prose reference, no property schedule |
| Country-keyword SEC search (MX/BR) | Mislabels US hotels; geography gate required |
| Bare `Franchise Expiration` EFTS query | Dominated by Reserve/Trigger hits |
| Wrong EDGAR path CIK (accession 1539497) | HTTP 404 / NoSuchKey stubs |
| Nested `<table>` regex only | Misses BR-split headers → 0 rows despite gold docs |
| EX-10 / 8-K name NER without schedule | High quarantine |
| Portal reachability / company registration | Not contractual intelligence |

---

## 2) Source-family discovery taxonomy (summary)

### US — SEC / CMBS (highest proven yield)
- Forms: **424B2**, 424H, 424B5  
- Gold column (multilingual N/A): *Expiration/Termination of Related License/ Franchise Agreement, Operating Agreement or Management Agreement*  
- Separate loan maturity column (never count as franchise/management)  
- Strategies: **B phrase-first**, **C document-type**, **E related 424H**, **F hybrid**

### Mexico — BMV / Emisnet
- Filing classes: Evento Relevante, Reporte Anual (411000-AR), trimestrales  
- Spanish terms: *contrato de administración / operación hotelera*, *franquicia*, *rescisión*, *cambio de marca*, *HMA*  
- Strategy: **D issuer/portfolio-first** (FibraHotel, GSF, RLH…) — do not literal-translate English keywords  
- Gap: native document retrieval still required for country ambition credit

### Brazil — CVM
- Categories: Fato Relevante, Formulário de Referência, IPE, prospectos  
- Portuguese: *gestão hoteleira*, *franquia*, *troca de bandeira*  
- Hard gate: do **not** misclassify *administração condominial / locação / multipropriedade* as HMA  
- Strategy: **D issuer-first** + classification gate

### UK — Companies House
- Supporting identity only (accounts, charges, insolvency)  
- Do **not** convert into ownership enrichment campaign  
- Prefer RNS / prospectus commercial events for opportunities

### Canada — SEDAR+
- AIF, MD&A, Material Change Report, Material Contract  
- Strategy: hospitality REIT issuer-first → material contracts / property schedules

Full machine taxonomy: `lib/hotel-intelligence/sec-edgar/hif-v6-source-intelligence-audit-v6.js`

---

## 3) Comparative discovery strategies (held-out)

| Strategy | Docs | Extractable | Findings | Net-new hotels |
|----------|-----:|------------:|---------:|---------------:|
| B Contractual phrase-first | 10 | 8 | 327 | **129** |
| C Document-type/section-first | 10 | 8 | 327 | **129** |
| A Hotel-name-first (control) | 9 | 7 | 310 | 128 |
| F Hybrid | 8 | 7 | 292 | 110 |

**Conclusion:** Phrase/document-type-first matches hotel-name-first yield on CMBS schedules while discovering hotels that were **not** in the query. Name-first is not required for this source family.

---

## 4) Extraction limitations (still real)
- Nested HTML table regex remains incomplete; plaintext gold-header path is the fix for this family  
- Independent / soft brands without brand tokens may under-extract  
- PDF-only annexes still not followed  
- Management-specific columns remain rare vs franchise/license column  
- MX/BR/GB/CA still need native document chains (not SEC bleed)

---

## 5) Changes implemented
1. `hif-v6-source-intelligence-audit-v6.js` — taxonomy + yield scoring (excludes Reserve/Trigger)  
2. `cmbs-property-schedule-extractor-v1.js` — **plaintext Related License schedule parser** + gold header recognition  
3. `hif-v6-source-intelligence-audit.mjs` — held-out validation harness (strategies A–F)  
4. `test-hif-v6-source-intelligence-audit.mjs` — regression for plaintext extraction  

---

## 6) Progress implication
Prior exact toward-1000 inventory ≈ **192**.  
This held-out name-free wave adds **+89 active dated** (after dedupe vs prior keys in the audit harness).  
Operational milestone ≈ **281 / 1000** if these rows are accepted into the shared inventory on merge (historical 40 preserved separately).

---

## 7) Recommended next research allocations
1. **Scale** `"Expiration/Termination of Related License"` across full 424B2/424H EFTS (paginate beyond first 100) — highest verified yield  
2. Expand plaintext parser for alternate column variants / missing `$ balance %` layouts  
3. Management-column campaigns remain low-volume — keep secondary  
4. MX: issuer-first Reporte Anual / Evento Relevante with Spanish HMA terminology  
5. BR: CVM Fato Relevante + condo-hotel classification gate  
6. Keep ownership enrichment deferred  
7. Clear open PRs #516/#517 after this audit merges (shared-file reconcile)

---

## Integration / backlog note
- #510–#515 and #518 already merged  
- #516 / #517 (country concentration) remain open — reconcile after this PR  
- Dirty HE/ADP/GDI/BE worktrees preserved; Integration dirty tree untouched  
