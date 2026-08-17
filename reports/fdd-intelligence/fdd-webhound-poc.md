# FDD_WEBHOUND_POC_COMPLETE

**Generated:** 2026-08-11  
**Safety:** Audit + local PoC only — no Airtable writes, no production changes, no FDD moves/deletes  
**Sessions:**  
- Discovery: https://webhound.ai/session/de3d5b36-7efa-4c9f-868a-827ac5d6178e ($2.44 / $8)  
- Extraction: https://webhound.ai/session/8c2c6570-4842-4e8e-834b-3fa66a4ca830 ($2.26 / $5)

---

## 1. Executive conclusion

**Proceed with a hybrid architecture.** Webhound is strong as a **public discovery + PDF verification + assisted extraction** layer when franchisors publish FDDs openly (Hilton Curio proved this end-to-end). It is **not reliable as the sole discovery system** for IHG/Choice brands whose current FDDs sit behind state portals or paywalled repositories.

Dealality already owns most of the hard assets for an FDD intelligence MVP: ~89 PDFs, Choice Item 6/17/19 tooling, Curio/Kimpton deterministic parsers, Brand Setup field model, and MN CARDS harvest. The missing core is a **canonical document registry + generalized parsers**, not more one-off brand scripts.

**Verdict class (five-brand average ~22/40):** **FEASIBLE BUT REQUIRES PIPELINE WORK.**

---

## 2. Existing FDD system maturity: **55 / 100**

| Strength | Weakness |
|---|---|
| Large Drive PDF library (54 brands, 2024–2026) | No SHA256/source URL registry |
| Choice batch extract → Brand Setup | Brand-hardcoded regex parsers |
| Curio + Kimpton deep economics | No Marriott/Tribute/Indigo parsers |
| IHG MN CARDS harvester works | WI portal often times out |
| Fee estimator + Financial Term Library UIs | Not wired to FDD data |

---

## 3. Webhound suitability: **68 / 100** (as discovery/verification sidecar)

| Role | Fit |
|---|---|
| Find franchisor-hosted public FDD PDFs | Excellent (Curio) |
| Find older public Marriott PDFs | Good but may miss latest year (Tribute 2022 vs local 2026) |
| Return state-portal direct download URLs (MN CARDS) | Weak in this run (missed known Kimpton/Indigo URLs despite sidecar notes) |
| Structured extraction from uploaded PDF | Excellent (Curio matched Dealality parser) |
| Become Dealality FDD database | **No — explicitly out of scope** |

---

## 4. Five-brand test results (scores /40)

| Brand | Disc | ID | PDF | DL | Meta | Parser | WH Extract | Prov | **Total** | Class |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| Curio | 5 | 5 | 5 | 5 | 4 | 5 | 5 | 5 | **39** | VERY EASY |
| Tribute | 3 | 5 | 4 | 5 | 4 | 2 | 4 | 4 | **31** | MODEST WORK |
| Kimpton | 3 | 4 | 1 | 0 | 3 | 0 | 0 | 2 | **13** | HIGH FRICTION |
| Radisson | 3 | 4 | 1 | 0 | 3 | 0 | 0 | 2 | **13** | HIGH FRICTION |
| Hotel Indigo | 3 | 4 | 1 | 0 | 3 | 0 | 0 | 2 | **13** | HIGH FRICTION |
| **Average** | | | | | | | | | **~22** | **PIPELINE WORK** |

---

## 5. FDDs successfully found

All 5 brands identified with franchisor legal entity + year signals.

| Brand | Best public source Webhound returned | Year |
|---|---|---|
| Curio | Hilton disclosure page → direct PDF | **2026** |
| Tribute | Marriott hotel-development PDF | **2022** (stale vs Dealality Drive 2026) |
| Kimpton | Franchimp listing (no free PDF) | 2024 listed |
| Radisson | Franchimp listing (no free PDF) | 2026 listed |
| Hotel Indigo | FranCloud listing (no free PDF) | 2026 listed |

---

## 6. FDDs successfully downloaded (Dealality-local)

| Brand | Path | SHA256 | Size |
|---|---|---|---|
| Curio 2026 | `data/fdd-test/raw/hilton/curio-collection/2026/` | `72a85c1ffd2e…` | 5,999,028 |
| Tribute 2022 | `data/fdd-test/raw/marriott/tribute-portfolio/2022/` | `5eb047e4805d…` | 3,800,458 |
| Kimpton / Radisson / Indigo | — | — | no Webhound direct URL |

---

## 7. Extraction accuracy

### Curio — Dealality parser vs new download vs Webhound

| Field | Existing fixture | New PDF (Dealality parser) | Webhound extract | Match |
|---|---|---|---|---|
| Royalty | 5% | 5% | 5% Gross Rooms | Yes |
| Program / marketing | 4% | 4% | 4% Program Fee | Yes |
| Application fee | $85,000 | $85,000 | $85,000 + $400/rm>250 | Yes |
| Item 7 low/high | $3,928,488 / $119,557,900 | same | same | Yes |
| Term (new dev) | 23 years | 23 | 23 | Yes |
| Honors loyalty | 4% folio | 4% | 4% folio | Yes |

**Principal validation: PASS.** Existing Curio parser + Webhound extraction agree with the newly downloaded 2026 FDD.

### Tribute

- Smoke scan: Items 5–20 present; fee language present.  
- No Dealality economics parser → Webhound extraction is the first structured pass (2022 vintage).  
- Dealality Drive already has **2026** Tribute PDF — Webhound public discovery returned older year.

### Kimpton / Radisson / Indigo

- Webhound structured extraction **not run** (no downloadable public PDF in discovery).  
- Dealality already has Kimpton MN filing + Choice Radisson text/economics paths locally.

---

## 8. Existing code reuse: **~45%**

Reusable now: PDF text extract, decode helpers, Curio/Kimpton parsers (pattern), Choice Item 6/17/19, MN harvest, Brand Setup Min/Max/Basis mapping, presentation slot bridges.  
Not reusable without rewrite: brand-literal regex amounts, lack of registry, fee estimator defaults.

---

## 9. Biggest technical gaps

1. Canonical FDD document registry (SHA256, source URL, year, franchisor, brand)  
2. State-portal discovery reliability (Webhound missed MN CARDS; Dealality harvest already solves IHG)  
3. Generalized Item 5/6/7/17/19/20 parsers (not brand literals)  
4. Historical versioning / change detection  
5. Search/compare API + fee estimator wiring  
6. Provenance retention through Brand Setup/Explorer (today mostly stripped for owner copy)

---

## 10. Estimated effort to MVP

| Workstream | Effort |
|---|---|
| Document registry + local downloader (no overwrite, SHA256) | 1–2 weeks |
| Hybrid discovery orchestrator (Webhound + MN/WI adapters) | 2–3 weeks |
| Generalize Item body + fee table parsers (seed from Curio/Kimpton/Choice) | 3–5 weeks |
| Brand Setup / internal FDD search API (read-only first) | 2–3 weeks |
| **MVP total** | **~8–12 weeks** (1 engineer, focused) |

---

## 11. Estimated cash/API cost to MVP

| Item | Observed / estimate |
|---|---|
| This PoC Webhound spend | **~$4.70** ($2.44 discovery + $2.26 extraction) |
| Avg discovery cost per brand (this run) | ~$0.49 gross; **~$1.22 per direct-PDF success** |
| Avg extraction cost per uploaded FDD | **~$1.13** |
| MVP R&D Webhound budget (25 brands hybrid) | **$75–$200** (discovery assist + hard cases) |
| Primary cost | Engineering time, not API |

Do **not** assume Webhound can replace MN CARDS harvest cost-effectively for IHG/Choice.

---

## 12. Cost to build 25 / 75 / 150-brand library

Assumptions (explicit):  
- ~40% brands have franchisor-public PDFs (Hilton-like) → Webhound discovery ~$1–2 + extract ~$1  
- ~60% need Dealality state-portal harvest (near-$0 API; engineering/compute)  
- Historical ×3 multiplies storage/parse, not always Webhound

| Scale | Webhound cash (order-of-magnitude) | Notes |
|---|---|---|
| 25 brands | **$40–$120** | Hybrid; many already on Drive |
| 75 brands | **$120–$400** | Still dominated by eng |
| 150 brands | **$250–$800** | |
| 150 × 3 years | **$500–$1,500** Webhound assist | Most years from portals/Drive, not WH |

Unknowns marked: exact WI automation success rate; franchisor public-PDF coverage over time.

---

## 13. Recommended architecture

```
PUBLIC WEB / STATE PORTALS / FRANCHISOR PAGES
        ↓
WEBHOUND  ── discovery + source verification (+ optional PDF extract assist)
        ↓
DEALALITY DISCOVERY ORCHESTRATOR
  (MN CARDS / WI DFI adapters + Webhound URL intake)
        ↓
DEALALITY RAW LIBRARY  (PDF + metadata.json + SHA256)  ← own forever
        ↓
DEALALITY EXTRACTION  (deterministic parsers; WH assist only)
        ↓
CANONICAL FDD DB
        ↓
Brand Explorer (sanitized) / internal FDD Search / Comparison / Fee Estimator
```

---

## 14. KEEP / GENERALIZE / RETIRE

| Component | Action |
|---|---|
| `decode-fdd-plain-text`, `extract-pdf-text.py` | **KEEP** |
| MN CARDS harvest / retry / probe | **KEEP → GENERALIZE** portal adapters |
| Choice Item 6/17/19 + inventory doc | **KEEP** as family module; feed registry |
| Curio / Kimpton parsers | **GENERALIZE** skeleton; retire hardcoded literals |
| Brand Setup fee/deal apply scripts | **KEEP** as writers behind validation |
| `uploads/fdd-intelligence` dupes | Dedup into registry; don’t treat as SoT |
| Franchimp/FranCloud as authority | **RETIRE** as authoritative source (hints only) |
| Financial Term Library | **KEEP separate** (glossary ≠ FDD DB) |
| Fee estimator | **KEEP**; wire later to canonical fees |

---

## 15. Recommended next implementation step

**Build `data/fdd-library/` registry MVP (read-only ingest):**  
1. Index existing Drive PDFs + PoC downloads with SHA256 + provenance  
2. Add hybrid discoverer: Webhound URL intake **or** MN CARDS adapter (reuse harvest)  
3. Run Curio-style parse validation gate before any Airtable apply  

Do **not** start production Airtable schema migrations yet.

---

## MOST IMPORTANT ANSWERS

**A. How much already exists?** ~55% of a usable system — strong library + partial parsers; weak registry/generalization.  

**B. Can Webhound reliably discover hotel FDDs?** **Sometimes.** Excellent for franchisor-public PDFs; unreliable for current IHG/Choice public direct PDFs in this test.  

**C. Can our code download/retain automatically?** **Yes** — proven for Curio + Tribute into `data/fdd-test/raw/` with SHA256 + metadata.  

**D. Can existing parsers extract meaningful standardized data?** **Yes for Curio/Kimpton/Choice family; no for Tribute/Indigo yet.** Curio validation perfect on new download.  

**E. Is Webhound materially better than hand-built discovery?** **Complementary, not replacement.** Better at open-web franchisor discovery; **worse than Dealality MN harvest** for IHG direct PDFs this run.  

**F. How hard to searchable/comparable FDD intelligence?** **Moderate** — ~8–12 weeks MVP with hybrid discovery + registry + generalized extractors.  

**G. Realistic cost?** PoC ~$5 Webhound; MVP API cash low hundreds; engineering is the real cost.  

**H. Should we proceed?** **YES — hybrid.** Webhound as discovery/verification sidecar; Dealality owns download, fingerprint, parse, and database.

---

## Gap analysis (build vs exists)

| Capability | Status |
|---|---|
| Generalized FDD discovery orchestrator | **Moderate build** (hybrid) |
| Document downloader | **Minor extension** (PoC script exists) |
| Duplicate detection (SHA256) | **Minor extension** (PoC has it) |
| Metadata registry | **Moderate build** |
| Canonical FDD schema | **Moderate build** (de facto fields exist) |
| Generalized Item boundary detector | **Moderate build** (sliceItemBody seed) |
| Generalized fee table parser | **Major build** |
| Generalized Item 19 / 20 parsers | **Major build** / **Major build** |
| Validation/reconciliation engine | **Moderate build** |
| Historical change detector | **Moderate build** |
| FDD search/compare APIs | **Moderate build** |
| Brand Explorer integration | **Minor–moderate** (sanitized path exists) |
| Fee estimator integration | **Moderate build** |
| Webhound as database | **Not required** |

---

## Per-brand cost table (this PoC)

| Brand | WH discovery share | PDF found | Downloaded | WH extraction | Notes |
|---|---:|---|---|---|---|
| Curio | ~$0.49 | Yes | Yes | ~$1.13 | Full success |
| Tribute | ~$0.49 | Yes (2022) | Yes | ~$1.13 | Stale year |
| Kimpton | ~$0.49 | No direct | No | — | Use MN harvest |
| Radisson | ~$0.49 | No direct | No | — | Use Choice library |
| Hotel Indigo | ~$0.49 | No direct | No | — | Use MN harvest |
| **Totals** | **$2.44** | 2/5 | 2/5 | **$2.26** | **~$4.70** |

---

## Artifacts produced

- `reports/fdd-intelligence/fdd-existing-system-audit.md` + `.json`  
- `reports/fdd-intelligence/fdd-document-inventory.md` + `.csv`  
- `reports/fdd-intelligence/fdd-webhound-capability-check.md`  
- `reports/fdd-intelligence/fdd-webhound-discovery-rows.json`  
- `reports/fdd-intelligence/fdd-poc-download-results.json`  
- `reports/fdd-intelligence/fdd-poc-extraction-smoke.json`  
- `reports/fdd-intelligence/fdd-webhound-extraction-rows.json`  
- `data/fdd-test/raw/...` (Curio 2026, Tribute 2022)

---

# FDD_WEBHOUND_POC_READY_FOR_FOUNDER_REVIEW
