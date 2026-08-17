# Dealality Existing FDD System Audit

**Generated:** 2026-08-11  
**Scope:** Read-only audit (no Airtable writes, no production changes, no FDD file moves/deletes)  
**Safety:** `FDD_AUDIT_SAFETY_CONFIRMED`

---

## Executive snapshot

Dealality already has a **substantial but fragmented** FDD capability:

| Layer | Maturity |
|---|---|
| Raw PDF library (Google Drive reference + uploads) | Strong (~89 FDD PDFs, 54 brands, years 2024–2026) |
| Public discovery / harvest automation | Partial (IHG MN CARDS harvester works; WI often times out) |
| Deterministic parsers | Brand-family specific (Choice Item 6/17/19; Curio; Kimpton) |
| Structured economics fixtures | Curio + Kimpton only (deep); Choice batch maps to Brand Setup |
| Canonical FDD database | **Does not exist** |
| Brand Explorer consumption | Indirect via Brand Setup Fee Structure / Deal Terms / Presentation slots; FDD language scrubbed from owner-facing copy |
| Fee estimator | Manual inputs; tier defaults; **not wired to FDD DB** |
| Financial Term Library | Separate educational glossary (not brand FDD economics) |

**Verdict:** We are **closer than expected on documents + Choice/IHG/Hilton extraction patterns**, but **far from a generalized searchable/comparable FDD intelligence system**. Biggest bottleneck = no canonical schema/registry + parsers are brand-hardcoded regexes.

---

## 1. Architecture map (as implemented)

```
SOURCE
  ├─ Minnesota CARDS (IHG harvest) — government_filing download URLs
  ├─ Wisconsin DFI (probed; often timeout from automation)
  ├─ Manual / franchisor / operator-materials drops → Google Drive Brand Reference Material
  └─ Choice numbered state filings → Drive FDDs/ + text extract → fixtures/choice-fdd-text
        ↓
DOWNLOAD / HARVEST
  ├─ scripts/harvest-ihg-state-fdds.mjs (+ retry / move / probe)
  ├─ scripts/attach-choice-fdd-materials-pdfs.mjs / rename-choice-fdd-pdfs.mjs
  └─ uploads/fdd-intelligence (ad-hoc upload API artifacts; many AC Hotels dupes)
        ↓
PDF / TEXT
  ├─ Drive: IHG/fdd, Hilton/fdd + operator-materials, Choice/FDDs, Marriott/fdd
  ├─ fixtures/choice-fdd-text/*.txt
  ├─ reports/curio-fdd-plain.txt, kimpton-fdd-plain.txt (+ extract)
  └─ scripts/lib/extract-pdf-text.py + decode-fdd-plain-text.mjs
        ↓
PARSER
  ├─ Choice: parse-choice-fdd-item6-fees / item17-deal-terms / choice-fdd-item19
  ├─ Hilton Curio: parse-curio-fdd-economics.mjs
  └─ IHG Kimpton: parse-kimpton-fdd-economics.mjs
        ↓
FIXTURE / STRUCTURED DATA
  ├─ fixtures/curio-fdd-economics.json
  ├─ fixtures/kimpton-fdd-economics.json (+ feeStructurePatch / dealTermsPatch)
  ├─ fixtures/brand-explorer-presentation-*-economics.json
  └─ scripts/lib/choice-fdd-item19.mjs constants
        ↓
AIRTABLE / OTHER STORE
  ├─ Brand Setup - Fee Structure (Min/Max/Basis Typical* fields) — APPLY scripts
  ├─ Brand Setup - Deal Terms — APPLY scripts
  ├─ Brand Basics linked children
  ├─ Loyalty commercial fields (Choice Item 19 contribution %)
  └─ Brand Explorer Presentation rows (economics slots) — FDD jargon scrubbed for owners
        ↓
OWNER-FACING OUTPUT
  ├─ Brand Explorer economics tab (sanitized copy; no FDD/Item 19 labels)
  ├─ public/franchise-fee-estimator.html (manual / tier defaults — not live FDD)
  └─ public/financial-term-library*.html (glossary terms — not brand FDD rows)
```

### Where each step happens

| Question | Answer |
|---|---|
| URLs discovered? | `probe-state-fdd-portals-ihg.mjs`, `harvest-ihg-state-fdds.mjs` → `reports/ihg-state-fdd-*.json` |
| PDFs downloaded? | Harvest `--apply` → Drive `IHG Hotels & Resorts/fdd/`; Choice/Marriott/Hilton largely manual |
| PDF → text? | `extract-pdf-text.py` + `decode-fdd-plain-text.mjs`; Choice texts pre-extracted to fixtures |
| FDD Items identified? | Regex `ITEM N` + heading gates in brand parsers |
| Tables parsed? | Heuristic regex (not table-structure parsers) |
| Economics normalized? | Into Brand Setup Min/Max/Basis fields + Curio/Kimpton fixture shapes |
| Provenance retained/lost? | **Mostly lost** at Brand Setup/Explorer layer; retained in fixtures `sourceDocument` + harvest JSON |
| Records written? | Only when `--apply` / `--overwrite` on apply-* scripts (Airtable) |
| Brand Explorer consumes? | Presentation economics slots + Brand Setup linked fields; copy governance forbids FDD labels |
| Fee estimator consumes? | **No** — hardcoded brand-tier defaults + user inputs |

---

## 2. Component inventory

For each: Path · Purpose · Status · Input · Output · Brands · Items · Reusable · Brand-specific · Generalizable · Production · Writes · Dependencies · Weaknesses · Future role

### Discovery / harvest

| Path | Purpose | Status | Input | Output | Brands | Items | Reusable? | Brand-specific? | Generalizable? | Prod? | Writes? | Deps | Weaknesses | Future role |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `scripts/harvest-ihg-state-fdds.mjs` | Harvest IHG FDDs from MN CARDS (+ WI attempt) | Working (MN); WI flaky | Franchisor search terms | Drive PDFs + harvest JSON | IHG family | Full PDF | Medium | IHG franchisors | Yes (portal adapter) | Ops script | Yes w/ `--apply` | puppeteer, reference paths | Portal timeouts, rate limits | **KEEP → generalize as state-portal adapters** |
| `scripts/retry-pending-ihg-fdds.mjs` | Retry pending IHG downloads | Working | Harvest pending list | PDFs | IHG | — | Medium | IHG | Yes | Ops | Yes w/ `--apply` | harvest | Same portal limits | KEEP as downloader retry |
| `scripts/probe-state-fdd-portals-ihg.mjs` | Probe portal reachability / hits | Working | Search terms | discovery JSON | IHG | — | High | Search list IHG | Yes | Ops | Local reports | puppeteer | WI timeout | KEEP discovery probe |
| `scripts/move-ihg-fdds-to-fdd-folder.mjs` | Organize harvested PDFs | Utility | Loose PDFs | Folder layout | IHG | — | Low | Path IHG | Medium | Ops | Filesystem move | paths | Path-specific | KEEP organizer pattern |
| `scripts/attach-choice-fdd-materials-pdfs.mjs` | Attach Choice FDD PDFs to materials | Utility | Choice PDF paths | Attachments / links | Choice | — | Low | Choice | Low | Ops | Likely Airtable | materials config | Brand-specific | KEEP Choice materials bridge |
| `scripts/rename-choice-fdd-pdfs.mjs` | Rename numbered Choice FDDs | Utility | Numbered PDFs | Named PDFs | Choice | — | Low | Choice | Medium | Ops | Filesystem | inventory map | ID→brand mapping brittle | KEEP mapping table idea |
| `scripts/_audit-fdd-materials.mjs` | Audit FDD materials presence | Audit | Materials / files | Report | Multi | — | High | No | Yes | Ops | No | fs | Ad hoc | KEEP registry audit |

### Parsers / extraction

| Path | Purpose | Status | Input | Output | Brands | Items | Reusable? | Brand-specific? | Generalizable? | Prod? | Writes? | Deps | Weaknesses | Future role |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `lib/.../parse-curio-fdd-economics.mjs` | Parse Curio economics | Production-path parser | Plain text | Structured item5/6/7/17/19 | Curio | 5,6,7,17,19 | Pattern yes | **Hardcoded Curio amounts/regex** | Partial (sliceItemBody reusable) | Used by apply script | No | decode helpers | Brittle literal fee strings | **GENERALIZE skeleton; retire literals** |
| `lib/.../parse-kimpton-fdd-economics.mjs` | Parse Kimpton economics | Production-path parser | Plain text | item5/6/7/17 | Kimpton | 5,6,7,17 | Pattern yes | **Hardcoded Kimpton** | Partial | Used by apply | No | — | Same | GENERALIZE skeleton |
| `lib/.../decode-fdd-plain-text.mjs` | UTF-16/UTF-8 decode | Stable | Buffer | Text | All | — | **High** | No | Yes | Yes | No | — | Narrow | **KEEP core** |
| `scripts/lib/extract-pdf-text.py` | PDF text extract | Stable | PDF path | stdout text | All | — | **High** | No | Yes | Yes | No | python/pdf tooling | Layout quality varies | **KEEP** |
| `scripts/lib/parse-choice-fdd-item6-fees.mjs` | Choice Item 6 fees | Working | Choice text | royalty/mkt/loyalty/tech | Choice brands | 6 | Medium | Choice labels | Medium | Batch apply | No | fixtures | Label variants | GENERALIZE fee-table parser |
| `scripts/lib/parse-choice-fdd-item17-deal-terms.mjs` | Choice Item 17 terms | Working | Choice text | term/renewal | Choice | 17 | Medium | Choice | Medium | Batch apply | No | fixtures | Limited fields | GENERALIZE term parser |
| `scripts/lib/choice-fdd-item19.mjs` | Hand-curated Item 19 metrics | Production constants | Manual extract | loyalty/enterprise % | ~16 Choice | 19 | Low | Choice | Needs automation | Loyalty batch | No | docs inventory | Not computed live from PDF every run | KEEP as seed; automate later |
| `scripts/extract-choice-fdd-item19.mjs` / `parse-choice-fdd-item19.mjs` | Item 19 extract/parse tooling | Working | Choice text | Reports / metrics | Choice | 19 | Medium | Choice | Medium | Ops | Local | fixtures | Brand heuristics | GENERALIZE Item 19 |
| `lib/.../build-kimpton-fee-structure-from-fdd.js` | Map Kimpton → Brand Setup fields | Working | Parsed econ | Fee/Deal patches | Kimpton | 5,6,7,17 | Mapping pattern | Kimpton | Medium | Apply script | Via apply | Brand Setup field names | One-brand mapper | Template for canonical→Brand Setup |
| `lib/.../build-curio-economics-presentation-slots.js` | Curio → Explorer slots | Working | Parsed + rows | Presentation rows | Curio | economics | Medium | Curio | Medium | Apply | Fixtures / optional AT | presentation | Brand-specific | KEEP presentation bridge |
| `lib/.../build-kimpton-economics-presentation-slots.js` | Kimpton → Explorer slots | Working | Parsed + rows | Presentation rows | Kimpton | economics | Medium | Kimpton | Medium | Apply | Fixtures | presentation | Brand-specific | KEEP |

### Apply / write paths (DANGEROUS for this PoC — do not run `--apply`)

| Path | Purpose | Writes? | Notes |
|---|---|---|---|
| `scripts/apply-curio-fdd-economics.mjs` | Curio → fixtures (+ optional AT) | Fixtures always; AT if `--apply` | Dry-run default for AT |
| `scripts/apply-kimpton-fdd-economics.mjs` | Kimpton → Fee Structure + Deal Terms + Explorer | Airtable if `--apply` | **High impact** |
| `scripts/apply-choice-fee-structure-batch.mjs` | Choice Item 6 → Fee Structure | `--overwrite` | **High impact** |
| `scripts/apply-choice-deal-terms-batch.mjs` | Choice Item 17 → Deal Terms | `--overwrite` | **High impact** |
| `scripts/apply-choice-loyalty-commercial-batch.mjs` | Item 19 → loyalty commercial | `--overwrite` | **High impact** |
| `scripts/strip-brand-explorer-fdd-caveats.mjs` | Remove FDD caveats from Explorer copy | Presentation | Copy governance |
| `scripts/fix-p1-wrong-fdd-blu-attachment.mjs` | Fix wrong Blu FDD attachment | Materials | One-off |

### Docs / fixtures / reports / UI / API

| Path | Purpose | Status | Future role |
|---|---|---|---|
| `docs/choice-fdd-inventory.md` | Choice FDD file↔brand map | Authoritative for Choice | **KEEP as brand-file registry seed** |
| `fixtures/choice-fdd-text/*` | Extracted Choice FDD text | Strong (~29 files) | KEEP raw library |
| `fixtures/curio-fdd-economics.json` | Curio structured economics | Gold sample | Canonical schema seed |
| `fixtures/kimpton-fdd-economics.json` | Kimpton structured + Brand Setup patches | Gold sample | Canonical schema seed |
| `reports/curio-fdd-plain.txt` / `kimpton-fdd-*.txt` | Cached plain text | Working | KEEP caches |
| `reports/ihg-state-fdd-discovery.json` / `harvest.json` | Portal discovery/download log | Working | Provenance registry seed |
| `uploads/fdd-intelligence/*` | Ad-hoc uploaded PDFs | Messy (dupes) | Dedup into registry; don't treat as SoT |
| `api/financial-term-library.js` | Glossary CRUD (ALT base) | Live API | **Not FDD economics DB** — KEEP separate |
| `public/franchise-fee-estimator.html` | Owner fee modeler | Live UI | Integrate later via canonical fees API |
| `public/franchise-application.html` + `api/franchise-application.js` | Application form | Separate product surface | Out of FDD pipeline core |
| `scripts/fdd-intelligence/` | Empty dir | Placeholder | Future orchestrator home |
| Env `FDD_INTELLIGENCE_MODEL_*` | LLM section helper config | Present in `.env` | Optional assistive layer only — not SoT |

---

## 3. Explicit architecture answers

### A. Are we close to a generalized FDD pipeline?

**Partially.** Document library + text extract + Choice batch apply + two deep brand parsers = ~40–50% of an MVP pipeline. Missing: canonical registry/schema, generalized item/fee parsers, automated multi-parent discovery, historical versioning, search/compare APIs.

### B. Reusable across franchisors?

- **High:** PDF text extract, decode, Drive folder conventions, MN CARDS download pattern, Brand Setup Min/Max/Basis field model, presentation-slot apply pattern, Item body slicing concept.
- **Medium:** Portal harvest orchestration (needs per-portal adapters).
- **Low as-is:** Curio/Kimpton regex fee literals; Choice Item 19 hand constants.

### C. Hardcoded to specific parents/brands?

| Family | Hardcoding |
|---|---|
| **Choice** | File maps, Item 6/17 parsers, Item 19 constants, fee tier profiles, loyalty batch |
| **Hilton / Curio** | Literal fee strings ($85k, 5%, 4%, Honors 4%, etc.) |
| **IHG / Kimpton** | Literal fee strings (6%, 3%, $500/room, etc.); harvest franchisor list IHG-wide |
| **Marriott / Tribute** | PDFs on Drive; **no economics parser found** |
| **Hotel Indigo** | PDF harvested; **no dedicated parser** |

### D. Biggest architectural bottleneck?

1. **No canonical FDD document registry** (SHA256, source URL, year, franchisor, brand) — provenance mostly lost after apply.  
2. **Parsers are brand-specific regexes**, not schema-driven item/table extractors.  
3. **Owner-facing systems deliberately strip FDD identity**, so Explorer cannot currently expose comparable FDD intelligence without a separate internal FDD layer.

---

## 4. Current field map (de facto schema — do not create yet)

### IDENTITY

| Desired field | Exists? | Current name | Object | Type | Normalized? | Citation? | Page/Item? | Historical? | Recommended canonical |
|---|---|---|---|---|---|---|---|---|---|
| Parent company | Partial | folder / company path | Drive / harvest JSON | string | folder-based | no | no | folder | `parent_company` |
| Brand | Yes | Brand Name / brandName | Brand Basics / fixtures | string | Airtable names vary | no | no | via files | `brand_slug` + `brand_name` |
| Franchisor | Partial | franchisor in harvest | harvest JSON | string | raw portal | yes (URL) | no | yes | `franchisor_legal_entity` |
| FDD year | Partial | filename / year | files / harvest | number/string | inconsistent | no | no | multi-file | `fdd_year` |
| Effective date | Rare | receivedDate/addedOn | harvest | string | portal dates ≠ FDD effective | partial | no | yes | `effective_date` |
| Amendment date | Rare | filename notes | files | string | no | no | no | occasional | `amendment_date` |
| Jurisdiction | Partial | MN / Mexico / Canada in names | files | string | ad hoc | partial | no | yes | `jurisdiction` |

### ITEM 5 / INITIAL FEES

| Desired | Exists? | Current | Object | Notes |
|---|---|---|---|---|
| Initial franchise / application fee | Yes | `Min/Max - Typical Application Fee` + Basis/Notes | Brand Setup - Fee Structure | Also Curio/Kimpton fixture `item5.*` |
| PIP / inspection fees | Partial | Kimpton/Curio item5; notes | fixtures / notes | Not standardized across brands |
| Training / opening fees | Partial | `Min/Max - Typical Training Fee` | Fee Structure | Choice uses template ranges; Kimpton from FDD |

### ITEM 6 / OTHER FEES

| Desired | Exists? | Current | Object |
|---|---|---|---|
| Royalty | Yes | `Min/Max - Typical Royalty Fee Range` | Fee Structure |
| Marketing / brand fund | Yes | `Min/Max - Typical Marketing Fee Range` | Fee Structure |
| Reservation / distribution | Partial | `Min/Max - Typical Reservation / Distribution Fee` | Fee Structure (often blank for Choice combined fee) |
| Technology | Yes | `Min/Max - Typical Tech` | Fee Structure |
| Loyalty | Yes | `Min/Max - Typical Loyalty Program Fee` | Fee Structure + Choice loyalty commercial |

### ITEM 7 / INITIAL INVESTMENT

| Desired | Exists? | Current | Object |
|---|---|---|---|
| Investment low/high | Partial | Curio/Kimpton `item7.totalInvestmentMin/Max` | fixtures only (not universal Brand Setup) |
| Per-room investment | Partial | Curio/Kimpton `perRoomMin/Max` | fixtures |

### ITEM 11 / SUPPORT

| Desired | Exists? | Notes |
|---|---|---|
| Training systems | Partial | Choice notes cite Item 11; no structured Item 11 schema |

### ITEM 12 / TERRITORY

| Desired | Exists? | Notes |
|---|---|---|
| Territory rights | **No structured FDD field found** | — |

### ITEM 17 / TERM

| Desired | Exists? | Current | Object |
|---|---|---|---|
| Initial term years | Yes | Deal Terms `Length - Typical Minimum Initial Term` + Curio/Kimpton item17 | Brand Setup - Deal Terms / fixtures |
| Renewal | Yes | `Renewal Structure` / notes | Deal Terms |
| Termination | Partial | termination fee structure text fields | Fee Structure / Deal Terms |

### ITEM 19 / FPR

| Desired | Exists? | Current | Object |
|---|---|---|---|
| Item 19 present | Implicit | Choice brands with/without CP% | `choice-fdd-item19.mjs` |
| Contribution / performance metrics | Yes (Choice) | loyaltyPct / enterprisePct / proprietaryPct | constants → loyalty commercial |
| Curio Honors contribution | Yes | `item19.*` | curio fixture |

### ITEM 20 / SYSTEM SIZE

| Desired | Exists? | Notes |
|---|---|---|
| Openings/closures/transfers | **Not found as structured extraction** | — |

### SOURCE PROVENANCE

| Desired | Exists? | Current | Object |
|---|---|---|---|
| Source URL | Partial | harvest `downloadUrl` | reports/ihg-state-fdd-harvest.json |
| Source page | Partial | portal notes | harvest JSON |
| SHA256 | **No systematic registry** | PoC inventory computes ad hoc | reports/fdd-intelligence |
| Page citations in Brand Setup | **No** (by design for owner copy) | Disclaimer text only | Fee Structure notes |

---

## 5. Data-flow answers for founder

1. **Existing system maturity:** document-rich, schema-poor, parser-fragmented.  
2. **Production use today:** Choice fee/deal/loyalty batches + Kimpton/Curio economics apply paths feed Brand Setup / Explorer.  
3. **Fee estimator:** disconnected calculator.  
4. **Financial Term Library:** pedagogical terms, not brand FDD store.  
5. **Write risk:** all Airtable mutation gated behind `--apply` / `--overwrite` (confirm disabled for this PoC).

---

## Change impact classification

- **This audit deliverable:** Low (reports only under `reports/fdd-intelligence/`, `data/fdd-test/`).  
- **Any future production FDD platform:** High (Airtable writes, schema, Brand Explorer, fee estimator).

## Regression checklist (audit itself)

- What could break: nothing production — read-only.  
- Retest: n/a.  
- Airtable fields touched: **none**.
