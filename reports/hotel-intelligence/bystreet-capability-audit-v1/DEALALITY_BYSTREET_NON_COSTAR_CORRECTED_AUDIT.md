# DEALALITY BYSTREET AUDIT — NON-COSTAR CORRECTION

**Marker:** `DEALALITY_BYSTREET_NON_COSTAR_CORRECTED_AUDIT_V1`  
**Date:** 2026-08-11  
**Hard rule:** CoStar = `MANUAL_COSTAR_EXPORT` / GTM-only / **0 points** toward ByStreet readiness. No CoStar API. Not a HI/Census/Webhound/product ownership source.

---

## A. What we already have without CoStar

### Product / Hotel Intelligence (repeatable)

| Capability | Executable location | Status |
| --- | --- | --- |
| CALA Hotel Census SoT | Airtable + `MAP_HOTEL_PROPERTY_CENSUS` | Active |
| Canonical hotel IDs + resolve | `identity-resolve.js`, `canonical-hotel.js` | Active |
| HI MCP tools | `mcp/hotel-intelligence/server.js` | Active (hotel identity/enrich/rooms) |
| Providers | census, HBX, GIATA Drive, SerpAPI, StayingAPI | Flag-gated; **identity/geo/brand/rooms**, not ownership |
| Field evidence store | `evidence-store.js` + confidence | Active for hotel fields |
| Bounded public research pattern | `room-count-research/*` (official site + capped SerpApi + fetch + extract + score) | Active; **best template** for ownership research |
| Discovery Factory | census expansion staging | Active; not owner research |

### Public / non-CoStar research surfaces (repeatable or semi-repeatable)

| Source | How Dealality uses it today | Ownership? |
| --- | --- | --- |
| SerpAPI | HI enrich + room-count research + Google Hotels | Indirect only |
| Official hotel websites | Room-count fetch; census Official Property URL | Indirect |
| Webhound MCP (`user-webhound`) | Agent-callable research; claims/sources/evidence pack | **Yes potential** — not HI-wired |
| Corporate web seed catalogs | `adapters/*corporate-web-seeds*` + resolver | Owner websites / contact paths (manual/seed) |
| MX SIGER / SAT / SIEM plans | `mx-siger-registry.js`, RNT configs | Registry research **plans** (manual steps), not auto-API |
| Brand Explorer / Operator Explorer / PI Source Library | Brand/operator evidence | Brand/operator ≠ property owner |
| Press/news (via Webhound or SerpApi search) | Room-count trust taxonomy includes news domains | Possible via research, not dedicated owner pipeline |

### Explicitly **not** available for product ownership

- CoStar API  
- CoStar exports as product inputs  
- GTM Airtable rows that originated as CoStar True Owner rollups (historical GTM data — do not use as ground truth or product SoT)

---

## B. Reusable GTM logic vs prohibited CoStar data

| Component | Classification | What can be reused without CoStar |
| --- | --- | --- |
| `normalize.js` / `normalizeOwnerKey` / legal-suffix stripping | `REUSABLE_LOGIC_NO_COSTAR_DEPENDENCY` | Entity name normalization |
| `owner-contact-sync.js` — indexes, label→owner resolve, broker pattern, contact scoring heuristics | `REUSABLE_AFTER_REMOVING_COSTAR_ASSUMPTIONS` | Matching/scoring works on any owner+contact list; strip CoStar source-file bonuses |
| `branding-owner-context.js` — house brand vs franchise, third-party operator RE, outreach tracks | `REUSABLE_LOGIC_NO_COSTAR_DEPENDENCY` | **Critical** anti-false-owner protection (brand ≠ owner) |
| `icp-classify.js` — franchisor allowlist, SPV/broker/REIT segments | `REUSABLE_AFTER_REMOVING_COSTAR_ASSUMPTIONS` | Role classification; inputs must come from public research, not CoStar True Owner |
| `branding-decision-signals.js` — signal taxonomy + weights + contact bonuses | `REUSABLE_AFTER_REMOVING_COSTAR_ASSUMPTIONS` | Signal IDs reusable; drop CoStar-specific labels/pipeline parsers; feed year_built/brand/operator from Census/public |
| `owner-portfolio-audit.js` — token match, operator-align rates, opaque SPV regex, confidence bands | `REUSABLE_AFTER_REMOVING_COSTAR_ASSUMPTIONS` | Portfolio confidence math reusable if portfolio hotels come from Census/public claims |
| `MANUAL_PORTFOLIO_AUDIT` overrides in same file | `COSTAR_DATA_DEPENDENT_NOT_REUSABLE` | Named CoStar SPV anecdotes |
| `company-profile-enrichments.js` | `COSTAR_DATA_DEPENDENT_NOT_REUSABLE` | Screenshot-derived CoStar stats/contacts |
| `costar-*.js` parsers / import scripts | `COSTAR_DATA_DEPENDENT_NOT_REUSABLE` | File import only |
| `field-map.js` CoStar Property ID / license enums | `GTM_ONLY_NOT_RELEVANT` (product) | Keep GTM-internal |
| `decision-opportunity-*.js` — opportunity + evidence schema, validation, stages/triggers | `REUSABLE_AFTER_REMOVING_COSTAR_ASSUMPTIONS` | Excellent **vocabulary** for Decision/Evidence; unlink CoStar Internal source type for product |
| `adapters/corporate-web-seeds*` + resolver | `REUSABLE_AFTER_REMOVING_COSTAR_ASSUMPTIONS` | Seed maps often cite CoStar labels in comments; websites/LinkedIn targets are public |
| `adapters/mx-siger-registry.js`, RNT, `mx-corporate-web-first.js` | `REUSABLE_AFTER_REMOVING_COSTAR_ASSUMPTIONS` | Search-term builders + manual registry plans; rename “True Owner” → entitySearchName |
| `registry-contact-verification.js` / phone verification | `REUSABLE_LOGIC_NO_COSTAR_DEPENDENCY` | Email/phone tiering for public contacts |
| `owner-lead-asset.js` | `REUSABLE_AFTER_REMOVING_COSTAR_ASSUMPTIONS` | Lead-asset pickers; some overrides CoStar-specific |
| `cala-footprint.js` | `REUSABLE_LOGIC_NO_COSTAR_DEPENDENCY` | Country/footprint helpers |

**Answer to “what intellectual work accelerates public Owner Intelligence?”**

1. **Role discrimination** (franchisor vs asset owner vs operator vs house brand) — already coded.  
2. **Contact verification tiers** — already coded.  
3. **Decision/evidence field contracts** — already designed.  
4. **Portfolio confidence heuristics** (operator-align, opaque SPV) — portable.  
5. **Corporate web + MX registry research playbooks** — portable as orchestration inputs.  
6. **Room-count research engine** — portable pattern for “bounded public research → evidence → confidence.”

Prohibited: any CoStar-derived ownership labels, property IDs, portfolio stats, or screenshot enrichments as product evidence.

---

## C. Webhound executable reality

### Classification: **D — combination**

| Layer | Reality |
| --- | --- |
| A. Cursor instructions | Yes — agent playbooks / MCP descriptions |
| B. Executable Dealality research crawler | **No** in-repo Webhound engine |
| C. External MCP/service | **Yes** — `user-webhound` tools (ready in this environment) |
| Repo artifacts | Prompts, payloads, learning scripts, Brand Explorer claim patches, census “webhound candidate” queues that **explicitly do not call Webhound** |

### Callable tools (external MCP examples)

`webhound_start_report`, `webhound_start_dataset`, `webhound_watch`/`wait`, `webhound_get_output`, `webhound_get_evidence_pack`, `webhound_get_claims`, `webhound_get_sources`, sidecar notes, export/share, account/budget tools.

### How a hotel becomes a target today

1. Human/agent writes a prompt (e.g. Mexico owner-intel Tier-1 schema).  
2. Agent calls `webhound_start_report` / `start_dataset` with budget.  
3. Webhound plans/searches/reads/verifies (recursive research inside **their** harness).  
4. Agent retrieves claims/sources/evidence pack when `done=true`.  
5. Dealality may later **learn patterns into code** — not write ownership to Census.

### HI MCP → Webhound?

```text
NOT_CONNECTED
```

Verified: **zero** `webhound` references under `lib/hotel-intelligence/` or `mcp/hotel-intelligence/`. Census autopilot queues mark `do_not_call_webhound_in_autopilot` / `webhound_direct_write: false`.

Webhound **can** discover owners/people/contacts/sources when prompted — but only via **agent/MCP**, not via `hotel_enrich` / automatic batch from Census.

---

## D. Current non-CoStar pipeline

```text
Census Hotel (dhl_* / Property Identity Key)
        ↓
HI hotel_get / hotel_resolve     ✅ identity, geo, brand family, website, rooms*
        ↓
HI hotel_enrich / room_count_research   ✅ optional public corroboration (not ownership)
        ↓
Local hotel field evidence        ✅
        ↓
[BREAK — no owner research tool]
        ↓
Manual/agent Webhound session (optional)  ⚠️ NOT_CONNECTED to HI
        ↓
Structured owner graph / edges    ❌ missing
        ↓
Portfolio / principal / contact product SoT   ❌ missing
```

\*rooms may still be incomplete pending research.

GTM Decision Radar / corporate seeds / SIGER plans sit **beside** this line — useful logic, not wired to Census hotels as a product loop.

---

## E. Missing connections

| Stage after Identity | Existing reusable pieces | Required |
| --- | --- | --- |
| Public research for ownership | Room-count research pattern; SerpAPI; Webhound MCP; corporate seeds; SIGER plans | **Orchestrator** that targets owner queries per hotel |
| Owner entity claim | Role classifiers (`branding-owner-context`, `icp-classify`) | Extraction + `OWNED_BY` edge model; never use brand alone |
| Ownership group / parent | Normalize + seeds | Public parent resolution + entity resolve |
| Principal | Contact verification tiers; Webhound people | People extraction + role tags |
| Portfolio | Portfolio-audit math; Census name/geo match | Link other Census hotels to same entity via public evidence |
| Contact | registry-contact-verification | Public email/phone/LinkedIn only; no invented contacts |
| Evidence | HI evidence-store shape; Decision Opportunity Evidence vocabulary; Webhound claims | Ownership-edge evidence records (multi-source) |
| Confidence | Field confidence; discovery tiers; signal weights | Ownership-relationship confidence (with brand≠owner hard fail) |

**Protection against `Owner = Marriott` when Marriott is only brand:**  
Exists in GTM logic (`FRANCHISOR_BRAND_OWNER_KEYS`, house-brand vs franchise, third-party operator RE) — **not enforced** in HI ownership path because that path does not exist. Must be a hard gate in any PoC.

### Ownership relationship model (non-CoStar)

| Role | Supported today? |
| --- | --- |
| Brand / franchisor | Yes (Census + classifiers) |
| Hotel operator / management company | Partial (GTM heuristics; weak on Census HI map) |
| Legal property owner | **No product structure** |
| Beneficial/controlling owner | Research-only (Webhound prompts) |
| Sponsor/investor / developer / asset manager | Decision Opportunity vocabulary (GTM schema) — not populated from Census |
| Individual principal | Contact models (GTM) + Webhound — not HI |

Canonical hotel has empty `owner.*` / `operator.*` slots; Census HI map has **no** owner fields.

### Evidence-first assertion support

| Element | Supported by existing code? |
| --- | --- |
| PROPERTY identity | ✅ HI / Census |
| PROBABLE OWNER string | ❌ no extractor/orchestrator |
| RELATIONSHIP `OWNED_BY` | ❌ no edge type in HI |
| CONFIDENCE on ownership edge | ❌ (hotel field confidence ≠ ownership) |
| Multi-source EVIDENCE list | ⚠️ pattern exists (evidence-store, Webhound claims, Decision Evidence schema) — not ownership-wired |
| PRINCIPAL | ⚠️ Webhound + GTM contact patterns |
| CONTACT | ⚠️ verification utils; no auto discovery from Census |
| PORTFOLIO of other hotels | ⚠️ audit math; needs public-linked hotels |

---

## F. Corrected readiness score (CoStar = 0)

| Component | Wt | Previous | Corrected | Notes |
| --- | ---: | ---: | ---: | --- |
| Hotel census | 10 | 9 | **9** | Unchanged |
| Hotel identity resolution | 10 | 8 | **8** | Unchanged |
| Public-web research | 15 | 6 | **7** | Room-count engine + SerpAPI + Webhound MCP exist; HI↔Webhound still disconnected |
| Owner/entity discovery | 15 | 4 | **2** | Removed CoStar credit; only seeds/Webhound/manual |
| Entity resolution | 10 | 3 | **3** | Hotel strong; org/person still weak (logic partial) |
| Ownership graph | 10 | 2 | **1** | Slots/docs only; no edges |
| Portfolio discovery | 5 | 3 | **1** | GTM CoStar portfolios out; reusable math remains unused |
| People discovery | 5 | 3 | **2** | Webhound capable; not automated |
| Contact discovery | 5 | 3 | **2** | Verification reusable; discovery not productized |
| Evidence/provenance | 5 | 4 | **4** | Hotel evidence + Webhound claims still real |
| Confidence scoring | 5 | 3 | **3** | Hotel/signal scores; no ownership-edge score |
| Opportunity monitoring | 5 | 3 | **2** | Decision schema reusable; no Census-driven monitor; CoStar triggers out |
| **TOTAL** | **100** | **51** | **44** | |

```text
PREVIOUS_SCORE = 51 / 100
CORRECTED_NON_COSTAR_SCORE = 44 / 100
```

**Difference (−7):** Prior score credited GTM CoStar-backed owner/portfolio/opportunity capability as if it were a product source. Corrected score counts only **repeatable non-CoStar** machinery; reusable GTM algorithms add little until fed public evidence and wired to Census.

---

## G. 25-hotel experiment design

### Objective

Stage-only, **no CoStar**, precision over coverage: for 25 CALA Census hotels produce evidence-backed Owner Intelligence or explicit `UNKNOWN`.

### Mix (deliberately hard)

| Bucket | ~N | Intent |
| --- | ---: | --- |
| Major-brand franchise (owner ≠ brand) | 6 | Catch false `Owner=Marriott/Hilton` |
| Soft-brand / collection | 3 | Brand ambiguity |
| Independent | 4 | Owner often on site/about |
| Known owner-operator (Iberostar-class) | 3 | House-brand path |
| Separate third-party operator | 3 | Owner≠operator |
| Investment group / opaque SPV signal | 3 | Expect many UNKNOWN |
| Recent open / development | 3 | Press/developer pages |

Pick hotels that already have strong Census identity (name, country, city, website when possible).

### Allowed sources only

- Census identity fields  
- Official property URL + owner/developer/about pages  
- SerpAPI / targeted Google search (capped)  
- Webhound for ≤8 hard cases (budgeted)  
- Corporate web seeds / MX SIGER **as search aids** (no CoStar labels as evidence)  
- Brand/operator press only to **disprove** brand-as-owner — not to assert ownership alone  

### Forbidden

- CoStar exports, GTM True Owner fields, `company-profile-enrichments.js` stats  
- Invented emails  
- Promoting brand parent company as property owner without explicit ownership language  

### Pipeline per hotel (manual/agent orchestration — not built yet)

1. `hotel_resolve` / Census load  
2. Role baseline: brand, parent company, known franchise flags  
3. Bounded search queries: `"{hotel}" owner`, `"{hotel}" desarrollador`, portfolio pages  
4. Optional Webhound for hard cases with Mexico-owner-intel Tier-1 prompt subset  
5. Emit staged JSON edge claims with sources  
6. Run franchisor/house-brand hard fail  
7. Human review all 25  

### Success thresholds (precision-first)

| Metric | Target |
| --- | --- |
| Hotel identity success | ≥95% |
| Probable owner | ≥40% (honest UNKNOWN OK) |
| High-confidence owner | ≥20% |
| Correct owner≠operator≠brand among asserted | ≥90% of asserted |
| Ownership group | ≥50% of high-conf owners |
| Principal | ≥30% of probable owners |
| Usable business contact | ≥20% of probable owners |
| Portfolio ≥1 other hotel | ≥30% of probable owners |
| ≥2 evidence sources | ≥70% of high-conf |
| ≥1 primary/official | ≥60% of high-conf |
| **False owner attribution** | **≤10% of asserted** (manual) |
| Cost / hotel | Track $ (SerpAPI + Webhound) |
| Time / hotel | Track |

Unanswered > wrong.

---

## H. Smallest implementation required (design only — do not build)

1. **Staging schema** for ownership edges (`hotel_id`, relationship, entity, role, confidence, evidence[]) — local JSON only.  
2. **Orchestrator script** cloning room-count research pattern with owner query packs + franchisor hard-fail.  
3. **Reuse** `branding-owner-context` + `icp-classify` + contact verification + portfolio-audit math on public claims.  
4. **Optional** agent-triggered Webhound for hard subset; export claims into staging.  
5. **Metrics rollup** + manual review checklist.  

Do **not**: new MCP, Airtable tables, CoStar import, HI rewrite, Webhound rewrite.

---

## I. What should NOT be rebuilt

- Census + `dhl_*` identity  
- HI evidence store / confidence for hotel fields  
- Room-count research engine pattern  
- Provider registry (non-ownership)  
- GTM role/contact/decision **algorithms** (ported carefully)  
- Webhound as external deep research  
- Partner Source Library governance for brands/operators  

---

## J. Recommendation

```text
EXTEND_EXISTING_COMPONENTS
```

Not `CONNECT_EXISTING_COMPONENTS` alone — HI and Webhound are **NOT_CONNECTED**, and there is no ownership edge layer to plug into.  

Not `BUILD_MISSING_OWNER_RESEARCH_LAYER` as a greenfield — room-count research, role classifiers, evidence patterns, and Webhound already cover ~70% of the **machinery**; what’s missing is a thin **Census→public-owner research→edge staging** extension.  

Not `CURRENT_STACK_INSUFFICIENT` — 44/100 with strong identity + public research primitives is enough to run the 25-hotel precision experiment.

---

### Commercial moat candidates (code-backed, non-CoStar)

| Candidate | Why it could matter |
| --- | --- |
| Census-keyed identity at CALA scale | Start from hotels others scrape ad hoc |
| Brand≠owner discrimination logic | Reduces catastrophic false owners |
| Evidence-first field research pattern | Defensible claims vs lead lists |
| Decision Opportunity vocabulary tied to Brand/Operator workflows | Ownership intel → Dealality process (unique product angle) |
| Webhound-as-hard-case + code learning loop | Cost control vs always-on research agents |

Not moats yet: GTM CoStar portfolios, unimplemented `owner_get`, geographic “Portfolio Coverage OS.”

---

*End of non-CoStar corrected audit.*
