# Brand AI Visibility — Phase 3A.5 Showcase Design Audit

> **Status:** Design-only / read-only complete · 2026-08-14  
> **Activity:** 0 live provider calls · 0 Airtable writes · 0 schema/UI/peer/prompt/deploy changes  
> **BUILD STATUS:** `BRAND_AI_VISIBILITY_PHASE_3A5_SHOWCASE_DESIGN_AUDIT_PASS`  
> **Next recommended phase:** `PHASE_3A6_SHOWCASE_DATA_GOVERNANCE`

This document is the canonical current-state handoff plus Marriott / Hilton / Choice showcase design. It does **not** authorize monitoring execution.

---

## Constitution compliance

Re-read and preserved:

- No arbitrary GEO / HDV composite score
- No client-facing arbitrary confidence score
- No fake history / interpolated trends
- No unsupported causal claims
- Company Validated > AI
- Centralized monitoring datasets; authorization at read-time
- Viewer ≠ Subject; Provider + Geography first-class
- Evidence / interpretation / action separated
- Competitor access comparative only
- **Actual portfolio ≠ competitive cohort** (locked in Part 3)

---

## 1. Product (current)

| Item | Value |
|------|--------|
| Product name | **Brand AI Visibility** |
| Target user | Brand development / brand strategy (demo via Dealality Brand Demo workspace) |
| Architecture | **Two tabs only:** Executive Summary (default) · Detailed View |
| Page | `/ai-visibility-brand.html` (+ Webflow embed path) |
| Filters | Geography · Provider (OpenAI-only today) · Brand (Detail) · Intent (Detail → Owner Questions only) |
| Availability states | `observed` · `zero` · `not_monitored` · `unavailable` · `partial` · `future_ready` (`ai_visibility_availability_v1`) |
| Demo behavior | `DEMO_BRAND_PORTFOLIO` cross-parent 7 brands entitled as one pretend portfolio (validation-era; **not** sales-ready) |

---

## 2. Executive Summary — section inventory (UI order)

| SECTION | PURPOSE | METRICS / CONTENT | DATA SOURCE | API / SERVICE | LEVEL | PROVIDER | GEO | AVAILABILITY | LIMITATION |
|---------|---------|-------------------|-------------|---------------|-------|----------|-----|--------------|------------|
| Portfolio Snapshot | Portfolio KPIs now | Decision Visibility Coverage, Top Decision Territory, Best Competitive Position, Questions Won/Missing, review-item count | HDV + portfolio metrics | `GET .../executive-summary` → `brand-executive-summary` + HDV | Portfolio | Yes | Yes | Observed when monitored | No composite score |
| Markets & Movement · Regional | Coverage by region | Brands monitored, leading brand, presence, best rank | Summaries / monitoring state | same | Portfolio | Yes | Multi-geo | Not monitored when no batch | Same-day duplicate batches exist |
| Markets & Movement · Chart | Presence over time | AI Presence series (top entitled brands) | Metric snapshots / trend | same + trend helpers | Portfolio | Yes | Selected | Needs ≥2 periods | Same-day runs ≠ 30d trend |
| Visibility Signals · Strengths | What’s working | Highest presence, best rank, Questions Won | Exec aggregator | same | Portfolio | Yes | Yes | Factual only | Non-causal |
| Visibility Signals · Gaps | What’s weak | Unmonitored brands, missing coverage, Questions Missing | same | same | Portfolio | Yes | Yes | Factual only | |
| Your Brands · Portfolio Overview | Brand table | Presence, Δ, Competitive Position, Rec Rate/Share, Top-3, First Rec, Won/Missing, Top Territory | Portfolio read | same | Brand rows | Yes | Yes | Per brand | Cross-parent demo today |
| Peer Context & Next Review · Competitive Context | Leading peer | Peer leader presence/rank | Competitors payload | nested `getBrandCompetitorsPayload` | Comparative | Yes | Yes | Comparative only | No peer deep-link |
| Priority Review Items | Evidence-backed review | Review rule cards | HDV review rules v1 | HDV | Portfolio/brand | Yes | Yes | Deterministic | Not Opportunities |
| Opportunity Engine | Placeholder | Future ready | — | — | — | — | — | Future ready | Not activated |
| Evidence Basis · Sources | Cited domains | Unique sources / recurring domains | Sources payload | nested sources | Brand (top) | Yes | Yes | Partial common | Association ≠ influence |
| AI Discoverability | Future crawl/impact | Connection-required rows + coming later | `future-discoverability` | placeholder | Portfolio | OpenAI-scoped copy | — | Future ready | Dealality build-out |

---

## 3. Detailed View — section inventory (UI order)

| SECTION | PURPOSE | METRICS / CONTENT | SOURCE | API | LEVEL |
|---------|---------|-------------------|--------|-----|-------|
| Brand Snapshot | Brand KPIs | AI Presence, Competitive Position, Questions Won/Missing, Rec Rate/Share, Top-3, First Rec, Citation Rate | overview | `.../overview` | Brand |
| Decision Patterns | Intent analytics | Owner Intent Coverage, Top / Weakest Territory, Intent Coverage Breadth | HDV intent rows | overview | Brand |
| Markets & Movement | Regional + trend | Regional Position; AI Presence Over Time | overview + trend | overview, trend | Brand |
| Owner Questions | Question drill-down | Won / Missing / All; Intent filter | questions | `.../questions` | Brand |
| Peer Context | Cohort peers | Comparative peer table | competitors | `.../competitors` | Comparative |
| AI vs Dealality Decision Context | Pattern vs Brand Basics | Question · AI Pattern · Dealality Context | HDV | overview | Brand |
| Review Items | Evidence-backed findings | Review cards | HDV rules | overview | Brand |
| Evidence Basis | Sources | Domains + evidence drawer | sources, evidence | `.../sources`, `.../evidence` | Brand |
| AI Discoverability | Future placeholders | Crawl / AI-originated impact | placeholder | overview | Brand |
| Opportunity Engine | Future | Not activated | — | — | — |

---

## 4. Metric dictionary (`ai_visibility_metrics_v1`)

| Product name | Definition | Numerator | Denominator | Scope | Status |
|--------------|------------|-----------|-------------|-------|--------|
| AI Presence | Share of successful cohort answers where brand appears | Present observations | Successful observations | Entity · geo · provider | READY |
| Competitive Position | Rank by AI Presence among peer set | Rank | Peer count | Entity · peers · geo · provider | READY |
| Recommendation Rate | ≥1 positive rec role in answer | Obs with positive rec | Successful obs | Entity · cohort | READY (eligibility = cohort, not addressable yet) |
| Recommendation Share | Brand recs / all peer recs | Brand recommendation mentions | All recommendation mentions | Entity · cohort | READY |
| Top-3 Recommendation Rate | Positions 1–3 only when ranked | Top-3 hits | Successful obs | Entity | READY |
| First Recommendation Rate | First recommended entity | First-rec hits | Successful obs | Entity | READY |
| Questions Won | Sole first-rec leader per prompt | Won prompt count | (display also % of prompt set) | Entity · prompts | READY |
| Questions Missing | Brand absent | Missing count | Prompt / obs set | Entity | READY |
| Citation Rate | First-party citation association | Cited obs | Successful obs | Entity | PARTIAL |
| Decision Visibility Coverage | Entitled presence in owner-decision cohort | ≥1 entitled present | Successful obs | Portfolio or brand (= presence) | READY |
| Owner Intent Coverage | Presence within each Intent Territory | Present in intent | Successful in intent | Portfolio/brand | READY (only intents present in evidence) |
| Top Decision Territory | Highest presence intent | — | — | Brand/portfolio | READY |
| Weakest Intent Territory | Lowest presence intent | — | — | Brand (UI-derived) | READY (Detail UI) |
| Intent Coverage Breadth | Intents with presence > 0 | Count present | Count monitored intents in evidence | Brand (UI-derived) | READY (Detail UI) |

Positive recommendation roles: `first_recommendation` · `ranked_recommendation` · `explicit_recommendation`.

---

## 5. Data architecture

| Layer | Implementation |
|-------|----------------|
| BATCH | `batches/*.json` — geography, provider, peerSetId, promptIds, usage/cost, cohortFingerprint |
| RUN | `runs/*.json` — one per prompt execution |
| RAW_RESPONSE | Stored with run / responses |
| MENTIONS | Extracted entity mentions + roles |
| CITATIONS | Extracted citation URLs / domains |
| EVIDENCE | `evidence/*.json` — prompt + response + mentions + citations + `intentTerritory` |
| METRIC_SNAPSHOTS | `metric-snapshots/*.json` — per entity × metric × period |
| PROVIDER | First-class (`openai` only live) |
| GEOGRAPHY | Global / Region (CALA, Europe, North America) / Country (prompts exist; sparse batches) |
| INTENT | From prompt `intentTerritory` onto evidence |
| PEER_SET | `peers_upper_upscale_brands_global_v1` (fixture) |
| STORAGE | `data/ai-visibility/runtime/phase2e` (recovered) via `resolveAiVisibilityStoreRoot` |
| HISTORY | Query by completed batches / snapshots; **same calendar day ≠ durable trend** |

**Execution model:** one provider call per prompt analyzes the **full peer universe** in one response. Calls do **not** multiply by brand count.

---

## 6. Authorization

| Concept | Behavior |
|---------|----------|
| Viewer | Authenticated Dealality user / company workspace |
| Subject | Brand or brand portfolio under analysis |
| Entitled brand | Company Profile linked brands (demo: `DEMO_BRAND_PORTFOLIO`) |
| Deep access | Entitled brands only |
| Comparative | Peer-set competitors — benchmark-safe KPIs only |
| Direct ID | Unauthorized brand IDs → deny / none |
| Demo | Brand Demo workspace gets cross-parent validation portfolio |

---

## 7. APIs / services

| Route / module | Purpose |
|----------------|---------|
| `GET /api/ai-visibility/brand/portfolio` | Entitled brands + availability |
| `GET /api/ai-visibility/brand/executive-summary` | Exec briefing |
| `GET /api/ai-visibility/brand/:id/overview` | Detail snapshot + HDV merge |
| `.../trend` | Presence over periods |
| `.../questions` | Owner questions |
| `.../competitors` | Peer context |
| `.../sources` | Citation association |
| `.../evidence` | Evidence drawer |
| `lib/ai-visibility/brand-executive-summary.js` | Exec aggregator |
| `lib/ai-visibility/brand-read-service.js` | Detail reads |
| `lib/ai-visibility/hotel-decision-visibility.js` | HDV internal |
| `lib/ai-visibility/authorization.js` + entitlements | Access depth |
| `lib/ai-visibility/metrics.js` | Deterministic metrics |
| `lib/ai-visibility/execute-cohort.js` | Monitoring execution |
| `lib/ai-visibility/future-discoverability.js` | Placeholders |

Public HDV route retired (3A.4).

---

## 8. Current monitoring inventory (live store)

Store root: `data/ai-visibility/runtime/phase2e` · Provider: **OpenAI** · Peer set: `peers_upper_upscale_brands_global_v1`

| Geography | Batches (2026-08-13) | Prompt runs (success) | Est. cost (USD) | Entities |
|-----------|----------------------|-----------------------|-----------------|----------|
| Global | 1 | 8 | 5.30 | 10 |
| CALA | 3 | 15 | 9.53 | 7–10 |
| Europe | 2 | 10 | 6.93 | 7–10 |
| North America | 1 | 3 | 2.63 | 10 |
| **Total** | **7** | **36** | **~24.38** | — |

Historical average ≈ **$0.68 / call** · ≈ **45k tokens / call**.

Comparable history: multiple same-day CALA/Europe batches — useful for validation Δ, **not** honest 30/90-day product trends.

---

## 9. Current demo portfolio audit

| Field | Value |
|-------|--------|
| CURRENT_DEMO_ENTITLED_BRANDS | Ascend, Autograph, Curio, Tribute, Hotel Indigo, Design Hotels, Westin |
| CURRENT_PARENT_COMPANIES | Choice · Marriott · Hilton · IHG · Marriott · Marriott · Marriott |
| CURRENT_PORTFOLIO_IS_REAL_COMPANY_PORTFOLIO | **NO** |

Acceptable for functional validation. **Not acceptable** as Marriott/Hilton/Choice sales showcase.

---

## 10–12. Showcase portfolios + shared cohort (governed Brand Basics IDs)

### Marriott — PRIMARY (5)

| Brand | ID | Why |
|-------|-----|-----|
| Autograph Collection | `recEJCTDj1zrsjPM6` | Soft-collection / conversion flagship |
| Tribute Portfolio | `recCvV0PuZOi8c3hC` | Lifestyle collection complement |
| Design Hotels | `rec02zPClpWUTCyXM` | Design-led collection |
| Westin | `recIPuBC50fv13zRR` | Upper-upscale hard brand comparator inside portfolio |
| AC Hotels by Marriott | `rec9aZp7GHtzUEg0c` | Lifestyle / urban positioning |

**Excluded (examples):** Courtyard (chain-scale quality risk / different owner decision), Sheraton & Marriott Hotels (too broad flagship), Aloft/Moxy (lower scale), StudioRes / Residence Inn / SpringHill (extended stay).

### Hilton — PRIMARY (4)

| Brand | ID | Why |
|-------|-----|-----|
| Curio Collection by Hilton | `receQkxgjlezsc1xg` | Soft-collection peer to Autograph |
| Tapestry Collection by Hilton | `reccXxMHEh7NNRhIE` | Accessible collection / conversion |
| Canopy by Hilton | `recsggfbKlJbjeRP9` | Lifestyle urban |
| Tempo by Hilton | `recqiHq3GHKMj8Meo` | Newer lifestyle / development narrative |

**Excluded:** Hilton Hotels & Resorts (flagship breadth), DoubleTree (different conversion lane), Garden Inn / Hampton / Tru / Spark / Motto (lower or select-service).

### Choice — PRIMARY (4)

| Brand | ID | Why |
|-------|-----|-----|
| Ascend Hotel Collection | `reclkgOzvAcBheUSo` | Soft-collection conversion |
| Radisson Individuals by Choice | `recRyvM8OmLlDj9G7` | Collection / individuality |
| Radisson Blu by Choice | `recWPEvxBQxVVzSq3` | Upper-upscale hard brand |
| Radisson RED by Choice | `recmKqo7M7mLZgRqQ` | Lifestyle |

**Excluded:** Comfort / Quality / WoodSpring / Suburban / Everhome (midscale–economy); Radisson by Choice optional later.

### Shared competitive cohort (15) — `Upper-Upscale / Collection / Lifestyle Owner Decision Cohort`

| Brand | Parent | Role |
|-------|--------|------|
| Autograph | Marriott | Soft collection |
| Tribute | Marriott | Lifestyle collection |
| Design Hotels | Marriott | Design collection |
| Westin | Marriott | UU hard brand |
| AC Hotels | Marriott | Lifestyle |
| Curio | Hilton | Soft collection |
| Tapestry | Hilton | Soft collection |
| Canopy | Hilton | Lifestyle |
| Tempo | Hilton | Lifestyle |
| Ascend | Choice | Soft collection |
| Radisson Individuals | Choice | Soft collection |
| Radisson Blu | Choice | UU hard brand |
| Hotel Indigo | IHG | Lifestyle |
| Kimpton | IHG | Lifestyle |
| MGallery Collection | Accor | Soft collection |

**CAN_ONE_SHARED_COHORT_SUPPORT_MARRIOTT_HILTON_CHOICE?** **YES**

Optional later sub-cohort: pure soft-collection-only (drop Westin / Blu) for collection-only decks.

---

## 13. Existing peer set assessment

| Field | Value |
|-------|--------|
| CURRENT_PEER_SET | `peers_upper_upscale_brands_global_v1` |
| CURRENT_COUNT | 10 |
| CURRENT_MEMBERS | Curio, Autograph, Hotel Indigo, Tribute, Kimpton, Design Hotels, Ascend, Canopy, Westin, Tapestry |
| FIT_FOR_SHOWCASE | **PARTIAL** — good core; too small; missing Choice UU (Blu/Individuals/RED), Marriott AC, Hilton Tempo, Accor MGallery |
| MISSING_BRANDS | AC Hotels, Tempo, Radisson Blu, Radisson Individuals, Radisson RED, MGallery (+ optional Bunkhouse) |
| BRANDS_THAT_SHOULD_BE_REMOVED | None required; keep Westin as hard-brand anchor |
| RECOMMENDED_NEW_VERSION_NAME | `peers_uu_collection_lifestyle_owner_decision_v2` |
| Action | **VERSION_NEW** (do not apply in 3A.5) |

---

## 14–15. Prompt audit + showcase intent taxonomy

**Governed library:** 38 active monitoring-eligible prompts (`phase2d-prompt-seed.json`); ~28 Brand-relevant.

Current Brand intents: Brand Selection, Conversion, Owner Flexibility, Branded Residences, Mixed Use, Development Strategy, New Build, HMA vs Franchise (operator-heavy), Chain Scale / Positioning, Owner Economics, Operator Selection (defer for Brand showcase).

### Recommended showcase taxonomy (7)

1. **Conversion** — KEEP (strong coverage)
2. **Collection / Soft Brand** — MERGE from soft-brand conversion + Owner Flexibility prompts
3. **Lifestyle Positioning** — KEEP/EDIT lifestyle urban family
4. **Upper-Upscale Positioning** — MERGE chain-scale / resort positioning (owner language, not SEO)
5. **New Build** — PARTIAL; many prompts are select-service — EDIT or ADD UU/collection new-build
6. **Branded Residences / Mixed Use** — MERGE residences + mixed use
7. **Owner Economics / Flexibility** — KEEP thin set; ADD 1–2 if needed

**DEFER:** Operator Selection, HMA vs Franchise (Operator AI Visibility), Distribution/Loyalty claims (unsupported), Development Credibility track-record claims (high hallucination risk).

**REUSE:** majority of UU conversion + soft-brand + lifestyle + residences regional prompts.  
**EDIT:** intent labels for consistency; select-service new-build → collection/UU where needed.  
**ADD:** ~4–8 prompts for gaps (UU new build; NA soft-brand; economics depth).  
**DEFER:** country packs beyond Mexico until after first showcase wave.

---

## 16–17. Addressable Decision Universe

| Dimension | Available in Brand Basics loader today? | Quality | Used for eligibility? | Deterministic? |
|-----------|------------------------------------------|---------|----------------------|----------------|
| Parent Company | YES | Good | No (auth only) | YES |
| Chain Scale | YES | Mixed (e.g. Courtyard labeled Upper Upscale) | Prompt filter only | PARTIAL |
| Brand Model (Hard/Collection/Lifestyle/…) | YES | Good | No | YES |
| Brand Status Active/Live | YES | Good | Entity load | YES |
| Geography / region offered | NOT in AI Visibility loader | Unknown | No | NEEDS AUDIT |
| Conversion eligibility | NOT loaded | — | No | NEEDS FIELD |
| New build eligibility | NOT loaded | — | No | NEEDS FIELD |
| Branded residences | NOT loaded | — | No | NEEDS FIELD |
| Franchise / HMA model | NOT loaded | — | No | NEEDS FIELD |
| Key-count bands | NOT at brand loader | — | No | FUTURE |
| First-party domains | Empty in loader | Weak | Citation only | IMPROVE |

**CAN_WE_BUILD_ADDRESSABLE_DECISION_UNIVERSE_V1_WITH_CURRENT_DATA?** **PARTIAL**

**V1 proposal (design only):**

- Version: `addressable_decision_universe_v1`
- Required: Brand Status Active/Live · Parent · Chain Scale · Brand Model · prompt `chainScale` / `developmentType` / `intentTerritory`
- Exclusion: hard conflict only (e.g. Economy brand vs Upper Upscale prompt)
- **UNKNOWN ≠ NOT ELIGIBLE** — unknown dimensions remain cohort-eligible with `eligibilityConfidence: unknown`
- No AI eligibility scores

---

## 18. Recommendation Rate future

| Mode | Denominator |
|------|-------------|
| CURRENT | Successful cohort-eligible responses |
| FUTURE | Successful responses where brand is addressable for that decision |

**Recommended product naming:** keep **Recommendation Rate** as primary; add secondary **Addressable Recommendation Rate** when ADU v1 ships (do not rename quietly).

---

## 19–22. Monitoring wave + cost + cadence

### Geographies

| Geo | Include in showcase wave 1? |
|-----|------------------------------|
| Global | YES |
| CALA | YES |
| Europe | YES |
| North America | YES |
| Mexico | OPTIONAL (high CALA demo value) |
| Spain / US / DR country packs | DEFER |

### Density targets

| Target | Value |
|--------|-------|
| Shared cohort brands | 15 |
| Marriott deep | 5 |
| Hilton deep | 4 |
| Choice deep | 4 |
| Decision territories | 7 |
| Prompts / region | 7 |
| Regions | 4 |
| Countries | 0–1 (Mexico optional) |
| **TOTAL_PROMPT_EXECUTIONS** | **28** core (+6 Mexico ≈ 34) |

### Cost (OpenAI-only, from Phase 2E/3A averages ≈ $0.68/call)

| Scenario | Calls | LOW | EXPECTED | HIGH |
|----------|-------|-----|----------|------|
| Core 4 geos × 7 | 28 | ~$15 | ~$19 | ~$28 |
| + Mexico × 6 | 34 | ~$18 | ~$23 | ~$34 |

### Trend cadence

| Item | Recommendation |
|------|----------------|
| CADENCE | **Biweekly** for showcase; monthly steady-state product |
| MONTHLY_CALLS (4 geos × 7 × ~2) | ~56 |
| MONTHLY_COST | ~$35–$55 expected |
| Change vs prior | Honest after **2** biweekly periods (~2–4 weeks) |
| 30-day | After **≥2** spaced periods spanning ≥30 days |
| 90-day | After **≥3–4** periods spanning ≥90 days |
| Provider persistence | Only after multi-provider runs (future) |

---

## 23–25. Company demo framing (no fabricated metrics)

### Marriott

- **PORTFOLIO:** Autograph, Tribute, Design Hotels, Westin, AC Hotels  
- **COMPETITIVE_COHORT:** shared 15  
- **EXEC:** Portfolio Snapshot + Your Brands = Marriott-only deep metrics; Peer Context = full cohort  
- **DETAIL:** Decision Patterns + Questions for selected Marriott brand; peers comparative  

### Hilton

- **PORTFOLIO:** Curio, Tapestry, Canopy, Tempo  
- **COMPETITIVE_COHORT:** same 15  
- Same tab grammar; deep = Hilton only  

### Choice

- **PORTFOLIO:** Ascend, Radisson Individuals, Radisson Blu, Radisson RED  
- **COMPETITIVE_COHORT:** same 15  
- Emphasize conversion / soft-brand territories  

---

## 26. Discoverability placeholders

| Surface | Present? | Structural change required? |
|---------|----------|-------------------------------|
| Exec AI Discoverability | YES | **NO** |
| Detail crawl + AI-originated impact placeholders | YES | **NO** |

---

## 27. Future provider compatibility

| Provider | Compatible without redesign? |
|----------|------------------------------|
| OpenAI | Current |
| Gemini | YES — provider-pure re-run |
| Perplexity | YES — provider-pure re-run |
| All AI blend | **FORBIDDEN** |
| Architecture change | **NO** if provider stays first-class dimension |

---

## 28. Data gaps

**BLOCKS_SHOWCASE (for sales demos, not for this audit):**

- Company-specific entitlements (replace cross-parent demo)
- Peer set v2 membership aligned to shared cohort
- Intent density across 7 territories in monitoring evidence

**IMPROVES_SHOWCASE:**

- Chain Scale data quality (Courtyard UU label)
- Brand geography / conversion / residences fields in eligibility loader
- First-party domains for citation quality
- Mexico country pack after core wave

**FUTURE_PRODUCT:**

- Full Addressable Decision Universe
- Operator Selection / HMA territories
- Gemini / Perplexity
- Opportunity Engine activation

---

## 29. Recommended build sequence

1. **PHASE_3A6_SHOWCASE_DATA_GOVERNANCE** — company portfolios + peer set v2 config + entitlement mapping (no provider runs)  
2. Prompt taxonomy edit/add dry-run (Airtable seed dry-run only)  
3. Monitoring dry-run plan lock + cost confirm  
4. Real OpenAI showcase wave  
5. Reprocess / read validation  
6. Company demo workspace entitlements  
7. Founder QA on Marriott / Hilton / Choice views  

---

## 30. Founder decisions needed

1. Approve Marriott (5) / Hilton (4) / Choice (4) deep portfolios  
2. Approve shared 15-brand cohort vs soft-collection-only sub-cohort  
3. Approve biweekly cadence vs monthly  
4. Include Mexico in wave 1?  
5. Ship Addressable Recommendation Rate as secondary metric later — yes/no  
6. Whether Westin / Radisson Blu remain in soft-brand-heavy decks  

---

## 31. Activity

```
LIVE_PROVIDER_CALLS: 0
AIRTABLE_WRITES: 0
SCHEMA_CHANGES: 0
DEMO_ENTITLEMENT_WRITES: 0
PEER_SET_CHANGES: 0
PROMPT_WRITES: 0
DEPLOYS: 0
```

## 32. Next phase

`PHASE_3A6_SHOWCASE_DATA_GOVERNANCE`

## 33. BUILD STATUS

`BRAND_AI_VISIBILITY_PHASE_3A5_SHOWCASE_DESIGN_AUDIT_PASS`
