# AI Visibility — Shared Executive Summary | Detailed View IA

> Locked product grammar for Brand, Operator, and Owner surfaces.
> Visual styling remains Dealality live shell/constitution — mockups are functional references only.

**Status:** Brand Phase 3A.1 implements this pattern. Operator and Owner are documented only (not built).

---

## Shared tabs

| Tab | Purpose |
|-----|---------|
| **Executive Summary** (default) | Management / portfolio briefing — current position, where strong/weak, what changed, what to review |
| **Detailed View** | Analytical drill-down — metrics, decision patterns, questions, peers, trends, sources, evidence |

**Phase 3A.4 (2026-08-13):** Brand product consolidates to **two tabs**. Hotel Decision Visibility proprietary intelligence is preserved inside Executive Summary + Detailed View (not a third visible tab). Duplicate HDV Portfolio Overview removed.

Do **not** require a mandatory Portfolio intermediate page. Portfolio tables live *inside* Executive Summary.

---

## Brand AI Visibility (implemented)

| Tab | Content |
|-----|---------|
| Executive Summary | Company-scoped entitled brands; Portfolio Snapshot includes Decision Visibility Coverage + Top Decision Territory; OpenAI Discoverability Future Ready block |
| Detailed View | Brand-scoped analytical drill-down including Decision Patterns, AI vs Dealality, Review Items, OpenAI Discoverability placeholders |

### Executive Summary theme groups

**Authority (2026-08-15 integration):** Restored OE spine remains. Surgical Wave merge — findings first; QM/citations into spine; Cross-Provider / Discoverability / Language as one conditional **AI Intelligence Overview** row. No “New AI Visibility Intelligence” mega-wrapper.

| # | Theme | Cards / content | Highlights |
|---|-------|-----------------|------------|
| 1 | **Executive Summary** | Compact insight tiles (3–5) | Title / finding / evidence / watch; hide when empty |
| 2 | **Portfolio Snapshot** | Equal KPI row | Where the portfolio stands now — no composite score |
| 3 | **Markets & Movement** | Regional / Market View + **AI Presence Over Time** | Coverage by geography + comparable monitoring-period trends |
| 4 | **Visibility Signals** | Top Strengths · Gaps / Risks (+ top prompt-family missing) | What’s working vs what needs attention |
| 5 | **Your Brands** | Portfolio Overview table with **Δ AI Presence** (+ Notable moves) | Brand-by-brand entitled overview |
| 6 | **Peer Context & Next Review** | Competitive Snapshot (~40%) · Priority Review (~60%, top 4) | Compact peer facts + review rows; Detail for full lists |
| 7 | **Evidence & Source Intelligence** | 2-col: Citation Intelligence (2×2 KPIs) · Source Landscape (Top Owned / Top External + recurring) | KPI column leads; External = non-owned under governed set |
| 8 | **AI Intelligence Overview** *(conditional)* | Provider Visibility · Public Discoverability · Language Comparison | One compact row; cards hide independently by existing eligibility rules |

Related cards share one **Operator Explorer–style section**:

1. **Outer box** (bordered panel)
2. **Section title** (title case, e.g. Portfolio Snapshot)
3. Optional muted help line
4. **Inner card** with **ALL CAPS** subheaders + body content

| Theme (legacy map) | Cards / content | Highlights |
|-------|-----------------|------------|
| **Portfolio Snapshot** | KPI row (Quick Facts–style label/value tiles) | Where the portfolio stands now — no composite score |
| **Markets & Movement** | Regional / Market View + **AI Presence Over Time** (LOI-style line chart) | Coverage by geography + actual monitoring-period trends (no interpolated points) |
| **Visibility Signals** | Two inner cards · Top Strengths · Gaps / Risks | What’s working vs what needs attention |
| **Your Brands** | Portfolio Overview table with **Δ AI Presence** (+ Notable moves for non-zero) | Brand-by-brand entitled overview; change beside level |
| **Peer Context & Next Review** | Competitive Context · Priority Review Items | Benchmark-safe peers + factual review items |
| **Evidence Basis** | Sources Summary | Cited / recurring sources — not influence rankings |
| **Evidence & Source Intelligence** *(current)* | Citation Intelligence + Source Landscape | Portfolio-style KPI cards primary; recurring sources secondary (3-col, truncatable) |

### Detailed View theme groups (same visual grammar)

| Theme | Cards / content | Highlights |
|-------|-----------------|------------|
| **Brand Snapshot** | Primary KPI row + secondary metrics (Recommendation Share, First Recommendation Rate, Citation Rate) | Brand-level position; Δ AI Presence when prior period exists; descriptions bottom-aligned |
| **Markets & Movement** | Regional Position table + **AI Presence Over Time** (same Chart.js LOI line rules as Exec) | Region-pure rows; single-brand series; legend + footnote below chart |
| **Owner Questions** | Question Results table · All / Won / Missing tabs | Intent filter applies **only** here |
| **Peer Context & Next Review** | Competitive Context (KV rows) · Opportunity Engine placeholder | No Priority Review card on Detail (brand-scoped peers + future OE only) |
| **Evidence Basis** | Sources Summary as domain links | Subnote: “Unique cited sources appearing in answers: N” |

### Hotel Decision Visibility theme groups

| Theme | Cards / content | Highlights |
|-------|-----------------|------------|
| **Headline metrics** | Decision Visibility Coverage · Top Decision Territory · Best Competitive Position · Evidence-backed Review Items count | Portfolio coverage ≠ brand AI Presence average; no HDV score |
| **Owner Intent Coverage** | Intent Territory rows with coverage % + optional prior-batch delta | Derived from evidence `intentTerritory` |
| **Decision Visibility by Geography** | Global / CALA / Europe / North America leaders | Region-pure; leading entitled brand |
| **AI vs Dealality Decision Context** | Question · AI Pattern · Dealality Context · status | Thin Brand Basics facts only; else “not yet available” |
| **Evidence-backed Review Items** | Deterministic cards with evidence links | `hotel_decision_visibility_review_rules_v1`; not Opportunities |
| **Portfolio Overview** | Entitled brands + Top Intent + prior-run delta | No fake sparklines |

Definitions: `docs/ai-build-system/AI_VISIBILITY_HOTEL_DECISION_VISIBILITY_DEFINITIONS.md`.

**Shared visual rules (Exec + Detail):**

- Outer `.aiv-theme-group` → title case `.aiv-theme-title` → `.aiv-theme-help` → inner `.aiv-theme-card` + white ALL CAPS `.aiv-theme-label`
- Label gap: **13px** below titles without description; **4px** when followed by `.aiv-theme-block-help`, then **14px** to content
- Charts: Chart.js line, `fill: false`, white LOI tooltips (`Brand: 80%`), `interaction.mode = index`, top padding above 100%, legend centered below canvas, footnote below legend
- Sources: clickable `https://` domain links; no “N appearances” body copy
- Empty / Not Monitored / Partial states explicit — never blank components

**Your Brands table:** Metric columns include Radar-style info icons (click → fixed dark popup with close ×). Explanations are geography-scoped and do not invent Opportunity Engine content.

**Market movement chart:** LOI Market Hub line styling (Chart.js). Exec series = entitled monitored brands (top 6 by latest presence). Detail series = selected brand only. X = completed monitoring **batch periods** (`batchId`, not calendar-day collapse — same-day repeats count). Y = AI Presence %. `chartReady` only when ≥2 periods. No interpolated midpoints; missing brand×period cells are gaps (`spanGaps: false`). Not a portfolio composite score.

Filters:

- Executive: Geography, Provider (Intent optional later)
- Detailed: Geography, Provider, Brand (entitled only), Intent
- Hotel Decision Visibility: Geography, Provider, Intent, Brand Portfolio (`Portfolio` default or entitled brand)

Provider options are filterable; enable additional providers in the dropdown when monitoring sources are added (currently OpenAI only).

---

## Operator AI Visibility (future — do not build yet)

| Tab | Content |
|-----|---------|
| Executive Summary | Operator-company management view: Current Position, Regional/Market View, What Changed, Top Strengths, Gaps/Risks, Competitive Context, Priority Review Items, Evidence/Sources |
| Detailed View | Operator intelligence: AI Presence, Competitive Position, Questions Won/Missing, trends, owner questions, peer context, sources, evidence |

Same decision structure; metrics may differ by stakeholder.

---

## Owner — AI Recommendation Intelligence (future — do not build yet)

Product name: **AI Recommendation Intelligence** (not “AI Visibility”).

| Tab | Content |
|-----|---------|
| Executive Summary | Deal/asset recommendation briefing: what AI is recommending, recurring brands/operators, strongest patterns, what changed, what deserves review, supporting evidence |
| Detailed View | Question-by-question recommendations, brand/operator patterns, sources, evidence; separate **AI Recommendation Pattern** vs **Dealality Analysis** |

---

## Honesty rules (all stakeholders)

- No portfolio / GEO / composite scores
- No fabricated deltas, opportunities, confidence, or executive narrative
- Observed / Zero / Not Monitored / Unavailable / Partial / Future Ready
- Not Monitored ≠ 0
- Region-pure headline geography (no country rollup into region headlines)
- Competitors: comparative access only unless entitled deep
