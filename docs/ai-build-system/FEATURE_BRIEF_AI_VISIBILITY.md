# Feature Brief: AI Visibility

See [DEALALITY_PRODUCT_CONSTITUTION.md](./DEALALITY_PRODUCT_CONSTITUTION.md) and [NAMING_AND_COPY_GUIDE.md](./NAMING_AND_COPY_GUIDE.md).

---

## 1. Objective

Measure how hotel brands and hotel operators appear when hotel owners, developers, and investors ask AI platforms commercially relevant questions.

Answer: presence, frequency, position/context, competitors, associated sources, absences, and change over time — without treating AI outputs as Dealality recommendations or building generic GEO software.

## 2. Primary Users

- Brand (development / brand strategy)
- Operator (business development / growth)
- Owner (asset / opportunity research)
- Admin (monitoring universe, prompts, cost)
- Demo User (preview only under existing Demo workspace rules)

## 3. User Value

- **Brand / Operator:** Evidence-backed view of when owners encounter them in AI answers, which peers appear instead, and which associated sources recur.
- **Owner:** Transparency into AI recommendation patterns for an asset profile, clearly separated from Dealality analysis.

## 4. Recurring Value

Governed monitoring periods → presence trends, competitor shifts, source-association changes, and opportunity lifecycle status.

## 5. Where It Appears

| Stakeholder | Product name | Placement (future phases) |
|-------------|--------------|---------------------------|
| Brand | **Brand AI Visibility** | Market Intelligence → Brand AI Visibility (`/ai-visibility`) — tabs: **Executive Summary** (default) \| **Detailed View** |
| Operator | **Operator AI Visibility** | Same Executive Summary \| Detailed View grammar (future) |
| Owner | **AI Recommendation Intelligence** | Same tab grammar for deal/asset briefing vs question-level detail (future) |
| Admin | Internal monitoring tools | Admin-only scripts/routes |

Shared IA: `docs/ai-build-system/AI_VISIBILITY_EXECUTIVE_SUMMARY_DETAILED_VIEW_IA.md`

## 6. Inputs Required

- Governed owner-intent prompt library (versioned)
- Provider/model configuration
- Canonical Brand Basics + Operator Master IDs and aliases
- Peer-set definitions and metric/rule config versions
- Optional deal/asset profile context (Owner view, later)
- Cost / run caps

## 7. Outputs Generated

- Deterministic metrics: AI Presence Rate, Recommendation Share, First Recommendation Rate, Questions Won, Questions Missing, Competitive Position (by Presence), Citation Rate
- Evidence records linking metrics → prompt/run/raw response/mentions/citations
- Future: opportunity workflow rows (human-reviewed)
- **No composite 0–100 GEO score**

## 8. AI Behavior

- Monitored AI platforms are **systems under observation**, not Dealality judgment.
- Assistive AI (entity disambiguation, representation-gap diagnosis, action drafting) is **approved for MVP but not Foundation Phase 1**.
- Sequence: raw collection → normalization → entity matching → citations → deterministic metrics → evidence → opportunity rules → **then** assistive interpretation.
- AI interpretations never overwrite raw evidence or Company Validated data.

## 9. Evidence / Interpretation / Next Action

| Layer | What |
|-------|------|
| **Evidence** | Prompt + version, provider/model, run timestamp, raw response, extracted mentions/citations, peer set, metric version |
| **Interpretation** | Opportunity diagnostics / representation-gap notes (post–Phase 1; labeled AI-Assisted / Needs Review) |
| **Next Action** | Human-owned Track / Review / Action Planned — no auto website edits or content publishing |

Owner copy must distinguish **AI Recommendation Pattern** from **Dealality Analysis**.

## 10. Data Model

### Persistence (founder-approved hybrid)

- **Airtable (operational SSOT):** governed config/workflow — live tables `AI Visibility - Prompts`, `AI Visibility - Opportunities` (Phase 2D).
- **Non-Airtable store (via abstraction):** high-volume runs, raw responses, mentions, citations, evidence. Phase 1 uses local/dev file store under ignored runtime path; not permanent production storage.
- **Canonical entities:** reuse `Brand Setup - Brand Basics`, `Operator Setup - Master`, Brand Alias Mapping — no parallel entity universe.

### Metrics

- Competitive Position ranks by **AI Presence Rate** (primary). Recommendation Share is secondary.
- No client-facing numeric confidence scores; use evidence descriptors (Repeated across engines/runs, Emerging pattern, Single-engine observation).

## 11. Permissions / Access

Reuse Memberstack Bearer + Company Profile Workspace Access + existing deal-record access for owner deal-scoped views. Brand/operator see only authorized company entities. Admin uses existing admin gates. No second auth system. (Routes/UI deferred past Phase 1.)

### Phase 2F — Viewer ≠ Subject (company-scoped intelligence)

- **Viewer** = authenticated Dealality user/workspace/company (authorization).
- **Subject** = brand / brand_portfolio / operator / deal / hotel_asset (analytical object).
- Brand & Operator product: **Brand AI Visibility** / **Operator AI Visibility** — deep access for entitled entities; **comparative** benchmark-safe access for peer-set competitors; otherwise none.
- Owner product: **AI Recommendation Intelligence** — deal/hotel_asset scoped (not “Owner AI Visibility”). Preserve three layers: AI Recommendation Pattern · Dealality Analysis · Owner Process (never merge into one score).
- Entitlements derive only from governed record links (Company Profile brand links, Users→Operator Master, Deals Company Profile). **No** name/parent/domain/LLM entitlement inference.
- Monitoring batches remain centrally generated (entity/geography/provider). Authorization applies at **read time** — no client-specific duplicate provider runs for the same cohort.
- Access depths: `deep` | `comparative` | `none`. Code: `lib/ai-visibility/authorization.js`, `entitlements.js`, `authorized-reads.js`.

## 12. UI Requirements

Deferred past Foundation Phase 1. Future: KPI cards, filters, trends, questions table, competitors, sources, opportunity queue, evidence drill-down; loading/empty/error/success states; no unexplained scores.

## 13. Copy / Tone Requirements

- Brand/Operator product: **Brand AI Visibility** / **Operator AI Visibility**
- Owner product: **AI Recommendation Intelligence**
- Prefer Presence, Competitive Position, Questions Won/Missing, Associated Sources
- Use “associated with” / “repeatedly cited alongside” — not unsupported causation
- Avoid: Dealality recommends, best brand/operator, guaranteed fit, composite GEO score, client-facing confidence scores

## 14. Edge Cases

Provider timeout/rate limit; missing citations; unresolved aliases; parent vs brand collision; partial runs; empty peer set; ties in Questions Won / Competitive Position (must not silently convert to sole wins).

## 15. QA Checklist

- Constitution / naming / evidence separation
- Deterministic metric fixtures
- Entity matching (parent ≠ brand; JW Marriott ≠ Marriott Hotels)
- Provider fixtures (no paid calls in unit tests)
- Zero Airtable writes in Phase 1 foundation tests
- Brand Explorer / Operator Explorer protected baselines untouched
- Live provider calls only with explicit `AI_VISIBILITY_LIVE_TEST=true`

## 16. Definition of Done

### Foundation Phase 1 (this increment)

- [x] Feature brief + durable build decision for hybrid persistence
- [ ] Provider abstraction + OpenAI adapter (citations only when provider-supplied)
- [ ] Storage abstraction + reprocessable raw responses
- [ ] Deterministic mention/citation/entity/metrics/evidence pipeline
- [ ] Fixtures + unit tests
- [ ] Controlled live-test script (flag-gated)
- [ ] Airtable ensure-script **dry-run proposal only** (no `--apply`)
- [ ] No UI, scheduler, Gemini/Perplexity, assistive diagnosis, deploy

### Later phases

- Operator AI Visibility UI
- Owner AI Recommendation Intelligence
- Opportunity engine + Opportunity Queue population
- Multi-provider monitoring (Gemini / Perplexity) after OpenAI path is trusted
- Production durable storage for runs/snapshots

### Phase 3A Brand UI (complete)

- [x] Authorized Brand read API (`/api/ai-visibility/brand/*`)
- [x] Portfolio + Brand Detail against functional mockup hierarchy
- [x] Dealality visual tokens (not mockup navy/gold clone)
- [x] Honest Not Monitored / Zero / Partial / Future-ready states
- [x] No Operator UI / Owner UI / scheduler / Opportunity writes
- [x] Hotel Decision Visibility intelligence (Phase 3A.3 tab; Phase 3A.4 merged into Exec + Detail)
- [x] Two-tab consolidation + Recommendation Rate / Top-3 from stored evidence (Phase 3A.4)
- [x] **Provider is a first-class analytical dimension** — data-driven filter options from completed monitoring; no All AI / blended presence / weights until a governed cross-provider methodology is approved

## Phase 1 Non-Goals

Client dashboards; recurring scheduler; production Airtable tables; Gemini/Perplexity; assistive AI diagnosis; content generation/publishing; website editing; billing; composite GEO score; production deploy.
