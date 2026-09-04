# Hotel Decision Visibility — Deterministic Definitions (v1)

> Phase 3A.3. No composite Hotel Decision Visibility / GEO score.
> Review rules version: `hotel_decision_visibility_review_rules_v1`
> Competitive gap pp threshold reuses `OPPORTUNITY_THRESHOLDS_V1.competitorDominance.presenceRateGapPp` (15).
>
> **Phase 3A.4:** Visible third tab and public `/hotel-decision-visibility` route retired.
> Definitions remain binding for the **internal** HDV service consumed by Executive Summary + Detailed View.

---

## DECISION_VISIBILITY_COVERAGE

**Portfolio scope**

```
numerator   = # successful questions in cohort where ≥1 entitled brand is present
denominator = # successful questions in cohort
value       = numerator / denominator
```

**Brand scope**

Same cohort; value = that brand’s AI Presence Rate (`ai_visibility_metrics_v1`).

**Cohort** = selected geography + provider + optional Intent Territory filter.

**Availability:** `not_monitored` when denominator is 0.

---

## OWNER_INTENT_COVERAGE

Per governed Intent Territory (labels from prompt validation only — never invent):

```
numerator   = # successful questions in that intent where ≥1 entitled brand is present
denominator = # successful questions in that intent (selected geo/provider)
value       = numerator / denominator
```

Only intents with ≥1 successful observation appear.

Optional delta: vs prior **comparable monitoring batch** (same geo + intent), never “vs prior 30 days”.

---

## TOP_DECISION_TERRITORY

Intent with highest Owner Intent Coverage (portfolio) or brand presence-within-intent (brand scope).

**Tie-break (stable):**

1. Questions Won (within intent)
2. Recommendation Share (within intent)
3. Alphabetical Intent Territory name

---

## Competitive Position in Owner Decisions

- **Portfolio:** Best Competitive Position = lowest (best) Competitive Position rank among entitled brands in the selected geography. Display brand name + `#N of peerSetSize`. **No** aggregate portfolio rank.
- **Brand:** That brand’s governed Competitive Position (by AI Presence Rate).

---

## REGIONAL_LEADER

Per geography row (Global / CALA / Europe / North America):

- Leading entitled brand by AI Presence in that geography’s latest completed batch
- AI Presence display
- Competitive Position when observed
- Coverage state: `observed` | `not_monitored` | `unavailable`

---

## REVIEW_ITEM_RULES_VERSION

`hotel_decision_visibility_review_rules_v1`

| Type | Rule |
|------|------|
| Competitive Gap | Subject presence trails regional leader by ≥ **15 pp** (reuse opportunity threshold) |
| Regional Difference | Same entitled brand, ≥ **15 pp** presence between two monitored regions |
| Questions Missing | Brand missing on ≥ **50%** of successful questions in selected cohort |
| Monitoring Gap | Entitled brand has no completed batch in selected geography |

**Hard rule:** no supporting `evidenceId` → item is not emitted.

**Provider scope:** every review item includes `provider` / `providerLabel` metadata. Copy refers to that provider’s monitoring only (e.g. “OpenAI monitoring”). Cross-provider / “across AI platforms” claims are not allowed until multiple providers are monitored and a governed methodology exists.

Not Airtable Opportunities. No impact/confidence scores.

---

## Provider dimension

Provider is a first-class analytical filter for Hotel Decision Visibility (and all Brand AI Visibility reads).

- Canonical ids: `openai`, `gemini`, `perplexity` (product labels: ChatGPT / Gemini / Perplexity).
- Available options = completed monitoring datasets only (`listAvailableAiVisibilityProviders`).
- No `All AI`, no blended presence, no provider weights.
- AI vs Dealality: Provider scopes **AI Pattern** only; Dealality Context is provider-independent Brand Basics facts.
- Trends / sources / evidence / review items are provider-pure.

---

## AI vs Dealality Decision Context

- **AI Pattern:** Top mentioned / recommended brands from the evidence response (comparative-safe names), scoped to the selected Provider.
- **Dealality Context:** Thin Brand Basics facts only — `Hotel Chain Scale`, `Brand Model`, `Parent Company`. Missing fields omitted. If none → `Dealality context not yet available`.
- Never invent conversion velocity, fit advice, or AI-authored Dealality analysis.
