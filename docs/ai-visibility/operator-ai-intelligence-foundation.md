# Operator AI Intelligence — Foundation V1

**Status:** Foundation (measurement-first)  
**Scheduler:** OFF  
**Provider wave:** NOT_RUN  
**Brand AI:** frozen except real longitudinal collection and genuine defects

## Product question

**Who should operate the hotel?**

Operator AI Intelligence measures how hotel management companies are represented by AI when owners, developers, and investors consider an operating partner.

This is B2B owner-decision intelligence. It is not Brand AI, guest sentiment, GEO/SEO ranking, or Owner AI.

- **Route:** `/operator/ai-intelligence`
- **API:** `/api/ai-visibility/operator/foundation`
- **Users:** Operator / management-company workspace (Admin included). Owners are not entitled by default.
- **Nav:** Market Intelligence → Operator AI Intelligence (operator + admin). Brand users continue to see Brand AI Intelligence only.

## Operator universe (hard limit 9)

| Founder name | Canonical Operator Master | ID | Monitored scope |
|---|---|---|---|
| Marriott International | Marriott International (Managed) | `recGmiPhRt6hiayd9` | GLOBAL |
| IHG | IHG Hotels & Resorts (Managed) | `rec7IXYQYpKMYsrDl` | GLOBAL |
| Hilton | Hilton (Managed) | `rec3Uwxe6ovpiokuN` | GLOBAL |
| Aimbridge LATAM | Aimbridge Hospitality (LATAM) | `recGWxIJqnYHkJZFD` | LATAM |
| Hotel Equities CALA | Hotel Equities (CALA) | `recWPKu5laVZxsvpn` | CALA |
| Arbor Lodging | Arbor Lodging (CALA) | `recF5Z87OAqFgndoq` | CALA |
| GHL | GHL Hoteles (GHL Holding) | `reciI2tYQBfMoMK9G` | CALA |
| Brittain Resorts | Brittain Resorts & Hotels (BRH) | `receHCdI6CEsJqdG4` | US_SOUTHEAST |
| Remington CALA | Remington Hospitality (CALA) | `rec6UB6RpMKSs2tAo` | CALA |

Regional operators are **canonical entity + monitored scope**. Do not invent a separate company for “Aimbridge LATAM” or “Remington CALA”.

Marriott / Hilton / IHG are measured as **managed operating capability**, not franchise brand portfolios.

Unapproved expansion (Highgate, Pyramid, Davidson, HEI, Crescent, CoralTree, MCR, …) = 0. Competitor mentions may appear in evidence but are not primary monitored entities. Remington was promoted from observed-competitor path — historical raw responses are not rewritten.

## Identity / aliases

Operator-only overlay (does not mutate Brand `runtime-alias-overlay-phase2b` or Brand `findEntitySpans`). Brand’s bare-parent blocklist would otherwise drop `Marriott International` / `IHG Hotels & Resorts` as operator mentions.

- Longest-match alias resolution (operator-specific span finder)
- Short parent names (`Marriott`, `Hilton`, `IHG`) require operating-context
- `HE` and bare `Arbor` are blocked
- Aimbridge / GHL / Brittain / Remington aliases documented in `lib/ai-visibility/operator-intelligence/aliases.js`
- Bare `Remington` is not aliased (firearms/appliance collision risk); require `Remington Hospitality` or longer

## Scenarios and prompts

See [operator-decision-scenarios-v1.md](./operator-decision-scenarios-v1.md).

- 12 decision scenarios
- 30 SCENARIO-origin prompts (12 CORE / 18 EXTENDED)
- OBSERVED = 0, DERIVED = 0, DataForSEO = 0
- CALA / English V1

## Presence

`OPERATOR_SIGNAL_PRESENCE` — substantive mention only.

- No Recommendation Rate / First Recommendation / Questions Won
- Source-domain-only and footer/nav mentions do not count
- Constructed DEV + sealed holdout exist
- **Status: RESEARCH_ONLY / PARTIAL** until a live provider corpus is scored without holdout leakage

## Shared execution and cost

Grain = **prompt × provider** (not × operator).

Proposed validation matrix:

- CORE: 12 prompts × OpenAI + Gemini + Perplexity + Claude
- EXTENDED: 18 prompts × OpenAI + Perplexity
- Total calls = 84
- Hard cap = $60
- Cost does not increase when a 10th operator is added later (until founder expands the universe)

The foundation **costs** this wave and does **not** execute it. Presence validation completed on corpus `aiv_operator_presence_validation_20260818_1342_20ee11` (`OPERATOR_AI_PRESENCE_PRODUCTION_VALIDATED`). Competitive intelligence: [operator-competitive-intelligence-v1.md](./operator-competitive-intelligence-v1.md).

## Gaps, eligibility, truth, associations

- Raw gaps: `PEER_PRESENT_OPERATOR_MISSING`, `PERSISTENT_SCENARIO_GAP`
- Eligibility: ELIGIBLE / CONDITIONALLY_ELIGIBLE / OUT_OF_SCOPE / UNKNOWN from governed facts
- Example: Marriott missing from brand-agnostic third-party context = expected positioning difference
- Truth: identity, lens, scope, domain — not marketing copy; Census reads = 0
- Associations: research taxonomy only

## Storage

`data/ai-visibility/runtime/operator/` (gitignored with Brand runtime). Do not mix Brand and Operator raw responses. Do not put raw responses in Airtable.

## Blocked

Recommendation metrics · Owner AI · Brand UI work · Census dependency · DataForSEO · scheduler · per-operator provider execution

## Tests

```bash
npm run test:ai-visibility-operator-intelligence-foundation
npm run test:operator-presence-validation-v1
npm run test:operator-competitive-intelligence-v1
```
