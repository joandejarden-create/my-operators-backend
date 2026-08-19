# Customer Prompt Disclosure Policy V1

**Status:** Permanent product governance  
**Principle:** SHOW WHAT WE MEASURE · PROTECT EXACTLY HOW WE MEASURE IT

## Summary

Dealality customers must understand **what owner/developer decisions** are being measured and **what the observations mean**, without receiving reproducible access to Dealality's canonical prompt library, variants, or testing sequence.

## Customer-visible layers

| Layer | Visible | Examples |
|-------|---------|----------|
| **Owner Intent** | YES | Soft Brand Affiliation, Conversion Suitability, Branded Residences |
| **Decision Context** | YES | Plain-English business situation behind the measurement |
| **Geography / Language** | YES (where useful) | Caribbean & Latin America, English |
| **Observation results** | YES | Presence, missing providers, core peers present, certified index |
| **High-level methodology** | YES | Representative scenarios, multi-provider, repeat observations |

## Internal-only layers

| Layer | Visible | Notes |
|-------|---------|-------|
| **Exact canonical production prompt** | NO | Full question wording, geo-specific sentence construction |
| **Prompt variants / IDs** | NO (customer) | `promptId` may remain opaque for keys; no semantic prompt library |
| **Prompt-generation rules** | NO | Mutation rules, selection logic, testing recipe |
| **Benchmark engine mechanics** | NO | Full peer matrix, certification thresholds, raw scores |

## Illustrative examples

Allowed when **explicitly labeled** as illustrative (e.g. "Illustrative Owner Question") with disclaimer that governed variants are used in production. Do not auto-show on every row.

## Server-side redaction

Customer-facing API responses for Brand AI (overview, questions/watchlist, evidence, executive summary) must redact at the **server layer** — not CSS or frontend-only hiding.

Module: `lib/ai-visibility/customer-prompt-disclosure.js`

Internal/admin diagnostics routes may retain full prompt access (e.g. benchmark diagnostics).

## UI sections

- **AI vs Dealality Context:** Owner Intent · Decision Context · AI Representation · Dealality Context
- **Questions Missing Watchlist:** Owner Intent · Decision Context · Geography · Missing providers · Core peers · Priority

## Client conversation support

When asked "What exactly are you prompting?":

> Dealality shows the owner/developer decision scenarios being measured and can provide illustrative examples of the types of questions used. We do not expose the full canonical prompt library or testing sequence because those are part of Dealality's proprietary measurement methodology.

## Tests

```bash
npm run test:brand-ai-customer-prompt-moat-ui-v1
```

Requires `CUSTOMER_API_PROMPT_LEAKS = 0` against canonical prompt fixtures.

## Related docs

- `competitive-data-access-policy-v1.md`
- `benchmark-engine-v1.md`
- `lib/ai-visibility/competitive-moat/info-contracts.js`
