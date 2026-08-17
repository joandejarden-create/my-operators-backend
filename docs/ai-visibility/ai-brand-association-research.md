# AI Brand Association Research (P0B)

> **Status:** Research only — not client-facing. Measurement layer frozen.

## Objective

Determine whether **Observed AI Brand Associations** can be extracted reliably from existing monitoring evidence without new provider calls.

Preferred terms: **Observed Association**, **AI Association**, **Repeated Association** — never "Why AI chose this brand" unless explicit causality exists in the response text.

## Controlled taxonomy

15 attributes in `lib/ai-visibility/associations/attribute-taxonomy.js`.

**Deferred (not production-eligible P0):** `ECONOMICS`, `DEVELOPMENT_SUPPORT`

**High-risk (stricter gate):** `OWNER_FLEXIBILITY`, `OWNER_CONTROL`, `CONVERSION_SUITABILITY`, `OPERATING_MODEL`, `MARKET_FIT`, plus deferred attrs.

## Extraction approach (recommended)

**HYBRID** — deterministic candidate generation + entity binding + span requirement today; LLM adjudication reserved for ambiguous holdout only.

| Approach | Precision | Cost | Span traceability |
|----------|-----------|------|-------------------|
| Deterministic | High on explicit language | $0 | Full |
| LLM-assisted | Higher recall, lower binding safety | Non-zero | Requires span enforcement |
| Hybrid (recommended) | Best balance | Low incremental | Full |

## Evidence contract

Every publishable association requires:

- `entityId` + `attributeId` + `supportingSpan` + `explicitness=EXPLICIT`
- Entity-bound sentence (no parent-only inheritance)
- Optional `citationIds` tracked separately (`HAS_PROVIDER_CITATION`)

## Golden Set

- Path: `data/ai-visibility/associations/research/golden-set-v1.json`
- Built from existing evidence — **no provider calls**
- Human labels: high-confidence oracle subset + pending review cases
- Classifier labels never overwrite `humanLabelled: true` cases

## Production gate (future)

- Overall precision ≥ 0.90
- High-risk attribute precision ≥ 0.95
- Entity binding error rate ≤ 0.02
- Span validity ≥ 0.95

## Commands

```bash
npm run ai-visibility:association-research
npm run test:ai-visibility-association-research
npm run ai-visibility:association-holdout
npm run test:ai-visibility-association-holdout
```

## P0B.1 Holdout (v1)

- Path: `data/ai-visibility/associations/research/holdout-v1.json`
- Hard negatives: `fixtures/ai-visibility/association-hard-negatives-v1.json`
- Dev/holdout split: ~70/30 sealed hash manifest
- Review mode: `SINGLE_REVIEWER_GOLDEN`

## Storage

File store only: `data/ai-visibility/associations/research/`

No Airtable writes. No client UI changes in P0B.
