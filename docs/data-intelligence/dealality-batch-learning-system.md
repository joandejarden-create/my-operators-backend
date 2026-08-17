# Dealality Batch Learning System

**Status:** `dealality_batch_learning_system_ready`  
**Version:** `dealality-batch-learning-system-v1`  
**Airtable writes:** never from this system  
**Brand Explorer patches:** never automatic

## Purpose

Dealality code should do the majority of the work over time. Only hard cases go to Webhound (pattern discovery) or steward review. Every Census or Brand Explorer batch must improve the codebase via fixtures, tests, and reusable rules.

## Learning loop (A→J)

1. **Run batch** (dry-run first)
2. **Capture result** (report JSON/MD)
3. **Classify** failures / blocked records
4. **Extract** reusable patterns
5. **Promote** patterns into code rules
6. **Add fixtures / examples**
7. **Add regression tests**
8. **Re-run dry-run**
9. **Confirm improvement**
10. **Send only unresolved hard cases** to Webhound (10–25) or steward

## Classification vocabulary

| Type | Meaning |
| --- | --- |
| `learned_code_rule` | Becomes executable code / extractor / gate |
| `learned_validation_rule` | Becomes validation / lint / confidence gate |
| `learned_source_pattern` | Official page / payload / URL pattern |
| `learned_block_reason` | Durable block taxonomy for routing |
| `steward_review_case` | Human judgment required |
| `Webhound_candidate` | Hard pattern-discovery sample only |
| `do_not_learn_noise` | Explicitly rejected / boundary noise |

## Census loop

Lanes: `coordinate_extraction`, `address_extraction`, `official_page_discovery`, `geocoding_fallback`, `amenities_extraction`, `description_extraction`, `property_type_classification`, `asset_context_classification`, `radar_public_eligibility`, `rooms_keys_extraction`, `property_name_cleanup`.

Rules:

- Code-based resolver runs first
- Official source extraction before geocoding
- Confirmed official address may support approved-provider geocode
- Webhound: hard cases only; never writes Airtable; learnings → fixtures/tests before trust
- Sample size normally 10–25

Each Census batch must produce: outcome summary, successful/failed source patterns, blocked reasons, proposed extractor + validation rules, fixtures, tests, steward cases, Webhound candidates.

## Brand Explorer loop

Lanes: `forbidden_language_detection`, `owner_facing_text_quality`, `webflow_product_render_readiness`, `census_crosscheck`, `property_example_validation`, `count_footprint_softening`, `recent_momentum_evidence_review`.

Rules:

- Do not patch Brand Explorer automatically
- Do not update Census from Brand Explorer
- Do not create Recent Momentum from property existence
- Do not use held or Brand-Unconfirmed Census as public proof
- Do not update global counts from Mexico Census alone
- Approved text cleanups → reusable lint rules where possible

Patch categories: `safe_text_cleanup`, `property_example_update`, `Webflow_render_fix`; Recent Momentum = separate approval only.

## Commands

```bash
npm run dealality:batch-learning-ledger
npm run dealality:batch-learning-audit
npm run test:dealality-batch-learning-system
```

## Checklist before claiming “system learned”

1. Was a reusable pattern identified?
2. Was code changed, or was the pattern documented as non-reusable?
3. Was a fixture added?
4. Was a regression test added?
5. Did the next dry-run improve?
6. Did false positives decrease?
7. Did blocked records decrease?
8. Did Webhound dependency decrease?
9. Did the change avoid production writes without approval?
10. Did Brand Explorer / Census boundaries remain intact?

## Related reports

- `reports/data-intelligence/dealality-batch-learning-ledger.json`
- `docs/data-intelligence/dealality-batch-learning-ledger.md`
- `reports/data-intelligence/dealality-batch-learning-system.json`
