# Query-Origin Regionalization Experiment v1

**Status:** RESEARCH_COMPLETE_NO_PRODUCTIZATION  
**Experiment ID:** `QUERY_ORIGIN_REGIONALIZATION_EXPERIMENT_V1`  
**Closure artifact:** `data/ai-visibility/validation/query-origin-research-closure.json`  
**Plan artifact:** `data/ai-visibility/validation/query-origin-regionalization-experiment-v1-plan.json`

## Outcome

Stage 1 OpenAI (Mexico asset; NY / Miami / CDMX / Madrid origins; 192/192 calls; gpt-4.1) → **NO_MEANINGFUL_REPEATABLE_DIFFERENCE**.

The experiment **successfully answered** the research question. It is **not** marked failed.

## Product decision

Do **not** add to production:

- query-origin selector
- query-origin metric
- query-origin monitoring dimension
- query-origin score

Do **not** expand the routine monitoring matrix by query origin.  
**STAGE_2_CLAUDE:** NOT_PLANNED.

## Preserve capability (research/storage only)

Retain `assetGeography`, `queryOriginGeography`, `providerLocationContext`, and experiment metadata so future experiments remain possible without making query origin a production dimension.

## Reopen conditions (no recurring schedule)

Reopen research only if:

- a provider materially changes localization behavior
- customer evidence suggests geographic divergence
- another asset geography shows a stronger hypothesis
- source/search localization becomes strategically important
- future monitoring detects unexplained regional response differences

## Geography model (do not conflate)

| Dimension | Meaning |
|-----------|---------|
| **ASSET_GEOGRAPHY** | Where the hotel/project is (constant = Mexico for v1) |
| **QUERY_ORIGIN_GEOGRAPHY** | Hypothetical asker market (US_NORTHEAST, US_SOUTHEAST, MEXICO, SPAIN) |
| **PROVIDER_LOCATION_CONTEXT** | Explicit API location payload (city/region/country/timezone) |

## Metrics

Presence / brand-set / source-domain differences only. **No composite Regionalization Score.**

## Future (do not build)

Client-facing **Query-Origin Intelligence** remains locked not-built unless reopen conditions are met and a new experiment mandate is issued.
