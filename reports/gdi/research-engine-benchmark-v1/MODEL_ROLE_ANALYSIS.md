# Model Role Analysis

## Models available (configured paths in repo)
- **GDI default extract:** gpt-4o-mini
- **Optional stronger:** Anthropic Claude / GPT-4o / Gemini / Perplexity (mostly AI Visibility & ADP — not wired as GDI native research planner)

## Live alternate-model lanes
**Not executed** (PART 0 credit preservation + no paid spend). Therefore:

| Question | Answer |
|---|---|
| Model with best HQ candidate yield | **UNKNOWN (not live-tested)** — baseline proxy = gpt-4o-mini |
| Best cost-adjusted yield | gpt-4o-mini (only measured production extract model) |
| Best local-language research | **TOOL/ORCHESTRATION effect dominates** in Webhound vs GDI delta; model effect unseparated |
| Best document extraction | UNKNOWN live; Webhound page_visit>search suggests tool loop > model swap |
| Best entity/controller reasoning | HYPOTHESIS: stronger reasoning helps; **evidence says recursion/pivots missing in GDI** more than mini failing extraction |

## Separation
- **MODEL_EFFECT:** unproven in this pack (no equal-tool A/B).
- **TOOL_EFFECT / ORCHESTRATION_EFFECT:** PRIMARY — Webhound recursive navigation vs GDI query-and-classify.

## If routing later (do not auto-route expensive)
| Role | Suggested | Why |
|---|---|---|
| RESEARCH_PLANNER | stronger reasoning (selective) | missing-pillar next-evidence policy |
| QUERY_GENERATOR | gpt-4o-mini or planner output | cheap reformulations |
| BROWSER_NAVIGATOR | Webhound/browser **only on blockers** | expensive |
| DOCUMENT_EXTRACTOR | gpt-4o-mini | adequate for programmes/PDFs |
| ENTITY_RESOLVER | mid/strong | intermediary vs end-client distinction |
| PACKET_SYNTHESIZER | gpt-4o-mini + **canonical gates** | gates stay code, not LLM |
