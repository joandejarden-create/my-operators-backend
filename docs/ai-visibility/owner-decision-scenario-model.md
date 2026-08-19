# Owner Decision Scenario Model (P0A)

> **Status:** P0A foundation — governance sidecar only. No measurement changes. No client UI.

## Concepts

| Concept | What it is | What it is not |
|---------|------------|----------------|
| **Scenario** | Governed owner-decision pattern (`scenarioId`) | Prompt ID, Prompt Family, or Semantic Pair ID |
| **Prompt** | Execution-level row sent to providers (`promptId`) | The business scenario itself |
| **Prompt Family** | Wording variant group across geographies | Scenario ID (families are variant groups *within* a scenario) |
| **Variant Group** | One or more prompt families asking the same underlying decision | A language pair |
| **Semantic Pair** | EN/ES equivalent prompts (`semanticPairId`) | A separate scenario — language stays inside one scenario |
| **Version** | Material prompt wording version | Scenario version (tracked separately as `scenarioVersion`) |

## Scenario ID vs Prompt ID

One scenario may include:

- Multiple **prompt families** (wording variants)
- Multiple **geographies** (on prompt rows)
- Multiple **languages** (EN/ES via semantic pairs)
- Multiple **versions** (prompt `Version` field)

Example:

- `scenario_soft_brand_collection_affiliation_v1` → variant groups → `p_cala_collection_affiliation_v1`, `p_cala_collection_affiliation_es_v1`, …

## Owner Priority

Controlled taxonomy describing **owner value focus** — not Intent Territory, not a score:

- Flexibility / Control
- Economics
- Distribution
- Loyalty
- Conversion Suitability
- Design Individuality
- Development Support
- Market Fit
- Positioning
- Branded Residences Capability

**Distribution + Loyalty:** Loyalty is not a separate scenario axis in P0A; distribution/loyalty scenarios use Owner Priority `Distribution` until dedicated prompts exist.

## Commercial Priority

Governed Dealality configuration — **not AI-generated, not a score**:

`CRITICAL` · `HIGH` · `STANDARD` · `INVESTIGATION`

Used later for gap classification (P0C). Stored in `fixtures/ai-visibility/scenario-registry-v1.json`.

## Monitoring Panel

| Panel | Meaning |
|-------|---------|
| **CORE** | Repeat governed monitoring panel |
| **INVESTIGATION** | Deeper variants / follow-up (manual or scheduled ad-hoc) |
| **TRIGGERED** | Manual only — never scheduler default |

## Registry location

- **Fixture:** `fixtures/ai-visibility/scenario-registry-v1.json`
- **Loader:** `lib/ai-visibility/scenario-registry.js`
- **Cohort enrichment:** `lib/ai-visibility/scenario-cohort.js` (`enrichPromptCohortWithScenarioMetadata`)

## Prompt ↔ Scenario link

Sidecar mapping via **variantGroups.promptFamilies** (least invasive). Optional explicit **promptMappings** overrides by `promptId`.

Unmapped prompts: `scenarioStatus = UNMAPPED` — execution continues unchanged.

Origin (`OBSERVED` / `DERIVED` / `SCENARIO`) is a separate dimension. See [prompt-provenance-observed-demand.md](./prompt-provenance-observed-demand.md). Scenario mapping does not imply observed search demand.


## Variant rules

Within a variant group, prompts must share comparable dimensions (`intentTerritory`, `entityScope`, `developmentType`, `chainScale` when set). Geography, language, and `Version` may differ. Non-neutral wording fails validation (`prompt-validation.js` bias patterns).

## Tests

```bash
npm run test:ai-visibility-scenario-foundation
```
