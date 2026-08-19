# Prompt origin and observed demand provenance

> **Status:** V1 seed **validated and attached** (`OBSERVED_DEMAND_SEED_V1_VALIDATED`). 9 observed + 2 derived prompts registered. Monitoring eligible **off**.  
> **Measurement:** Unchanged (Presence / QM / All Providers / citations / P0C / Truth) on the current 122 monitored prompts.  
> **Scheduler:** OFF · **Provider calls:** 0 · **Census:** none · **Recommendation research:** none · **DataForSEO new calls:** 0

## Why this exists

Dealality can distinguish:

| Origin | Meaning |
|--------|---------|
| **OBSERVED** | Question or theme grounded in an external demand signal |
| **DERIVED** | Controlled variant of an observed parent |
| **SCENARIO** | Expert-created owner/developer decision situation |
| **LEGACY_UNCLASSIFIED** | Internal only — unknown provenance; monitoring continues |

Client UI never treats `LEGACY_UNCLASSIFIED` as a positive category.

Observed demand and Scenario intelligence are complementary. Neither replaces the other. Origin, Owner Intent, and Scenario are orthogonal:

```text
Prompt → Origin → Owner Intent → Scenario → Geography → Provider runs → Observations
```

A prompt may be `promptOrigin = OBSERVED` **and** still have a `scenarioId`. Existing SCENARIO prompt IDs were not rewritten.

## Copy (client-safe)

**Current:** We use both observed demand and expert scenario intelligence. Observed demand reflects externally measured query themes. Scenario intelligence tests commercially important owner and developer decisions that may not appear as literal high-volume searches.

Also allowed: “Observed demand and scenario intelligence are complementary.”

Forbidden: “We monitor what hotel owners are actually searching.” / “These are real owner searches.” / “owner search volume.” Licensed volume is query demand, not verified owner identity.

## Demand tier

Qualitative only: `HIGH` · `MEDIUM` · `LOW` · `UNKNOWN`.

Relative within one source country and language cohort. Do not compare US English HIGH to Mexico Spanish HIGH as absolute volume. Source geography is independent of Brand AI monitoring geography and is not labeled CALA unless the evidence country is a CALA country.

## File store

| File | Role |
|------|------|
| `fixtures/ai-visibility/prompt-provenance-v1.json` | Overlay by new observed/derived `promptId` |
| `fixtures/ai-visibility/observed-demand-prompts-v1.json` | 9 OBSERVED + 2 DERIVED rows (`monitoringEligible=false`) |
| `fixtures/ai-visibility/demand-signals-v1.json` | Normalized evidence objects (not raw dumps) |
| `fixtures/ai-visibility/observed-demand-seed-v1.json` | V1 seed status |
| `fixtures/ai-visibility/scenario-registry-v1.json` | Unchanged scenario sidecar |

Airtable origin fields are **proposed, not applied** (`schemaApply: false`). Operational set is eight short fields only. Large PAA captures stay out of Airtable.

## UI

- Executive **Prompt Intelligence** is methodology context (Observed demand · Expert scenarios). Not a performance KPI. Do not hero counts.
- Detail / watchlist origin badges: Observed, Derived, Scenario. Evidence details on hover. Legacy prompts show no badge.

## Next

`REPEATED_TESTING_AND_STABILITY` — still no monitoring until explicitly approved. Source acquisition is closed for V1.

## Tests

```bash
npm run test:ai-visibility-prompt-provenance
npm run test:ai-visibility-observed-demand-source
```
