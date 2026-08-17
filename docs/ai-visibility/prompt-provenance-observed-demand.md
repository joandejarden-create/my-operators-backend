# Prompt origin and observed demand provenance

> **Status:** Contract + adapters live. Observed seed **partial** (9 distinct file-store themes; not attached to live prompts). Prompt Mix hidden until ≥10.  
> **Measurement:** Unchanged (Presence / QM / All Providers / citations / P0C / Truth).  
> **Scheduler:** OFF · **Provider calls:** 0 · **Census:** none · **Recommendation research:** none

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

A prompt may be `promptOrigin = OBSERVED` **and** still have a `scenarioId`.

## Copy (client-safe)

**Current (scenario-led library):** Our prompt architecture distinguishes observed demand from expert scenario intelligence. The current monitored library is scenario-led while observed demand sources are being validated.

**After observed activation (≥10 validated observed themes):** We use both observed demand and expert scenario intelligence.

Allowed now: “Observed-demand prompts are grounded in external query or demand signals.” / “Scenario prompts are expert-designed owner/developer decision situations.”

Forbidden until observed is live in the monitored library: “We monitor what hotel owners are actually searching.” / “These are real owner searches.” File-store volume is licensed search demand, not “owner searches.”

## Demand tier

Qualitative only: `HIGH` · `MEDIUM` · `LOW` · `UNKNOWN`.

`HIGH` / `MEDIUM` / `LOW` require a recorded methodology plus supporting evidence. Otherwise `UNKNOWN`. That is acceptable.

Source confidence is descriptive (`DIRECT_MEASURED`, `STRONG_OBSERVED`, `SUPPORTED`, `WEAK`, `UNKNOWN`) — never a numeric probability.

Prompt origin source ≠ AI response citation source.

## File store

| File | Role |
|------|------|
| `fixtures/ai-visibility/prompt-provenance-v1.json` | Optional overlay by `promptId` |
| `fixtures/ai-visibility/demand-signals-v1.json` | Normalized evidence objects (not raw dumps) |
| `fixtures/ai-visibility/observed-demand-seed-v1.json` | Seed status + candidate themes |
| `fixtures/ai-visibility/scenario-registry-v1.json` | Unchanged scenario sidecar |

Airtable may later store governed origin metadata (proposal only; not applied). Large PAA captures, query datasets, and API payloads stay out of Airtable.

## UI

- Executive **Prompt mix** stays hidden until at least 10 validated observed themes exist. Do not show `0 observed` as a performance problem.
- Detail / watchlist origin badges: Observed, Derived, Scenario. Legacy prompts show no badge.

## Next

Budget-capped DataForSEO sample + one targeted refinement stored 13 signal rows / 9 distinct themes after PAA quality filter. Live overlay classifications remain empty. Activation gate failed (9 < 10). Do not spend remaining phase budget without approval. Next, only if asked: another approved source pass, or `OBSERVED_DEMAND_ACTIVATION` after ≥10 themes. See [observed-demand-source-acquisition.md](./observed-demand-source-acquisition.md).

## Tests

```bash
npm run test:ai-visibility-prompt-provenance
```
