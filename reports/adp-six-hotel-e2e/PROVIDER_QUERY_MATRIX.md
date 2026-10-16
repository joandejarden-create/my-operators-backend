# Provider / query matrix (preparation)

**Providers (canonical):** openai · gemini · perplexity · claude  
**Config source:** `lib/ai-demand-positioning/execution/multi-provider-runner.js` `PROVIDER_CONFIGS`  
**Scenario builder:** `buildScenarioUniverse(profile)` — standard pack + property-specific

## Sampling policy

- Full activated hotel assessment = planned scenarios × selected providers (default all four with keys present).  
- Dry-run validates structure only — **does not** count as provider coverage.  
- Fixtures / replay / seed periods ≠ live research history.  
- Failed/timeout/refusal ≠ “hotel not recommended.”  
- Missing citation ≠ “no source exists.”  
- Resume recoverable job/period IDs — do not duplicate chargeable submits after timeout when recoverable state exists.  
- One answer ≠ stable market-wide behavior; use established repeated-observation policy where required by certification.

## Metric separations (must keep distinct in scoring)

| Lens | Meaning |
|------|---------|
| Unprompted discovery / recommendation | Hotel appears without being named in the query |
| Branded / prompted mention | Query includes hotel name or strong alias — **not** unprompted success |
| Factual accuracy | Correct rooms, location, amenities vs verified profile |
| Citation / source visibility | Sources shown when provider supplies them |

Avoid leading queries that force a recommendation.

## Stable comparable core

Use standard traveler intents from `TRAVELER_INTENTS`:

`business` · `leisure` · `couples` · `family` · `group_meeting` · `wellness` · `adventure` · `celebration`

Plus justified **property_specific** scenarios from profile attributes (airport, marina, theater district, etc.). Do **not** force every intent onto every hotel when profile applicability is weak (e.g. do not invent ski-adventure for Tampa Airport).

## Planned call math (current fixtures)

| Hotel | Planned scenarios (builder) | ×4 providers | Est. USD (`estimateCost`) |
|-------|----------------------------:|-------------:|--------------------------:|
| Renaissance | 80 (std 50 / spec 15 + other) | 320 | **~10.40** |
| Waterstone | 93 | 372 | **~12.09** |
| WASFP/WASPY/CLTCC/TPAAP | TBD after profile (~60–80 typical) | TBD | **~7.80–10.40** provisional |

Published Ren/Waterstone periods used **65 / 78** scenario denominators in payload — differ from current builder totals. On activation, freeze the **actual** scenario list into the run manifest and score only that denominator.

## Competitor selection (process)

Distinguish and persist separately:

1. **Declared** hotel competitive set (`declaredCompSet` on profile).  
2. **AI-observed** alternatives from research responses.  
3. **Analytical benchmark / core peers** from property core governance (when certified).

Never present an inferred AI set as the hotel’s official STR set. Compare alternatives in the **same persona/query context**. Boutique waterfront (Waterstone) vs large convention hotel only with explicit use-case framing.
