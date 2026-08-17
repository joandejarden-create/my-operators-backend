# AI Intelligence — Signal / Flag Architecture (Adopted)

**Status:** ADOPTED as production client contract  
**Module:** `lib/ai-visibility/signal-architecture/`  
**Version:** `ai_intelligence_signal_architecture_v1`

## Production model

Independent signals (may coexist). **No composite score. No forced mutually exclusive role.**

| Signal ID | Key |
|-----------|-----|
| `AI_SIGNAL_PRESENCE` | PRESENCE |
| `AI_SIGNAL_RECOMMENDED` | RECOMMENDED |
| `AI_SIGNAL_FIRST_RECOMMENDATION` | FIRST_RECOMMENDATION |
| `AI_SIGNAL_NEGATIVE_OR_QUALIFIED` | NEGATIVE_OR_QUALIFIED |
| `AI_SIGNAL_COMPARATOR` | COMPARATOR |

Each signal payload includes: `value`, `evidenceRefs`, `validationStatus`, `classifierVersion`, `sourceResponseId`, `provider`, `language`, `geography`.

## Internal 10-class taxonomy

`recommendationStatus` (10 roles) remains for **validation / audit / research / debugging** only.

- **Production contract:** NO  
- **Preserved as:** `INTERNAL_RESEARCH_VALIDATION`  
- Historical multiclass metrics are **not deleted**; they no longer control client release.

## Production gates (per signal, no composite)

- `PRESENCE_GATE`
- `RECOMMENDED_GATE`
- `FIRST_REC_GATE`
- `NEGATIVE_GATE`
- `COMPARATOR_GATE`

## Readiness states

`VALIDATED` | `PROVISIONAL` | `NOT_READY` | `NOT_GOVERNED`

One failed signal must **not** block a validated signal.

## Publication

Client visibility is per signal. Unavailable metrics use:

> Validated monitoring data is not currently available.

Never replace unavailable with `0`.

## Metric contracts (unchanged)

- **AI Presence Rate** — PRESENCE  
- **Recommendation Share** — first + ranked + explicit only (**not** `associated_option`)  
- **First Recommendation** — FIRST_RECOMMENDATION flag  
- **Questions Won** — strict first-recommendation leader  
- **Questions Missing** — entity absent (Presence)  
- **Competitive Position** — AI Presence Rate rank  

## Current DEV readiness (v4.1, n=290)

| Signal | Status |
|--------|--------|
| PRESENCE | VALIDATED |
| RECOMMENDED | NOT_READY (recall workstream A) |
| FIRST_RECOMMENDATION | NOT_READY (recall workstream B) |
| NEGATIVE / COMPARATOR | NOT_READY / sparse |

Holdout remains sealed until separately authorized. Presence is prepared for holdout; Recommended/First are not.

## Tests

```bash
npm run test:ai-intelligence-signal-architecture-adoption
npm run ai-intelligence:signal-architecture-adoption
```
