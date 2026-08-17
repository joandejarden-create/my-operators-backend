# Brand AI Visibility — Phase 3B.2 Controlled Multi-Provider Live Validation

> **Status:** PARTIAL PASS (Perplexity complete; Gemini credential missing; Claude billing blocked)  
> **Date:** 2026-08-14  
> **Parent validation ID:** `aiv_mult_provider_validation_20260814_0944_04ba6b`

## Credential Preflight

| Provider | Status | Env var used |
|----------|--------|--------------|
| Gemini | **MISSING** | — |
| Perplexity | **PRESENT** | `PERPLEXITY_API_KEY` |
| Claude | **MAPPED** | `ANTHROPIC_API_KEY` (from `CLAUDE_API_KEY`) |

No secret values logged. `GOOGLE_MAPS_API_KEY` is **not** mapped to Gemini.

## Live Execution Summary

| Provider | Planned | Succeeded | Failed | Activation gate | Go/No-Go |
|----------|---------|-----------|--------|-----------------|----------|
| Gemini | 12 | 0 | 0 | — | **BLOCKED** (missing credential) |
| Perplexity | 12 | 12 | 0 | **PASS** | **GO** |
| Claude | 12 | 0 | 3 | **FAIL** | **BLOCKED** (Anthropic credit balance) |

**LIVE_OPENAI_CALLS:** 0  
**TOTAL_LIVE_LOGICAL_CALLS:** 12 (Perplexity only)

## Wave IDs

- **Parent:** `aiv_mult_provider_validation_20260814_0944_04ba6b`
- **Perplexity:** `aiv_validation_perplexity_20260814_0944_87c483`
- **Claude:** `aiv_validation_claude_20260814_0945_57fa71` (stopped after gate)

## Storage (isolated from OpenAI Wave-1)

- Perplexity: `data/ai-visibility/runtime/provider-validation/perplexity`
- Claude: `data/ai-visibility/runtime/provider-validation/claude`
- OpenAI baseline untouched: `data/ai-visibility/runtime/wave1-showcase`

All validation runs tagged `monitoringRunPurpose: validation` — excluded from measured provider UI.

## Perplexity Validation Highlights

- Model: `sonar` (requested = returned)
- Cost: **$0.06729** (12 calls; provider-reported)
- Avg latency: ~5.6s (min 4.4s, max 8.1s)
- Citations: 235 normalized / 149 unique domains
- Peer mentions: 132 / recommended (role): 21
- EN: 8 / ES: 4 responses
- Entity resolver + classifier pipeline: operational

## Claude Blocker

Anthropic API returned billing error after 3 attempts (not adapter/auth misconfiguration):

> Credit balance too low to access the Anthropic API.

**Recommended action:** Add Anthropic credits, then re-run Claude validation only.

## Cost Calibration (Perplexity)

| | USD |
|---|-----|
| Validation (12 calls) | 0.067 |
| Avg per successful call | ~0.0056 |
| Projected 84-call EXPECTED | ~0.47 |
| Recommended 84-call hard cap | **$15** (prudent buffer) |

## Next Recommended Phase

**PHASE_3B3_PARTIAL_PROVIDER_EXPANSION**

1. Perplexity → full 84-call baseline (GO)
2. Add `GEMINI_API_KEY` → Gemini 12-call validation then 84 if pass
3. Add Anthropic credits → Claude 12-call validation then 84 if pass

## Tests

```bash
npm run test:ai-visibility-phase3b2
npm run ai-visibility:phase3b2-validation-execute
```

Regression: `test:ai-visibility-phase3b1`, `test:ai-visibility-phase3a11`, `test:ai-visibility-phase3a10`
