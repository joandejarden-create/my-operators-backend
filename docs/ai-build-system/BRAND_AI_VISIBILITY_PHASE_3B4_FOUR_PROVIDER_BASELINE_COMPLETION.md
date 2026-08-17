# Brand AI Visibility — Phase 3B.4 Four-Provider Baseline Completion

**Status:** Implementation + live execution via `npm run ai-visibility:phase3b4-execute`

## Scope

| Provider | Action | Notes |
|----------|--------|-------|
| OpenAI | **0 calls** | FULL_BASELINE untouched |
| Perplexity | **0 calls** | FULL_BASELINE untouched |
| Gemini | Probe → 12 validation → 84 baseline if GO | Prefer `gemini-3.6-flash`; fallback `gemini-3-flash-preview` |
| Claude | Tool audit → billing probe → 12 validation → 84 baseline if GO | `claude-sonnet-4-6` · `web_search_20260209` · `allowed_callers:["direct"]` · 300s timeout |

## Evidence Footprint + Cited Source Intelligence

Deterministic, non-composite. Integrated into Detailed View → Evidence Basis.

- Brand Mentions / Recommendation Mentions / Evidence-Bearing Responses / Unique Cited Sources
- Top Cited Sources sorted by distinct monitored responses
- No Visibility Score, authority score, All AI, or longitudinal source movement

## Commands

```bash
npm run test:ai-visibility-phase3b4
node scripts/ai-visibility-phase3b4-live-env.mjs --verify-only
npm run ai-visibility:phase3b4-execute
```
