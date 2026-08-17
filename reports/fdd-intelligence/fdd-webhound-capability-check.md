# Webhound Capability Check (FDD PoC)

**WEBHOUND_AVAILABLE = yes**

## Integration

- MCP server: `user-webhound` (authenticated, ready)
- Research harness: Hound (DeepSeek V4 Pro + GPT-5.4 orchestration; budget-controlled)
- Account (no secrets): ~$21.13 available credits at PoC start
- Defaults: $5 report budget; `use_free_run_when_available=true` (free run not available this session)

## PoC session

- Dataset session: `de3d5b36-7efa-4c9f-868a-827ac5d6178e`
- Title: Dealality PoC — Five-Brand Public FDD Discovery
- Budget: $8
- URL: https://webhound.ai/session/de3d5b36-7efa-4c9f-868a-827ac5d6178e

## Capabilities relevant to FDD discovery

| Capability | Supported? | Notes |
|---|---|---|
| Web search | Yes | `search` operations billed |
| Page visit / reading | Yes | `page_visit` operations |
| PDF discovery (URL finding) | Yes (research task) | Can identify PDF/landing URLs in structured dataset rows |
| Direct PDF reading inside Webhound | Likely via page/file tools in harness | Prefer Dealality local download for retention |
| Source URL return | Yes | Schema fields `direct_pdf_url`, `source_page_url` |
| Structured output | Yes | Datasets with native attribute schema |
| Citations / provenance | Yes | Evidence pack / traces / sources on completion |
| Confidence / evidence | Partial | We requested `discovery_confidence` + `brand_match_evidence` |
| Downloadable source identification | Yes (goal of PoC) | **Key question:** can it return ORIGINAL/CREDIBLE public FDD PDF URLs for Dealality to download? |

## Architecture role (intended)

Webhound = **public discovery + source verification sidecar**  
Dealality = **download, SHA256, retain, parse, own structured intelligence**

Webhound must **not** become Dealality’s database.

## Cost model observed

- Rule of thumb exposed by product: ~$1 ≈ 15 minutes research
- Actual costs captured from `webhound_watch` / account (do not invent)
