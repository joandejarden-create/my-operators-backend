# Research Provider Inventory

Audit date: 2026-10-08. **No paid providers added.**

| provider | model | tool access | browser? | search? | PDF? | local-lang? | marginal cost | configured? | usable now? | historical traces? |
|---|---|---|---|---|---|---|---|---|---|---|
| OpenAI (GDI native extract) | `gpt-4o-mini` (env `GDI_NATIVE_EXTRACT_MODEL` / `OPENAI_MODEL`) | chat completions JSON extract | NO | via SerpAPI only | via HTTP fetch text | YES if prompted | ~$0.15–0.60 / 1M tok | YES if `OPENAI_API_KEY` | YES when key present | YES (native discovery logs) |
| OpenAI stronger (repo optional) | `gpt-4o` / other if env set | same | NO | SerpAPI | fetch | YES | higher | PARTIAL (not GDI default) | if key+model set | limited |
| Anthropic | `claude-sonnet-4-6` (AI Visibility / ADP paths) | chat | NO | NO native | partial | YES | unknown per call | PARTIAL (`ANTHROPIC_API_KEY`) | ADP/AI Visibility; **not** GDI native path | ADP traces |
| Google Gemini | `gemini-2.5-flash` (AI Visibility) | chat | NO | NO native GDI | partial | YES | unknown | PARTIAL | AI Visibility only | AI Visibility |
| Perplexity | `sonar` (AI Visibility) | provider-native web | NO browser | YES provider | unknown | YES | unknown | PARTIAL | AI Visibility only | AI Visibility |
| SerpAPI | n/a | Google organic SERP | NO | YES | NO | YES (gl/hl) | ~$0.01/search (repo ledger) | YES if `SERPAPI_KEY` | YES — **GDI production search** | YES |
| DataForSEO | n/a | SEO discovery candidates | NO | YES | NO | YES | paid | PARTIAL (.env.example) | discovery-only if creds | limited |
| Direct HTTP fetch | n/a | `fetchResearchPage` | NO | NO | YES (HTML/PDF text path) | YES | ~$0 bandwidth | YES | YES | YES |
| Webhound (Hound) | Hound 1.0 research harness | search + page_visit + LLM | YES (page visit agent) | YES | YES (followed when linked) | YES | budgeted $; Mallorca $5 | YES (MCP) | **credit-limited ($1.03)** | YES (48 sessions) |
| Parallel / fallback gate | n/a | gated | NO | NO | NO | n/a | n/a | code present | OFF for prod GDI blind | n/a |
| Surfe | n/a | contacts | NO | NO | NO | n/a | paid | OFF in success forensics | NO for this pack | n/a |
| Apify | actors | scrape | YES | optional | optional | YES | paid | **DISALLOWED for this GDI work** | NO | n/a |
| LangSmith | traces | observability | n/a | n/a | n/a | n/a | ~$0 read | if configured | read-only | UNKNOWN in this pack |
| Internal research agents (HI native LangChain) | gpt-4o-mini default | SerpAPI + fetch tools | NO | YES | fetch | YES | Serp+$LLM | YES for Hotel Intelligence | separate from GDI opp engine | HI runs |

## Models actually tested in this benchmark
| Lane | Mode | Notes |
|---|---|---|
| BASELINE_GDI | `gpt-4o-mini` + SerpAPI | Reconstructed from production/yield forensic — **no re-spend** |
| RECURSIVE_RESEARCH_V1 | same tools, different orchestration | Offline simulation from Webhound source graph + intermediary graph |
| MODEL_ALT_* | **NOT LIVE-RUN** | Credit-preservation: no alternate-model API burn; strengths inferred from repo roles only |

## Fairness label
Any future live model comparison without equal SerpAPI+fetch budget must be marked `TOOL_ACCESS_DIFFERENCE`.
