# RESEARCH_PROMPT_VERSIONING

> Packet 2.8A · Version identity for templates, compiler, preamble, screens, output contract

## Version surfaces

| Surface | Constant / field | Current (Packet 2.8A) |
|---------|------------------|------------------------|
| Template registry | `TEMPLATE_REGISTRY_VERSION` | `hotel-intelligence-research-templates-v2` |
| Per-template | `template.version` | e.g. Full HI `1.0.0`, C&O `1.1.0` |
| Prompt compiler | `PROMPT_COMPILER_VERSION` | `webhound-prompt-compiler-v1` |
| Common preamble | `COMMON_PREAMBLE_VERSION` | `hi-common-preamble-v1` |
| Negative screens | `HI_NEGATIVE_SCREENS_REGISTRY_VERSION` | (registry module) |
| Output contract | `OUTPUT_CONTRACT_VERSION` | `hi-research-output-contract-v1` |
| Investigation spec | `spec_version` on `ResearchInvestigationSpec` | set by `investigation-spec.js` |

## Persistence (every future Webhound run)

Must store with the run / job payload:

- `template_id`
- `template_version`
- `prompt_compiler_version`
- `prompt_hash`
- `investigation_id` (when present)
- provider + budget + raw artifact pointers

Provider `buildJobPayload` already attaches `prompt_hash` and `prompt_compiler_version` via `compileWebhoundPrompt`.

## Change policy

1. Intentional prompt behavior change → **bump** template and/or compiler version  
2. Update frozen snapshots under `fixtures/hotel-intelligence/prompts/snapshots/`  
3. Run `npm run test:hotel-intelligence-prompt-compiler-2-8a`  
4. Do **not** silently mutate historical prompt identity  
5. Learnings **cannot** edit production prompts without this versioning path

## Prompt change evaluation (before promote)

Against frozen research fixtures where possible:

- Critical fact recall  
- False confident claims  
- Source quality  
- People / relationship / temporal precision  
- Profile URL extraction  
- Research cost  

More text ≠ better prompt.

## Snapshot tests

Gate freezes / compares snapshots for Full HI and Change & Opportunity (KGPV / Cambridge seeds). Unexpected drift fails the gate until version bump + snapshot update.
