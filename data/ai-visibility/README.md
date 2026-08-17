# AI Visibility runtime storage

Local development / Phase 1 file-store root.

- Committed fixtures live in `fixtures/ai-visibility/`
- Runtime writes under `data/ai-visibility/runtime/` are gitignored
- Not permanent production storage — replace via storage abstraction

## Phase folders (typical)

- `runtime/phase2a/` — stored live OpenAI responses + cohort universe snapshot
- `runtime/phase2b/` — reprocess report (resolver v2 / classifier v2 era)
- `runtime/phase2c/` — reprocess report (classifier v3 + geography v1)

Reprocess scripts read stored Phase 2A responses only (no provider calls):

```bash
npm run ai-visibility:phase2b-reprocess
npm run ai-visibility:phase2c-reprocess
npm run test:ai-visibility-phase2c
```
