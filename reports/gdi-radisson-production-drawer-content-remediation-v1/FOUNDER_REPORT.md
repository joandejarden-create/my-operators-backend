# Radisson production drawer content remediation

## Production deployment (Phase 1) — COMPLETE

| Check | Before | After |
| --- | --- | --- |
| Share HTML cache-bust | `gdi-segment-class-preview-ui-v1` | **`gdi-drawer-truncation-remediation-v1`** |
| Prod UI Surfe share guidance | false | **true** |
| HTML Last-Modified | `Mon, 16 Nov 2026 21:59:57 GMT` | **`Mon, 16 Nov 2026 22:44:11 GMT`** |
| Approved deploy SHA | (undeployed merges) | **`dee9ca74e693d915a555d069fd25cacf4bdda7b0`** |
| Railway deployment | prior revision | **`266111d1-3c54-4847-a0c4-39c27d833b9e`** |
| Release workflow | — | https://github.com/joandejarden-create/my-operators-backend/actions/runs/43972615588 |

`/health` still reports lean `buildSha=ffc15db0…` (Railway lean image quirk). Asset-level proof is cache-bust + Surfe guidance + Last-Modified, not `/health` alone. UI byte SHA differs local vs prod by CRLF only; content markers match.

## Truncation remediation (Phase 2–3)

| Metric | Before | After (staged) |
| --- | --- | --- |
| Truncation detections | 42 | **0** |
| Fully certified drawers | 24 | **66** |
| Missing commercial content | 0 | 0 |

Root cause: research packaging used hard `.slice(0, 260)` / `.slice(0, 600)` / composed hotel-fit cuts. Full text recovered from existing Exa raw-run ledgers (`futureEdition`, `hotelProductFit`, `recommendedNextStep`). Tempo rewrite merged from preserved staged package `3f342937…`.

New staged fingerprint:

`f2bfebc96d75b87b9df5ae239b9e48502e117a4a6c51b344e65375a8a5a87e9d`

Live external share still serves CURRENT `533f2642…`, so Tempo `whyThisHotel` remains truncated on production until this fingerprint is published.

## Publication governance

- CURRENT preserved: `533f2642f8c74af881ff525c2335f9a5471f5ef1d04ac16f7646131176f8706d`
- Prior Tempo staged preserved immutable: `3f342937994ebaa36769e8c90af56e9cb26effeb940fa8a4de2be6cce4f995a8`
- **No automatic CUSTOMER_PUBLISHED replace**
- Exact approval required:

```
FOUNDER_APPROVED_FOR_PUBLICATION f2bfebc96d75b87b9df5ae239b9e48502e117a4a6c51b344e65375a8a5a87e9d
```

## Surfe / validation

- Authenticated Surfe on-click retained; no paid calls this task.
- Read-only external token unchanged; guidance instead of dead controls.
- Feedback-enabled Radisson token steps documented in `04-SURFE_AND_VALIDATION_CAPABILITIES.json`; not activated.

## Permanent gates

- `gdi-customer-prose-completeness-v1`
- Publication quality gate blocks `TRUNCATED_ESSENTIAL_COMMERCIAL_PROSE`
- Research scripts switched to `softClampCustomerProse`
- `npm run test:gdi-radisson-production-drawer-content-remediation-v1`

## GitHub

| Item | Value |
| --- | --- |
| Implementation PR | [#320](https://github.com/joandejarden-create/my-operators-backend/pull/320) → merge `dee9ca74…` |
| Closeout reports PR | [#322](https://github.com/joandejarden-create/my-operators-backend/pull/322) → merge `75c76969e` |
| CI / gitleaks | PASS |
| `main` synchronized | yes (`75c76969e`) |
| Worktree | `wt-radisson-6seg` on `main` |

## Browser QA (production external)

- Inventory 66 = 50 Hotel + 16 CMI visible
- Toronto Tempo drawer: no duplicate title; no echo timing/location labels; Potential Business + next step complete; Surfe guidance (no dead button); validation read-only message present
- Content truncations on live CURRENT package: still present until founder publishes `f2bfebc9…`
