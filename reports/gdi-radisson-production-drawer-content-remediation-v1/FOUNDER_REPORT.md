# Radisson production drawer content remediation

## Production deployment gap (Phase 1)

Production host still served **pre-#318** frontend assets at task start:

| Check | Result |
| --- | --- |
| `/health` buildSha | `ffc15db0…` (not `af04b05` / `df16a0b`) |
| Share HTML cache-bust | `gdi-segment-class-preview-ui-v1` |
| Local intended cache-bust | `gdi-drawer-truncation-remediation-v1` |
| Prod UI has Surfe share guidance | **false** |
| Prod UI SHA matches local | **false** |

Merged PRs ≠ deployed. Frontend release via GitHub Actions production workflow is required.

## Truncation remediation (Phase 2–3)

| Metric | Before | After (staged) |
| --- | --- | --- |
| Truncation detections | 42 | **0** |
| Fully certified drawers | 24 | **66** |
| Missing commercial content | 0 | 0 |

Root cause: research packaging used hard `.slice(0, 260)` / `.slice(0, 600)` / composed hotel-fit cuts. Full text recovered from existing Exa raw-run ledgers (`futureEdition`, `hotelProductFit`, `recommendedNextStep`). Tempo rewrite merged from preserved staged package `3f342937…`.

New staged fingerprint:

`f2bfebc96d75b87b9df5ae239b9e48502e117a4a6c51b344e65375a8a5a87e9d`

## Publication governance

- CURRENT preserved: `533f2642…`
- Prior Tempo staged preserved immutable: `3f342937…`
- **No automatic CUSTOMER_PUBLISHED replace**
- Approval phrase: `FOUNDER_APPROVED_FOR_PUBLICATION f2bfebc96d75b87b9df5ae239b9e48502e117a4a6c51b344e65375a8a5a87e9d`

## Surfe / validation

- Authenticated Surfe on-click retained; no paid calls this task.
- Read-only external token unchanged; guidance instead of dead controls.
- Feedback-enabled Radisson token steps documented; not activated.

## Permanent gates

- `gdi-customer-prose-completeness-v1`
- Publication quality gate blocks `TRUNCATED_ESSENTIAL_COMMERCIAL_PROSE`
- Research scripts switched to `softClampCustomerProse`
- `npm run test:gdi-radisson-production-drawer-content-remediation-v1`
