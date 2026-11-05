# GDI — Permanent One-Click Publication Architecture

**Status:** Local architecture complete. Production deploy + Spice apply **stopped** pending founder GO.

## Verdict

| Question | Answer |
|----------|--------|
| What was simplified? | One hotel publish operation; locked published presentation; shared UI components |
| Duplicate pathways removed? | Manual volume migrations for normal publishes; share CQ recompute on locked snaps; lean publishes; dual Customer/Research lists |
| One-click E2E? | **Local + isolated apply: YES.** Production live publish: **NOT RUN** |
| Spice production readiness? | Dry-run PASS (7 IDs, 1/6, no Aventa). Live repair ready after PR #136 deploy + GO |
| Genuine blockers? | PR #136 still open (deploy-source risk); founder GO required; ship only architecture files from dirty tree |

## Artifacts

- `00-executive-verdict.json`
- `01-architecture.md`
- `02-production-go-sequence.json`
- `03-spice-island-one-click-dry-run.json` (from `npm run gdi:one-click-publish-spice-dry-run`)

## Tests

```bash
npm run test:gdi-one-click-hotel-publish-v1
npm run gdi:one-click-publish-spice-dry-run
npm run test:gdi-universal-presentation-publication-parity-v1
npm run test:gdi-single-inventory-local-publication-parity-v1
```
