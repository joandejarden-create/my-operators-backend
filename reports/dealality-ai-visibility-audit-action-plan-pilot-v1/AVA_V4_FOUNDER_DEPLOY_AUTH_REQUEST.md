# AVA V4 Founder Deploy Authorization Request

Generated: 2026-11-05

## Verdict

**AVA_V4_RELEASE_READY_FOR_FOUNDER_DEPLOY_AUTH**

## Release identity

| Item | Value |
|---|---|
| Branch | `release/ava-v4-portfolio-certified` |
| Content commit (security-cleaned) | `2f14f0e6bea272b02e8c1f367f0b0e26e0e0544c` |
| Base | `origin/main` |
| Hotels certified | 38 / 38 |
| Manifest hash matches | 38 / 38 (reused unchanged) |
| Unresolved PDF defects | 0 |

## What is ready

1. AVA-only code release (Admin API, V4 renderer, Action Register, CQA, storage channels).
2. Exact certified PDFs under `portfolio-v4-local-staging/` matching `AVA_38_HOTEL_FINAL_MANIFEST.json`.
3. Draft staging script (fail-closed SHA verify; never overwrites published).
4. Publication script (separate auth; not executed).
5. Deploy procedure: `AVA_V4_RELEASE_EXECUTION_PROCEDURE.md`.

## Authorization requested now

**A. Production deploy + transfer of 38 certified PDFs into durable Admin drafts**

Reply with explicit approval to:
1. Deploy the PR #138 branch tip (includes content commit `2f14f0e6bea272b02e8c1f367f0b0e26e0e0544c`)
2. Run production draft transfer with `--authorize-production-draft-transfer`

**B. Customer publication** — not requested yet (separate gate).

## Not done / not requested

- Production deploy
- Customer publication
- Share-token rotation
- ADP methodology or GDI changes
