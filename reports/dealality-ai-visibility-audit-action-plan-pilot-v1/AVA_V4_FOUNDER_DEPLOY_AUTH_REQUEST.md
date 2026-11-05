# AVA V4 Founder Deploy Authorization Request

Generated: 2026-11-05

## Verdict

**AVA_V4_RELEASE_READY_FOR_FOUNDER_DEPLOY_AUTH**

## Release identity

| Item | Value |
|---|---|
| Branch | `release/ava-v4-portfolio-certified` |
| Commit | `ae907633c90d5e63d42b294fe6c0710e9934c220` |
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
1. Deploy commit `ae907633c90d5e63d42b294fe6c0710e9934c220`
2. Run production draft transfer with `--authorize-production-draft-transfer`

**B. Customer publication** — not requested yet (separate gate).

## Not done / not requested

- Production deploy
- Production draft write
- Customer publication
- Share-token rotation
- ADP/GDI changes

