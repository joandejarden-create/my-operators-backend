# GDI Source / Asset / Runtime / UI Parity V1

**Verdict: NOT CERTIFIED**

- Local: `http://127.0.0.1:8080` (customer share preview)
- Production: `https://my-operators-backend-production.up.railway.app` (Railway external share)
- Hotel: `recLuxvwwxID7U2B8`
- Package: `gdi_pub_recLuxvwwxID7U2B8_20261113_reconciliation_v1`
- Fingerprint: `441773211494cebaf50c03ef8921ccac6c57f31ac5cb2398585352b425e36f7f`
- Git HEAD at audit: `1a4b497cf034c516846ccd052ff3e9f26a61486e`
- Both environments visited: local=true production=true

## Gates

| Gate | Result |
|------|--------|
| HTTP asset parity (EOL-normalized) | FAIL (2) |
| Publication fingerprint | PASS |
| List API 67 / id set | PASS |
| Controlled list field diffs | PASS |
| Detail API (all_67) | PASS |
| UI toolbar/cards | PASS |
| Guided tour | EXECUTED |
| Mobile inventory chrome | PASS |

## Failures

- `DEPLOYED_ASSET_DIFFERENCE`: /group-demand-intelligence-share.html
- `DEPLOYED_ASSET_DIFFERENCE`: /group-demand-intelligence.html

## Root causes

- **DEPLOYED_ASSET_DIFFERENCE** · /group-demand-intelligence-share.html — Redeploy matching tip SHA; confirm lean stage includes public assets; bust caches.
- **DEPLOYED_ASSET_DIFFERENCE** · /group-demand-intelligence.html — Redeploy matching tip SHA; confirm lean stage includes public assets; bust caches.

## Notes

- Asset comparison uses **HTTP-served bytes** from local and production (not disk-only).
- Feedback / validation history fields are excluded from controlled DTO diffs.
- Do not treat `/health` buildSha alone as frontend identity under lean path-as-root deploys.
