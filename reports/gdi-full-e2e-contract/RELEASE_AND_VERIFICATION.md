# Release / local / production proof

## This PR

| Check | Result |
|-------|--------|
| `node scripts/test-gdi-full-e2e-contract-v1.mjs` | PASS (0 paid spend) |
| `node scripts/gdi-full-e2e-status-v1.mjs --write-contract-doc` | Wrote `full-e2e-contract.json` |
| Smoke: `--mode full-e2e --dry-run --stage evaluate_completion` (Mayflower id) | `computedOverallStatus: PARTIAL` (expected without evidence) |
| Fixture: association-only + plan-only applicable | `PARTIAL` → `COMPLETION_EVALUATION_ASSOC_ONLY_PARTIAL.json` |

## Deploy policy

- Deploy shared code from **clean main** via `npm run deploy:production` (never raw `railway up`).
- Pack lean: `.railwayignore` must keep estimated upload **under ~200 MB** Cloudflare CLI limit. GDI customer PDFs only under `reports/group-demand-intelligence/pdf/`; do not un-ignore the whole GDI reports tree. Marketing multi-MB local textures stay ignored when CDN hosts them.
- Data-only hotel updates still require production verification proofs; deploy SUCCESS alone ≠ delivery.
- Do not interrupt active Renaissance / Tampa / ADP execution ownership.
- Preserve current customer inventory; this contract does not authorize new research spend or hotel activation.

## Local primary

- Serving process: repository build on **`:8080`**
- Record proofs in `full-e2e-evidence.json` under the run dir.

## Production

- Verify deployed code/assets + canonical data + hotel scope + real customer list/detail/share/PDF.
- Reload + hotel switching; no provider metadata leakage.
- Tie proofs to run id, hotel, release/data revision, verification time; invalidate when later changes stale them.
