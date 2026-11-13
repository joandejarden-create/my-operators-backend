# GDI Source / Asset / Runtime / UI Parity V1

**Verdict: CLIENT READY**

Local founder-approved customer **share** preview and Railway external customer share match on approved source, controlled content, and customer-facing UI behavior.

| Identity | Value |
|----------|--------|
| Local | `http://127.0.0.1:8080` share preview |
| Production | `https://my-operators-backend-production.up.railway.app` |
| Hotel | `recLuxvwwxID7U2B8` |
| Package | `gdi_pub_recLuxvwwxID7U2B8_20261113_reconciliation_v1` |
| Fingerprint | `441773211494cebaf50c03ef8921ccac6c57f31ac5cb2398585352b425e36f7f` |
| Merge SHA | `b667aec6411206abefb3424fcfd919e035ac26a2` |
| Railway deployment | `a4ab3513-7dbb-47b3-b899-3bedd208ad4f` |
| Both environments visited | local=true · production=true |

## Forensic findings

1. **HTTP asset parity** — After aligning CSS cache-bust markers to `gdi-howto-read-v1-1g` and deploying tip `b667aec64`, EOL-normalized local↔production hashes match for the full customer asset closure.
2. **Publication / DTO** — Fingerprint identical; 67/67 list IDs match; controlled list + all-67 detail fields match (feedback paths excluded).
3. **UI** — Toolbar, Reset View, filters, card set/fingerprint, Need-Date status, inventory chrome 67/61/10 match.
4. **Guided tour** — Step 6 drawer `y=0`, full-rail highlight, coachmark left of drawer on both environments; no blank-center overlay.
5. **Duplicated renderers** — Share uses `share-app.js`; founder auth uses `app.js`; shared customer UI is `dealality-gdi-ui.js` + guided-tour modules (expected). No divergent customer renderer for share chrome.

## Permanent gate

```bash
npm run test:gdi-complete-release-parity
```

Compares HTTP-served local vs Railway (never local↔local). Failures cannot be reported as PASS.

## Ship path

- PR [#253](https://github.com/joandejarden-create/my-operators-backend/pull/253)
- GHA release [43430579480](https://github.com/joandejarden-create/my-operators-backend/actions/runs/43430579480)
- Control plane: `release/gha-deploy-detached-head-guard-v1`

## Notes / limitations

- `/health` `buildSha` can lag lean path-as-root uploads — use HTTP asset hashes + HTML markers.
- Main-branch production-release workflow still needs the detached-HEAD guard merged from the control-plane branch for future main-ref dispatches.
- Feedback / validation history intentionally differ and are excluded from controlled DTO diffs.
