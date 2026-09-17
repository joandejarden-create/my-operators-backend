# Client share release contract — 2026-09-17

## Commands

```bash
npm run test:gdi-share-durability
npm run verify:production-share-contract
npm run verify:production-share-contract:live   # hits production with pre-existing tokens
```

Wired into `npm run deploy:production`:

1. **Pre-deploy:** `verify:production-share-contract` (local registry + route markers + crypto when secret available)
2. **Post-deploy:** `verify:production-share-contract --live` (existing Bethesda token must stay 200)

## Contract token fixture

`config/client-share/production-share-contract-tokens.json`

Contains the **existing** Bethesda Rad token — never regenerate during the test.

## Fail conditions

- Missing production share secret (production-like runtime)
- DEV secret allowed in production
- Missing `active-tokens.json` or `revoked-token-ids.json` in tree
- Bethesda contract token not ACTIVE / durably revoked
- Share / ADP / Hotel Explorer route markers missing from `server.js`
- Live: page/resolve/brief/opportunities non-200 for contract token

## Operating law

See `reports/deployment/share-link-durability-audit.md` — SHARE LINK DURABILITY LAW.

## Deploy note

Durability + UI parity code must be **deployed** for production share HTML/JS cache bust and capability fields to appear. Existing Bethesda URL already works on current production crypto/registry; next deploy ships UI parity + boot guards + durable revoke list.
