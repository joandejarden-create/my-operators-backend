# Share link durability audit — 2026-09-17

## SHARE LINK DURABILITY LAW

> A valid client-facing Dealality share URL must survive application commits, application restarts, Railway redeployments, and horizontal instance changes. A URL becomes invalid only because of explicit expiration, explicit revocation, or an intentional security migration.

## Exact root cause (historical failures)

**Validity model: C — both cryptographic HMAC and server-side registry.**

1. Tokens are signed with durable Railway env `GDI_SHARE_CAPABILITY_SECRET` (64 chars, not generated at startup).
2. After signature OK, verify consulted `config/client-share/gdi-share-registry/active-tokens.json` on the **app filesystem**.
3. Railway redeploys replace the filesystem. Runtime-written registry rows disappeared → `SHARE_UNKNOWN` even when the HMAC was still valid.
4. Incomplete coexist deploys also removed share static routes/assets (separate class of failure).

Mitigations already partially present: HMAC signature self-heal (default ON) re-seeds ACTIVE rows from claims. Gaps remaining before this fix: no durable revoke list (self-heal could resurrect revoked IDs after wipe), no previous-key verification, no production boot fail if secret missing, no pre/post deploy token regression.

## Token structure

`gdishare.v1.<base64url(payload)>.<base64url(HMAC-SHA256)>`

Payload: `v`, `tid`, `hotelId`, `surfaces`, `mode`, `iat`, `exp` (+ optional `capabilities` on newly issued tokens).

## Signing secret

| Item | Status |
|---|---|
| Env | `GDI_SHARE_CAPABILITY_SECRET` |
| Railway production | Set (durable across deploys) |
| Generated at startup | **No** |
| Previous keys | `GDI_SHARE_CAPABILITY_SECRET_PREVIOUS` (comma-separated) — **implemented** |
| Production boot fail if missing | **implemented** (`assertGdiShareProductionConfig` in `server.js`) |

## Registry / revoke durability

| Store | Path | Durable? |
|---|---|---|
| ACTIVE registry | `config/client-share/gdi-share-registry/active-tokens.json` | Shipped in deploy artifact + self-heal |
| Durable revoke list | `config/client-share/gdi-share-registry/revoked-token-ids.json` | **New** — must ship in deploy; blocks self-heal resurrection |

## Lifecycle files

| Step | File / function |
|---|---|
| Issue / sign | `lib/group-demand-intelligence/share/gdi-signed-share-capability-v1.js` → `issueGdiShareCapability` |
| Verify | `verifyGdiShareCapability` |
| Self-heal | `upsertGdiShareRegistryFromClaims` |
| Revoke | `revokeGdiShareCapability` (+ durable list) |
| API gate | `api/group-demand-intelligence.js` → `requireGdiShare` |
| Static page | `public/group-demand-intelligence-share.html` |
| Client | `public/js/group-demand-intelligence/share-app.js` |

## Exact Bethesda token test (2026-09-17)

Token id: `gdisht_47c25d74c79216021fb36150` (existing Rad URL — **not reissued**)

| Check | Result |
|---|---|
| TOKEN_SIGNATURE | PASS |
| TOKEN_EXPIRATION | PASS (exp 2026-12-31) |
| TOKEN_REGISTRY | PASS (ACTIVE + not durably revoked) |
| HOTEL_RESOLUTION | PASS `recLuxvwwxID7U2B8` |
| SHARE_PAGE | PASS 200 |
| BRIEF_API | PASS 200 |
| OPPORTUNITIES_API | PASS 200 |
| DETAIL_API | PASS (opportunities list returns rows; detail gated by opportunity_detail) |

## Fixes implemented

1. Durable revoke list + self-heal never resurrects revoked tids  
2. Previous-secret verification for safe rotation  
3. Production boot fails without durable secret / with DEV secret enabled  
4. Explicit capabilities (`CAN_VALIDATE` derived for legacy `opportunity_detail` tokens)  
5. Customer-safe inactive-link messages  
6. `npm run verify:production-share-contract` (+ `--live`) wired into `deploy:production` pre + post  

## Verdict (durability)

TOKEN SECRET DURABLE: **YES** (Railway env)  
SHARE REGISTRY DURABLE: **YES** (artifact + self-heal + durable revoke)  
ROUTE DURABLE: **YES** (assert + contract markers)  
EXISTING BETHESDA TOKEN: **WORKING**
