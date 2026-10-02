# GDI Share Root Cause — Bethesda Pilot P0

**Date:** 2026-10-02  
**Hotel:** Bethesda Marriott (`recLuxvwwxID7U2B8`)  
**Stable registry token:** `gdisht_47c25d74c79216021fb36150` (preserved)

---

## Exact customer failure

User-facing copy on the share page:

> Access link unavailable  
> This Dealality access link is temporarily unavailable. Please try again later or contact Dealality.

That string is bound **only** to:

- `SHARE_SECRET_MISSING`
- `INTERNAL_ERROR`

(in `lib/group-demand-intelligence/share/gdi-signed-share-capability-v1.js`)

A bad signature would say “no longer active,” not “temporarily unavailable.”

---

## Classification

**Primary:** `PRODUCTION_ENV_MISSING_SECRET`

**Secondary (Admin path):** `BAD_URL_CONSTRUCTION`

Evidence:

1. Exact inactive copy matches `SHARE_SECRET_MISSING` / `INTERNAL_ERROR`, not `SHARE_BAD_SIGNATURE`.
2. Production resolve of the **sealed Bethesda contract envelope** now returns HTTP 200 / `ok:true` for hotel `recLuxvwwxID7U2B8` (see `share-probe.json`).
3. Admin link generator could still mark a GDI URL “available” without verifying the envelope in the same runtime, and could emit a non-canonical host (e.g. localhost) when public-base resolution drifted — Open/Copy then handed the founder a dead client destination.
4. Token formats are **not** interchangeable:
   - `gdisht_…` = durable **registry token id**
   - `gdishare.v1.<payload>.<sig>` = **signed access envelope** whose payload `tid` = `gdisht_…`

---

## Token relationship

| Layer | Value |
|-------|--------|
| Registry / contract id | `gdisht_47c25d74c79216021fb36150` |
| Access envelope prefix | `gdishare.v1.` |
| Contract envelope (masked) | `gdishare.v1.eyJ2Ij…VOQXY-gc` (len=320) |
| Canonical path | `/group-demand-intelligence-share.html?share=gdishare.v1.…` |
| Canonical host | `https://my-operators-backend-production.up.railway.app` |

Admin must hand out the **sealed contract envelope** for Bethesda, not a freshly reconstructed HMAC body.

---

## Fix (no rotation)

File: `lib/admin/report-external-client-links-v1.js`

1. `assertShareTokenVerifiable` — refuse `available:true` unless this runtime can verify the envelope.
2. Bethesda always loads the sealed token from `config/client-share/production-share-contract-tokens.json`.
3. Bethesda Open/Copy always uses the **production public base**, even if Admin is local / secret-less (`servedFromProductionHost: true`).
4. Fingerprint guard in `api/admin-external-client-links.js` — refuse response if Bethesda contract token mutates.

**Stable token preserved:** YES — no rotation.

---

## Post-fix production probe (pre-deploy of this hotfix)

| Check | Result |
|-------|--------|
| Health | 200 |
| Resolve sealed contract | 200, hotel Bethesda Marriott |
| Unauth share page | Demand Report + Opportunities APIs 200 |
| Invalid / missing token | Safe inactive states (unchanged) |

Admin Open/Copy behavior on the **live** Railway revision still required post-deploy (row UX + link harden not yet on production tip before this deploy).
