# Localhost Admin Share Hydration Parity V1

**Date:** 2026-10-02  
**Hotel:** Bethesda Marriott (`recLuxvwwxID7U2B8`)

## Root cause

**MISSING_ENV_SECRET** + **LOCAL_HOST_CONSTRUCTION** (ADP) / **CATALOG_HANDLER_ENV_GUARD** (GDI verify)

| Surface | Local before | Why |
|---------|--------------|-----|
| ADP Client | `adpShareAvailable=false` → UI `—` | Local `.env` has no `ADP_SHARE_CAPABILITY_SECRET`. Resolver required a signing secret to reconstruct URLs. No sealed ADP contract fallback (unlike GDI). |
| GDI Client | `—` when DEV/wrong secret present | Sealed Bethesda envelope verified with local secret → `SHARE_BAD_SIGNATURE` → `available=false`. Fallback only covered `SHARE_SECRET_MISSING`. |
| Public base | `http://localhost:3000` when reconstructing | Open/Copy would point at a non-production host even if flagged available. |

Production remains correct because Railway has real `ADP_SHARE_CAPABILITY_SECRET` + `GDI_SHARE_CAPABILITY_SECRET`.

## Fix (Option C — sealed production catalog)

1. Added ADP Bethesda sealed envelope to `config/client-share/production-share-contract-tokens.json` (`sht_24ff4ada…`).
2. Local/default `preferProductionShareCatalog()` serves sealed ADP + GDI contract URLs on the **production** host — no minting, no frontend secrets.
3. GDI Bethesda contract fallback now covers **any** local verify failure (`SHARE_SECRET_MISSING` or `SHARE_BAD_SIGNATURE`).
4. Startup logs share hydration presence (names only).
5. `.env.example` documents `USE_PRODUCTION_SHARE_CATALOG`, ADP/GDI share vars.
6. AI Demand Reviews: dedicated **ADP Client** `[ Open ] [ Copy URL ]` column + cache-bust query.

## Env comparison (names only)

| ENV VAR | LOCAL PRESENT? | PRODUCTION PRESENT? | REQUIRED FOR CATALOG? | REQUIRED FOR OPEN/COPY? |
|---------|----------------|---------------------|-----------------------|-------------------------|
| ADP_SHARE_CAPABILITY_SECRET | NO | YES | NO (sealed ADP contract) | NO for Bethesda sealed; YES to reconstruct other hotels |
| ADP_SHARE_CAPABILITY_ALLOW_DEV_SECRET | YES (`1`) | NO | NO | NO (must not be used to mint Bethesda client URLs) |
| ADP_SHARE_CAPABILITY_ENFORCE | YES | YES | NO | NO |
| ADP_SHARE_REGISTRY_DIR | NO | NO | NO (default path) | NO |
| GDI_SHARE_CAPABILITY_SECRET | NO | YES | NO (sealed GDI contract) | NO for Bethesda sealed |
| GDI_SHARE_CAPABILITY_SECRET_PREVIOUS | NO | NO | NO | NO |
| GDI_SHARE_CAPABILITY_ALLOW_DEV_SECRET | NO | YES* | NO | NO |
| GDI_SHARE_CAPABILITY_ENFORCE | NO | YES | NO | NO |
| GDI_SHARE_REGISTRY_DIR | NO | NO | NO | NO |
| DEALALITY_PUBLIC_BASE_URL / PUBLIC_BASE_URL / ADP_/GDI_PUBLIC_BASE_URL | NO | NO | NO | NO for sealed contracts |
| RAILWAY_PUBLIC_DOMAIN / RAILWAY_ENVIRONMENT | NO | YES | NO | NO |
| USE_PRODUCTION_SHARE_CATALOG | NO (defaults on for local) | NO | YES (policy) | YES (policy) |

\*Production currently lists `GDI_SHARE_CAPABILITY_ALLOW_DEV_SECRET` present — out of scope; local fix does not change Railway vars.

## Catalog Bethesda (after)

| Field | Value |
|-------|-------|
| hotelId | `recLuxvwwxID7U2B8` |
| gdiShareAvailable | **true** |
| adpShareAvailable | **true** |
| pdfAvailable | true |
| reportStatus | READY |
| lastGeneratedAt | `2026-10-02T12:36:43.337Z` |

## Tokens

| Check | Result |
|-------|--------|
| NEW TOKENS CREATED? | **NO** |
| PRODUCTION SHARE TOKENS CHANGED? | **NO** (GDI sha12 `9237540e872c`, ADP sha12 `c5e256b6277e`) |
| Prod GDI resolve | 200 |
| Prod ADP resolve | 200 |

## Operator note

Restart local `npm start` / Admin server so it loads the updated resolver + HTML cache-bust (`?v=local-share-hydration-parity-v1`). Hard-refresh Admin.
