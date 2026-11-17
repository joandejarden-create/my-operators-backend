# YOTEL Geneva Lake — Local STAGED Preview Reconciliation V1

**Status:** FIXED (local integration) + permanent certification gate merged readiness  
**Hotel:** YOTEL Geneva Lake (`recrPQcZg7SFARRb2`)  
**No Exa / Surfe / Airtable writes / CUSTOMER_PUBLISHED / Railway**

## 1. Root cause

Canonical localhost:8080 was serving `C:\Dev\dealality\my-operators-backend` at detached `629cdae04` (**before PR #332**).

That checkout’s YOTEL `STAGED.json` still pointed at the prior commercial-quality package:

- Fingerprint `41a1b1ca0c34eb5da4b2569db499743ef1721f52376d141c379677e4cd6c054f`
- Inventory **17 Hotel Market / 5 CMI / 22 total**

PR #332 / main already contained the $30 Exa scale package, but it had not been synchronized into the primary 8080 checkout.

## 2. Running server

| Item | Value |
|---|---|
| Port | **8080** (8092 idle; alternate Node on **8091**) |
| PID | `22284` (`node server.js`) |
| Checkout | `C:\Dev\dealality\my-operators-backend` (basename guard for 8080) |
| Pre-fix commit | `629cdae04` — **did not include PR #332** |
| Package before sync | Old STAGED `41a1b1ca…` |
| Package after sync | New STAGED `35343009…` from `origin/main` |

Dirty primary untracked/unrelated work was preserved. No `reset --hard` / `clean -fd`.

## 3. STAGED package verification

| Check | Result |
|---|---|
| Expected fingerprint | `353430092ceea5626c85e04bffbeeb84d0738bf7a5382c5177d5fdf37413eba3` |
| Publication version | `gdi_pub_recrPQcZg7SFARRb2_20261117_yotel_geneva_lake_exa_30usd_scale_v1` |
| Integrity / unique IDs | 42 / 42, zero duplicates |
| Hotel Market | 34 |
| CMI | 8 |
| Prior package retained | `41a1b1ca…` immutable bytes kept on disk |

## 4. Local fix applied

Targeted sync (approved mechanism):

```bash
node scripts/gdi-sync-bundled-staged-packages-from-ref.mjs --ref origin/main --hotel recrPQcZg7SFARRb2
```

run inside `C:\Dev\dealality\my-operators-backend`.

**Files synchronized (from `origin/main` tip `4d27467c0`):**

- `lib/group-demand-intelligence/bundled-publication-packages/recrPQcZg7SFARRb2/STAGED.json`
- `lib/group-demand-intelligence/bundled-publication-packages/recrPQcZg7SFARRb2/gdi_pub_recrPQcZg7SFARRb2_20261117_yotel_geneva_lake_exa_30usd_scale_v1/**`
- prior YOTEL package dirs refreshed to match main (immutable history preserved)

No server restart required — publication packages are read from disk per request. Process `22284` continued serving 8080.

## 5. Local API fingerprint

`GET /api/group-demand-intelligence/hotels/recrPQcZg7SFARRb2/opportunities?publicationPackage=1&surface=customer&preview=staged`

| Field | Value |
|---|---|
| `publication.contentFingerprintSha256` | `353430092ceea5626c85e04bffbeeb84d0738bf7a5382c5177d5fdf37413eba3` |
| `publication.previewSource` | `STAGED` |
| `publication.publicationPackageMode` | `PACKAGE` |
| Counts | all 42 / hotel 34 / competitive 8 |
| Opportunity IDs | exact match to immutable package bytes |

Detail drawers (hotel-scoped detail route): **42/42 PASS**, named contacts present, no truncated essential prose.

## 6. Browser QA

| Surface | Result |
|---|---|
| `http://localhost:8080/group-demand-intelligence.html?hotelId=recrPQcZg7SFARRb2&preview=staged` | PASS — YOTEL selected; All (42) / Hotel (34) / Competitive (8) |
| Filters Hotel / Competitive / Reset View | 34 / 8 / 42 cards |
| All 42 `#gdiDrawer` opens | PASS — commercial overview + contact signals; no Failed to Fetch; no Incomplete |
| `http://localhost:8080/app#/group-demand-intelligence?hotelId=…&preview=staged` | Shell requires Memberstack login in this automation context; embed target is the same `group-demand-intelligence.html` route verified above |

Final working URL:

`http://localhost:8080/group-demand-intelligence.html?hotelId=recrPQcZg7SFARRb2&preview=staged`

Authenticated shell after login: `http://localhost:8080/app#/group-demand-intelligence` (select YOTEL Geneva Lake).

## 7. Regression prevention

Added permanent gate:

- `lib/group-demand-intelligence/publication-package/gdi-local-staged-preview-certification-gate-v1.js`
- `npm run test:gdi-local-staged-preview-certification-gate-v1`
- Wired into Full E2E template stage 8; certificate evaluator fails if a pack claims `localBrowserVerified` without this gate.

A campaign must not be marked locally browser-verified until STAGED fingerprint, exact opportunity IDs, counts, and drawer bodies certify.

## 8. Governance confirmation

- No new Exa / Surfe / paid research
- No Airtable writes
- No CUSTOMER_PUBLISHED advancement
- No Railway deploy
- No fingerprint mutation of immutable packages
- Other hotels untouched

## 9. GitHub

Code correction (certification gate) ships as a focused PR from this worktree.  
Local YOTEL inventory fix itself was checkout synchronization only (packages already on main via PR #332).
