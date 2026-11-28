# Hotel Explorer P1 Integration Recovery — Founder Report

## Classification

**HE_P1_IDENTITY_RECOVERY_PASS** (pending IC merge of this PR)

## 1. Root cause of missing hotel IDs

`HotelExplorer.open()` accepted a bare HPC string (`"rec…"`) and stored it as `state.hotel`.

Later ownership merge used `Object.assign` / `mergeHotelState` against that primitive string, which copies character indexes and blanks `id` / `recordId`. Subsequent Contact Intelligence and Ownership fetches then used an empty hotel id:

- `GET /api/contact-intelligence/hotels/?audience=internal` → 404
- `GET /api/golden-demo/ownership/hotel/` → 404

Deep-link seeds that already used `{ id, recordId }` objects worked; Playwright audit / string `open(id)` paths did not.

PR #694 / #695 Contact V2 architecture was already on `main` — the failure was identity plumbing, not missing Contact V2 code.

## 2. Code changes

| File | Change |
|------|--------|
| `public/js/hotel-explorer.js` | `normalizeHotelInput()`; expand `recordId()` (`hpcId`, etc.); coerce string→object at `open()`; explicit identity-error UI |
| `public/js/hotel-contact-intelligence.js` | Fail closed on empty `hotelId` (no empty API URLs) |
| `scripts/hotel-explorer/test-he-p1-hotel-identity-plumbing-v1.mjs` | Permanent regression |
| `scripts/hotel-explorer/run-he-p1-identity-recert-matrix-v1.mjs` | 11×5 + 7-hotel smoke |

## 3–4. SHA / 8080

| Item | Value |
|------|-------|
| Base tip before PR | `2429e4ff71f3b89d55f56092b84156b1ace8d580` (`origin/main`, includes #705) |
| Feature branch | `fix/he-p1-hotel-identity-plumbing-v1` |
| 8080 process | `ws-gdi` PID preserved (dirty GDI **not** reset) |
| Verification method | Overlay of patched HE JS onto preserved `ws-gdi` public assets |
| Clean tip checkout | `C:\Dev\dealality\wt-he-p1-8080-serve` @ same tip (available for controlled port transfer post-merge) |

## 5–6. API status (Bethesda `recLuxvwwxID7U2B8`)

| API | Status |
|-----|--------|
| Contact Intelligence | **200** with hotel id; 5 people package |
| Ownership | **200** with hotel id |

Empty-id requests after fix: **0**

## 7. Bethesda Airtable read-back (via canonical resolver API)

- 5 unique professionals in directory
- 3 verified business emails hydrated without Find click: `jeff.mullen@…`, `kbutton@…`, `preece@…`
- Direct phones displayed for Katherine / Patrick (formatted): `+1 (267) 475-4267`, `+1 (972) 233-5600`
- No paid discovery rerun; no Airtable mutations

## 8. Screenshot

`reports/hotel-explorer/he-p1-identity-recovery/SCREENSHOTS/bethesda-contacts-identity-fixed-1440x900.png`

## 9–10. Playwright

| Suite | Result |
|-------|--------|
| 11 tabs × 5 viewports | **55/55 PASS** (identity + no empty hotel URLs) |
| 7 HE50 smoke hotels | **7/7 PASS** |

Evidence: `PLAYWRIGHT_RECERT_MATRIX.json`, `IDENTITY_FIX_RESULTS.json`

## 11. Remaining issues

| ID | Severity | Notes |
|----|----------|-------|
| Scout `market-coverage` 500 | P2 | Independent of hotel-id plumbing; not fixed in this PR |
| Overview generic “Hotel” / weak hero identity on some smoke hotels | P2 | Presentation/data binding; separate focused task |
| 8080 still owned by dirty `ws-gdi` | Ops | Controlled transfer to clean tip recommended after merge; dirty tree preserved |

## 12. Customer-demo readiness

**READY for Contact Intelligence / Ownership identity + Airtable hydration demo** on the patched 8080 assets for Bethesda.

Still recommend IC merge of this PR + controlled 8080 transfer to clean tip for durable certification.

Restrictions honored: no paid research, no Airtable writes, no prod deploy, no dirty-tree reset, no bulk unrelated merges.
