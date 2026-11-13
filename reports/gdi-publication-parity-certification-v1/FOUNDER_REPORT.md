# GDI Zero-Variation Publication System — Founder Report

**Date:** 2026-11-13  
**Hotel (first application):** Bethesda Marriott `recLuxvwwxID7U2B8`  
**Contract:** `gdi-customer-publication-package-v1`  
**Status:** Infrastructure shipped + Bethesda candidate staged — **not** customer-published

---

## Verdict

Local and production Bethesda inventory counts now match (**67 / 61 / 10**), but **customer-facing content still diverges** (59 field diffs, 34 rendered-text diffs) because production continues to recompute presentation from live inventory. The immutable **GDI Customer Publication Package** is the permanent fix. A Bethesda candidate package is staged with fingerprint below; quality gate still blocks certification, and founder fingerprint approval is required before CUSTOMER_PUBLISHED.

---

## 1. Exact reasons local and production differed

| Root cause | Effect |
|---|---|
| Independent presentation recompute | Sales-action resolver / sanitizers run separately on local vs prod → Potomac Memorial: raw **Build Account**, local **Win Room Block**, production **Qualify Opportunity** |
| Summary when org = title | “Woman's Club of Bethesda is associated with Woman's Club of Bethesda — Preferred Lodging Partnership” |
| Tile line embeds sourcing badge text | “Target-hotel sourcing not established” shown twice |
| No immutable content fingerprint | 67 inventory recovery locked IDs, not final DTO wording |
| Live Airtable re-read on share | Post-approval refreshes / deploy drift alter served copy |

Empty parentheses (“Bethesda Premier Cup ()”) come from date wrappers with empty values in summary/title composition — generation now strips them.

---

## 2. Systems changed

- `lib/group-demand-intelligence/publication-package/*` — package, fingerprint, lifecycle, parity, quality, store, share resolver
- `api/group-demand-intelligence.js` — share serves CUSTOMER_PUBLISHED package first; fail-closed on broken pointer
- Presentation generation fixes: `opportunity-summary-v1.js`, `opportunity-list-dto.js`, `customer-presentation-sanitize-v1.js`, `commercial-presentation-sanitizer-v1.js`
- Scripts: build + certify + automated tests; npm `test:gdi-customer-publication-package-parity-v1`

---

## 3. Canonical implementation

**One path:** founder-approved `GDI Customer Publication Package` (frozen customer DTOs + SHA-256).  
Local preview and production share must resolve the same `publicationVersion` + fingerprint.  
Legacy share path remains only for hotels without a CUSTOMER_PUBLISHED pointer (migration-safe).

---

## 4–6. Parity status (Bethesda candidate vs live production share)

| Gate | Result |
|---|---|
| Inventory parity | **PASS** — All 67 · Hotel 61 · Competitive 10 (identical ID sets / scopes) |
| Content parity | **FAIL** — 59 field-level differences |
| Rendered-text parity | **FAIL** — 34 opportunities |
| Quality gate (candidate) | **FAIL** — 25 blocking findings (conflicts, placeholders, repeated sourcing in narrative blob, etc.) |
| Production package serve | **Not active** — legacy path still serving |

---

## 7. Publication fingerprint (candidate)

- **Publication ID:** `gdi_pubpkg_recLuxvwwxID7U2B8_def63132b667b2f3`
- **Version:** `gdi_pub_recLuxvwwxID7U2B8_20261113_candidate`
- **Content SHA-256:** `807b74be1e87411cf8ccf71e875b309a4fe21f1b62ef263c0ebcafc7783e7fdf`
- **Lifecycle:** PRODUCTION_STAGED (local store) · approvalStatus `CANDIDATE`
- **Share token:** existing `gdishare…` preserved (not rotated)

---

## 8. Browser / production verification

- API fetch of current public share: HTTP OK, count 67, Market Scope 61/10.
- Production does **not** yet advertise `publication` meta (package not CUSTOMER_PUBLISHED).
- Full card/drawer browser pass against the **approved package** is deferred until quality remediation + founder fingerprint approval + production activation.
- Independent production verification gate remains mandatory before client-ready.

---

## 9. Remaining mismatches

- Sales-action / recommendation conflicts (e.g. Potomac, Premier Cup, ACVNU: pill vs next-step).
- Narrative quality: placeholder/TBD fragments; unsupported confirmed-demand language; residual repeated sourcing phrases inside body copy.
- Content diffs vs current production until production serves this package version.

---

## 10. PR / commit

- **Commit:** `f7c6bd5e1`
- **Branch:** `feat/gdi-customer-publication-parity-v1`
- PR URL recorded after open. No production CUSTOMER_PUBLISHED activation in this PR.

---

## 11. Founder approval requirements

To make Bethesda customer-visible on the package path:

1. Quality gate must pass on the package (or an explicit founder waiver per finding — not recommended).
2. Approve **exact** fingerprint `807b74be1e87411cf8ccf71e875b309a4fe21f1b62ef263c0ebcafc7783e7fdf` (or a successor version’s fingerprint after content fixes).
3. Deploy code + package artifacts; set CUSTOMER_PUBLISHED pointer on production.
4. Re-run production share + browser verification; parity must pass.
5. Do **not** silently replace with Airtable if package load fails.

Until then: inventory-count recovery remains on the legacy approved-67 path; zero-variation content lock is **not** certified.

---

## Artifacts

`reports/gdi-publication-parity-certification-v1/`

- `01-SOURCE-OF-TRUTH-ANALYSIS.json`
- `02-OPPORTUNITY-PARITY.json`
- `03-CONTENT-DIFFERENCES.json`
- `04-PRESENTATION-DIFFERENCES.json`
- `05-PUBLICATION-PACKAGE-MANIFEST.json`
- `06-QUALITY-GATE-RESULTS.json`
- `07-PRODUCTION-VERIFICATION.json`
- `08-RELEASE-AND-ROLLBACK-PLAN.md`
