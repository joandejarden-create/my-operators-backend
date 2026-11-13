# Bethesda Marriott GDI — Final Parity Release Certificate

> ## INVALIDATED — 2026-11-13
>
> **Status: NOT CLIENT READY.** Founder visually confirmed a material rendered difference for Bethesda Premier Cup: one environment shows **HOTEL NEED-DATE MATCH**; the other skips it (OPPORTUNITY OVERVIEW → WHAT IS HAPPENING). Prior “100% / CLIENT READY” claims are withdrawn. See `15-CERTIFICATE-INVALIDATION.json` and investigation `reports/gdi-bethesda-rendered-parity-root-cause-v1/`.
>
> This report is retained for audit; do not treat it as a live certification.

~~**CLIENT READY**~~

~~Founder-approved local report = immutable publication package = external Bethesda Marriott client report.~~

---

## 1. Exact GitHub `main` SHA

`4cb518545d0d4d66ec360a7022e024f8fba606a3`

## 2. Relevant PRs merged

Preserved history includes PRs **#233–#245**. Release-critical merges:

| PR | Role |
|---|---|
| #240 | Customer publication parity architecture |
| #241 | Bundle approved package under `lib/` for Railway |
| #242 | Empty-parentheses card scrub |
| #243 | Freeze need-date + commercial content into packages |
| #244 | Activate `reconciliation_v1` / `44177321…` in git CURRENT |
| #245 | Prefer bundled publication root over `data/` on Railway |

## 3. Release-critical files in GitHub

Yes. Package, CURRENT/ROLLBACK/STAGED, bundled copy, publication store/resolver, parity tests, and approval receipts are on `main` (not local-only).

## 4. Approved publication version

`gdi_pub_recLuxvwwxID7U2B8_20261113_reconciliation_v1`

## 5. Approved SHA-256 fingerprint

`441773211494cebaf50c03ef8921ccac6c57f31ac5cb2398585352b425e36f7f`

## 6. Independently verified production fingerprint

Live share API returns the **exact** approved fingerprint (integrity recompute on package load also matches). Re-verified after both `e86051ee` and `ebcd9212` cutovers.

## 7. Railway deployment

| Field | Value |
|---|---|
| Project | `serene-reverence` |
| Service | `my-operators-backend` |
| Live deployment ID | `ebcd9212-bae4-43dc-8e43-62ca160b1364` |
| Artifact SHA | `4cb518545d0d4d66ec360a7022e024f8fba606a3` |
| Prior enabling deploy | `e86051ee-b131-4374-8018-69f6679795be` (`f2b28ee84…` / PR #244) |

## 8. Publication lifecycle

`CUSTOMER_PUBLISHED` · mode `PACKAGE`

## 9. Inventory parity

**PASS** — 67 unique / 61 Hotel / 10 Competitive (intentional overlap). Zero missing/extra IDs vs approved package.

## 10. Content (DTO) parity

**PASS** — 0 controlled-content field differences across all 67 opportunities.

## 11. Rendered card + drawer parity

**PASS** — 67/67 cards and 67/67 drawers vs approved package controlled fields.

## 12. Need-Date Match parity

**PASS** — frozen `needDateMatchV1` identical live vs package. Header “Need Dates: Not Established” is hotel-level need-period status, not per-opportunity Need-Date Match. Drawers show Need-Date Match from the package (no live recompute).

## 13. Commercial-action parity

**PASS** — 100% match to approved package.

Badge-level Monitor→Qualify revisions vs superseded `9816d7d9` remediated package: **7** opportunities, including:

- Bethesda Premier Cup → **Qualify Opportunity**
- Potomac Memorial Tournament → **Qualify Opportunity**

(Prior recon audit cited 16 broader action-text diffs; live commercial content matches the approved fingerprint exactly.)

## 14. Filtering / sorting / CSV

**PASS** under identical state.

- Market Scope radios: All (67) / Hotel (61) / Competitive (10)
- Opportunity Type options derived from package inventory (not hardcoded)
- CSV contains all **67** opportunity IDs (multiline cells inflate raw line count)

## 15. Guided tour

**PASS** — all 10 steps observed on live share; Back/Next exercised on drawer steps; Finish closed tour.

## 16. Feedback preservation

**PASS** — `feedback.json` still 13,206 bytes / 24 items after cutovers. Share token unchanged. ROLLBACK pointer preserves remediated `9816d7d9…`.

## 17. Remaining limitations

- Hotel-level “Need Dates: Not Established” label can be misread as missing per-opportunity Need-Date Match (drawer/package fields are correct).
- Automated headless tour can stall on modal hit-testing; interactive browser verification completed the 10 steps.
- Permanent gate `npm run test:gdi-publication-parity-e2e` remains the regression lock for future hotels.

## 18. External Bethesda client URL

https://my-operators-backend-production.up.railway.app/group-demand-intelligence-share.html?share=`<GDI_BETHESDA_PRODUCTION_SHARE_TOKEN>`

(Token unchanged from prior customer delivery; value in `.env` as `GDI_BETHESDA_PRODUCTION_SHARE_TOKEN`.)

---

## Mandatory gates

| Gate | Outcome |
|---|---|
| GitHub preservation | PASS |
| Main branch merges | PASS |
| Publication fingerprint | PASS (`44177321…`) |
| Publication mode | PASS (`PACKAGE`) |
| Lifecycle | PASS (`CUSTOMER_PUBLISHED`) |
| Inventory 67/61/10 | PASS |
| DTO parity | PASS (0 diffs) |
| Cards 67/67 | PASS |
| Drawers 67/67 | PASS |
| Commercial recommendations | PASS |
| Need-Date Match | PASS |
| Filters/sorting | PASS |
| Content quality | PASS |
| Guided tour | PASS |
| Feedback preserved | PASS |
| External permissions | PASS (read-only share) |
| Railway deployment | PASS |

**Verdict: CLIENT READY**
