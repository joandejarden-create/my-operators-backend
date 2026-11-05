# Bethesda Marriott — NIH Evidence Correction & Executive Page Repair

**Generated:** 2026-11-05T13:13:00.746Z  
**Status preserved:** `AUDIT_VALIDATED` · Founder customer approval unchanged (`false`)  
**Paid research:** $0 · **Publication:** not authorized

## Verdict

**PASS** — corrected PDF staged for founder review.

## 1. NIH shuttle evidence

| Question | Finding |
|---|---|
| Broken `cc.nih.gov/about/visitor/hotels` citation | Removed from all payload / PDF occurrences |
| NIH-operated Patient Hotel Shuttles list Bethesda Marriott? | **No** (live ORS page lists other Bethesda hotels; Marriott Pooks Hill absent) |
| Hotel-operated Metro / Downtown Bethesda shuttle? | **Verified** on Marriott overview |
| Hotel-operated NIH / Clinical Center shuttle? | **Not verified** → `REQUIRES_HOTEL_CONFIRMATION` |
| Replacement NIH citation | [NIH Patient Hotel Shuttles](https://ors.od.nih.gov/pes/dats/PatientServices/PatientShuttles/Pages/hotel_shuttles.aspx) (supports absence / peer contrast only) |

## 2. Executive page fit

- Page 2 contains Executive Audit Summary + Takeaway: **true**
- Takeaway absent from page 3: **true**
- Page 3 begins Action Overview / actions: **true**

Previews:
- `reports/dealality-ai-visibility-audit-action-plan-pilot-v1/bethesda-marriott/pdf-staged/page-previews-nih-correction/page-2.png`
- `reports/dealality-ai-visibility-audit-action-plan-pilot-v1/bethesda-marriott/pdf-staged/page-previews-nih-correction/page-3.png`

## 3. Evidence-link validation

- Unique URLs: 5
- Pass: 55 / 55
- Fail: 0
- Flag counts: {}

## 4. PDF inventory

- Staged customer filename: `Dealality - AI Visibility Audit & Action Plan - Bethesda Marriott - NOVEMBER2026.pdf`
- Path: `reports/dealality-ai-visibility-audit-action-plan-pilot-v1/bethesda-marriott/pdf-staged/Dealality - AI Visibility Audit & Action Plan - Bethesda Marriott - NOVEMBER2026.pdf`
- Durable current: `data/ava-data/pdfs/adp_bethesda_marriott/current.pdf`
- Production archive current: `reports/ai-visibility-audit-action-plan/archive/adp_bethesda_marriott/current.pdf`
- SHA-256 (staged = durable = archive): `43a0e22d6ee8a8f4faea915ae4d12c0a748b5f062ebf1cd2f78515572bb7020a`
- Prior PDF preserved under: `pdf-staged/pre-nih-evidence-correction/` and `archive/.../pre-nih-evidence-correction/`
- Status preserved: `AUDIT_VALIDATED` · `founderApprovedForCustomer=false`

## 5. Quality re-certification (honest)

Overall **9.2/10** (target ≥ 9.0). Evidence quality raised after removing the unsupported NIH-route claim.

## 6. Portfolio lesson applied

- SOP updated: `AVA_STANDARD_OPERATING_PROCEDURE_V1.md` §2.6–2.7
- Module: `lib/ai-visibility-audit-action-plan/evidence-link-validation-v1.js`
- Scan: `node scripts/ava-evidence-link-validation-scan-v1.mjs`
- Portfolio results: `AVA_EVIDENCE_LINK_VALIDATION_PORTFOLIO_SCAN.json`
- Rule: every customer-facing citation must resolve **and** directly support its claim

### Portfolio scan snapshot (resolution pass)

| Metric | Value |
|---|---|
| Payload hotels scanned | 27 |
| Hotels with resolution/support flags | 14 |
| Citation occurrences flagged | 182 (many duplicates of the same URL) |

Material **404** findings requiring follow-up (not bot-block noise):

- UMusic Hotel Madrid — `.../happenings/splash-pass-2025` → **404**
- Waterstone Boca — `waterstoneboca.com/dining/` → **404**

Most other flags are automated **403**/timeout/self-signed certificate against brand CDNs (Hilton, IHG, Marriott homepage, Wyndham, Spice Island). Those require human/browser re-check before treating as broken customer evidence—same class as the Hyatt HTML block that led to Bethesda’s PDF-asset swap.

Bethesda Marriott: **0 failing citations** after correction.

## 7. PDF hyperlink annotation acceptance (mandatory)

Verification artifacts:
- `AVA_BETHESDA_PDF_LINK_ANNOTATION_VERIFICATION.json`
- `AVA_BETHESDA_PDF_NIH_LINK_CLICK_TEST.json`

| Check | Result |
|---|---|
| Broken URL in current payload evidence | **0** |
| Broken URL in PDF raw bytes | **0** |
| Broken URL in PDF `/URI` annotations | **0** of 33 annotations |
| Replacement NIH ORS annotations | **6** |
| Click-test replacement link | **HTTP 200**; Patient Hotel Shuttles page; Marriott **not** listed |
| AI Admin Preview SHA | matches corrected PDF |
| AI Admin Download (`?download=1`) SHA | matches corrected PDF |
| Status / founder approval preserved | `AUDIT_VALIDATED` / `founderApprovedForCustomer=false` |
| Prior PDF preserved | `pre-nih-evidence-correction/` |

## 8. Founder notes

- No automatic `FOUNDER_APPROVED_FOR_CUSTOMER`.
- Hotel must still confirm any hotel-operated NIH shuttle before buyer-facing claims.
- Preview/Download should resolve to the corrected checksum above.
- Hyatt consumer HTML was bot-blocked; capacity claims use Hyatt-published PDF assets.
- Historical version JSON under older `phase8` / `footer_fix` snapshots still contains the retired URL by design (immutable history). Current report version and current PDF do not.
