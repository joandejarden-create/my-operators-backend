# AI Visibility Audit & Action Plan — Standard Operating Procedure (V1)

**Status:** Internal operating procedure (Phase 8)  
**Contract:** `AI_VISIBILITY_AUDIT_ACTION_PLAN_PDF_V1`  
**Hard bans:** no paid CORE remeasurement, no certified ADP baseline edits, no production Airtable writes, no customer email/PDF publication without founder gate, no monthly ADP register imports to production.

---

## Purpose

Produce a hotel-specific, evidence-backed AI Visibility Audit & Action Plan for any eligible ADP hotel, using the Spice Island founder-reviewed report as the quality benchmark.

## Formal gates (must remain separate)

| Gate | Meaning | Does **not** mean |
|---|---|---|
| **AUDIT_VALIDATED** | Research, evidence, recommendations, and staged PDF passed internal certification | Customer distribution authorized |
| **FOUNDER_APPROVED_FOR_CUSTOMER** | Founder explicitly authorized this report version for customer distribution | Already published / emailed |

PDF generation success and a 9+/10 quality score **never** auto-promote to `CUSTOMER_READY` or `PUBLISHED`.

---

## Procedure for any eligible ADP hotel

### 1. Baseline selection
1. Confirm the hotel is in the staged ADP universe (`loadAdpBaselineForAudit`).
2. Reuse the certified ADP period KPIs (AI Consideration, Scenario Presence, Certified Peer Position).
3. Do **not** remeasure CORE/Loyalty providers unless founder authorizes paid research in the same turn.

### 2. Research and evidence collection
1. Run free public research against official hotel/brand pages and authoritative public sources.
2. Capture working HTTPS citations for every significant customer-facing claim.
3. Classify each finding:
   - `VERIFIED_ISSUE`
   - `REQUIRES_HOTEL_CONFIRMATION`
   - `RECOMMENDED_INVESTIGATION`
   - `OPTIONAL_COMMERCIAL_OPPORTUNITY`
4. Never treat ADP attribute weakness as proof of a website defect.
5. Never treat an automated HTTP 403 as proof that AI crawlers are blocked globally.
6. **Evidence & hyperlink certification (mandatory gate):** every customer-facing citation must both resolve successfully **and** directly support the claim attached to it. A live URL alone is not enough. Classify each source as `VERIFIED_CURRENT` | `VERIFIED_WITH_QUALIFICATION` | `REQUIRES_MANUAL_REVIEW` | `BROKEN_LINK` | `UNSUPPORTED_CLAIM` | `OUTDATED_SOURCE`. Flag and replace before certification:
   - HTTP 404 / 410 → `BROKEN_LINK` (blocks `AUDIT_VALIDATED`)
   - HTTP 403 / 429 / bot / timeout → `REQUIRES_MANUAL_REVIEW` (not automatic broken; browser/manual verification with explanation required)
   - Redirects to unrelated pages/hosts
   - Historical or archived sources presented as current
   - Sources that do not support the specific claim
   - Conflicts between current and archived information
   - Operator-vs-third-party service mismatches (e.g. hotel shuttle vs NIH-operated patient shuttle)
7. After PDF generation, extract `/URI` annotations and compare to the evidence register. Rejected URLs (including `cc.nih.gov/about/visitor/hotels`) must be absent.
8. Run `node scripts/ava-evidence-hyperlink-certify-phase8-six-v1.mjs` (cohort) or Admin **Evidence Validation → Run validation** before `AUDIT_VALIDATED`. Modules: `evidence-hyperlink-certification-v1.js`, `evidence-link-validation-v1.js`, `pdf-hyperlink-annotations-v1.js`.

### 3. Recommendation qualification
For every proposed action, answer:
- Hotel-specific?
- Evidence-supported?
- Real issue or credible opportunity?
- Why management should care?
- Commercial relevance?
- Exact change or investigation?
- Measurable completion criteria?

Discard padded recommendations that exist only to inflate action count.

### 4. Executive summary (mandatory 9+ page)
Write a standalone one-A4 GM briefing:
- A. Overall assessment (hotel-specific)
- B. Performance snapshot (3 KPIs + qualifications)
- C. What we discovered (top 3)
- D. Recommended management priorities (owner + timing)
- E. Executive takeaway

No jargon, no inflated claims, no font shrinking to force fit.

### 5. PDF generation
1. Preserve any prior staged PDF (e.g. `pre-phase8-certification/`).
2. Generate via `generateAiVisibilityAuditPdfV1` or AI Admin **Generate staged PDF**.
3. Keep status at `PDF_STAGED` or preserve `AUDIT_VALIDATED` / founder statuses — never auto-publish.

### 6. Independent quality certification
Score eight dimensions (1–10):
1. Executive summary quality  
2. Competitive positioning accuracy  
3. Hotel-specific commercial relevance  
4. Evidence quality  
5. Action specificity  
6. GM readability  
7. Agency implementation usefulness  
8. Visual consistency  

**Minimum overall target: 9.0/10.**  
If below 9.0, document residual gaps honestly — do not inflate.

On pass, set status to **`AUDIT_VALIDATED`**.

### 7. Founder approval
1. Present the founder package (PDFs, scores, evidence notes, residual confirmations).
2. Only on explicit founder authorization, set **`FOUNDER_APPROVED_FOR_CUSTOMER`** with `founderApprovedForCustomer=true`.
3. Distribution still requires separate customer publication authorization before `CUSTOMER_READY` / `PUBLISHED`.

### 8. Customer distribution authorization
Requires **all** of:
- Status `CUSTOMER_READY` or `PUBLISHED`
- `founderApprovedForCustomer=true`
- `founderPublicationAuthorized=true`
- `customerPublicationAuthorized=true`

### 9. Future monthly action tracking (proposal only)
- Bridge file: `proposed-monthly-action-register-bridge.json`
- Preserve ADP monthly action-register integration design
- **Do not** activate production imports in this SOP version

---

## AI Admin workflow checklist

1. Select eligible hotel  
2. Confirm certified ADP baseline  
3. Generate / load audit  
4. Review research findings  
5. Preview staged PDF  
6. Review recommendations + evidence  
7. Approve / reject / defer actions  
8. Preserve versions  
9. Mark `AUDIT_VALIDATED`  
10. Record founder approval (`FOUNDER_APPROVED_FOR_CUSTOMER`) only when authorized  
11. Enforce publication permissions  

UI: AI Demand Admin → **AI Visibility Audit** tab.

---

## Reusability

This procedure applies across brands, markets, and property types (luxury resort, urban full-service, marina resort, golf resort, soft brand, etc.) as long as a certified ADP baseline exists and hotel-specific evidence is gathered afresh.
