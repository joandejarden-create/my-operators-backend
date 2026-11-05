# AVA Digital Content Readiness Certification

**Contract:** `AVA_DIGITAL_CONTENT_READINESS_V1`  
**Generated:** 2026-11-05  
**Pilot:** Six commercial-priority hotels + regression (Bethesda, Wyndham San Diego Bayside, Spice Island)  
**Paid research:** **$0**  
**Publication / founder approval:** unchanged · not authorized  
**Machine results:** `AVA_DIGITAL_CONTENT_READINESS_PILOT_RESULTS.json`

---

## Final verdict: `PASS_WITH_LIMITATIONS`

| Cohort | Result |
|---|---|
| Six-hotel DCR pilot | **6/6 PASS** (evidence gate + DCR merge + PDF) |
| Regression (Bethesda / Wyndham SD / Spice) | **3/3 PASS** (no NIH broken URL; gates hold) |
| Portfolio auto-expansion | **Not started** (per instruction — wait for pilot pass) |

Limitations are research-scope (public partial; brand pages sometimes bot-limited; Google/Tripadvisor/OTA deep scrapes not exhaustively automated) — not unresolved customer-facing broken links in the pilot set.

---

## 1. Architecture and files changed

| Path | Role |
|---|---|
| `lib/ai-visibility-audit-action-plan/digital-content-readiness-v1.js` | Core module: source inventory, Hotel Facts Register, traveler-question coverage, platform consistency, correction ownership, idempotent merge |
| `lib/ai-visibility-audit-action-plan/digital-content-readiness/wave1-pilot-dcr-v1.json` | Six-hotel pilot enrichment |
| `lib/ai-visibility-audit-action-plan/build-audit-pdf-html-v1.js` | Customer PDF section **AI Discoverability & Digital Content Readiness** |
| `public/css/adp-ai-visibility-audit-report-v1.css` | DCR table styles |
| `lib/ai-visibility-audit-action-plan/modules-v1.js` | Module `M11_DIGITAL_CONTENT_READINESS` |
| `lib/ai-visibility-audit-action-plan/ava-full-e2e-pipeline-v1.js` | Full E2E stages expanded (source discovery, TQ/consistency, DCR mandatory) |
| `lib/ai-visibility-audit-action-plan/evidence-link-validation-v1.js` | Collect DCR evidence URLs into permanent gate |
| `scripts/ava-dcr-pilot-six-certify-v1.mjs` | Pilot apply + evidence/PDF certify + regression |
| `scripts/test-ava-digital-content-readiness-idempotency-v1.mjs` | Idempotency regression |

Reusable storage: DCR blocks persist on AVA payloads under durable `AVA_STORAGE_MODE=local` audit versions / PDF archive (same abstraction as DA). No Railway volume assumed.

---

## 2. Source-discovery methodology

For each hotel:

1. Classify **brand property page** vs **hotel-operated website** separately (never assume one replaces the other).
2. Inventory hotel-operated subpages (meetings, dining, spa) when reachable.
3. Add commercially relevant destination / specialist sources already verified in Digital Authority work.
4. Record controlling organization, purpose, last-checked date, and evidence URL.
5. Mark brand pages `UNABLE_TO_VERIFY` when HTTP/browser probes fail — **not** as factual defects.

OTAs / GBP / Tripadvisor are modeled in the schema and used when material (e.g. Condesa dining conflict). Exhaustive multi-OTA scraping was not authorized and is not required for pilot PASS.

---

## 3. Hotel Facts Register design

Each fact stores: `attributeId`, `value`, `source`, `evidenceUrl`, `verificationDate`, `confidence`, `authorityStatus`, `hotelConfirmationRequired`.

Authority rule: brand ≠ automatic truth; hotel site ≠ automatic truth. Conflicts use `HOTEL_CONFIRMATION_REQUIRED`.

---

## 4. Traveler Question Coverage

Mandatory matrix fields: traveler intent, information required, brand coverage, hotel-site coverage, third-party coverage, classification (`ANSWERED_AND_VERIFIED` / `PARTIALLY_ANSWERED` / `INCONSISTENT_INFORMATION` / `MISSING_INFORMATION` / `UNABLE_TO_VERIFY`), verified gaps, recommended action, responsible party.

Questions are tied to ADP-relevant demand (meetings, marina, golf, MICE, dining, luxury wellness) — not generic SEO queries. Missing content is framed as a **plausible improvement**, not a proven AI ranking cause.

---

## 5. Cross-platform consistency

Classifications: `CONSISTENT` / `INCONSISTENT` / `MISSING` / `OUTDATED` / `HOTEL_CONFIRMATION_REQUIRED` / `UNABLE_TO_VERIFY`.

Critical Waterstone rule applied: Hilton Curio meeting figure is **not** treated as verified customer evidence while the brand URL errors; hotel-operated 18,899 sq ft stands as a verified publication awaiting hotel definition sign-off.

---

## 6. Correction ownership workflow

Each material correction carries: finding ID, platform, URL, current vs approved information, recommended correction, priority, responsible organization/role, required approval, status lifecycle (`IDENTIFIED → FACT_CONFIRMATION → … → VERIFIED`), completion criteria, verification method.

Unknown ownership uses `RESPONSIBLE_PARTY_TO_CONFIRM`. Submission ≠ implementation.

DCR actions merge idempotently; IDs do not overwrite core AVA or Digital Authority actions.

---

## 7. Six-hotel results

| Hotel | Platforms | Brand page | Hotel site | TQ assessed | Material gaps | Discrepancies / confirmations | Corrections | Evidence | PDF |
|---|---:|---|---|---:|---:|---:|---:|---|---|
| Waterstone Resort & Marina | 5 | Unable to verify (prior error) | Yes | 3 | 2 | Meetings HOTEL_CONFIRMATION_REQUIRED | 1 | PASS | Regenerated |
| Castillo Hotel Son Vida | 4 | Yes (Marriott) | Not separate | 3 | 2 | Spa 900/990 confirmation | 1 | PASS | Regenerated |
| Sheraton Mallorca Arabella Golf | 4 | Yes (Marriott) | Not separate | 2 | 2 | Golf+family packaging; arabellagolf excluded | 1 | PASS | Regenerated |
| AC Hotel A Coruña | 3 | Yes (Marriott) | Not primary | 2 | 2 | CVB vs Marriott MICE + missing official href | 1 | PASS | Regenerated |
| Grand Fiesta Americana Chapultepec | 1 | Yes (Travelty) | Not separate | 2 | 2 | Capacity language confirmation | 1 | PASS | Regenerated |
| Fiesta Americana Condesa Cancún AI | 2 | Yes | Not separate | 2 | 2 | Dining count brand vs OTA | 1 | PASS | Regenerated |

Idempotency: triple-merge clean on all six (no duplicate DCR action IDs).

---

## 8. Important findings and proposed corrections

### Waterstone
- **Finding:** Hotel site publishes 18,899 sq ft meetings; Hilton figure not re-verified.  
- **Correction:** Events leadership signs indoor/outdoor/total definitions → update hotel meetings page → align Hilton when reachable.  
- **Owner:** Hotel Sales & Marketing (+ Hilton content when accessible).

### Castillo
- **Finding:** Spa size 900 vs 990 m² conflict on brand materials; Visit Palma already backlinks Marriott URL.  
- **Correction:** Approve one spa m²; update Marriott + proof Visit Palma.  
- **Owner:** Hotel Operations / Brand Content.

### Sheraton Mallorca
- **Finding:** Golf/family offer exists but is under-packaged; arabellagolf.com TLS excluded.  
- **Correction:** Approved golf+Kids Club block on Marriott + Visit Palma.  
- **Owner:** Hotel Sales & Marketing.

### AC Hotel A Coruña
- **Finding:** CVB MICE facts conflict with Marriott; CVB lacks official-site hyperlink.  
- **Correction:** Sign Marriott record; push CVB capacities + Marriott URL href.  
- **Owner:** Hotel Sales & Marketing / CVB account.

### Grand Fiesta Americana Chapultepec
- **Finding:** Capacity language conflict; destination lodging thin.  
- **Correction:** Signed capacity schedule on brand page; then one destination listing.  
- **Owner:** Hotel Sales + Brand Content.

### Fiesta Americana Condesa Cancún
- **Finding:** Dining outlet counts conflict across brand/distributor.  
- **Correction:** Approve outlet definition; align brand + OTAs; then Turismo Cancún listing.  
- **Owner:** Hotel Sales / Revenue & Distribution.

---

## 9. Automated QA results

```text
node scripts/test-ava-digital-content-readiness-idempotency-v1.mjs  → ok
node scripts/ava-dcr-pilot-six-certify-v1.mjs
  pilot 6/6 PASS · regression 3/3 PASS · paid $0
```

Evidence gate: mandatory; skip throws. DCR URLs enter the permanent evidence collector. NIH Clinical Center `/about/visitor/hotels` remains absent on Bethesda regression.

---

## 10. PDF QA and archive verification

- New customer section: **AI Discoverability & Digital Content Readiness** (executive assessment, readiness table, platform correction priorities, traveler question gaps, next steps).
- Section numbering adjusts when DA + DCR both present.
- Prior PDFs preserved under `pdf-staged/pre-dcr-pilot/`.
- Staged via `stageAvaPdf` with SHA-256 checksums; founder flags unchanged.
- No internal status codes or Dealality pipeline jargon in customer tables (owners mapped to plain language).

---

## 11. Research cost

**$0.** Public partial only. Ahrefs/Semrush/Majestic not used.

---

## 12. Remaining limitations

1. Not a full crawl of every OTA/GBP/Tripadvisor field for every hotel.
2. Brand CDN bot blocks remain a verification constraint (classified `UNABLE_TO_VERIFY`, not “wrong content”).
3. Google Business Profile / Apple Maps / Bing Places deep verification not completed in this unpaid pilot.
4. Competitor TQ packaging comparisons are qualitative where peer page HTML was not fully harvested.
5. Portfolio (remaining 32) not auto-processed until this pilot certification.

---

## 13. Scalability assessment (38 hotels)

| Capability | Ready? |
|---|---|
| Schema + idempotent merge | Yes |
| PDF section + CSS | Yes |
| Full E2E stage contract | Yes |
| Evidence gate integration | Yes |
| Pilot quality bar demonstrated | Yes (6/6) |
| Remaining portfolio batch | Ready to reuse pilot script pattern after founder go-ahead |

Estimated steady-state effort with DCR: **+45–90 minutes per hotel** on top of existing AVA Full E2E (more when brand WAF blocks require destination replacements).

---

## Acceptance checklist (pilot)

| Criterion | Result |
|---|---|
| Brand vs hotel-operated assessed independently | PASS |
| Relevant external platforms identified | PASS (scope-appropriate) |
| Material findings evidence-backed | PASS |
| Traveler question coverage evaluated | PASS |
| Cross-platform discrepancies classified accurately | PASS |
| Approved vs confirmation-required facts distinguished | PASS |
| Corrections identify affected source + owner (or TO_CONFIRM) | PASS |
| No unsupported Hilton-as-verified Waterstone claim | PASS |
| No ADP measurement changes | PASS |
| Founder/publication flags unchanged | PASS |
| PDF layout + hyperlinks certified | PASS |
| Idempotent repeats | PASS |

**Do not proceed to automatic remaining-portfolio DCR until founder authorizes the next wave.**
