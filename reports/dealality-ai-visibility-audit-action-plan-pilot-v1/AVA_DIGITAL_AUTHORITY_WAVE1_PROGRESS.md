# AVA Digital Authority — Wave 1 Progress Report

**Generated:** 2026-11-05T14:07:13.226Z
**Paid research:** $0 · **Publication:** not authorized · **Deployment:** none

## Wave 1 hotels

| Hotel | Status | Gate | DA sources | Opportunities | DA actions | PDF | Blockers |
|---|---|---|---|---|---|---|---|
| AC Hotel A Coruña | `AUDIT_VALIDATED` | PASS | 4 | 3 | AVA-ACOR-DA-001 | staged | 0 |
| Grand Fiesta Americana Chapultepec | `AUDIT_VALIDATED` | PASS | 1 | 2 | AVA-GFAC-DA-001 | staged | 0 |
| Fiesta Americana Condesa Cancún All Inclusive | `AUDIT_VALIDATED` | PASS | 2 | 2 | AVA-FACC-DA-001 | staged | 0 |
| Waterstone Resort & Marina | `VALIDATION_REQUIRED` | FAIL | 2 | 2 | AVA-WS-DA-001 | staged | 2 |
| Castillo Hotel Son Vida, a Luxury Collection Hotel, Mallorca | `VALIDATION_REQUIRED` | FAIL | 3 | 2 | AVA-CSV-DA-001 | staged | 1 |
| Sheraton Mallorca Arabella Golf Hotel | `VALIDATION_REQUIRED` | FAIL | 3 | 2 | AVA-SMAG-DA-001 | staged | 1 |

## Findings summary

### AC Hotel A Coruña

AC Hotel A Coruña already appears on the A Coruña Convention & Visitors Bureau hotel and MICE listings, which is a strong meetings-demand authority signal. The practical gap is accuracy and completeness of those CVB facts versus Marriott, plus ensuring English/Spanish CVB profiles and the MICE guide stay aligned so planners and AI systems do not reconcile conflicting capacity or address details.

Top opportunities:
- Reconcile CVB MICE profile with Marriott source of truth
- Keep AC featured in annual Guía MICE updates
- Confirm canonical hotel URL on CVB profiles

PDF: `reports/dealality-ai-visibility-audit-action-plan-pilot-v1/ac-hotel-a-coruna/pdf-staged/Dealality - AI Visibility Audit & Action Plan - AC Hotel A Coruña - NOVEMBER2026.pdf` · SHA-256 `22082c75164dab4f22c5bcf171da941d0e68bda058d3c35b1b46e53345168d81` · prior preserved: n/a

### Grand Fiesta Americana Chapultepec

Public research did not surface a strong, hotel-specific Mexico City / Chapultepec destination-board lodging profile comparable to the A Coruña CVB case. Authority coverage today is concentrated on brand and OTA pages. The highest-value next step is securing verified destination or cultural-district listings near Chapultepec/Reforma rather than chasing generic directory volume.

Top opportunities:
- Pursue Chapultepec / Reforma cultural-district lodging listing
- Protect brand-page factual consistency for AI reuse

PDF: `reports/dealality-ai-visibility-audit-action-plan-pilot-v1/grand-fiesta-americana-chapultepec/pdf-staged/Dealality - AI Visibility Audit & Action Plan - Grand Fiesta Americana Chapultepec - NOVEMBER2026.pdf` · SHA-256 `f5efaf59d0ed9167878da4da5fb434800c1db5c6b54ba87be8eba8eacc323d8e` · prior preserved: n/a

### Fiesta Americana Condesa Cancún All Inclusive

The resort’s strongest verified digital surfaces today are brand and major leisure-distribution pages. Official Cancún tourism content reviewed in this pass did not show a hotel-specific lodging fiche. Closing that destination-authority gap—and keeping tour-operator profiles factually aligned—is the priority for leisure AI discovery context.

Top opportunities:
- Seek official Cancún tourism lodging inclusion
- Keep major leisure-distributor facts aligned

PDF: `reports/dealality-ai-visibility-audit-action-plan-pilot-v1/fiesta-americana-condesa-cancun-ai/pdf-staged/Dealality - AI Visibility Audit & Action Plan - Fiesta Americana Condesa Cancún All Inclusive - NOVEMBER2026.pdf` · SHA-256 `034a5d8967a1967bc9b2bd484181b1e167e350092d982b2ea521d57e43a78ef3` · prior preserved: n/a

### Waterstone Resort & Marina

Local leisure guides already mention Waterstone’s marina dock partner and on-site SeaSpray dining—useful verified external mentions. The larger authority risk remains domain identity (waterstoneboca.com vs legacy waterstoneresort.com) and thin CVB/DMO lodging coverage relative to the marina positioning.

Top opportunities:
- Seek Boca / Palm Beach destination lodging inclusion
- Formalize marina-partner and dining guide consistency

PDF: `reports/dealality-ai-visibility-audit-action-plan-pilot-v1/waterstone-boca-raton/pdf-staged/Dealality - AI Visibility Audit & Action Plan - Waterstone Resort & Marina - NOVEMBER2026.pdf` · SHA-256 `ec38d4ee43f11113aa1b7f5c2fdde99b7afc4d78aa5eea46bfceb8bf3d9caf22` · prior preserved: n/a

Unresolved blockers:
- REQUIRES_MANUAL_REVIEW: https://www.hilton.com/en/hotels/bctbuqq-waterstone-resort-and-marina-boca-raton/ (Automated access restricted (HTTP_403). Not treated as a definitive broken link.)
- REQUIRES_MANUAL_REVIEW: https://www.waterstoneresort.com/ (Automated access restricted (FETCH_FAILED). Not treated as a definitive broken link.)

### Castillo Hotel Son Vida, a Luxury Collection Hotel, Mallorca

Visit Palma destination inventories already list Castillo Hotel Son Vida alongside Sheraton Mallorca Arabella Golf—strong destination authority for luxury and meetings demand. Prioritize keeping Visit Palma hotel and Urban Meetings profiles accurate (spa, golf, adults-only) and ensuring golf-specialist mentions stay fact-true.

Top opportunities:
- Proof Visit Palma Urban Meetings facts after spa reconciliation
- Keep Golfbreaks and golf-specialist pages aligned

PDF: `reports/dealality-ai-visibility-audit-action-plan-pilot-v1/castillo-hotel-son-vida/pdf-staged/Dealality - AI Visibility Audit & Action Plan - Castillo Hotel Son Vida, a Luxury Collection Hotel, Mallorca - NOVEMBER2026.pdf` · SHA-256 `7074bccd241516e14a494b9aee6a06cc451fce88acb49fce9f0edab19d4ff799` · prior preserved: n/a

Unresolved blockers:
- REQUIRES_MANUAL_REVIEW: https://www.arabellagolf.com/ (Automated access restricted (FETCH_FAILED). Not treated as a definitive broken link.)

### Sheraton Mallorca Arabella Golf Hotel

Sheraton Mallorca Arabella Golf is already listed on Visit Palma destination hotel inventories and Urban Meetings materials alongside Castillo Son Vida. Golf-media coverage (e.g., Golf News) further supports the 63-hole Arabella proposition. The priority is keeping destination and golf-specialist facts synchronized with Marriott—not inventing new directory volume.

Top opportunities:
- Refresh Visit Palma meetings and hotel facts for golf + family offer
- Maintain golf-media factual consistency

PDF: `reports/dealality-ai-visibility-audit-action-plan-pilot-v1/sheraton-mallorca-arabella-golf/pdf-staged/Dealality - AI Visibility Audit & Action Plan - Sheraton Mallorca Arabella Golf Hotel - NOVEMBER2026.pdf` · SHA-256 `b9d7f1175daa53bbd261c0df5f97900323785a72d91d60c26e47fda83a0eb9df` · prior preserved: n/a

Unresolved blockers:
- REQUIRES_MANUAL_REVIEW: https://www.arabellagolf.com/ (Automated access restricted (FETCH_FAILED). Not treated as a definitive broken link.)

## Totals

{
  "hotels": 6,
  "pdfReady": 6,
  "gatePass": 3,
  "gateFail": 3,
  "validationRequired": 3
}

Stop for founder review before Wave 2 (Ambridge + Bethesda enhancement).
