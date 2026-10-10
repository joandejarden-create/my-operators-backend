# Palexpo SA — Art Genève 2027 forensic

## Record
`gdi_opp_ycamp_art_geneve_2027_palexpo_sa_venue_operator`

## Answers
1. **Accommodation control?** Not evidenced — no official housing page / hotel list on the record.
2. **Buyer vs venue?** **Venue operator / context** — not the traveling buyer. Prior stamp “Palexpo Hotel Reservation” was speculative.
3. **Art Genève / Palexpo homepage as contact?** `artgeneve.ch` home = SOURCE_PAGE; `palexpo.ch` root = GENERAL_ORG_CONTACT — **not** a sales contact path.
4. **Overflow Only supported?** **NO** — stripped to UNKNOWN / non-OVERFLOW type.
5. **Competitor-hotel association?** Thesis claimed competitor lodging with **UNKNOWN** hotels — **UNSUPPORTED**; sanitized.
6. **Geneva/Palexpo geography?** Reconciled to `Palexpo, Geneva, Switzerland`.
7. **Evidence Confidence 0 verified?** Stale explanation ignored `knownVsEstimated.verified` — rebuild wired.
8. **Duplicate Hotel Fit?** Overall + component both labeled “Hotel Fit” — UI **Overall Hotel Fit**.
9. **Tournament housing lead?** Overflow template leak on non-sports — fixed for Art Genève.
10. **READY vs qualify-before-ready?** Contradictory — status after audit: **FUTURE_WATCH**.

## Root cause
Homepage + speculative Palexpo Housing desk treated as buyer path; OVERFLOW_ONLY without housing-program evidence; competitor UNKNOWN claim; whyNow contradicted READY; sports action / evidence / geo mapping bugs.

## After
**FUTURE_WATCH** — confirm public housing/function path before customer-ready.
