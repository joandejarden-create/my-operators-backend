# ADP/GDI Report Recovery Wave — Closeout (Nov 19, 2026)

**STOP FOR FOUNDER REVIEW BEFORE MERGE.** No new customer shares. No paid research. No unauthorized publication.

## 1. ADP complete count

| Metric | Before | After |
|---|---:|---:|
| COMPLETE | 29 | 30 |
| Admin inventory | 35 | 38 |
| REVIEW_REBUILDABLE | 5 | 0 |
| PDF_REBUILDABLE | 1 | 0 |
| REGISTRY_REPAIR | 3 | 0 |
| Authorized external links | 30 | 30 |

Mayflower moved COMPLETE (authorized share + rebuilt review/PDF). Eight rebuilt hotels remain `REQUIRES_FOUNDER_APPROVAL` for customer share minting (not authorized by this wave).

## 2. Review artifacts rebuilt (9)

| Hotel | Review ID | Period |
|---|---|---|
| The Mayflower Hotel | `adp_mr_mayflower_hotel_washingt_2026-10_v2` | `…20261013213022_f9bdd0` |
| Charlotte Marriott City Center | `adp_mr_charlotte_marriott_city__2026-11_v1` | `…20261105221934_1831f7` |
| Falls Church Marriott Fairview Park | `adp_mr_falls_church_marriott_fa_2026-11_v1` | `…20261105224238_bec296` |
| Nobu Hotel London Portman Square | `adp_mr_nobu_london_portman_2026-11_v1` | `…20261105235337_a53216` |
| Renaissance Arlington Capital View | `adp_mr_renaissance_arlington_ca_2026-11_v1` | `…20261106004624_f01f1e` |
| Tampa Airport Marriott | `adp_mr_tampa_airport_marriott_2026-11_v1` | `…20261106013505_73d688` |
| Fiesta Americana Condesa Cancún | `adp_mr_fiesta_americana_condesa_2026-10_v1` | `…20261022082249_9aac31` |
| Grand Fiesta Americana Chapultepec | `adp_mr_grand_fiesta_americana_c_2026-10_v1` | `…20261021225401_0726b1` |
| UMusic Hotel Madrid | `adp_mr_umusic_hotel_madrid_2026-11_v1` | `…20261103073304_4bec18` |

Prior review versions preserved in archive. `LIVE_PROVIDER_CALLS=0`.

## 3. Performance Review PDFs rebuilt

All 9 above — PDF status READY; available in AI Admin monthly-review PDF routes.

## 4. GDI PDF count

| Metric | Before | After |
|---|---:|---:|
| PDF READY | 11 | 11 |
| PDF rebuildable (eligible ops) | 2 applied earlier (Condesa, Chapultepec) | 0 remaining |
| Commercial PDF candidates left | — | 0 |

## 5. WATCH_ONLY PDF classifications

- **WATCH_ONLY** (PDF already on disk): The Westin Tampa Waterside (1)
- **AWAITING_REPORT_POLICY** (zero eligible ops; no auto empty commercial PDF): 9 hotels (AC A Coruña, Hilton NYTS, Castillo Son Vida, W Rome, Westin Grand München, NOW NOW NOHO, Cambridge Beaches, Sheraton Mallorca, Spice Island)
- **WATCH_ONLY_PDF_NOT_APPLICABLE**: 0

## 6. Previously authorized links repaired

- Root cause of false 401s: probes using raw `sht_…` token IDs (`malformed_share_capability`) or CLI dotenv secret divergence (`invalid_share_signature`).
- Repair: health check resolves sealed URLs via admin runtime (production-matching secret). **No tokens minted or rotated.**
- ACTIVE ADP sealed share health: **30/30 OK**, fail401=0, fail403=0.
- Mayflower production report: HTTP 200; local Performance Review PDF `%PDF` 255030 bytes.

## 7. Remaining 401/403 production share failures

**None** for ACTIVE ADP sealed shares when probed correctly.

## 8. Hotels awaiting new publication approval

**ADP share (reports ready, no mint):** Charlotte, Falls Church, Condesa, Chapultepec, Nobu London, Arlington, Tampa Airport, UMusic.

**GDI share (ADP≠GDI):** Mayflower, Nobu London, Condesa, Chapultepec, Radisson Santo Domingo — prepared only; no new GDI shares.

## 9. Hotels genuinely missing evidence

- ADP: `adp_smoke_test_synthetic` only (excluded from customer reports).
- GDI: 14 hotels classified `EVIDENCE_MISSING` (canonical opportunity data unavailable) — no paid research initiated.

## 10. Controls

- No new customer shares
- No paid AI research
- No unauthorized publication
- Durable CURRENT hotels added to admin inventory (Fiesta / Chapultepec / UMusic)
- Reconciler extended: WATCH_ONLY policy classes + sealed share health
