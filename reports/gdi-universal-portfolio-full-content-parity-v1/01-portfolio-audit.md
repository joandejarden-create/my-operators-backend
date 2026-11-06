# GDI Universal Portfolio Full-Content Parity Audit

- Status: **GDI_UNIVERSAL_PORTFOLIO_FULL_CONTENT_PARITY_PASS**
- Generated: 2026-11-06T10:50:19.986Z
- Inventory root: `reports/gdi-universal-full-presentation-parity-v1/work-gdi-published-inventory`

## Summary

- TOTAL_GDI_HOTELS: 18
- PUBLISHED_HOTELS: 1
- VERIFIED_HOTELS: 1
- MISMATCH_HOTELS: 0
- NOT_PUBLISHED_HOTELS: 8
- NOT_VERIFIABLE_HOTELS: 9
- VERIFICATION_PENDING_HOTELS: 0
- TOTAL_OPPORTUNITIES_VERIFIED: 7
- TOTAL_FIELDS_COMPARED: 12805
- TOTAL_CRITICAL_DIFFERENCES: 0

## Hotels

| Hotel | Published | Parity | Schema-ready | Legacy | Share | Migration |
| --- | ---: | --- | ---: | ---: | --- | --- |
| AC Hotel A Coruña | 0 | NOT_PUBLISHED | 0 | 0 | NONE | — |
| Bethesda Marriott | 0 | NOT_VERIFIABLE | 0 | 0 | ACTIVE | LEGACY_SHARE_WITHOUT_CANONICAL_REGISTRY |
| Cambridge Beaches Resort & Spa | 0 | NOT_VERIFIABLE | 0 | 0 | ACTIVE | LEGACY_SHARE_WITHOUT_CANONICAL_REGISTRY |
| Castillo Hotel Son Vida, a Luxury Collection Hotel, Mallorca | 0 | NOT_PUBLISHED | 0 | 0 | NONE | — |
| Hilton New York Times Square | 0 | NOT_VERIFIABLE | 0 | 0 | ACTIVE | LEGACY_SHARE_WITHOUT_CANONICAL_REGISTRY |
| Nobu Hotel London Portman Square | 0 | NOT_PUBLISHED | 0 | 0 | NONE | — |
| NOW NOW NOHO | 0 | NOT_VERIFIABLE | 0 | 0 | ACTIVE | LEGACY_SHARE_WITHOUT_CANONICAL_REGISTRY |
| Radisson Hotel Santo Domingo | 0 | NOT_PUBLISHED | 0 | 0 | NONE | — |
| Renaissance New York Times Square Hotel | 0 | NOT_VERIFIABLE | 0 | 0 | ACTIVE | LEGACY_SHARE_WITHOUT_CANONICAL_REGISTRY |
| Sheraton Mallorca Arabella Golf Hotel | 0 | NOT_PUBLISHED | 0 | 0 | NONE | — |
| Spice Island Beach Resort | 7 | VERIFIED | 7 | 0 | NONE | — |
| The Mayflower Hotel, Autograph Collection | 0 | NOT_PUBLISHED | 0 | 0 | NONE | — |
| The Westin Grand München | 0 | NOT_PUBLISHED | 0 | 0 | NONE | — |
| The Westin Tampa Waterside | 0 | NOT_VERIFIABLE | 0 | 0 | ACTIVE | LEGACY_SHARE_WITHOUT_CANONICAL_REGISTRY |
| W Rome | 0 | NOT_VERIFIABLE | 0 | 0 | ACTIVE | LEGACY_SHARE_WITHOUT_CANONICAL_REGISTRY |
| Waterstone Resort & Marina Boca Raton | 0 | NOT_VERIFIABLE | 0 | 0 | ACTIVE | LEGACY_SHARE_WITHOUT_CANONICAL_REGISTRY |
| Wyndham San Diego Bayside | 0 | NOT_VERIFIABLE | 0 | 0 | ACTIVE | LEGACY_SHARE_WITHOUT_CANONICAL_REGISTRY |
| YOTEL Geneva Lake | 0 | NOT_PUBLISHED | 0 | 0 | NONE | — |

## Critical differences

_None._

## Safety notes

- Read-only audit. No production writes. No share token rotation.
- Unpublished and NOT_VERIFIABLE hotels are reported separately and never counted as PASS.
- Legacy share hotels without canonical registry snapshots are migration candidates.
