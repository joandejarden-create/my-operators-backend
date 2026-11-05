# AVA Digital Platform Coverage Report

**Scope:** Three-hotel zero-cost HTTP assessment + portfolio gap analysis
**Research mode:** ZERO_COST_HTTP
**Paid research:** $0

## Method

1. Dynamically select platforms by hotel demand positioning.
2. Reuse URLs already present in identity graph / DA / DCR evidence.
3. Probe with free HTTP only.
4. Classify `VERIFIED_CORRECT` (reachable), `ACCESS_RESTRICTED`, `IDENTIFIED_NOT_ASSESSED`, `NOT_APPLICABLE`, `NOT_FOUND`.
5. Never mark an unassessed listing as incorrect.

## Per-hotel coverage

### Waterstone Resort & Marina

| Platform | Category | URL | Status | Notes |
|---|---|---|---|---|
| Waterstone hotel-operated website | HOTEL_OPERATED_WEBSITE | https://www.waterstoneboca.com/ | VERIFIED_CORRECT | Page reachable; no Schema.org JSON-LD detected in HTML sample. |
| Waterstone meetings page | HOTEL_OPERATED_SUBPAGE | https://www.waterstoneboca.com/boca-raton-meeting-space/ | VERIFIED_CORRECT | Page reachable; no Schema.org JSON-LD detected in HTML sample. |
| Waterstone dining page | HOTEL_OPERATED_SUBPAGE | https://waterstoneboca.com/boca-raton-restaurants/ | VERIFIED_CORRECT | Page reachable; no Schema.org JSON-LD detected in HTML sample. |
| Hilton Curio brand property page | BRAND_PROPERTY_PAGE | — | ACCESS_RESTRICTED | Automated access restricted; not treated as a broken customer page. |
| Great Locations Boca Raton guide | SPECIALIST_DIRECTORY | https://greatlocations.com/things-to-do-in-boca-raton-this-summer/ | VERIFIED_CORRECT | Page reachable; structured data (Schema.org / JSON-LD) detected. |
| Brand property page | BRAND_PROPERTY_PAGE | — | IDENTIFIED_NOT_ASSESSED | Relevant platform identified; no verified listing URL in current evidence graph. Deeper discovery requires authorized se |
| Google Business Profile / Google Maps | GOOGLE_BUSINESS_PROFILE | — | IDENTIFIED_NOT_ASSESSED | Relevant platform identified; no verified listing URL in current evidence graph. Deeper discovery requires authorized se |
| Tripadvisor | TRIPADVISOR | — | IDENTIFIED_NOT_ASSESSED | Relevant platform identified; no verified listing URL in current evidence graph. Deeper discovery requires authorized se |
| Booking.com | OTA_BOOKING | — | IDENTIFIED_NOT_ASSESSED | Relevant platform identified; no verified listing URL in current evidence graph. Deeper discovery requires authorized se |
| Expedia / Hotels.com | OTA_EXPEDIA | — | IDENTIFIED_NOT_ASSESSED | Relevant platform identified; no verified listing URL in current evidence graph. Deeper discovery requires authorized se |
| Apple Maps | APPLE_MAPS | — | IDENTIFIED_NOT_ASSESSED | Relevant platform identified; no verified listing URL in current evidence graph. Deeper discovery requires authorized se |
| Bing Places / Bing Maps | BING_PLACES | — | IDENTIFIED_NOT_ASSESSED | Relevant platform identified; no verified listing URL in current evidence graph. Deeper discovery requires authorized se |
| Cvent | CVENT | — | IDENTIFIED_NOT_ASSESSED | Relevant platform identified; no verified listing URL in current evidence graph. Deeper discovery requires authorized se |
| Destination / CVB / tourism board | DESTINATION_CVB | — | IDENTIFIED_NOT_ASSESSED | Relevant platform identified; no verified listing URL in current evidence graph. Deeper discovery requires authorized se |

Summary: reachable=4, identified_not_assessed=9, access_restricted=1, new_corrections=1

### AC Hotel A Coruña

| Platform | Category | URL | Status | Notes |
|---|---|---|---|---|
| Marriott AC Hotel A Coruña overview | BRAND_PROPERTY_PAGE | https://www.marriott.com/es/hotels/lcgco-ac-hotel-a-coruna/overview/ | ACCESS_RESTRICTED | Automated access restricted; not treated as a broken customer page. |
| A Coruña CVB hotel fiche | DESTINATION_CVB | https://acorunacb.com/ficha/ac-by-marriot-a-coruna/ | VERIFIED_CORRECT | Page reachable; no Schema.org JSON-LD detected in HTML sample. |
| A Coruña CVB MICE fiche | DESTINATION_CVB | https://acorunacb.com/ficha/espacios-mice-ac-by-marriot-a-coruna/ | VERIFIED_CORRECT | Page reachable; no Schema.org JSON-LD detected in HTML sample. |
| Hotel-operated website | HOTEL_OPERATED_WEBSITE | — | IDENTIFIED_NOT_ASSESSED | Relevant platform identified; no verified listing URL in current evidence graph. Deeper discovery requires authorized se |
| Google Business Profile / Google Maps | GOOGLE_BUSINESS_PROFILE | — | IDENTIFIED_NOT_ASSESSED | Relevant platform identified; no verified listing URL in current evidence graph. Deeper discovery requires authorized se |
| Tripadvisor | TRIPADVISOR | — | IDENTIFIED_NOT_ASSESSED | Relevant platform identified; no verified listing URL in current evidence graph. Deeper discovery requires authorized se |
| Booking.com | OTA_BOOKING | — | IDENTIFIED_NOT_ASSESSED | Relevant platform identified; no verified listing URL in current evidence graph. Deeper discovery requires authorized se |
| Expedia / Hotels.com | OTA_EXPEDIA | https://en.acorunahotels.com/ac-hotel-a-coruna-by-marriott | VERIFIED_CORRECT | Listing URL discovered in prior research graph and reachable via HTTP. Detailed field-level fact audit still requires ho |
| Apple Maps | APPLE_MAPS | — | IDENTIFIED_NOT_ASSESSED | Relevant platform identified; no verified listing URL in current evidence graph. Deeper discovery requires authorized se |
| Bing Places / Bing Maps | BING_PLACES | — | IDENTIFIED_NOT_ASSESSED | Relevant platform identified; no verified listing URL in current evidence graph. Deeper discovery requires authorized se |
| Cvent | CVENT | — | IDENTIFIED_NOT_ASSESSED | Relevant platform identified; no verified listing URL in current evidence graph. Deeper discovery requires authorized se |
| Wedding directory | SPECIALIST_DIRECTORY | — | NOT_APPLICABLE | Not prioritized for this hotel’s demand positioning in this pass. |
| Golf / spa / marina specialist | SPECIALIST_DIRECTORY | — | IDENTIFIED_NOT_ASSESSED | Relevant platform identified; no verified listing URL in current evidence graph. Deeper discovery requires authorized se |

Summary: reachable=3, identified_not_assessed=8, access_restricted=1, new_corrections=0

### Castillo Hotel Son Vida, a Luxury Collection Hotel, Mallorca

| Platform | Category | URL | Status | Notes |
|---|---|---|---|---|
| Marriott Luxury Collection brand page | BRAND_PROPERTY_PAGE | https://www.marriott.com/en-us/hotels/pmilc-castillo-hotel-son-vida-a-luxury-collection-hotel-mallorca/overview/ | ACCESS_RESTRICTED | Automated access restricted; not treated as a broken customer page. |
| Marriott events page | BRAND_PROPERTY_PAGE | https://www.marriott.com/en-us/hotels/pmilc-castillo-hotel-son-vida-a-luxury-collection-hotel-mallorca/events/ | VERIFIED_CORRECT | Page reachable; structured data (Schema.org / JSON-LD) detected. |
| Visit Palma hotel listing | DESTINATION_CVB | http://www.visit-palma.com/hotel-castillo-son-vida | VERIFIED_CORRECT | Page reachable; structured data (Schema.org / JSON-LD) detected. |
| Golfbreaks listing | SPECIALIST_DIRECTORY | https://www.golfbreaks.com/en-gb/holidays/palma/castillo-hotel-son-vida/ | VERIFIED_CORRECT | Page reachable; structured data (Schema.org / JSON-LD) detected. |
| Hotel-operated website | HOTEL_OPERATED_WEBSITE | — | IDENTIFIED_NOT_ASSESSED | Relevant platform identified; no verified listing URL in current evidence graph. Deeper discovery requires authorized se |
| Google Business Profile / Google Maps | GOOGLE_BUSINESS_PROFILE | — | IDENTIFIED_NOT_ASSESSED | Relevant platform identified; no verified listing URL in current evidence graph. Deeper discovery requires authorized se |
| Tripadvisor | TRIPADVISOR | — | IDENTIFIED_NOT_ASSESSED | Relevant platform identified; no verified listing URL in current evidence graph. Deeper discovery requires authorized se |
| Booking.com | OTA_BOOKING | — | IDENTIFIED_NOT_ASSESSED | Relevant platform identified; no verified listing URL in current evidence graph. Deeper discovery requires authorized se |
| Expedia / Hotels.com | OTA_EXPEDIA | — | IDENTIFIED_NOT_ASSESSED | Relevant platform identified; no verified listing URL in current evidence graph. Deeper discovery requires authorized se |
| Apple Maps | APPLE_MAPS | — | IDENTIFIED_NOT_ASSESSED | Relevant platform identified; no verified listing URL in current evidence graph. Deeper discovery requires authorized se |
| Bing Places / Bing Maps | BING_PLACES | — | IDENTIFIED_NOT_ASSESSED | Relevant platform identified; no verified listing URL in current evidence graph. Deeper discovery requires authorized se |
| Cvent | CVENT | — | IDENTIFIED_NOT_ASSESSED | Relevant platform identified; no verified listing URL in current evidence graph. Deeper discovery requires authorized se |

Summary: reachable=3, identified_not_assessed=8, access_restricted=1, new_corrections=0

## Portfolio implication

GBP, OTAs, and maps remain largely `IDENTIFIED_NOT_ASSESSED` until listing-URL discovery and field audit are founder-authorized. Official brand/hotel pages and known CVB/specialist URLs can be deepened at $0 where HTTP succeeds.

## Permanent module

- `platform-coverage-assessment-v1.js`
- `digital-platform-correction-register-v1.js`
