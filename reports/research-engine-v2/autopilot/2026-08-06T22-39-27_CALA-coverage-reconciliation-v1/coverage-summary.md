# Production Census Coverage Reconciliation v1

**Status:** `production_census_coverage_reconciliation_v1_partial_missing_remaining`
**Objective:** `coverage-reconciliation-v1`
**Region:** CALA
**Parent company:** Marriott
**Brand filter:** Sheraton
**Write target:** Hotel Property Census (`tbl9aY5ijiuIzzWam`)
**Airtable writes:** no (controlled)

## Summary

- Official inventory count: **388**
- Census inventory count (scoped brand): **6**
- Exact matches: **336**
- Probable matches: **0**
- Missing High: **52**
- Missing steward: **0**
- Duplicate risks: **0**
- Source blocked: **0**
- Source insufficient: **0**
- Inserted: **0**
- Stewarded (held): **0**

## Official sources used

- Marriott: Official country hotel-sitemaps (MARSHA + property URL); HQV not used for discovery
- VIC_evidence: official_family_directory_adapter
- Sheraton destination page (secondary / JS shell — not sole SoT): https://sheraton.marriott.com/es-XM/destinos-hotel/
- Primary Marriott SoT: country hotel-sitemaps ([object Object])

## Brand rollup

| Brand | Official | Census | Missing | Coverage % | Action |
| --- | ---: | ---: | ---: | ---: | --- |
| Sheraton | 388 | 6 | 52 | 86.6 | insert_high_confidence_missing |

## Missing hotels (sample)

- **The Ocean Club, a Luxury Collection Resort, Costa Norte** (Sheraton, ?, Dominican Republic) — `missing_high_confidence` — https://www.marriott.com/en-us/hotels/poplc-the-ocean-club-a-luxury-collection-resort-costa-norte/overview — MARSHA/code: POPLC
- **Sanctuary Cap Cana, a Luxury Collection Resort, Dominican Republic, Adult All-Inclusive** (Sheraton, ?, Dominican Republic) — `missing_high_confidence` — https://www.marriott.com/en-us/hotels/pujlc-sanctuary-cap-cana-a-luxury-collection-resort-dominican-republic-adult-all-inclusive/overview — MARSHA/code: PUJLC
- **JW Marriott Hotel Santo Domingo** (Sheraton, ?, Dominican Republic) — `missing_high_confidence` — https://www.marriott.com/en-us/hotels/sdqjw-jw-marriott-hotel-santo-domingo/overview — MARSHA/code: SDQJW
- **The Residences at The St. Regis Cap Cana Resort** (Sheraton, ?, Dominican Republic) — `missing_high_confidence` — https://www.marriott.com/en-us/hotels/pujrx-the-residences-at-the-st-regis-cap-cana-resort/overview — MARSHA/code: PUJRX
- **The St. Regis Cap Cana Resort** (Sheraton, ?, Dominican Republic) — `missing_high_confidence` — https://www.marriott.com/en-us/hotels/pujxr-the-st-regis-cap-cana-resort/overview — MARSHA/code: PUJXR
- **W Punta Cana, Adult All-Inclusive** (Sheraton, ?, Dominican Republic) — `missing_high_confidence` — https://www.marriott.com/en-us/hotels/pujwh-w-punta-cana-adult-all-inclusive/overview — MARSHA/code: PUJWH
- **Donoma Las Terrenas Resort & Villas, Autograph Collection** (Sheraton, ?, Dominican Republic) — `missing_high_confidence` — https://www.marriott.com/en-us/hotels/azsak-donoma-las-terrenas-resort-and-villas-autograph-collection/overview — MARSHA/code: AZSAK
- **Renaissance Santo Domingo Jaragua Hotel & Casino** (Sheraton, ?, Dominican Republic) — `missing_high_confidence` — https://www.marriott.com/en-us/hotels/sdqgw-renaissance-santo-domingo-jaragua-hotel-and-casino/overview — MARSHA/code: SDQGW
- **Sheraton Santo Domingo Hotel** (Sheraton, ?, Dominican Republic) — `missing_high_confidence` — https://www.marriott.com/en-us/hotels/sdqds-sheraton-santo-domingo-hotel/overview — MARSHA/code: SDQDS
- **The Westin Puntacana Resort** (Sheraton, ?, Dominican Republic) — `missing_high_confidence` — https://www.marriott.com/en-us/hotels/pujwi-the-westin-puntacana-resort/overview — MARSHA/code: PUJWI
- **Aloft by Marriott Santo Domingo Piantini** (Sheraton, ?, Dominican Republic) — `missing_high_confidence` — https://www.marriott.com/en-us/hotels/sdqal-aloft-santo-domingo-piantini/overview — MARSHA/code: SDQAL
- **Four Points by Sheraton Puntacana** (Sheraton, ?, Dominican Republic) — `missing_high_confidence` — https://www.marriott.com/en-us/hotels/pujfp-four-points-puntacana/overview — MARSHA/code: PUJFP
- **Four Points by Sheraton Santo Domingo** (Sheraton, ?, Dominican Republic) — `missing_high_confidence` — https://www.marriott.com/en-us/hotels/sdqfp-four-points-santo-domingo/overview — MARSHA/code: SDQFP
- **JW Marriott Costa Elena Resort & Spa, All-Inclusive** (Sheraton, ?, Costa Rica) — `missing_high_confidence` — https://www.marriott.com/en-us/hotels/lircj-jw-marriott-costa-elena-resort-and-spa-all-inclusive/overview — MARSHA/code: LIRCJ
- **JW Marriott Guanacaste Beach Resort** (Sheraton, ?, Costa Rica) — `missing_high_confidence` — https://www.marriott.com/en-us/hotels/sjojw-jw-marriott-guanacaste-beach-resort/overview — MARSHA/code: SJOJW
- **W Costa Rica - Reserva Conchal** (Sheraton, ?, Costa Rica) — `missing_high_confidence` — https://www.marriott.com/en-us/hotels/lirwh-w-costa-rica-reserva-conchal/overview — MARSHA/code: LIRWH
- **Planet Hollywood Costa Rica by Royalton, An Autograph Collection All-Inclusive Resort** (Sheraton, ?, Costa Rica) — `missing_high_confidence` — https://www.marriott.com/en-us/hotels/lirph-planet-hollywood-costa-rica-by-royalton-an-autograph-collection-all-inclusive-resort/overview — MARSHA/code: LIRPH
- **El Mangroove, Autograph Collection** (Sheraton, ?, Costa Rica) — `missing_high_confidence` — https://www.marriott.com/en-us/hotels/lirel-el-mangroove-autograph-collection/overview — MARSHA/code: LIREL
- **Delta Hotels by Marriott San Jose Aurola** (Sheraton, ?, Costa Rica) — `missing_high_confidence` — https://www.marriott.com/en-us/hotels/sjode-delta-hotels-san-jose-aurola/overview — MARSHA/code: SJODE
- **Hotel Belmar, a Member of Design Hotels™** (Sheraton, ?, Costa Rica) — `missing_high_confidence` — https://www.marriott.com/en-us/hotels/lirhb-hotel-belmar-a-member-of-design-hotels/overview — MARSHA/code: LIRHB
- **Esh Hotel & Spa, a Member of Design Hotels™** (Sheraton, ?, Costa Rica) — `missing_high_confidence` — https://www.marriott.com/en-us/hotels/lireh-esh-hotel-and-spa-a-member-of-design-hotels/overview — MARSHA/code: LIREH
- **Marriott Vacation Club at Los Sueños** (Sheraton, ?, Costa Rica) — `missing_high_confidence` — https://www.marriott.com/en-us/hotels/sjomv-marriott-vacation-club-at-los-suenos/overview — MARSHA/code: SJOMV
- **Fairfield by Marriott San Jose Airport Alajuela** (Sheraton, ?, Costa Rica) — `missing_high_confidence` — https://www.marriott.com/en-us/hotels/sjofa-fairfield-san-jose-airport-alajuela/overview — MARSHA/code: SJOFA
- **Four Points by Sheraton San Jose Costa Rica** (Sheraton, ?, Costa Rica) — `missing_high_confidence` — https://www.marriott.com/en-us/hotels/sjofp-four-points-san-jose-costa-rica/overview — MARSHA/code: SJOFP
- **Four Points by Sheraton San Jose Sabana** (Sheraton, ?, Costa Rica) — `missing_high_confidence` — https://www.marriott.com/en-us/hotels/sjoph-four-points-san-jose-sabana/overview — MARSHA/code: SJOPH
- **JW Marriott Hotel Bogota** (Sheraton, ?, Colombia) — `missing_high_confidence` — https://www.marriott.com/en-us/hotels/bogjw-jw-marriott-hotel-bogota/overview — MARSHA/code: BOGJW
- **W Bogota** (Sheraton, ?, Colombia) — `missing_high_confidence` — https://www.marriott.com/en-us/hotels/bogwh-w-bogota/overview — MARSHA/code: BOGWH
- **The Artisan D.C. Hotel, Autograph Collection** (Sheraton, ?, Colombia) — `missing_high_confidence` — https://www.marriott.com/en-us/hotels/bogak-the-artisan-dc-hotel-autograph-collection/overview — MARSHA/code: BOGAK
- **Wake BioHotel, a Member of Design Hotels™** (Sheraton, ?, Colombia) — `missing_high_confidence` — https://www.marriott.com/en-us/hotels/mdewt-wake-biohotel-a-member-of-design-hotels/overview — MARSHA/code: MDEWT
- **Wake Medellin, a Member of Design Hotels™** (Sheraton, ?, Colombia) — `missing_high_confidence` — https://www.marriott.com/en-us/hotels/mdewm-wake-medellin-a-member-of-design-hotels/overview — MARSHA/code: MDEWM
- **Sheraton Bogota Hotel** (Sheraton, ?, Colombia) — `missing_high_confidence` — https://www.marriott.com/en-us/hotels/bogsi-sheraton-bogota-hotel/overview — MARSHA/code: BOGSI
- **Aloft by Marriott Bogota Airport** (Sheraton, ?, Colombia) — `missing_high_confidence` — https://www.marriott.com/en-us/hotels/bogal-aloft-bogota-airport/overview — MARSHA/code: BOGAL
- **City Express Junior by Marriott Bogota Aeropuerto** (Sheraton, ?, Colombia) — `missing_high_confidence` — https://www.marriott.com/en-us/hotels/bogjo-city-express-junior-bogota-aeropuerto/overview — MARSHA/code: BOGJO
- **City Express Plus by Marriott Bogota Aeropuerto** (Sheraton, ?, Colombia) — `missing_high_confidence` — https://www.marriott.com/en-us/hotels/bogpo-city-express-plus-bogota-aeropuerto/overview — MARSHA/code: BOGPO
- **City Express Plus by Marriott Cali Colombia** (Sheraton, ?, Colombia) — `missing_high_confidence` — https://www.marriott.com/en-us/hotels/clopc-city-express-plus-cali-colombia/overview — MARSHA/code: CLOPC
- **City Express Plus by Marriott Medellin Colombia** (Sheraton, ?, Colombia) — `missing_high_confidence` — https://www.marriott.com/en-us/hotels/mdepm-city-express-plus-medellin-colombia/overview — MARSHA/code: MDEPM
- **Fairfield by Marriott Bogota Embajada** (Sheraton, ?, Colombia) — `missing_high_confidence` — https://www.marriott.com/en-us/hotels/bogfi-fairfield-bogota-embajada/overview — MARSHA/code: BOGFI
- **Fairfield by Marriott Medellin Sabaneta** (Sheraton, ?, Colombia) — `missing_high_confidence` — https://www.marriott.com/en-us/hotels/mdefi-fairfield-medellin-sabaneta/overview — MARSHA/code: MDEFI
- **Four Points by Sheraton Barranquilla** (Sheraton, ?, Colombia) — `missing_high_confidence` — https://www.marriott.com/en-us/hotels/baqfp-four-points-barranquilla/overview — MARSHA/code: BAQFP
- **Four Points by Sheraton Bogota** (Sheraton, ?, Colombia) — `missing_high_confidence` — https://www.marriott.com/en-us/hotels/bogfp-four-points-bogota/overview — MARSHA/code: BOGFP

## Safety

- Hotel Property Census only
- Brand Setup / Brand Explorer untouched
- No owner/operator/date / Recent Momentum / Company Validated writes
- No fuzzy auto-insert; no hotel-name-only insert
- No lat/long/phone/rooms on coverage inserts
