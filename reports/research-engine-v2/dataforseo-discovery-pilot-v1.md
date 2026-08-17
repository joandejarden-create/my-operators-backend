# DataForSEO Discovery Pilot v1

**Status:** `dataforseo_discovery_pilot_v1_partial`
**Recommendation:** **adjust**
**Generated:** 2026-08-07T20:53:14.382Z
**Mode:** candidates-only (no Hotel Property Census writes)
**Records piloted:** 100

## Cost

- Queries run: **200**
- Estimated / reported API cost: **$0.0680**
- Useful candidates: **132**
- Cost per useful candidate: **$0.0005**

## Candidate yields

- Official hotel URLs: **101**
- Room evidence pages: **65**
- Address candidates: **17**
- Phone candidates: **16**
- Google Maps / local candidates: **17**
- Lat/Long candidates: **17**

## Rejected sources

- `rejected_ota_or_ugc_host`: 26
- `no_useful_candidate_signal`: 34
- `title_name_mismatch`: 4

## Safety

- Census writes: **0**
- Brand Setup / Brand Explorer writes: **0**
- `DATAFORSEO_WRITE_CANDIDATES_ONLY`: true
- `ENABLE_DATAFORSEO_VALIDATED_WRITES`: false

## Recommendation

Moderate yield — **adjust** before scaling:
1. Prefer brand-official hosts only (`marriott.com`, `ihg.com`, `hilton.com`, …); reject `hoteles.com` / `*-hotels.com` affiliates (classifier tightened after pilot).
2. Add Spanish queries (`sitio oficial`, `habitaciones`) for MX/CO/DR.
3. Increase Maps depth for address/phone gaps (Maps yield was thinner than SERP URL yield).
4. Keep `DATAFORSEO_WRITE_CANDIDATES_ONLY=1` until a steward validation path writes High/Medium URL/rooms from candidates.

## Sample useful candidates

- Staybridge Suites Puebla → official_hotel_url_candidate | https://www.ihg.com/staybridge/hotels/us/es/puebla/pueaa/hoteldetail
- Staybridge Suites Puebla → official_hotel_url_candidate | https://www.hoteles.com/ho536163/staybridge-suites-puebla-puebla-mexico/
- Staybridge Suites Puebla → official_hotel_url_candidate, rooms_evidence_page_candidate | https://staybridge-suites.puebla-hotels.com/es/
- La Purificadora, Puebla, a Member of Design Hotels™ → official_hotel_url_candidate | https://www.marriott.com/es/hotels/pbcds-la-purificadora-puebla-a-member-of-design-hotels/overview/
- La Purificadora, Puebla, a Member of Design Hotels™ → official_hotel_url_candidate, rooms_evidence_page_candidate | https://www.marriott.com/es/hotels/pbcds-la-purificadora-puebla-a-member-of-design-hotels/rooms/
- La Purificadora, Puebla, a Member of Design Hotels™ → official_hotel_url_candidate, rooms_evidence_page_candidate | https://rooms.aero/marriott/hotel/2o4bKAQMtedVnOBThZDblshLEfr
- Le Méridien Mexico City Reforma → official_hotel_url_candidate | https://www.marriott.com/es/hotels/mexdm-le-meridien-mexico-city/overview/
- Le Méridien Mexico City Reforma → official_hotel_url_candidate | https://le-meridien.mexico-city-hotels.net/en/
- Le Méridien Mexico City Reforma → official_hotel_url_candidate | https://www.hoteles.com/ho422120/le-meridien-mexico-city-ciudad-de-mexico-mexico/
- City Express by Marriott Puebla Centro → official_hotel_url_candidate, rooms_evidence_page_candidate | https://www.marriott.com/es/hotels/pbcxc-city-express-puebla-centro/rooms/
- City Express by Marriott Puebla Centro → official_hotel_url_candidate | https://www.marriott.com/es/hotels/pbcxc-city-express-puebla-centro/overview/
- City Express by Marriott Puebla Centro → official_hotel_url_candidate, rooms_evidence_page_candidate | https://www.travelweekly.com/Hotels/Puebla-Mexico/City-Express-by-Marriott-Puebla-Centro-p59092916
- City Express Plus by Marriott Ciudad de México Reforma El Ángel → official_hotel_url_candidate | https://www.marriott.com/es/hotels/mexpg-city-express-plus-ciudad-de-mexico-reforma-el-angel/overview/
- City Express Plus by Marriott Ciudad de México Reforma El Ángel → official_hotel_url_candidate, rooms_evidence_page_candidate | https://www.hoteles.com/ho328465/city-express-plus-reforma-el-angel-ciudad-de-mexico-mexico/
- City Express Plus by Marriott Ciudad de México Reforma El Ángel → official_hotel_url_candidate, rooms_evidence_page_candidate | https://www.marriott.com/es/hotels/mexpg-city-express-plus-ciudad-de-mexico-reforma-el-angel/rooms/
- Holiday Inn Express Mexico City Satelite → official_hotel_url_candidate | https://www.ihg.com/holidayinnexpress/hotels/us/es/mexico-city/mexca/hoteldetail
- Holiday Inn Express Mexico City Satelite → official_hotel_url_candidate, rooms_evidence_page_candidate | https://www.hoteles.com/ho482277/city-express-plus-satelite-naucalpan-mexico/
- Holiday Inn Express Mexico City Satelite → official_hotel_url_candidate | https://www.travelweekly.com/Hotels/Mexico-City/Holiday-Inn-Express-Mexico-City-Satelite-p59071498
- Holiday Inn Express & Suites Chihuahua Juventud → official_hotel_url_candidate | https://www.ihg.com/holidayinnexpress/hotels/us/es/chihuahua/cuugt/hoteldetail
- Holiday Inn Express & Suites Chihuahua Juventud → official_hotel_url_candidate, rooms_evidence_page_candidate | https://www.hoteles.com/ho512197/holiday-inn-express-suites-chihuahua-juventud-chihuahua-mexico/
