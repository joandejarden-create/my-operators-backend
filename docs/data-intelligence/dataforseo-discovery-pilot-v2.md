# DataForSEO Discovery Pilot v2

**Status:** `production_census_dataforseo_discovery_pilot_v2_partial_policy_decision_needed`
**Objective:** `dataforseo-discovery-pilot-v2`
**Recommendation:** **adjust_then_scale_candidates**
**Generated:** 2026-08-07T21:32:33.662Z
**Mode:** candidates-only (no Hotel Property Census writes)
**Census mode:** field-completion-only
**Records piloted:** 200

## Cost

- Queries run: **1200**
- Estimated / reported API cost: **$0.3960**
- Useful candidates: **584**
- Trusted secondary (non-official): **48**
- Cost per useful candidate: **$0.0007**

## Candidate yields

- Official hotel URLs: **566**
- Room evidence pages: **207**
- Address candidates: **55**
- Phone candidates: **52**
- Google Maps / local candidates: **55**
- Lat/Long candidates: **55**
- Tourism registry candidates: **5**

## Source classifier

- Precision estimate (official URL candidates): **0.804**
- Brand-official URL candidates: **320**
- Hotel-official URL candidates: **246**
- Classifier version: `dataforseo-candidate-classifier-v2`

## Rejected sources by category

- `rejected_ota_or_ugc_host`: 386
- `no_useful_candidate_signal`: 236
- `rejected_affiliate_mirror`: 144
- `title_name_mismatch`: 111

## Source tier counts

- `hotel_official`: 246
- `brand_official`: 320
- `hospitality_trade_secondary`: 48
- `google_maps_local`: 12
- `convention_bureau`: 4
- `factsheet_pdf`: 1
- `tourism_board`: 1

## Safety

- Census writes: **0**
- Brand Setup / Brand Explorer writes: **0**
- DataForSEO treated as source of truth: **false**
- `DATAFORSEO_WRITE_CANDIDATES_ONLY`: true
- `ENABLE_DATAFORSEO_VALIDATED_WRITES`: false
- Google Maps: candidate-only (no writes)

## Recommended write policy

No Census writes yet. Promote only brand_official URL candidates and tourism_registry / factsheet rooms evidence after human or High extractor validation. Reject affiliate mirrors (already classifier-hard). Travel Weekly = verification only. Maps = candidate-only.

## Scale estimate (full 1224 incomplete)

- Estimated queries: **7344**
- Estimated cost: **$2.42**
- Estimated useful candidates: **3574**
- Estimated official URL candidates: **3464**

## Recommendation

Moderate stricter-classifier yield — keep candidate-only, prefer site:brand-domain + Spanish/Portuguese rooms queries, deepen Maps only for contact/geo gaps, then re-pilot before full 1,224 scale.

## Top 25 validated-looking official URLs

- [1] Comfort Inn & Suites Veracruz · `brand_official` · https://www.choicehotels.com/en-mx/veracruz/veracruz/comfort-suites-hotels
- [1] Comfort Inn Puebla Centro Histórico · `brand_official` · https://www.choicehotels.com/es-mx/puebla/puebla/comfort-inn-hotels/mx224?gclid=CjwKCAiAg9urBhB_EiwAgw88mYvzgFOQZJvbiKVIf38BnTgEIAJ54jTG-H1pip1O8wteZSanxVy0RhoCZREQAvD_BwE
- [1] Radisson Puebla Angelopolis · `brand_official` · https://www.choicehotels.com/mexico/puebla/radisson-hotels/mx227?mc=llgoxxpx
- [1] Radisson Puebla Angelopolis · `brand_official` · https://www.choicehotels.com/mexico/puebla/radisson-hotels/mx227
- [1] Radisson Puebla Angelopolis · `brand_official` · https://www.choicehotels.com/mexico/puebla/radisson-hotels/mx227?mc=llgoxxpx
- [1] Quality Inn Aguascalientes · `hotel_official` · http://www.choicehotelsmexico.com/es/quality-inn-aguascalientes-aguascalientes-hotel-MX063
- [1] Quality Inn Aguascalientes · `brand_official` · https://www.choicehotels.com/es-mx/aguascalientes/aguascalientes/quality-inn-hotels/mx063
- [1] Quality Inn Aguascalientes · `brand_official` · https://www.choicehotels.com/mexico/aguascalientes/quality-inn-hotels/mx063
- [1] Quality Inn Chihuahua · `hotel_official` · https://hotelsanfranciscocuu.com/
- [1] Comfort Inn Monterrey Norte · `brand_official` · https://www.choicehotels.com/es-mx/nuevo-leon/nuevo-leon/comfort-inn-hotels/mx080
- [1] Comfort Inn Monterrey Norte · `brand_official` · https://www.choicehotels.com/mexico/monterrey,-nuevo-leon/comfort-inn-hotels/mx080
- [1] Sleep Inn Puebla Centro Histórico · `brand_official` · https://www.choicehotels.com/es-mx/puebla/puebla/sleep-inn-hotels/mx225
- [1] Sleep Inn Puebla Centro Histórico · `brand_official` · https://www.choicehotels.com/mexico/puebla/sleep-inn-hotels/mx225
- [1] Comfort Inn Chihuahua · `brand_official` · https://www.choicehotels.com/es-mx/chihuahua/chihuahua/comfort-inn-hotels/mx077
- [1] Comfort Inn Chihuahua · `brand_official` · https://www.choicehotels.com/es-mx/mexico/chihuahua/comfort-inn-hotels/mx077
- [1] Comfort Inn Chihuahua · `brand_official` · https://www.choicehotels.com/mexico/chihuahua/comfort-inn-hotels/mx077
- [1] Amberes 64, an Ascend Collection Hotel · `brand_official` · https://www.choicehotels.com/en-mx/mexico/mexico-city/ascend-hotels/mx228
- [1] Amberes 64, an Ascend Collection Hotel · `brand_official` · https://www.choicehotels.com/es-mx/mexico/mexico-city/ascend-hotels/mx228
- [1] Amberes 64, an Ascend Collection Hotel · `brand_official` · https://www.choicehotels.com/en-mx/mexico/mexico-city/ascend-hotels/mx228
- [1] Amberes 64, an Ascend Collection Hotel · `brand_official` · https://www.choicehotels.com/en-mx/mexico/mexico-city/ascend-hotels/mx228
- [1] La Purificadora, Puebla, a Member of Design Hotels™ · `brand_official` · https://www.marriott.com/es/hotels/pbcds-la-purificadora-puebla-a-member-of-design-hotels/overview/
- [1] La Purificadora, Puebla, a Member of Design Hotels™ · `brand_official` · https://www.marriott.com/en-us/hotels/pbcds-la-purificadora-puebla-a-member-of-design-hotels/overview/
- [1] La Purificadora, Puebla, a Member of Design Hotels™ · `brand_official` · https://www.marriott.com/en-us/hotels/pbcds-la-purificadora-puebla-a-member-of-design-hotels/reviews/
- [1] La Purificadora, Puebla, a Member of Design Hotels™ · `brand_official` · https://www.marriott.com/es/hotels/pbcds-la-purificadora-puebla-a-member-of-design-hotels/overview/
- [1] La Purificadora, Puebla, a Member of Design Hotels™ · `brand_official` · https://www.marriott.com/es/hotels/pbcds-la-purificadora-puebla-a-member-of-design-hotels/overview/

## Top 25 room evidence candidates

- [1] Comfort Inn & Suites Veracruz · `brand_official` · https://www.choicehotels.com/en-mx/veracruz/veracruz/comfort-suites-hotels
- [1] Quality Inn Aguascalientes · `brand_official` · https://www.choicehotels.com/es-mx/aguascalientes/aguascalientes/quality-inn-hotels/mx063
- [1] Quality Inn Aguascalientes · `brand_official` · https://www.choicehotels.com/mexico/aguascalientes/quality-inn-hotels/mx063
- [1] Comfort Inn Monterrey Norte · `brand_official` · https://www.choicehotels.com/es-mx/nuevo-leon/nuevo-leon/comfort-inn-hotels/mx080
- [1] Comfort Inn Monterrey Norte · `brand_official` · https://www.choicehotels.com/mexico/monterrey,-nuevo-leon/comfort-inn-hotels/mx080
- [1] Comfort Inn Chihuahua · `brand_official` · https://www.choicehotels.com/es-mx/chihuahua/chihuahua/comfort-inn-hotels/mx077
- [1] Comfort Inn Chihuahua · `brand_official` · https://www.choicehotels.com/es-mx/mexico/chihuahua/comfort-inn-hotels/mx077
- [1] Comfort Inn Chihuahua · `brand_official` · https://www.choicehotels.com/mexico/chihuahua/comfort-inn-hotels/mx077
- [1] La Purificadora, Puebla, a Member of Design Hotels™ · `brand_official` · https://www.marriott.com/en-us/hotels/pbcds-la-purificadora-puebla-a-member-of-design-hotels/rooms/
- [1] La Purificadora, Puebla, a Member of Design Hotels™ · `brand_official` · https://www.marriott.com/en-us/hotels/pbcds-la-purificadora-puebla-a-member-of-design-hotels/photos/
- [1] La Purificadora, Puebla, a Member of Design Hotels™ · `brand_official` · https://www.marriott.com/es/hotels/pbcds-la-purificadora-puebla-a-member-of-design-hotels/rooms/
- [1] La Purificadora, Puebla, a Member of Design Hotels™ · `brand_official` · https://www.marriott.com/en-us/hotels/pbcds-la-purificadora-puebla-a-member-of-design-hotels/rooms/
- [1] La Purificadora, Puebla, a Member of Design Hotels™ · `brand_official` · https://www.marriott.com/es/hotels/pbcds-la-purificadora-puebla-a-member-of-design-hotels/photos/
- [1] City Express by Marriott Ciudad De Mexico Alameda · `brand_official` · https://www.marriott.com/en-us/hotels/mexxd-city-express-ciudad-de-mexico-alameda/overview/
- [1] CONDESA df, Mexico City, a Member of Design Hotels™ · `brand_official` · https://www.marriott.com/en-us/hotels/mexds-condesa-df-mexico-city-a-member-of-design-hotels/overview/
- [1] CONDESA df, Mexico City, a Member of Design Hotels™ · `brand_official` · https://www.marriott.com/en-us/hotels/mexds-condesa-df-mexico-city-a-member-of-design-hotels/rooms/
- [1] CONDESA df, Mexico City, a Member of Design Hotels™ · `brand_official` · https://www.marriott.com/en-us/hotels/mexds-condesa-df-mexico-city-a-member-of-design-hotels/experiences/
- [1] CONDESA df, Mexico City, a Member of Design Hotels™ · `brand_official` · https://www.marriott.com/en-us/hotels/mexds-condesa-df-mexico-city-a-member-of-design-hotels/rooms/
- [1] CONDESA df, Mexico City, a Member of Design Hotels™ · `brand_official` · https://www.marriott.com/en-us/hotels/mexds-condesa-df-mexico-city-a-member-of-design-hotels/rooms/
- [1] Courtyard by Marriott Mexico City Toreo · `brand_official` · https://www.marriott.com/en-us/hotels/mexna-courtyard-mexico-city-toreo/overview/
- [1] Courtyard by Marriott Mexico City Toreo · `brand_official` · https://www.marriott.com/en-us/hotels/mexna-courtyard-mexico-city-toreo/rooms/
- [1] Courtyard by Marriott Mexico City Toreo · `brand_official` · https://www.marriott.com/en-us/hotels/mexna-courtyard-mexico-city-toreo/overview/
- [1] Courtyard by Marriott Mexico City Toreo · `brand_official` · https://www.marriott.com/en-us/hotels/mexna-courtyard-mexico-city-toreo/rooms/
- [1] Gran Hotel de Puebla by HNF · `brand_official` · https://www.marriott.com/en-us/hotels/pbcde-gran-hotel-de-puebla-by-hnf/overview/
- [1] Gran Hotel de Puebla by HNF · `brand_official` · https://www.marriott.com/en-us/hotels/pbcde-gran-hotel-de-puebla-by-hnf/overview/
