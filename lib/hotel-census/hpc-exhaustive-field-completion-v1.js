/**
 * Exhaustive HPC field audit + completion for ADP hotels.
 * Classifies every live schema field; proposes NULL_FILL only with evidence.
 */

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  isFieldEmpty,
  COMPUTED_FIELD_TYPES,
  FIELD_STATUS,
  HPC_MINIMUM_IDENTITY_FIELDS,
  HPC_ADP_ESSENTIAL_FIELDS,
} from "./hpc-full-onboarding-contract-v1.js";
import {
  planAdpHpcFieldPopulation,
  CONDITIONAL_OR_PROCESS_FIELDS,
  HPC_ADP_FIELD_POPULATION_V1,
} from "./hpc-adp-field-population-v1.js";

export const HPC_EXHAUSTIVE_FIELD_COMPLETION_V1 = "HPC_EXHAUSTIVE_FIELD_COMPLETION_V1";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");

/**
 * Official-source research pack (verified this pass). Do not invent beyond these.
 * Rooms Evidence Tier must match source quality.
 */
export const OFFICIAL_RESEARCH_PACK_V1 = Object.freeze({
  adp_ac_hotel_a_coruna: {
    "Hotel Description - Source Text": {
      value:
        "Located in what has been known for some time as the new business and residential area of A Coruña, this hotel is near the Expocoruña Exhibition Centre and the Coliseum Leisure Centre. The AC Hotel A Coruña is near the train station, city airport, and A-6 motorway allowing quick entry or exit to or from A Coruña. This urban hotel, perfectly located for enjoying the city or for business in A Coruña, features 116 guest rooms and a fitness center. Our guests can enjoy a delicious meal in the AC Lounge, with a varied menu open for lunch and dinner and a complete and healthy breakfast buffet in a polyvalent room. AC hotel A Coruña also features five exquisitely designed event rooms perfect for holding anything from business meetings to social get together.",
      source: "marriott_overview_hero_copy",
      sourceUrl:
        "https://www.marriott.com/en-us/hotels/lcgco-ac-hotel-a-coruna/overview/",
    },
    "Amenities - Source Text": {
      value:
        "On-site amenities (Marriott overview): Restaurant On-Site (2), Convenience Store, Meeting Space, Fitness Center (complimentary), On-Site Laundry, Complimentary Wi-Fi for Marriott Bonvoy members; on-site parking (€14/day); airport shuttle available for a fee.",
      source: "marriott_overview_amenities_list",
      sourceUrl:
        "https://www.marriott.com/en-us/hotels/lcgco-ac-hotel-a-coruna/overview/",
    },
  },
  adp_falls_church_marriott_fairview_park: {
    "Rooms / Keys": {
      value: 395,
      sourceUrl:
        "https://www.marriott.com/en-us/hotels/wasfp-falls-church-marriott-fairview-park/events/meetings-and-events/",
      roomsConfidence: "High",
      roomsEvidenceTier: "Tier 1 Official Parent / Brand Source",
      source: "marriott_events_page_guest_rooms",
    },
  },
  adp_tampa_airport_marriott: {
    "Rooms / Keys": {
      value: 298,
      sourceUrl:
        "https://www.marriott.com/content/dam/marriott-digital/mc/us-canada/hws/t/tpaap/en_us/document/assets/mc-tpaap-hotel-map-and-attractions-40076.pdf",
      roomsConfidence: "High",
      roomsEvidenceTier: "Tier 2 Official Hotel Website",
      source: "marriott_official_hotel_map_pdf",
    },
    "Pool Flag": {
      value: true,
      source: "marriott_overview_outdoor_pool",
      sourceUrl:
        "https://www.marriott.com/en-us/hotels/tpaap-tampa-airport-marriott/overview/",
    },
    "Fitness Flag": {
      value: true,
      source: "marriott_overview_fitness_center",
      sourceUrl:
        "https://www.marriott.com/en-us/hotels/tpaap-tampa-airport-marriott/overview/",
    },
  },
  adp_westin_tampa_waterside: {
    "Rooms / Keys": {
      value: 309,
      sourceUrl:
        "https://rockpoint.com/2025/09/15/rockpoint-and-newbond-holdings-acquire-the-westin-tampa-waterside/",
      roomsConfidence: "High",
      roomsEvidenceTier: "Tier 4 Owner / Developer Source",
      source: "rockpoint_acquisition_pr_309_guestrooms",
    },
  },
  adp_renaissance_arlington_capital_view: {
    "Rooms / Keys": {
      value: 300,
      sourceUrl:
        "https://www.travelweekly.com/Hotels/Arlington-VA/Renaissance-Arlington-Capital-View-Hotel-p4053732",
      roomsConfidence: "Medium",
      roomsEvidenceTier: "Tier 6 Trusted Industry Source",
      source: "travel_weekly_gds_300_rooms",
    },
  },
  adp_charlotte_marriott_city_center: {
    "Rooms / Keys": {
      value: 446,
      sourceUrl:
        "https://watermarkcap.com/carey-watermark-investors-2-acquires-the-charlotte-marriott-city-center/",
      roomsConfidence: "High",
      roomsEvidenceTier: "Tier 4 Owner / Developer Source",
      roomsSourceType: "official_press_release",
      source: "watermark_cwi2_acquisition_pr_446_rooms_corroborated_cvent",
    },
    "Opening Date": {
      value: "1984-01-01",
      sourceUrl:
        "https://www.cvent.com/venues/charlotte/hotel/charlotte-marriott-city-center/venue-f272a334-7904-4b00-b486-2b6ff52ce013",
      source: "cvent_venue_built_1984",
    },
    "Opening Date Source URL": {
      value:
        "https://www.cvent.com/venues/charlotte/hotel/charlotte-marriott-city-center/venue-f272a334-7904-4b00-b486-2b6ff52ce013",
      source: "cvent_venue_built_1984",
    },
    "Operator / Management Company": {
      value: "Marriott International",
      source: "watermark_cwi2_long_term_management_agreement",
      sourceUrl:
        "https://watermarkcap.com/carey-watermark-investors-2-acquires-the-charlotte-marriott-city-center/",
    },
    "Operator Confidence": {
      value: "High",
      source: "watermark_cwi2_long_term_management_agreement",
    },
    "Operator Source URL": {
      value:
        "https://watermarkcap.com/carey-watermark-investors-2-acquires-the-charlotte-marriott-city-center/",
      source: "watermark_cwi2_long_term_management_agreement",
    },
    "Fitness Flag": {
      value: true,
      source: "marriott_overview_fitness_center",
      sourceUrl:
        "https://www.marriott.com/en-us/hotels/cltcc-charlotte-marriott-city-center/overview/",
    },
    "F&B Flag": {
      value: true,
      source: "marriott_overview_restaurants_on_site",
      sourceUrl:
        "https://www.marriott.com/en-us/hotels/cltcc-charlotte-marriott-city-center/overview/",
    },
    "Parking Flag": {
      value: true,
      source: "marriott_overview_valet_self_parking",
      sourceUrl:
        "https://www.marriott.com/en-us/hotels/cltcc-charlotte-marriott-city-center/overview/",
    },
    "Meeting Space Flag": {
      value: true,
      source: "marriott_overview_meeting_space_20000_sf",
      sourceUrl:
        "https://www.marriott.com/en-us/hotels/cltcc-charlotte-marriott-city-center/overview/",
    },
    "Hotel Description - Source Text": {
      value:
        "Experience the pulse of the vibrant uptown district from Charlotte Marriott City Center. Our hotels in uptown Charlotte, NC offers chic guest rooms and suites featuring floor-to-ceiling windows with city skyline views, gleaming hardwood floors and high speed Wi-Fi.",
      source: "marriott_overview_hero_copy",
      sourceUrl:
        "https://www.marriott.com/en-us/hotels/cltcc-charlotte-marriott-city-center/overview/",
    },
    "Amenities - Source Text": {
      value:
        "On-site amenities (Marriott overview): Restaurant On-Site (5), Gift Shop, Convenience Store, Club Lounge, Business Center, Meeting Space, Fitness Center (complimentary), Complimentary Wi-Fi for Marriott Bonvoy members; valet and self parking available.",
      source: "marriott_overview_amenities_list",
      sourceUrl:
        "https://www.marriott.com/en-us/hotels/cltcc-charlotte-marriott-city-center/overview/",
    },
    "Rooms Notes": {
      value:
        "Rooms/Keys=446 per Watermark CWI2 owner PR + Marriott 8-K + Cvent. Non-controlling near-conflicts preserved: LG/SVC case study 444; TravelAge West directory 438; LG PR 450 TVs (not keys). Inventory basis: post-2016 renovation current keys.",
      source: "trust_verification_v1_rooms_conflict_annotation",
    },
    "Building / Asset Notes": {
      value:
        "Opening Date 1984-01-01 is YEAR-ONLY padding (sources: Marriott M Beta PR / Cvent Built 1984). No verified calendar day/month. Day-specific confidence=Low; year confidence=Medium. Operator=Marriott International via long-term management agreement (Watermark/CWI2 = owner, not operator).",
      source: "trust_verification_v1_opening_year_grain_annotation",
    },
  },
  adp_castillo_hotel_son_vida: {
    Phone: {
      value: "+34 971-493493",
      sourceUrl:
        "https://www.marriott.com/en-us/hotels/pmilc-castillo-hotel-son-vida-a-luxury-collection-hotel-mallorca/overview/",
      phoneSourceType: "official_property_page",
      phoneConfidence: "High",
      source: "marriott_overview_tel",
    },
    "Postal Code": {
      value: "07013",
      source: "marriott_overview_address",
      sourceUrl:
        "https://www.marriott.com/en-us/hotels/pmilc-castillo-hotel-son-vida-a-luxury-collection-hotel-mallorca/overview/",
    },
    "Fitness Flag": {
      value: true,
      source: "marriott_overview_fitness_center",
      sourceUrl:
        "https://www.marriott.com/en-us/hotels/pmilc-castillo-hotel-son-vida-a-luxury-collection-hotel-mallorca/overview/",
    },
    "Pool Flag": {
      value: true,
      source: "marriott_overview_indoor_outdoor_pool",
      sourceUrl:
        "https://www.marriott.com/en-us/hotels/pmilc-castillo-hotel-son-vida-a-luxury-collection-hotel-mallorca/overview/",
    },
    "Parking Flag": {
      value: true,
      source: "marriott_overview_complimentary_valet_parking",
      sourceUrl:
        "https://www.marriott.com/en-us/hotels/pmilc-castillo-hotel-son-vida-a-luxury-collection-hotel-mallorca/overview/",
    },
    "Airport Shuttle Flag": {
      value: true,
      source: "marriott_overview_airport_shuttle_fee",
      sourceUrl:
        "https://www.marriott.com/en-us/hotels/pmilc-castillo-hotel-son-vida-a-luxury-collection-hotel-mallorca/overview/",
    },
    "Spa Flag": {
      value: true,
      source: "marriott_overview_spa",
      sourceUrl:
        "https://www.marriott.com/en-us/hotels/pmilc-castillo-hotel-son-vida-a-luxury-collection-hotel-mallorca/overview/",
    },
    "F&B Flag": {
      value: true,
      source: "marriott_overview_restaurants_on_site",
      sourceUrl:
        "https://www.marriott.com/en-us/hotels/pmilc-castillo-hotel-son-vida-a-luxury-collection-hotel-mallorca/overview/",
    },
    "Meeting Space Flag": {
      value: true,
      source: "marriott_overview_meeting_space",
      sourceUrl:
        "https://www.marriott.com/en-us/hotels/pmilc-castillo-hotel-son-vida-a-luxury-collection-hotel-mallorca/overview/",
    },
    "Hotel Description - Source Text": {
      value:
        "Rising above the island like a private refuge, Castillo Hotel Son Vida is the Balcony of Mallorca, an adults-only hideaway overlooking the bay of Palma. Set away from the shoreline, the hotel offers space, calm and perspective during the height of summer.",
      source: "marriott_overview_hero_copy",
      sourceUrl:
        "https://www.marriott.com/en-us/hotels/pmilc-castillo-hotel-son-vida-a-luxury-collection-hotel-mallorca/overview/",
    },
    "Amenities - Source Text": {
      value:
        "On-site amenities (Marriott overview): Restaurant On-Site (3), Indoor Pool, Outdoor Pool, Whirlpool, Meeting Space, Spa, Fitness Center, On-Site Golf, Complimentary Wi-Fi for Marriott Bonvoy members; complimentary on-site/valet parking; airport shuttle available for a fee.",
      source: "marriott_overview_amenities_list",
      sourceUrl:
        "https://www.marriott.com/en-us/hotels/pmilc-castillo-hotel-son-vida-a-luxury-collection-hotel-mallorca/overview/",
    },
  },
  adp_westin_grand_munchen: {
    Phone: {
      value: "+49 89-92640",
      sourceUrl:
        "https://www.marriott.com/en-us/hotels/mucwi-the-westin-grand-munich/overview/",
      phoneSourceType: "official_property_page",
      phoneConfidence: "High",
      source: "marriott_overview_tel",
    },
    "Postal Code": {
      value: "81925",
      source: "marriott_overview_address",
      sourceUrl:
        "https://www.marriott.com/en-us/hotels/mucwi-the-westin-grand-munich/overview/",
    },
    "Fitness Flag": {
      value: true,
      source: "marriott_overview_westinworkout",
      sourceUrl:
        "https://www.marriott.com/en-us/hotels/mucwi-the-westin-grand-munich/overview/",
    },
    "Pool Flag": {
      value: true,
      source: "marriott_overview_indoor_pool_isarspa",
      sourceUrl:
        "https://www.marriott.com/en-us/hotels/mucwi-the-westin-grand-munich/overview/",
    },
    "Parking Flag": {
      value: true,
      source: "marriott_overview_onsite_parking",
      sourceUrl:
        "https://www.marriott.com/en-us/hotels/mucwi-the-westin-grand-munich/overview/",
    },
    "Spa Flag": {
      value: true,
      source: "marriott_overview_isarspa",
      sourceUrl:
        "https://www.marriott.com/en-us/hotels/mucwi-the-westin-grand-munich/overview/",
    },
    "F&B Flag": {
      value: true,
      source: "marriott_overview_restaurants_on_site",
      sourceUrl:
        "https://www.marriott.com/en-us/hotels/mucwi-the-westin-grand-munich/overview/",
    },
    "Meeting Space Flag": {
      value: true,
      source: "marriott_overview_meeting_space",
      sourceUrl:
        "https://www.marriott.com/en-us/hotels/mucwi-the-westin-grand-munich/overview/",
    },
    "Hotel Description - Source Text": {
      value:
        "Experience a relaxing city escape at The Westin Grand Munich, a hotel in central Munich that travelers can enjoy in the quiet Arabellapark district of Bogenhausen. Just minutes from the English Garden and 10 minutes by public transportation from Munich’s historic city center.",
      source: "marriott_overview_hero_copy",
      sourceUrl:
        "https://www.marriott.com/en-us/hotels/mucwi-the-westin-grand-munich/overview/",
    },
    "Amenities - Source Text": {
      value:
        "On-site amenities (Marriott overview): Restaurant On-Site (3), Club Lounge, Indoor Pool, Business Center, Meeting Space, Spa, Fitness Center, Hair salon; on-site parking; hotel does not provide airport shuttle.",
      source: "marriott_overview_amenities_list",
      sourceUrl:
        "https://www.marriott.com/en-us/hotels/mucwi-the-westin-grand-munich/overview/",
    },
  },
  adp_westin_tampa_waterside: {
    Phone: {
      value: "+1 813-229-5000",
      sourceUrl:
        "https://www.marriott.com/en-us/hotels/tpawh-westin-tampa-downtown-waterside/overview/",
      phoneSourceType: "official_property_page",
      phoneConfidence: "High",
      source: "marriott_overview_tel",
    },
    "Postal Code": {
      value: "33602",
      source: "marriott_overview_address",
      sourceUrl:
        "https://www.marriott.com/en-us/hotels/tpawh-westin-tampa-downtown-waterside/overview/",
    },
    "Fitness Flag": {
      value: true,
      source: "marriott_overview_fitness_center",
      sourceUrl:
        "https://www.marriott.com/en-us/hotels/tpawh-westin-tampa-downtown-waterside/overview/",
    },
    "Pool Flag": {
      value: true,
      source: "marriott_overview_outdoor_pool",
      sourceUrl:
        "https://www.marriott.com/en-us/hotels/tpawh-westin-tampa-downtown-waterside/overview/",
    },
    "Parking Flag": {
      value: true,
      source: "marriott_overview_valet_parking",
      sourceUrl:
        "https://www.marriott.com/en-us/hotels/tpawh-westin-tampa-downtown-waterside/overview/",
    },
    "Beach / Waterfront Flag": {
      value: true,
      source: "marriott_overview_harbour_island_waterfront",
      sourceUrl:
        "https://www.marriott.com/en-us/hotels/tpawh-westin-tampa-downtown-waterside/overview/",
    },
    "F&B Flag": {
      value: true,
      source: "marriott_overview_restaurants_on_site",
      sourceUrl:
        "https://www.marriott.com/en-us/hotels/tpawh-westin-tampa-downtown-waterside/overview/",
    },
    "Meeting Space Flag": {
      value: true,
      source: "marriott_overview_meeting_space",
      sourceUrl:
        "https://www.marriott.com/en-us/hotels/tpawh-westin-tampa-downtown-waterside/overview/",
    },
    "Hotel Description - Source Text": {
      value:
        "Edging the shimmering waterfront near celebrated downtown attractions, The Westin Tampa Downtown Waterside is a haven of wellness and renewal. Relax in newly renovated upscale accommodations with modern essentials, plush furnishings, and signature amenities.",
      source: "marriott_overview_hero_copy",
      sourceUrl:
        "https://www.marriott.com/en-us/hotels/tpawh-westin-tampa-downtown-waterside/overview/",
    },
    "Amenities - Source Text": {
      value:
        "On-site amenities (Marriott overview): Restaurant On-Site (3), Outdoor Pool, Meeting Space, Fitness Center; valet parking; hotel does not provide airport shuttle.",
      source: "marriott_overview_amenities_list",
      sourceUrl:
        "https://www.marriott.com/en-us/hotels/tpawh-westin-tampa-downtown-waterside/overview/",
    },
  },
  adp_mayflower_hotel_washington_dc: {
    Phone: {
      value: "+1 202-347-3000",
      sourceUrl: "https://www.themayflowerhotel.com/",
      phoneSourceType: "official_property_page",
      phoneConfidence: "High",
      source: "official_hotel_website_contact",
    },
    "Opening Date": {
      value: "1925-01-01",
      sourceUrl: "https://www.themayflowerhotel.com/",
      source: "official_site_established_1925",
    },
    "Opening Date Source URL": {
      value: "https://www.themayflowerhotel.com/",
      source: "official_site_established_1925",
    },
    "Postal Code": {
      value: "20036",
      source: "official_hotel_website_address",
      sourceUrl: "https://www.themayflowerhotel.com/",
    },
    "F&B Flag": {
      value: true,
      source: "official_site_edgar_bar_kitchen",
      sourceUrl: "https://www.themayflowerhotel.com/",
    },
    "Meeting Space Flag": {
      value: true,
      source: "official_site_meetings_events",
      sourceUrl: "https://www.themayflowerhotel.com/",
    },
    "Hotel Description - Source Text": {
      value:
        "The Mayflower Hotel, part of The Autograph Collection, stands as a Washington DC original where guests experience a modern vibrancy further enhanced by its rich history through personalized service, thoughtful design & creative amenities.",
      source: "official_hotel_website_hero",
      sourceUrl: "https://www.themayflowerhotel.com/",
    },
    "Amenities - Source Text": {
      value:
        "Official site: 581 guest rooms and suites; Edgar Bar & Kitchen on-site dining; Meetings & Events facilities; landmark downtown Washington, DC location at 1127 Connecticut Ave NW.",
      source: "official_hotel_website_amenities",
      sourceUrl: "https://www.themayflowerhotel.com/",
    },
  },
  adp_nobu_london_portman: {
    Phone: {
      value: "+44 20 3988 5888",
      sourceUrl: "https://www.nobuhotels.com/london-portman/",
      phoneSourceType: "official_property_page",
      phoneConfidence: "High",
      source: "official_nobu_contact_us",
    },
    "Fitness Flag": {
      value: true,
      source: "official_nobu_pilates_wellness",
      sourceUrl: "https://www.nobuhotels.com/london-portman/",
    },
    "Spa Flag": {
      value: true,
      source: "official_nobu_spa_treatments",
      sourceUrl: "https://www.nobuhotels.com/london-portman/",
    },
    "F&B Flag": {
      value: true,
      source: "official_nobu_four_dining_destinations",
      sourceUrl: "https://www.nobuhotels.com/london-portman/",
    },
    "Hotel Description - Source Text": {
      value:
        "This luxury lifestyle hotel is in the heart of London’s coveted Marylebone neighbourhood, tucked away yet conveniently close to the bustling streets of Mayfair and Soho. Experience Nobu’s iconic cuisine and renowned hospitality where Japanese minimalism meets London’s cosmopolitan culture.",
      source: "official_nobu_hero_copy",
      sourceUrl: "https://www.nobuhotels.com/london-portman/",
    },
    "Amenities - Source Text": {
      value:
        "Official site: four dining destinations (Nobu Restaurant, The Lounge, Nobu Bar, private dining); Nobu Pilates & Wellness with fitness facilities, personal training, and spa treatments.",
      source: "official_nobu_amenities",
      sourceUrl: "https://www.nobuhotels.com/london-portman/",
    },
  },
  adp_sheraton_mallorca_arabella_golf: {
    Phone: {
      value: "+34 971-787100",
      sourceUrl:
        "https://www.marriott.com/en-us/hotels/pmisi-sheraton-mallorca-arabella-golf-hotel/overview/",
      phoneSourceType: "official_property_page",
      phoneConfidence: "High",
      source: "marriott_overview_tel",
    },
    "Postal Code": {
      value: "07013",
      source: "marriott_overview_address",
      sourceUrl:
        "https://www.marriott.com/en-us/hotels/pmisi-sheraton-mallorca-arabella-golf-hotel/overview/",
    },
    "Fitness Flag": {
      value: true,
      source: "marriott_overview_fitness_center",
      sourceUrl:
        "https://www.marriott.com/en-us/hotels/pmisi-sheraton-mallorca-arabella-golf-hotel/overview/",
    },
    "Pool Flag": {
      value: true,
      source: "marriott_overview_indoor_outdoor_pool",
      sourceUrl:
        "https://www.marriott.com/en-us/hotels/pmisi-sheraton-mallorca-arabella-golf-hotel/overview/",
    },
    "Parking Flag": {
      value: true,
      source: "marriott_overview_property_details_parking",
      sourceUrl:
        "https://www.marriott.com/en-us/hotels/pmisi-sheraton-mallorca-arabella-golf-hotel/overview/",
    },
    "Airport Shuttle Flag": {
      value: true,
      source: "marriott_overview_airport_shuttle_fee",
      sourceUrl:
        "https://www.marriott.com/en-us/hotels/pmisi-sheraton-mallorca-arabella-golf-hotel/overview/",
    },
    "Spa Flag": {
      value: true,
      source: "marriott_overview_spa",
      sourceUrl:
        "https://www.marriott.com/en-us/hotels/pmisi-sheraton-mallorca-arabella-golf-hotel/overview/",
    },
    "F&B Flag": {
      value: true,
      source: "marriott_overview_restaurants_on_site",
      sourceUrl:
        "https://www.marriott.com/en-us/hotels/pmisi-sheraton-mallorca-arabella-golf-hotel/overview/",
    },
    "Meeting Space Flag": {
      value: true,
      source: "marriott_overview_meeting_space",
      sourceUrl:
        "https://www.marriott.com/en-us/hotels/pmisi-sheraton-mallorca-arabella-golf-hotel/overview/",
    },
    "Amenities - Source Text": {
      value:
        "On-site amenities (Marriott overview): Restaurant On-Site (4), Indoor Pool, Outdoor Pool, Whirlpool, Business Center, Meeting Space, Spa, Fitness Center; airport shuttle available for a fee.",
      source: "marriott_overview_amenities_list",
      sourceUrl:
        "https://www.marriott.com/en-us/hotels/pmisi-sheraton-mallorca-arabella-golf-hotel/overview/",
    },
  },
  adp_wyndham_san_diego_bayside: {
    Phone: {
      value: "+1 619-232-3861",
      sourceUrl: "https://www.wyndhamsandiegobay.com/contact-us",
      phoneSourceType: "official_property_page",
      phoneConfidence: "High",
      source: "official_wyndham_contact_page",
    },
    "Postal Code": {
      value: "92101",
      source: "official_wyndham_contact_address",
      sourceUrl: "https://www.wyndhamsandiegobay.com/contact-us",
    },
    "Fitness Flag": {
      value: true,
      source: "official_wyndham_schema_fitness_center",
      sourceUrl: "https://www.wyndhamsandiegobay.com/",
    },
    "Pool Flag": {
      value: true,
      source: "official_wyndham_schema_outdoor_pool",
      sourceUrl: "https://www.wyndhamsandiegobay.com/",
    },
    "Parking Flag": {
      value: true,
      source: "official_wyndham_schema_parking_valet",
      sourceUrl: "https://www.wyndhamsandiegobay.com/",
    },
    "Airport Shuttle Flag": {
      value: true,
      source: "official_wyndham_schema_complimentary_airport_shuttle",
      sourceUrl: "https://www.wyndhamsandiegobay.com/",
    },
    "Beach / Waterfront Flag": {
      value: true,
      source: "official_wyndham_bayfront_positioning",
      sourceUrl: "https://www.wyndhamsandiegobay.com/",
    },
    "F&B Flag": {
      value: true,
      source: "official_wyndham_schema_restaurants",
      sourceUrl: "https://www.wyndhamsandiegobay.com/",
    },
    "Meeting Space Flag": {
      value: true,
      source: "official_wyndham_schema_27000_sf_meeting",
      sourceUrl: "https://www.wyndhamsandiegobay.com/",
    },
    "Hotel Description - Source Text": {
      value:
        "Opposite San Diego Bay, this upscale waterfront hotel is 0.9 miles from the Gaslamp Quarter, 3.4 miles from Balboa Park, and 7 miles from SeaWorld San Diego.",
      source: "official_wyndham_schema_description",
      sourceUrl: "https://www.wyndhamsandiegobay.com/",
    },
    "Amenities - Source Text": {
      value:
        "Official site schema amenities: 27,000 sq ft of meeting spaces; complimentary airport shuttle; outdoor swimming pool; fitness center; parking / valet parking; on-site restaurants.",
      source: "official_wyndham_schema_amenities",
      sourceUrl: "https://www.wyndhamsandiegobay.com/",
    },
  },
  adp_yotel_geneva_lake: {
    "Opening Date": {
      value: "2024-04-18",
      sourceUrl:
        "https://www.yotel.com/en/press/yotel-geneva-lake-officially-opens-its-doors",
      source: "yotel_official_opening_press_18_april_2024",
    },
    "Opening Date Source URL": {
      value:
        "https://www.yotel.com/en/press/yotel-geneva-lake-officially-opens-its-doors",
      source: "yotel_official_opening_press_18_april_2024",
    },
    "Operator / Management Company": {
      value: "YOTEL",
      source: "yotel_official_opening_press_brand_operated",
      sourceUrl:
        "https://www.yotel.com/en/press/yotel-geneva-lake-officially-opens-its-doors",
    },
    "Operator Confidence": {
      value: "High",
      source: "yotel_official_opening_press_brand_operated",
    },
    "Operator Source URL": {
      value:
        "https://www.yotel.com/en/press/yotel-geneva-lake-officially-opens-its-doors",
      source: "yotel_official_opening_press_brand_operated",
    },
    "Owner Name": {
      value: "Conamo SA",
      source: "yotel_switzerland_announcement_owner_conamo",
      sourceUrl:
        "https://www.yotel.com/en/press/yotel-announces-newest-property-to-open-in-switzerland",
    },
    "Owner Confidence": {
      value: "High",
      source: "yotel_switzerland_announcement_owner_conamo",
    },
    "Sub-Continent": {
      value: "Western Europe",
      source: "dealality_country_sub_continent_map_switzerland",
    },
    "Fitness Flag": {
      value: true,
      source: "yotel_official_site_ultra_modern_gym",
      sourceUrl: "https://www.yotel.com/en/hotels/yotel-geneva-lake",
    },
    "Airport Shuttle Flag": {
      value: true,
      source: "yotel_official_site_free_airport_transfers",
      sourceUrl: "https://www.yotel.com/en/hotels/yotel-geneva-lake",
    },
    "Hotel Description - Source Text": {
      value:
        "YOTEL Geneva Lake is in Founex, Switzerland, ten minutes by road from Geneva Airport on the Vaud shore of Lake Geneva (Lac Léman). A free daily airport shuttle and a spot just off the A1 make it a practical base for early flights, late arrivals and stopovers, with the Lake Geneva region (Coppet, Nyon, the La Côte vineyards and Lausanne) on the doorstep.",
      source: "yotel_official_hotel_about_copy",
      sourceUrl: "https://www.yotel.com/en/hotels/yotel-geneva-lake",
    },
    "Amenities - Source Text": {
      value:
        "Official YOTEL Geneva Lake facilities: ultra-modern gym; meeting spaces; destination dining / rooftop; auditorium; free airport transfers; park and fly options; self-service check-in.",
      source: "yotel_official_hotel_facilities",
      sourceUrl: "https://www.yotel.com/en/hotels/yotel-geneva-lake",
    },
  },
});

export function loadBrandExplorerSlugMap() {
  const p = path.join(ROOT, "reports/brand-explorer-62-active-public-full-baseline.json");
  const map = new Map();
  if (!fs.existsSync(p)) return map;
  try {
    const data = JSON.parse(fs.readFileSync(p, "utf8"));
    for (const b of data.brands || []) {
      const name = String(b.brandName || b.name || "").trim().toLowerCase();
      if (name && b.slug) map.set(name, b.slug);
    }
  } catch {
    /* ignore */
  }
  const aliases = [
    ["marriott hotels", "marriott-hotels-resorts"],
    ["marriott", "marriott-hotels-resorts"],
    ["ac hotels by marriott", "ac-hotels"],
    ["ac hotel", "ac-hotels"],
    ["westin", "westin"],
    ["the westin", "westin"],
    ["sheraton", "sheraton"],
    ["renaissance", "renaissance-hotels"],
    ["jw marriott", "jw-marriott"],
    ["st. regis", "st-regis"],
    ["st regis", "st-regis"],
    ["w hotels", "w-hotels"],
    ["autograph collection", "autograph-collection"],
    ["curio collection by hilton", "curio-collection"],
    ["curio collection", "curio-collection"],
    ["hilton", "hilton-hotels-resorts"],
    ["the luxury collection", "the-luxury-collection"],
    ["luxury collection", "the-luxury-collection"],
    ["radisson", "radisson"],
    ["wyndham", "wyndham"],
    ["yotel", "yotel"],
  ];
  for (const [k, v] of aliases) {
    if (!map.has(k)) map.set(k, v);
  }
  return map;
}

export function resolveBrandExplorerSlug(hpcFields = {}, profile = {}, slugMap) {
  const brand = String(hpcFields["Current Brand"] || profile?.brand || "")
    .trim()
    .toLowerCase();
  if (!brand || !slugMap?.size) return null;
  if (slugMap.has(brand)) return slugMap.get(brand);
  for (const [k, v] of slugMap.entries()) {
    if (brand.includes(k) || k.includes(brand)) return v;
  }
  return null;
}

/**
 * Merge profile plan + official research + brand slug into proposals.
 */
export function planExhaustiveHpcCompletion({
  propertyId,
  profile,
  hpcFields = {},
  schema,
  slugMap,
} = {}) {
  const base = planAdpHpcFieldPopulation({ profile, hpcFields });
  const proposals = [...base.proposals];
  const seen = new Set(proposals.map((p) => p.field));
  const researchRequired = [];
  const corrections = [];

  function propose(field, value, source, confidence, category = "EXISTING_OR_RULES") {
    if (seen.has(field)) return;
    if (isFieldEmpty(value) && value !== false && value !== 0) return;
    if (!isFieldEmpty(hpcFields[field])) return;
    seen.add(field);
    proposals.push({
      field,
      value,
      source,
      confidence,
      mode: "NULL_FILL",
      category,
    });
  }

  // Official research pack
  const pack = OFFICIAL_RESEARCH_PACK_V1[propertyId] || {};
  for (const [field, spec] of Object.entries(pack)) {
    propose(field, spec.value, spec.source, "OFFICIAL_RESEARCH", "OFFICIAL_SOURCE");
    if (field === "Rooms / Keys" && spec.roomsConfidence) {
      propose(
        "Rooms Confidence",
        spec.roomsConfidence,
        spec.source,
        "OFFICIAL_RESEARCH",
        "OFFICIAL_SOURCE"
      );
    }
    if (field === "Rooms / Keys" && spec.roomsEvidenceTier) {
      propose(
        "Rooms Evidence Tier",
        spec.roomsEvidenceTier,
        spec.source,
        "OFFICIAL_RESEARCH",
        "OFFICIAL_SOURCE"
      );
    }
    if (field === "Rooms / Keys" && spec.roomsSourceType) {
      propose(
        "Rooms Source Type",
        spec.roomsSourceType,
        spec.source,
        "OFFICIAL_RESEARCH",
        "OFFICIAL_SOURCE"
      );
    }
    if (spec.sourceUrl && isFieldEmpty(hpcFields["Rooms Source URL"]) && field === "Rooms / Keys") {
      propose(
        "Rooms Source URL",
        spec.sourceUrl,
        spec.source,
        "OFFICIAL_RESEARCH",
        "OFFICIAL_SOURCE"
      );
    }
    if (field === "Phone" && spec.phoneConfidence) {
      propose(
        "Phone Confidence",
        spec.phoneConfidence,
        spec.source,
        "OFFICIAL_RESEARCH",
        "OFFICIAL_SOURCE"
      );
    }
    if (field === "Phone" && spec.phoneSourceType) {
      propose(
        "Phone Source Type",
        spec.phoneSourceType,
        spec.source,
        "OFFICIAL_RESEARCH",
        "OFFICIAL_SOURCE"
      );
    }
    if (field === "Phone" && spec.sourceUrl) {
      propose(
        "Phone Source URL",
        spec.sourceUrl,
        spec.source,
        "OFFICIAL_RESEARCH",
        "OFFICIAL_SOURCE"
      );
    }
    if (field === "Opening Date" && spec.sourceUrl) {
      propose(
        "Opening Date Source URL",
        spec.sourceUrl,
        spec.source,
        "OFFICIAL_RESEARCH",
        "OFFICIAL_SOURCE"
      );
    }
    if (field === "Operator / Management Company" && spec.sourceUrl) {
      propose(
        "Operator Source URL",
        spec.sourceUrl,
        spec.source,
        "OFFICIAL_RESEARCH",
        "OFFICIAL_SOURCE"
      );
    }
  }

  // Brand Explorer slug
  if (isFieldEmpty(hpcFields["Brand Explorer Slug if mapped"])) {
    const slug = resolveBrandExplorerSlug(hpcFields, profile, slugMap);
    if (slug) {
      propose(
        "Brand Explorer Slug if mapped",
        slug,
        "brand_explorer_slug_map",
        "TAXONOMY",
        "DEALALITY_RULES"
      );
    }
  }

  // Family / Source Family for IHG soft brands etc.
  if (isFieldEmpty(hpcFields["Family / Source Family"])) {
    const parent = String(profile?.parentCompany || hpcFields["Brand Family"] || "").toLowerCase();
    const brand = String(hpcFields["Current Brand"] || profile?.brand || "").toLowerCase();
    if (/ihg|intercontinental|vignette|holiday inn|hotel indigo|voco/i.test(parent + brand)) {
      propose("Family / Source Family", "IHG", "parent_company_ihg", "TAXONOMY", "DEALALITY_RULES");
    } else if (/choice|radisson|ascend|park plaza/i.test(parent + brand)) {
      propose(
        "Family / Source Family",
        "Choice",
        "parent_company_choice",
        "TAXONOMY",
        "DEALALITY_RULES"
      );
    } else if (/hilton|curio|tapestry|lxr/i.test(parent + brand)) {
      propose(
        "Family / Source Family",
        "Hilton",
        "parent_company_hilton",
        "TAXONOMY",
        "DEALALITY_RULES"
      );
    } else if (/marriott|westin|sheraton|renaissance|jw|st\.?\s*regis|autograph|w hotel|ac hotel|luxury collection/i.test(parent + brand)) {
      propose(
        "Family / Source Family",
        "Marriott",
        "parent_company_marriott",
        "TAXONOMY",
        "DEALALITY_RULES"
      );
    } else if (/wyndham/i.test(parent + brand)) {
      propose(
        "Family / Source Family",
        "Wyndham",
        "parent_company_wyndham",
        "TAXONOMY",
        "DEALALITY_RULES"
      );
    } else if (/independent|cambridge beaches|spice island|waterstone|casas/i.test(brand)) {
      propose(
        "Family / Source Family",
        "Independent",
        "independent_brand",
        "TAXONOMY",
        "DEALALITY_RULES"
      );
    }
  }

  // Tag proposal categories
  for (const p of proposals) {
    if (!p.category) {
      if (String(p.source || "").startsWith("adp_profile")) p.category = "EXISTING_DATA";
      else if (/TAXONOMY|derived|dealality|composite|affiliation|country_/i.test(p.source + p.confidence)) {
        p.category = "DEALALITY_RULES";
      } else p.category = "EXISTING_DATA";
    }
  }

  // Filter to writable schema fields
  const writable = new Set(
    (schema?.fields || []).filter((f) => f.writable && !f.computed).map((f) => f.name)
  );
  const safeProposals = proposals.filter((p) => writable.has(p.field));

  // Research-required empties (writable, not proposed, not conditional process)
  const conditional = new Set(CONDITIONAL_OR_PROCESS_FIELDS);
  for (const f of schema?.fields || []) {
    if (!f.writable || f.computed) continue;
    if (conditional.has(f.name)) continue;
    if (!isFieldEmpty(hpcFields[f.name])) continue;
    if (seen.has(f.name)) continue;
    if (safeProposals.some((p) => p.field === f.name)) continue;
    researchRequired.push({
      field: f.name,
      type: f.type,
      reason: "EMPTY_NO_RELIABLE_EVIDENCE",
    });
  }

  return {
    contractVersion: HPC_EXHAUSTIVE_FIELD_COMPLETION_V1,
    populationContract: HPC_ADP_FIELD_POPULATION_V1,
    proposals: safeProposals,
    ambiguous: base.ambiguous,
    notApplicable: base.notApplicable,
    researchRequired,
    corrections,
    dryRunPatch: Object.fromEntries(safeProposals.map((p) => [p.field, p.value])),
  };
}

/**
 * Classify one field for the matrix.
 */
export function classifySchemaField(schemaField, value, proposal) {
  const computed =
    schemaField?.computed || COMPUTED_FIELD_TYPES.includes(schemaField?.type);
  if (computed) {
    return {
      status: FIELD_STATUS.COMPUTED_DEPENDENT,
      empty: false,
      writable: false,
      disposition: "COMPUTED",
    };
  }
  if (CONDITIONAL_OR_PROCESS_FIELDS.includes(schemaField?.name)) {
    return {
      status: isFieldEmpty(value)
        ? FIELD_STATUS.NOT_APPLICABLE
        : FIELD_STATUS.VERIFIED_EXISTING,
      empty: isFieldEmpty(value),
      writable: true,
      disposition: isFieldEmpty(value) ? "CONDITIONAL_PROCESS_LEAVE_EMPTY" : "POPULATED",
    };
  }
  if (!isFieldEmpty(value)) {
    return {
      status: FIELD_STATUS.VERIFIED_EXISTING,
      empty: false,
      writable: true,
      disposition: "POPULATED",
    };
  }
  if (proposal) {
    return {
      status: FIELD_STATUS.RESEARCH_REQUIRED,
      empty: true,
      writable: true,
      disposition: "FILLABLE",
      fillSource: proposal.source,
      fillCategory: proposal.category,
    };
  }
  return {
    status: FIELD_STATUS.EMPTY_NO_REASON,
    empty: true,
    writable: true,
    disposition: "RESEARCH_REQUIRED",
  };
}

export function hotelCompletenessScore(fieldRows) {
  const applicable = fieldRows.filter(
    (r) =>
      r.disposition !== "COMPUTED" &&
      r.disposition !== "CONDITIONAL_PROCESS_LEAVE_EMPTY"
  );
  const populated = applicable.filter((r) => !r.empty).length;
  return {
    applicable: applicable.length,
    populated,
    empty: applicable.length - populated,
    pct: applicable.length
      ? Math.round((1000 * populated) / applicable.length) / 10
      : 0,
  };
}

export {
  HPC_MINIMUM_IDENTITY_FIELDS,
  HPC_ADP_ESSENTIAL_FIELDS,
};
