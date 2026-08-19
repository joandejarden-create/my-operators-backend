/**
 * AI Demand Positioning — Standard Scenario Library.
 * Market + chain-scale templates for cross-property benchmarking.
 * Prompts are INTERNAL ONLY — never exposed to customers.
 */

export const TRAVELER_INTENTS = Object.freeze({
  BUSINESS: "business",
  LEISURE: "leisure",
  COUPLES: "couples",
  FAMILY: "family",
  GROUP_MEETING: "group_meeting",
  WELLNESS: "wellness",
  ADVENTURE: "adventure",
  CELEBRATION: "celebration",
});

export const DECISION_FRAMES = Object.freeze({
  BEST_FOR: "best_for",
  WHERE_SHOULD: "where_should",
  RECOMMEND: "recommend",
  COMPARE: "compare",
});

/**
 * Standard scenario library for Boca Raton / Upper Upscale / Waterfront.
 * Each scenario has a unique ID, intent, frame, and the actual prompt text.
 */
export const BOCA_RATON_UPPER_UPSCALE_WATERFRONT_SCENARIOS = Object.freeze([
  // --- BUSINESS (10) ---
  { scenarioId: "std_boca_biz_01", intent: "business", frame: "best_for", query: "Best upscale hotel in Boca Raton for a business trip" },
  { scenarioId: "std_boca_biz_02", intent: "business", frame: "recommend", query: "Recommend a hotel in Boca Raton for a corporate executive visiting for meetings" },
  { scenarioId: "std_boca_biz_03", intent: "business", frame: "where_should", query: "Where should I stay in Boca Raton for a week-long business engagement?" },
  { scenarioId: "std_boca_biz_04", intent: "business", frame: "best_for", query: "Best hotel near Mizner Park Boca Raton for business travelers" },
  { scenarioId: "std_boca_biz_05", intent: "business", frame: "recommend", query: "Upscale hotel in Palm Beach County with good WiFi and work-friendly environment" },
  { scenarioId: "std_boca_biz_06", intent: "business", frame: "best_for", query: "Best waterfront hotel in Boca Raton for a business traveler who wants to relax after work" },
  { scenarioId: "std_boca_biz_07", intent: "business", frame: "compare", query: "Compare upscale hotels in Boca Raton for a business trip with good dining options" },
  { scenarioId: "std_boca_biz_08", intent: "business", frame: "where_should", query: "Where should I stay in South Florida for a client meeting in Boca Raton?" },
  { scenarioId: "std_boca_biz_09", intent: "business", frame: "recommend", query: "Hotel in Boca Raton with a boutique feel for a senior executive" },
  { scenarioId: "std_boca_biz_10", intent: "business", frame: "best_for", query: "Best hotel in Boca Raton with Hilton Honors points for business travel" },

  // --- LEISURE (8) ---
  { scenarioId: "std_boca_lei_01", intent: "leisure", frame: "best_for", query: "Best resort in Boca Raton for a relaxing vacation" },
  { scenarioId: "std_boca_lei_02", intent: "leisure", frame: "where_should", query: "Where should I stay in Boca Raton for a long weekend getaway?" },
  { scenarioId: "std_boca_lei_03", intent: "leisure", frame: "recommend", query: "Recommend a waterfront resort in South Florida with a pool and ocean views" },
  { scenarioId: "std_boca_lei_04", intent: "leisure", frame: "best_for", query: "Best upscale hotel in Boca Raton near the beach" },
  { scenarioId: "std_boca_lei_05", intent: "leisure", frame: "compare", query: "Compare luxury and upscale resorts in Boca Raton for a leisure trip" },
  { scenarioId: "std_boca_lei_06", intent: "leisure", frame: "recommend", query: "Upscale resort with waterfront dining in the Boca Raton area" },
  { scenarioId: "std_boca_lei_07", intent: "leisure", frame: "where_should", query: "Where to stay in Palm Beach County for a quiet relaxing vacation near water?" },
  { scenarioId: "std_boca_lei_08", intent: "leisure", frame: "best_for", query: "Best pet-friendly upscale hotel in Boca Raton" },

  // --- COUPLES (7) ---
  { scenarioId: "std_boca_cpl_01", intent: "couples", frame: "best_for", query: "Best romantic hotel in Boca Raton for a couples weekend" },
  { scenarioId: "std_boca_cpl_02", intent: "couples", frame: "recommend", query: "Recommend a waterfront hotel in South Florida for an anniversary trip" },
  { scenarioId: "std_boca_cpl_03", intent: "couples", frame: "where_should", query: "Where should my partner and I stay in Boca Raton for a romantic getaway?" },
  { scenarioId: "std_boca_cpl_04", intent: "couples", frame: "best_for", query: "Best hotel in Boca Raton with sunset views and fine dining for couples" },
  { scenarioId: "std_boca_cpl_05", intent: "couples", frame: "recommend", query: "Boutique resort in Palm Beach area for a honeymoon or anniversary" },
  { scenarioId: "std_boca_cpl_06", intent: "couples", frame: "compare", query: "Compare romantic waterfront hotels in Boca Raton" },
  { scenarioId: "std_boca_cpl_07", intent: "couples", frame: "best_for", query: "Best hotel with private balcony and water views near Boca Raton" },

  // --- GROUP / MEETING (10) ---
  { scenarioId: "std_boca_grp_01", intent: "group_meeting", frame: "best_for", query: "Best hotel in Boca Raton for a small corporate retreat" },
  { scenarioId: "std_boca_grp_02", intent: "group_meeting", frame: "recommend", query: "Recommend a hotel in South Florida for a team offsite of 30 people" },
  { scenarioId: "std_boca_grp_03", intent: "group_meeting", frame: "where_should", query: "Where should I host an executive leadership meeting in Boca Raton?" },
  { scenarioId: "std_boca_grp_04", intent: "group_meeting", frame: "best_for", query: "Best hotel with meeting space and waterfront views in Palm Beach County" },
  { scenarioId: "std_boca_grp_05", intent: "group_meeting", frame: "recommend", query: "Hotel in Boca Raton for a company event with 100-200 guests" },
  { scenarioId: "std_boca_grp_06", intent: "group_meeting", frame: "best_for", query: "Best venue for a corporate incentive trip in Boca Raton" },
  { scenarioId: "std_boca_grp_07", intent: "group_meeting", frame: "where_should", query: "Where to hold a board meeting for 20 executives in South Florida with waterfront setting?" },
  { scenarioId: "std_boca_grp_08", intent: "group_meeting", frame: "recommend", query: "Resort in Boca Raton with outdoor event space for a company celebration" },
  { scenarioId: "std_boca_grp_09", intent: "group_meeting", frame: "compare", query: "Compare meeting hotels in Boca Raton for a mid-size corporate event" },
  { scenarioId: "std_boca_grp_10", intent: "group_meeting", frame: "best_for", query: "Best hotel for a sales kickoff meeting in the Boca Raton area" },

  // --- FAMILY (5) ---
  { scenarioId: "std_boca_fam_01", intent: "family", frame: "best_for", query: "Best family-friendly resort in Boca Raton with a pool" },
  { scenarioId: "std_boca_fam_02", intent: "family", frame: "recommend", query: "Recommend a hotel in Boca Raton for a family with young kids near the beach" },
  { scenarioId: "std_boca_fam_03", intent: "family", frame: "where_should", query: "Where should I stay with my family in South Florida for spring break?" },
  { scenarioId: "std_boca_fam_04", intent: "family", frame: "best_for", query: "Best hotel in Palm Beach County for a multigenerational family trip" },
  { scenarioId: "std_boca_fam_05", intent: "family", frame: "compare", query: "Compare family resorts in the Boca Raton area" },

  // --- CELEBRATION (5) ---
  { scenarioId: "std_boca_cel_01", intent: "celebration", frame: "best_for", query: "Best hotel in Boca Raton for a birthday celebration dinner and overnight" },
  { scenarioId: "std_boca_cel_02", intent: "celebration", frame: "recommend", query: "Recommend a venue in Boca Raton for a rehearsal dinner or small wedding" },
  { scenarioId: "std_boca_cel_03", intent: "celebration", frame: "where_should", query: "Where to host an engagement party with waterfront views in South Florida?" },
  { scenarioId: "std_boca_cel_04", intent: "celebration", frame: "best_for", query: "Best waterfront event venue in Boca Raton for 100-150 guests" },
  { scenarioId: "std_boca_cel_05", intent: "celebration", frame: "recommend", query: "Upscale hotel in Palm Beach area for a milestone celebration" },

  // --- WELLNESS / ADVENTURE (5) ---
  { scenarioId: "std_boca_wel_01", intent: "wellness", frame: "best_for", query: "Best hotel in Boca Raton with spa and wellness amenities" },
  { scenarioId: "std_boca_wel_02", intent: "wellness", frame: "recommend", query: "Resort in South Florida with yoga, pool, and healthy dining" },
  { scenarioId: "std_boca_adv_01", intent: "adventure", frame: "best_for", query: "Best hotel in Boca Raton with water sports and boating" },
  { scenarioId: "std_boca_adv_02", intent: "adventure", frame: "recommend", query: "Hotel in South Florida near boating, kayaking, and paddleboarding" },
  { scenarioId: "std_boca_adv_03", intent: "adventure", frame: "where_should", query: "Where to stay in Boca Raton if I want easy access to fishing and water activities?" },
]);

export const NYC_TIMES_SQUARE_UPPER_UPSCALE_SCENARIOS = Object.freeze([
  // --- BUSINESS (10) ---
  { scenarioId: "std_nyc_biz_01", intent: "business", frame: "best_for", query: "Best hotel in Times Square for a business trip to New York" },
  { scenarioId: "std_nyc_biz_02", intent: "business", frame: "recommend", query: "Recommend a hotel in Midtown Manhattan for a corporate executive" },
  { scenarioId: "std_nyc_biz_03", intent: "business", frame: "where_should", query: "Where should I stay in NYC for meetings in Midtown?" },
  { scenarioId: "std_nyc_biz_04", intent: "business", frame: "best_for", query: "Best full-service hotel near Times Square with meeting rooms" },
  { scenarioId: "std_nyc_biz_05", intent: "business", frame: "recommend", query: "Upscale hotel near Penn Station and Times Square for business" },
  { scenarioId: "std_nyc_biz_06", intent: "business", frame: "compare", query: "Compare upscale business hotels in the Times Square area" },
  { scenarioId: "std_nyc_biz_07", intent: "business", frame: "best_for", query: "Best hotel in Midtown Manhattan with Marriott Bonvoy points for business" },
  { scenarioId: "std_nyc_biz_08", intent: "business", frame: "where_should", query: "Where to stay in New York City near Broadway for work and evening entertainment?" },
  { scenarioId: "std_nyc_biz_09", intent: "business", frame: "recommend", query: "Hotel in Times Square area with a rooftop bar and business amenities" },
  { scenarioId: "std_nyc_biz_10", intent: "business", frame: "best_for", query: "Best design-forward hotel in Midtown Manhattan for a creative industry professional" },

  // --- LEISURE (8) ---
  { scenarioId: "std_nyc_lei_01", intent: "leisure", frame: "best_for", query: "Best hotel in Times Square for a first-time visit to New York City" },
  { scenarioId: "std_nyc_lei_02", intent: "leisure", frame: "where_should", query: "Where should I stay in Manhattan for sightseeing and Broadway shows?" },
  { scenarioId: "std_nyc_lei_03", intent: "leisure", frame: "recommend", query: "Recommend a hotel in the heart of Times Square for a weekend trip to NYC" },
  { scenarioId: "std_nyc_lei_04", intent: "leisure", frame: "best_for", query: "Best upscale hotel near Broadway theaters in New York" },
  { scenarioId: "std_nyc_lei_05", intent: "leisure", frame: "compare", query: "Compare Times Square hotels for a 3-night leisure trip to NYC" },
  { scenarioId: "std_nyc_lei_06", intent: "leisure", frame: "recommend", query: "Upscale hotel in Midtown with great views of NYC and walkable to attractions" },
  { scenarioId: "std_nyc_lei_07", intent: "leisure", frame: "where_should", query: "Where to stay in New York near Central Park and Times Square?" },
  { scenarioId: "std_nyc_lei_08", intent: "leisure", frame: "best_for", query: "Best pet-friendly hotel in the Times Square area" },

  // --- COUPLES (7) ---
  { scenarioId: "std_nyc_cpl_01", intent: "couples", frame: "best_for", query: "Best romantic hotel in Times Square for a couples getaway" },
  { scenarioId: "std_nyc_cpl_02", intent: "couples", frame: "recommend", query: "Recommend a hotel in Midtown Manhattan for an anniversary trip" },
  { scenarioId: "std_nyc_cpl_03", intent: "couples", frame: "where_should", query: "Where should my partner and I stay in NYC for a Broadway and dining weekend?" },
  { scenarioId: "std_nyc_cpl_04", intent: "couples", frame: "best_for", query: "Best hotel in Times Square with rooftop bar for a date night" },
  { scenarioId: "std_nyc_cpl_05", intent: "couples", frame: "recommend", query: "Stylish boutique-feel hotel in Midtown for a romantic NYC trip" },
  { scenarioId: "std_nyc_cpl_06", intent: "couples", frame: "compare", query: "Compare romantic upscale hotels near Times Square and Theater District" },
  { scenarioId: "std_nyc_cpl_07", intent: "couples", frame: "best_for", query: "Best hotel with city views and fine dining near Times Square for couples" },

  // --- GROUP / MEETING (10) ---
  { scenarioId: "std_nyc_grp_01", intent: "group_meeting", frame: "best_for", query: "Best hotel in Times Square for a small corporate retreat" },
  { scenarioId: "std_nyc_grp_02", intent: "group_meeting", frame: "recommend", query: "Recommend a hotel in Midtown Manhattan for a team offsite of 30 people" },
  { scenarioId: "std_nyc_grp_03", intent: "group_meeting", frame: "where_should", query: "Where should I host a board meeting in Midtown New York?" },
  { scenarioId: "std_nyc_grp_04", intent: "group_meeting", frame: "best_for", query: "Best hotel near Times Square with ballroom and meeting space" },
  { scenarioId: "std_nyc_grp_05", intent: "group_meeting", frame: "recommend", query: "Hotel in the Theater District for a company awards dinner for 150 guests" },
  { scenarioId: "std_nyc_grp_06", intent: "group_meeting", frame: "best_for", query: "Best NYC hotel for a sales conference in Midtown" },
  { scenarioId: "std_nyc_grp_07", intent: "group_meeting", frame: "where_should", query: "Where to host a product launch event in Times Square area?" },
  { scenarioId: "std_nyc_grp_08", intent: "group_meeting", frame: "recommend", query: "Full-service hotel near Grand Central for a 3-day corporate meeting" },
  { scenarioId: "std_nyc_grp_09", intent: "group_meeting", frame: "compare", query: "Compare meeting and event hotels in Midtown Manhattan" },
  { scenarioId: "std_nyc_grp_10", intent: "group_meeting", frame: "best_for", query: "Best hotel for a corporate incentive trip in NYC with nightlife access" },

  // --- FAMILY (5) ---
  { scenarioId: "std_nyc_fam_01", intent: "family", frame: "best_for", query: "Best family-friendly hotel in Times Square for a NYC vacation" },
  { scenarioId: "std_nyc_fam_02", intent: "family", frame: "recommend", query: "Recommend a hotel in Midtown Manhattan for a family with teenagers" },
  { scenarioId: "std_nyc_fam_03", intent: "family", frame: "where_should", query: "Where should I stay with kids in New York near Broadway and Central Park?" },
  { scenarioId: "std_nyc_fam_04", intent: "family", frame: "best_for", query: "Best hotel near Times Square for a multigenerational family trip to NYC" },
  { scenarioId: "std_nyc_fam_05", intent: "family", frame: "compare", query: "Compare family hotels in the Times Square and Midtown area" },

  // --- CELEBRATION (5) ---
  { scenarioId: "std_nyc_cel_01", intent: "celebration", frame: "best_for", query: "Best hotel in Times Square for a birthday celebration in New York" },
  { scenarioId: "std_nyc_cel_02", intent: "celebration", frame: "recommend", query: "Recommend a hotel in Midtown with a rooftop for a private celebration" },
  { scenarioId: "std_nyc_cel_03", intent: "celebration", frame: "where_should", query: "Where to host a bachelorette party in Times Square area?" },
  { scenarioId: "std_nyc_cel_04", intent: "celebration", frame: "best_for", query: "Best upscale hotel in NYC for New Year's Eve near the ball drop" },
  { scenarioId: "std_nyc_cel_05", intent: "celebration", frame: "recommend", query: "Hotel near Broadway for a graduation celebration weekend in NYC" },

  // --- WELLNESS / ADVENTURE (5) ---
  { scenarioId: "std_nyc_wel_01", intent: "wellness", frame: "best_for", query: "Best hotel in Midtown Manhattan with spa or wellness amenities" },
  { scenarioId: "std_nyc_wel_02", intent: "wellness", frame: "recommend", query: "Hotel in Times Square area with fitness center and healthy dining" },
  { scenarioId: "std_nyc_adv_01", intent: "adventure", frame: "best_for", query: "Best hotel in Times Square for exploring NYC nightlife and culture" },
  { scenarioId: "std_nyc_adv_02", intent: "adventure", frame: "recommend", query: "Upscale hotel in Midtown for a food and theater enthusiast" },
  { scenarioId: "std_nyc_adv_03", intent: "adventure", frame: "where_should", query: "Where to stay in NYC for easy access to museums, Broadway, and restaurants?" },
]);

/**
 * Downtown / NoHo / SoHo / Lower Manhattan — for properties outside Midtown/Times Square.
 */
export const NYC_DOWNTOWN_UPPER_UPSCALE_SCENARIOS = Object.freeze([
  // --- BUSINESS (10) ---
  { scenarioId: "std_nyc_dt_biz_01", intent: "business", frame: "best_for", query: "Best upscale hotel in NoHo or SoHo for a business trip to New York" },
  { scenarioId: "std_nyc_dt_biz_02", intent: "business", frame: "recommend", query: "Recommend a design-forward hotel in downtown Manhattan for a corporate executive" },
  { scenarioId: "std_nyc_dt_biz_03", intent: "business", frame: "where_should", query: "Where should I stay in Lower Manhattan for meetings in the Financial District?" },
  { scenarioId: "std_nyc_dt_biz_04", intent: "business", frame: "best_for", query: "Best boutique hotel in SoHo with meeting space for small groups" },
  { scenarioId: "std_nyc_dt_biz_05", intent: "business", frame: "recommend", query: "Upscale hotel near NYU and Washington Square Park for business travel" },
  { scenarioId: "std_nyc_dt_biz_06", intent: "business", frame: "compare", query: "Compare upscale business hotels in NoHo, SoHo, and Greenwich Village" },
  { scenarioId: "std_nyc_dt_biz_07", intent: "business", frame: "best_for", query: "Best hotel in downtown NYC with Hyatt World of Hyatt for business" },
  { scenarioId: "std_nyc_dt_biz_08", intent: "business", frame: "where_should", query: "Where to stay in Manhattan below 14th Street for a week of client meetings?" },
  { scenarioId: "std_nyc_dt_biz_09", intent: "business", frame: "recommend", query: "Hotel in NoHo or Tribeca with a creative-industry vibe for business travelers" },
  { scenarioId: "std_nyc_dt_biz_10", intent: "business", frame: "best_for", query: "Best full-service hotel in Lower Manhattan for media and tech professionals" },

  // --- LEISURE (8) ---
  { scenarioId: "std_nyc_dt_lei_01", intent: "leisure", frame: "best_for", query: "Best hotel in SoHo for a first-time visit focused on shopping and galleries" },
  { scenarioId: "std_nyc_dt_lei_02", intent: "leisure", frame: "where_should", query: "Where should I stay in downtown Manhattan for a weekend in Greenwich Village?" },
  { scenarioId: "std_nyc_dt_lei_03", intent: "leisure", frame: "recommend", query: "Recommend a lifestyle hotel in NoHo for a leisure trip to New York" },
  { scenarioId: "std_nyc_dt_lei_04", intent: "leisure", frame: "best_for", query: "Best upscale hotel near Washington Square Park and NYU" },
  { scenarioId: "std_nyc_dt_lei_05", intent: "leisure", frame: "compare", query: "Compare boutique hotels in NoHo and SoHo for a 3-night NYC getaway" },
  { scenarioId: "std_nyc_dt_lei_06", intent: "leisure", frame: "recommend", query: "Upscale hotel in Lower Manhattan with walkable access to SoHo dining and shops" },
  { scenarioId: "std_nyc_dt_lei_07", intent: "leisure", frame: "where_should", query: "Where to stay in downtown NYC to avoid Midtown crowds but still explore the city?" },
  { scenarioId: "std_nyc_dt_lei_08", intent: "leisure", frame: "best_for", query: "Best pet-friendly boutique hotel in NoHo or SoHo" },

  // --- COUPLES (7) ---
  { scenarioId: "std_nyc_dt_cpl_01", intent: "couples", frame: "best_for", query: "Best romantic hotel in SoHo for a couples weekend in New York" },
  { scenarioId: "std_nyc_dt_cpl_02", intent: "couples", frame: "recommend", query: "Recommend a design hotel in NoHo for an anniversary trip downtown" },
  { scenarioId: "std_nyc_dt_cpl_03", intent: "couples", frame: "where_should", query: "Where should my partner and I stay in Greenwich Village for a romantic NYC trip?" },
  { scenarioId: "std_nyc_dt_cpl_04", intent: "couples", frame: "best_for", query: "Best boutique hotel in Lower Manhattan with great dining and nightlife nearby" },
  { scenarioId: "std_nyc_dt_cpl_05", intent: "couples", frame: "recommend", query: "Stylish hotel in SoHo or NoHo for a design-focused couples getaway" },
  { scenarioId: "std_nyc_dt_cpl_06", intent: "couples", frame: "compare", query: "Compare romantic boutique hotels in downtown Manhattan" },
  { scenarioId: "std_nyc_dt_cpl_07", intent: "couples", frame: "best_for", query: "Best hotel near Lafayette Street and SoHo for a date-night weekend" },

  // --- GROUP / MEETING (8) ---
  { scenarioId: "std_nyc_dt_grp_01", intent: "group_meeting", frame: "best_for", query: "Best hotel in downtown Manhattan for a small creative team offsite" },
  { scenarioId: "std_nyc_dt_grp_02", intent: "group_meeting", frame: "recommend", query: "Recommend a boutique hotel in SoHo with meeting space for 20-30 people" },
  { scenarioId: "std_nyc_dt_grp_03", intent: "group_meeting", frame: "where_should", query: "Where to host a board dinner in NoHo or Tribeca with a private room?" },
  { scenarioId: "std_nyc_dt_grp_04", intent: "group_meeting", frame: "best_for", query: "Best hotel in Lower Manhattan for a product launch with downtown nightlife access" },
  { scenarioId: "std_nyc_dt_grp_05", intent: "group_meeting", frame: "recommend", query: "Upscale hotel near NYU for a university or nonprofit leadership retreat" },
  { scenarioId: "std_nyc_dt_grp_06", intent: "group_meeting", frame: "best_for", query: "Best venue in SoHo for a company celebration of 80-100 guests" },
  { scenarioId: "std_nyc_dt_grp_07", intent: "group_meeting", frame: "where_should", query: "Where to hold an intimate executive meeting in downtown Manhattan?" },
  { scenarioId: "std_nyc_dt_grp_08", intent: "group_meeting", frame: "compare", query: "Compare meeting-friendly boutique hotels in NoHo and Greenwich Village" },

  // --- FAMILY (5) ---
  { scenarioId: "std_nyc_dt_fam_01", intent: "family", frame: "best_for", query: "Best family-friendly hotel in downtown Manhattan near Washington Square Park" },
  { scenarioId: "std_nyc_dt_fam_02", intent: "family", frame: "recommend", query: "Recommend a hotel in SoHo or NoHo for a family with teenagers visiting NYC" },
  { scenarioId: "std_nyc_dt_fam_03", intent: "family", frame: "where_should", query: "Where should I stay below 14th Street with kids who want to explore downtown?" },
  { scenarioId: "std_nyc_dt_fam_04", intent: "family", frame: "best_for", query: "Best hotel in Lower Manhattan for a multigenerational family trip" },
  { scenarioId: "std_nyc_dt_fam_05", intent: "family", frame: "compare", query: "Compare family hotels in Greenwich Village and SoHo" },

  // --- CELEBRATION (5) ---
  { scenarioId: "std_nyc_dt_cel_01", intent: "celebration", frame: "best_for", query: "Best hotel in SoHo for a birthday celebration dinner and overnight" },
  { scenarioId: "std_nyc_dt_cel_02", intent: "celebration", frame: "recommend", query: "Recommend a boutique hotel in NoHo for a private rooftop or lounge celebration" },
  { scenarioId: "std_nyc_dt_cel_03", intent: "celebration", frame: "where_should", query: "Where to host a rehearsal dinner in downtown Manhattan with gallery or design vibe?" },
  { scenarioId: "std_nyc_dt_cel_04", intent: "celebration", frame: "best_for", query: "Best upscale hotel in Lower Manhattan for a milestone celebration weekend" },
  { scenarioId: "std_nyc_dt_cel_05", intent: "celebration", frame: "recommend", query: "Hotel in Greenwich Village or SoHo for a graduation celebration in NYC" },

  // --- WELLNESS / ADVENTURE (5) ---
  { scenarioId: "std_nyc_dt_wel_01", intent: "wellness", frame: "best_for", query: "Best hotel in downtown Manhattan with fitness center and wellness amenities" },
  { scenarioId: "std_nyc_dt_wel_02", intent: "wellness", frame: "recommend", query: "Boutique hotel in NoHo with gym and healthy dining options" },
  { scenarioId: "std_nyc_dt_adv_01", intent: "adventure", frame: "best_for", query: "Best hotel in SoHo for exploring galleries, restaurants, and downtown nightlife" },
  { scenarioId: "std_nyc_dt_adv_02", intent: "adventure", frame: "recommend", query: "Upscale hotel in Lower Manhattan for a food and culture enthusiast" },
  { scenarioId: "std_nyc_dt_adv_03", intent: "adventure", frame: "where_should", query: "Where to stay in NYC for easy access to SoHo, the Village, and the High Line?" },
]);

export const BERMUDA_LUXURY_RESORT_SCENARIOS = Object.freeze([
  // --- BUSINESS (6) ---
  { scenarioId: "std_bda_biz_01", intent: "business", frame: "best_for", query: "Best luxury resort in Bermuda for a corporate executive retreat" },
  { scenarioId: "std_bda_biz_02", intent: "business", frame: "recommend", query: "Recommend an exclusive resort in Bermuda for a board meeting" },
  { scenarioId: "std_bda_biz_03", intent: "business", frame: "where_should", query: "Where should I host a C-suite retreat in Bermuda?" },
  { scenarioId: "std_bda_biz_04", intent: "business", frame: "best_for", query: "Best private resort in Bermuda for an offsite with 20 executives" },
  { scenarioId: "std_bda_biz_05", intent: "business", frame: "compare", query: "Compare luxury resorts in Bermuda for a corporate incentive trip" },
  { scenarioId: "std_bda_biz_06", intent: "business", frame: "recommend", query: "Intimate luxury resort in Bermuda for a financial services retreat" },

  // --- LEISURE (8) ---
  { scenarioId: "std_bda_lei_01", intent: "leisure", frame: "best_for", query: "Best luxury resort in Bermuda for a relaxing beach vacation" },
  { scenarioId: "std_bda_lei_02", intent: "leisure", frame: "where_should", query: "Where should I stay in Bermuda for a quiet luxury getaway?" },
  { scenarioId: "std_bda_lei_03", intent: "leisure", frame: "recommend", query: "Recommend the most exclusive beach resort in Bermuda" },
  { scenarioId: "std_bda_lei_04", intent: "leisure", frame: "best_for", query: "Best resort in Bermuda with private beaches and ocean access" },
  { scenarioId: "std_bda_lei_05", intent: "leisure", frame: "compare", query: "Compare luxury resorts in Bermuda for a week-long vacation" },
  { scenarioId: "std_bda_lei_06", intent: "leisure", frame: "recommend", query: "Boutique resort in Bermuda with cottage-style accommodation" },
  { scenarioId: "std_bda_lei_07", intent: "leisure", frame: "where_should", query: "Where to stay in Bermuda for the best snorkeling and swimming?" },
  { scenarioId: "std_bda_lei_08", intent: "leisure", frame: "best_for", query: "Best adults-only resort in Bermuda" },

  // --- COUPLES (8) ---
  { scenarioId: "std_bda_cpl_01", intent: "couples", frame: "best_for", query: "Best romantic resort in Bermuda for a honeymoon" },
  { scenarioId: "std_bda_cpl_02", intent: "couples", frame: "recommend", query: "Recommend an intimate luxury resort in Bermuda for an anniversary" },
  { scenarioId: "std_bda_cpl_03", intent: "couples", frame: "where_should", query: "Where should we go in Bermuda for a romantic couples trip?" },
  { scenarioId: "std_bda_cpl_04", intent: "couples", frame: "best_for", query: "Best resort in Bermuda with private coves and spa for couples" },
  { scenarioId: "std_bda_cpl_05", intent: "couples", frame: "recommend", query: "Most romantic oceanfront hotel in Bermuda" },
  { scenarioId: "std_bda_cpl_06", intent: "couples", frame: "compare", query: "Compare honeymoon resorts in Bermuda" },
  { scenarioId: "std_bda_cpl_07", intent: "couples", frame: "best_for", query: "Best adults-only resort in Bermuda for a proposal trip" },
  { scenarioId: "std_bda_cpl_08", intent: "couples", frame: "where_should", query: "Where to stay in Bermuda for a babymoon or romantic escape?" },

  // --- GROUP / MEETING (6) ---
  { scenarioId: "std_bda_grp_01", intent: "group_meeting", frame: "best_for", query: "Best resort in Bermuda for a destination corporate retreat" },
  { scenarioId: "std_bda_grp_02", intent: "group_meeting", frame: "recommend", query: "Recommend a private resort in Bermuda for a team offsite" },
  { scenarioId: "std_bda_grp_03", intent: "group_meeting", frame: "where_should", query: "Where to host a small leadership retreat in Bermuda?" },
  { scenarioId: "std_bda_grp_04", intent: "group_meeting", frame: "best_for", query: "Best venue in Bermuda for a private group event of 80 guests" },
  { scenarioId: "std_bda_grp_05", intent: "group_meeting", frame: "recommend", query: "Exclusive resort in Bermuda with event space for a company celebration" },
  { scenarioId: "std_bda_grp_06", intent: "group_meeting", frame: "compare", query: "Compare Bermuda resorts for a destination board meeting" },

  // --- CELEBRATION (7) ---
  { scenarioId: "std_bda_cel_01", intent: "celebration", frame: "best_for", query: "Best resort in Bermuda for a destination wedding" },
  { scenarioId: "std_bda_cel_02", intent: "celebration", frame: "recommend", query: "Recommend a luxury resort in Bermuda for a wedding reception" },
  { scenarioId: "std_bda_cel_03", intent: "celebration", frame: "where_should", query: "Where to host an intimate destination wedding in Bermuda?" },
  { scenarioId: "std_bda_cel_04", intent: "celebration", frame: "best_for", query: "Best beachfront wedding venue in Bermuda for 80-100 guests" },
  { scenarioId: "std_bda_cel_05", intent: "celebration", frame: "recommend", query: "Intimate resort in Bermuda for a milestone anniversary celebration" },
  { scenarioId: "std_bda_cel_06", intent: "celebration", frame: "best_for", query: "Best resort in Bermuda for a vow renewal on a private beach" },
  { scenarioId: "std_bda_cel_07", intent: "celebration", frame: "where_should", query: "Where to celebrate a 50th birthday in Bermuda with ocean views?" },

  // --- WELLNESS / ADVENTURE (10) ---
  { scenarioId: "std_bda_wel_01", intent: "wellness", frame: "best_for", query: "Best spa resort in Bermuda for a wellness retreat" },
  { scenarioId: "std_bda_wel_02", intent: "wellness", frame: "recommend", query: "Recommend a resort in Bermuda with ocean-view spa treatments" },
  { scenarioId: "std_bda_wel_03", intent: "wellness", frame: "where_should", query: "Where to stay in Bermuda for yoga, spa, and tranquility?" },
  { scenarioId: "std_bda_wel_04", intent: "wellness", frame: "best_for", query: "Best wellness-focused resort in Bermuda with healthy dining" },
  { scenarioId: "std_bda_wel_05", intent: "wellness", frame: "compare", query: "Compare spa resorts in Bermuda for a couples wellness escape" },
  { scenarioId: "std_bda_adv_01", intent: "adventure", frame: "best_for", query: "Best resort in Bermuda for snorkeling, kayaking, and water sports" },
  { scenarioId: "std_bda_adv_02", intent: "adventure", frame: "recommend", query: "Resort in Bermuda with direct ocean access for scuba diving and paddleboarding" },
  { scenarioId: "std_bda_adv_03", intent: "adventure", frame: "where_should", query: "Where to stay in Bermuda for the best water sports and reef access?" },
  { scenarioId: "std_bda_adv_04", intent: "adventure", frame: "best_for", query: "Best resort in Bermuda for an active outdoor vacation" },
  { scenarioId: "std_bda_adv_05", intent: "adventure", frame: "recommend", query: "Luxury resort in Bermuda that offers tennis, croquet, and ocean activities" },
]);

export function getStandardScenarios(market, chainScale) {
  if (market === "boca_raton" && chainScale === "upper_upscale") {
    return BOCA_RATON_UPPER_UPSCALE_WATERFRONT_SCENARIOS;
  }
  if (market === "nyc_downtown" && chainScale === "upper_upscale") {
    return NYC_DOWNTOWN_UPPER_UPSCALE_SCENARIOS;
  }
  if (market === "nyc_times_square" || market === "New York City" || market === "nyc") {
    return NYC_TIMES_SQUARE_UPPER_UPSCALE_SCENARIOS;
  }
  if (market === "Bermuda" || market === "bermuda") {
    return BERMUDA_LUXURY_RESORT_SCENARIOS;
  }
  return [];
}
