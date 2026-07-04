/**
 * Tier 1 CHI brands — brand-specific facts for full Explorer presentation generation.
 * Sources: 2026 FDD text, media press kits, FDD Item 19 (choice-fdd-item19.mjs), architecture deck.
 */
import { FDD_ITEM19 } from "./choice-fdd-item19.mjs";

/** @typedef {'economy'|'midscale'|'upperMidscale'|'upscale'|'extendedStay'|'softCollection'} Segment */

/**
 * @typedef {Object} Tier1Profile
 * @property {string} name — exact Brand Basics name in Airtable
 * @property {string} slug
 * @property {Segment} segment
 * @property {string} scaleLabel
 * @property {string} tagline
 * @property {string} royaltyLabel — e.g. "6.0% royalty on gross room revenues"
 * @property {string} fddFile
 * @property {string|null} pressKitFile — under fixtures/choice-media-center-text/
 * @property {string} positioning
 * @property {string} developmentModel
 * @property {string} typicalUseCase
 * @property {string[]} scenarios
 * @property {string[]} bestAt
 * @property {string[]} growthThemes
 * @property {string} footprintEditorial
 * @property {string} pipelineStats — press-kit / FDD scale sentence
 * @property {string} heroPurpose
 * @property {string[]} similarBrands — for insight.similar
 * @property {import('./choice-tier1-explorer-profiles.mjs').FootprintOpeningCard[]=} footprintOpenings — optional footprint.openings rows
 * @property {import('./choice-tier1-explorer-profiles.mjs').MaterialsCaseStudyOverride=} materialsCaseStudy — optional materials.caseStudy override
 */

/**
 * @typedef {Object} FootprintOpeningCard
 * @property {string} title
 * @property {string} body
 * @property {number=} sort
 * @property {string} caseSummaryOverview
 * @property {string} caseSummaryOwnerObjective
 * @property {string} caseSummaryBrandRelevance
 * @property {string} caseSummaryInterpretation
 * @property {string} caseSummaryTags
 */

/**
 * @typedef {Object} MaterialsCaseStudyOverride
 * @property {string} title
 * @property {string} body
 * @property {string} caseSummaryOverview
 * @property {string} caseSummaryOwnerObjective
 * @property {string} caseSummaryBrandRelevance
 * @property {string} caseSummaryInterpretation
 * @property {string} caseSummaryTags
 */

/** @type {Tier1Profile[]} */
export const TIER1_BRANDS = [
  {
    name: "Comfort Inn & Suites",
    slug: "comfort-inn-suites",
    segment: "upperMidscale",
    scaleLabel: "upper-midscale",
    tagline: "Trusted reliability, value, and convenience for business and leisure travelers.",
    royaltyLabel: "6.0% royalty on gross room revenues (Comfort FDD; confirm marketing/reservation and other fees in disclosure)",
    fddFile: "35771-202604-09.txt",
    pressKitFile: "comfort-press-kit.txt",
    positioning:
      "Refreshed upper-midscale flag with strong North American footprint—complimentary hearty breakfast, smoke-free positioning, and Choice Privileges—not economy roadside or extended-stay.",
    developmentModel: "New construction and conversion; major brand renaissance with updated prototype and logo; pipeline weighted to new build per press materials.",
    typicalUseCase:
      "Interstate and suburban corridors, small-city business travel, and owners seeking a recognizable upper-midscale flag with breakfast-led value proposition and high guest-satisfaction momentum post-transformation.",
    scenarios: [
      "Independent or tired midscale reflag needing recognizable upper-midscale retail and complimentary breakfast economics.",
      "New construction along growth corridors where smoke-free, breakfast-led limited-service competes on value and consistency.",
      "Dual-flag or portfolio owners standardizing on Choice upper-midscale after prototype refresh.",
    ],
    bestAt: [
      "Corridors where complimentary hearty breakfast and smoke-free positioning differentiate against bare-bones economy flags.",
      "Owners who can execute refreshed guestroom and lobby standards without full-service F&B complexity.",
      "Markets where Choice Privileges direct mix and enterprise distribution lift repeat and midweek demand.",
    ],
    growthThemes: [
      "Suburban and interstate upper-midscale",
      "New construction pipeline (300+ properties in development)",
      "Conversion of tired midscale assets",
      "Smoke-free, breakfast-led limited-service",
    ],
    footprintEditorial:
      "Comfort is one of Choice’s largest upper-midscale systems—1,600+ U.S. hotels and 2,100+ globally per press materials, with 300+ properties in pipeline (~80% new construction). Evaluate fit on breakfast operating model, prototype compliance, and loyalty contribution in your comp set.",
    pipelineStats: "1,600+ U.S. hotels; 2,100+ globally; 300+ properties in pipeline (~80% new construction).",
    heroPurpose:
      "Deliver a refreshed, welcoming upper-midscale stay with RAIO bath amenities, pillow choice, and complimentary breakfast—operators who run consistent limited-service with strong QA on breakfast and guestroom standards.",
    similarBrands: ["Quality Inn", "Sleep Inn", "Country Inn & Suites by Radisson (Choice)"],
  },
  {
    name: "Sleep Inn",
    slug: "sleep-inn",
    segment: "midscale",
    scaleLabel: "midscale",
    tagline: "Simply stylish, timeless new construction with affordable style.",
    royaltyLabel: "5.5% royalty on gross room revenues (Sleep Inn FDD)",
    fddFile: "35785-202604-09.txt",
    pressKitFile: "sleep-inn-press-kit.txt",
    positioning:
      "Midscale new-construction leader with efficient footprint and lowest build costs in segment—Morning Medley breakfast and nature-inspired design, not upper-midscale full breakfast or extended-stay.",
    developmentModel: "Primarily new construction; dual-brand with MainStay Suites in select markets; evolved prototype with efficient room layout.",
    typicalUseCase:
      "Developers seeking midscale NC with smart footprint, dual-brand Sleep + MainStay sites, and simply stylish guest experience at affordable ADR.",
    scenarios: [
      "Greenfield midscale where build cost and footprint efficiency drive returns.",
      "Dual-brand pad with MainStay extended stay on the same site.",
      "Markets needing reliable midscale without upper-midscale breakfast capex.",
    ],
    bestAt: [
      "New construction with efficient prototype and lowest-in-segment build cost narrative.",
      "Dual-brand Sleep + MainStay developments (90+ dual projects in pipeline).",
      "Operators who deliver consistent Morning Medley breakfast and Zenses amenities without upscale public-space theater.",
    ],
    growthThemes: ["Midscale new construction", "Sleep + MainStay dual-brand", "Efficient footprint NC", "550+ open or pipeline worldwide"],
    footprintEditorial:
      "550+ Sleep Inn hotels open or in pipeline worldwide with rapid U.S. expansion; 10 open dual-brands with MainStay and 90+ dual projects in pipeline. Model midscale economics and dual-brand shared back-of-house where applicable.",
    pipelineStats: "550+ open or in pipeline worldwide; 90+ Sleep/MainStay dual-brand projects in pipeline.",
    heroPurpose:
      "Affordable, simply stylish stays with nature-inspired design and Dream Cup coffee—operators strong on midscale NC execution and optional dual-brand extended-stay pairing.",
    similarBrands: ["Quality Inn", "MainStay Suites", "Rodeway Inn"],
  },
  {
    name: "Quality Inn",
    slug: "quality-inn",
    segment: "midscale",
    scaleLabel: "midscale",
    tagline: "Quality everywhere you want to be.",
    royaltyLabel: "5.25% royalty on gross room revenues (Quality Inn FDD)",
    fddFile: "35778-202604-09.txt",
    pressKitFile: "quality-press-kit.txt",
    positioning:
      "Core Choice midscale flag for conversions and new builds—value-oriented guestrooms and breakfast-led limited-service across suburban and highway markets.",
    developmentModel: "Conversion and new construction; broad geographic footprint; franchisee-friendly midscale economics.",
    typicalUseCase:
      "Highway and suburban conversions, portfolio standardization, and owners who want midscale Choice distribution with recognizable Quality retail.",
    scenarios: [
      "Tired independent or economy conversion seeking midscale flag and Choice systems.",
      "Portfolio roll-up to midscale with consistent breakfast and guestroom standards.",
      "Markets where midscale ADR supports limited-service without upscale F&B.",
    ],
    bestAt: [
      "Conversion economics where midscale royalty and PIP are manageable versus upscale repositioning.",
      "Corridors with blended leisure and business demand at midscale price points.",
      "Owners prioritizing Choice Privileges and enterprise channels at midscale fee levels.",
    ],
    growthThemes: ["Midscale conversion", "Highway and suburban", "Portfolio standardization", "Breakfast-led limited-service"],
    footprintEditorial:
      "Quality Inn is a foundational midscale system in the Choice portfolio—confirm current open and pipeline counts in your FDD Item 20. Focus diligence on conversion PIP, breakfast model, and loyalty contribution versus local comp set.",
    pipelineStats: "Confirm open/pipeline counts in FDD Item 20 — midscale system scale across North America.",
    heroPurpose:
      "Reliable midscale stays with recognizable Quality retail—operators who execute conversion PIPs and breakfast-led service without over-building public space.",
    similarBrands: ["Comfort Inn & Suites", "Sleep Inn", "Clarion"],
  },
  {
    name: "Cambria Hotels",
    slug: "cambria-hotels",
    segment: "upscale",
    scaleLabel: "upscale",
    tagline: "Contemporary essentials and approachable indulgences for modern travelers.",
    royaltyLabel: "6% royalty on gross room revenues (Cambria FDD)",
    fddFile: "35798-202604-03.txt",
    pressKitFile: "cambria-press-kit.txt",
    positioning:
      "Upscale lifestyle-oriented flag with design-forward rooms, local F&B, rooftop bars, and meeting product—J.D. Power-ranked guest satisfaction, not upper-midscale limited-service.",
    developmentModel: "New construction, conversion, and adaptive reuse near corporate demand, conventions, and attractions.",
    typicalUseCase:
      "Urban and suburban upscale NC or conversion where owners want bar/restaurant, meetings, and design-forward guestrooms with Choice Privileges distribution.",
    scenarios: [
      "Urban adaptive reuse or conversion to upscale with local F&B and meeting space.",
      "NC near convention center or corporate campus with rooftop/bar amenity stack.",
      "Owners moving from soft brand or independent upscale needing Choice systems and loyalty.",
    ],
    bestAt: [
      "Gateway and downtown markets where upscale F&B, bar, and meetings justify higher capex.",
      "Developers who can deliver design-forward prototype with spa-inspired baths and local décor narrative.",
      "Assets where J.D. Power–level guest experience expectations match operator upscale depth.",
    ],
    growthThemes: [
      "Urban NC and adaptive reuse",
      "Convention and corporate adjacency",
      "Upscale F&B and rooftop activation",
      "Fastest-growing upscale pipeline",
    ],
    footprintEditorial:
      "Cambria is growing faster than ever in upscale—site criteria emphasize corporate, convention, and attraction adjacency. Confirm open count and pipeline in FDD; budget for restaurant, bar, meetings, and design-forward FF&E.",
    pipelineStats: "Ranked #1 upscale in J.D. Power 2023 North America guest satisfaction (2023 study); 8 locations opened in 2022—confirm current counts in your franchise disclosure document.",
    heroPurpose:
      "Help guests maximize time away from home with locally inspired design, premium bedding, immersive baths, and F&B—operators with proven upscale F&B and meetings capability.",
    similarBrands: ["Ascend Hotel Collection", "Clarion", "Radisson Blu (Choice)"],
  },
  {
    name: "MainStay Suites",
    slug: "mainstay-suites",
    segment: "extendedStay",
    scaleLabel: "extended-stay",
    tagline: "Residential-style extended stay for project-based and relocating guests.",
    royaltyLabel: "6.0% royalty on gross room revenues (MainStay FDD)",
    fddFile: "35775-202604-09.txt",
    pressKitFile: "mainstay-suites-press-kit.txt",
    positioning:
      "Extended-stay with in-suite kitchen and residential feel—paired with Sleep Inn in dual-brand developments, not nightly transient midscale.",
    developmentModel: "New construction; dual-brand with Sleep Inn; weekly/monthly rate mix.",
    typicalUseCase:
      "Extended-stay pads near employment centers, hospitals, and project corridors; dual-brand sites with Sleep Inn for blended demand.",
    scenarios: [
      "Greenfield extended-stay with kitchen-equipped suites and weekly revenue mix.",
      "Dual-brand development sharing site costs with Sleep Inn midscale.",
      "Conversion of older extended-stay product needing Choice systems refresh.",
    ],
    bestAt: [
      "Markets with project, relocation, or medical extended-stay demand.",
      "Developers using Sleep + MainStay dual-brand efficiency on one pad.",
      "Operators who manage housekeeping cadence and kitchen FF&E for longer stays.",
    ],
    growthThemes: ["Extended-stay NC", "Sleep + MainStay dual-brand", "Weekly rate mix", "Employment-center adjacency"],
    footprintEditorial:
      "MainStay competes in Choice’s extended-stay tier—confirm system size in FDD Item 20. Underwrite weekly/monthly mix, kitchen FF&E, and dual-brand economics when paired with Sleep Inn.",
    pipelineStats: "Dual-brand: 10 open Sleep+MainStay, 90+ dual projects in pipeline—confirm MainStay-specific counts in your franchise disclosure document.",
    heroPurpose:
      "Residential comfort for extended guests with in-suite kitchens—operators who understand weekly housekeeping, kitchen wear, and dual-brand site planning.",
    similarBrands: ["Suburban Studios", "Everhome Suites", "Sleep Inn"],
  },
  {
    name: "Ascend Hotel Collection",
    slug: "ascend-hotel-collection",
    segment: "softCollection",
    scaleLabel: "soft collection / upscale independent",
    tagline: "Unique, independent-spirited hotels united by Choice distribution.",
    royaltyLabel: "5.0% membership fee on gross room revenues (Ascend FDD; plus marketing & reservation fees per disclosure)",
    fddFile: "35768-202604-08.txt",
    pressKitFile: "ascend-hotel-collection-press-kit.txt",
    positioning:
      "Soft collection preserving local character while accessing Choice Privileges and CRS—boutique and independent personality, not rigid prototype limited-service.",
    developmentModel: "Conversion of unique independents and boutique assets; flexibility within collection standards.",
    typicalUseCase:
      "Independent upscale or boutique hotels that want loyalty and distribution without losing local identity; historic and design-forward conversions.",
    scenarios: [
      "Independent boutique seeking Choice Privileges without full prototype homogenization.",
      "Historic or design hotel conversion with local F&B retained.",
      "Owners balancing uniqueness with enterprise and member channels.",
    ],
    bestAt: [
      "Assets with genuine local story where collection flexibility is contractual.",
      "Markets where guests pay for character but owners need Choice distribution.",
      "Operators experienced in boutique service and variable F&B models.",
    ],
    growthThemes: [
      "Boutique and independent conversion",
      "Historic and design-forward assets",
      "Urban and resort unique properties",
      "Collection flexibility within Choice stack",
    ],
    footprintEditorial:
      "Ascend is a collection, not a single prototype—142-hotel Item 19 sample cited in FDD notes. Confirm property count and fit criteria; diligence on how much local identity survives standards review.",
    pipelineStats: "Item 19 sample: 142 hotels (FY 2025 FDD notes) — confirm collection size in Item 20.",
    heroPurpose:
      "Celebrate unique hotels with Choice backing—operators who can maintain local differentiation while meeting collection compliance and loyalty fulfillment.",
    similarBrands: ["Cambria Hotels", "Clarion", "Radisson Individual (Choice)"],
  },
  {
    name: "Clarion",
    slug: "clarion",
    segment: "midscale",
    scaleLabel: "midscale / upper-midscale meetings",
    tagline: "Meetings-capable midscale with flexible event space.",
    royaltyLabel: "5.5% royalty on gross room revenues (Clarion FDD; shared performance sample with Clarion Pointe)",
    fddFile: "35770-202604-09.txt",
    pressKitFile: "clarion-press-kit.txt",
    positioning:
      "Meetings-oriented midscale flag with event space and group potential—distinct from limited-service-only Sleep or economy Rodeway.",
    developmentModel: "Conversion and new construction; assets with meeting and small-group capability.",
    typicalUseCase:
      "Suburban and highway hotels with meeting space, SMERF and corporate small groups, and owners who can staff events without full upscale F&B.",
    scenarios: [
      "Conversion with viable meeting space needing midscale flag with group narrative.",
      "Secondary markets with local group demand and modest F&B.",
      "Portfolio owners separating meetings-capable midscale from pure limited-service.",
    ],
    bestAt: [
      "Assets with functional meeting space and group sales capability.",
      "Markets where small meetings supplement transient without resort capex.",
      "Operators who can run modest F&B and event service at midscale cost structure.",
    ],
    growthThemes: ["Meetings-capable midscale", "Conversion with event space", "Group + transient mix", "Suburban corporate corridors"],
    footprintEditorial:
      "Clarion shares Item 19 performance representation with Clarion Pointe (155-hotel combined sample per FDD). Confirm open counts and meeting requirements in disclosure before capital planning.",
    pipelineStats: "Combined Clarion + Clarion Pointe Item 19 sample: 155 hotels — FDD FY 2025.",
    heroPurpose:
      "Deliver meetings-friendly midscale stays—operators with group sales discipline and event operations without upscale full-service overhead.",
    similarBrands: ["Clarion Pointe", "Quality Inn", "Cambria Hotels"],
  },
  {
    name: "Clarion Pointe",
    slug: "clarion-pointe",
    segment: "midscale",
    scaleLabel: "midscale select-service",
    tagline: "Streamlined Clarion experience for select-service and conversion-friendly assets.",
    royaltyLabel: "5.5% royalty on gross room revenues (shared Clarion FDD; confirm Pointe-specific fees)",
    fddFile: "35770-202604-09.txt",
    pressKitFile: "clarion-pointe-press-kit.txt",
    positioning:
      "Select-service expression of Clarion family—lighter meetings/F&B scope than full Clarion, still group-capable versus pure limited-service.",
    developmentModel: "Conversion-focused; efficient prototype for assets that cannot support full Clarion meetings stack.",
    typicalUseCase:
      "Conversions needing Clarion family distribution with reduced meetings capex; highway and suburban select-service.",
    scenarios: [
      "Conversion where full Clarion meetings product is not viable but Clarion retail is desired.",
      "Select-service highway asset with modest meeting room.",
      "Owners trading down from full Clarion scope while staying in Clarion system.",
    ],
    bestAt: [
      "Conversion economics with Clarion brand family access at select-service cost.",
      "Modest meeting room without full banquet infrastructure.",
      "Markets where Pointe prototype fits existing building constraints.",
    ],
    growthThemes: ["Select-service conversion", "Clarion family distribution", "Modest meetings", "Efficient prototype"],
    footprintEditorial:
      "Clarion Pointe shares financial performance disclosures with Clarion in the current FDD—model loyalty and enterprise metrics from combined Item 19 tables; confirm Pointe-specific standards separately.",
    pipelineStats: "Shared Item 19 with Clarion (155-hotel sample) — separate Pointe counts in Item 20.",
    heroPurpose:
      "Clarion recognition with select-service efficiency—operators who right-size meetings and F&B to the physical asset.",
    similarBrands: ["Clarion", "Quality Inn", "Comfort Inn & Suites"],
  },
  {
    name: "Econo Lodge",
    slug: "econo-lodge",
    segment: "economy",
    scaleLabel: "economy",
    tagline: "Affordable economy lodging for value-conscious travelers.",
    royaltyLabel: "5.0% royalty on gross room revenues (Econo Lodge FDD)",
    fddFile: "35773-202604-09.txt",
    pressKitFile: "econo-lodge-press-kit.txt",
    positioning:
      "Economy tier focused on price-sensitive transient—minimal amenity stack versus midscale breakfast brands.",
    developmentModel: "Conversion and new build in economy corridors; lean operations.",
    typicalUseCase:
      "Highway economy sites, budget conversions, and owners optimizing for lowest operating cost per occupied room.",
    scenarios: [
      "Economy conversion or NC on price-sensitive interstate.",
      "Portfolio rationalization to economy fee stack.",
      "Owners exiting failed midscale who need economy positioning.",
    ],
    bestAt: [
      "Markets where rate-sensitive leisure and contractor demand dominate.",
      "Operators expert at lean staffing and minimal public-space cost.",
      "Assets where economy royalty and PIP beat midscale repositioning ROI.",
    ],
    growthThemes: ["Economy highway", "Conversion to lean prototype", "Price-led transient", "Lowest amenity stack"],
    footprintEditorial:
      "Econo Lodge sits in Choice’s economy tier—confirm system size in FDD. Underwrite on ADR sensitivity, low amenity opex, and lower loyalty contribution versus midscale flags.",
    pipelineStats: "Confirm open/pipeline in FDD Item 20 — economy system within Choice portfolio.",
    heroPurpose:
      "Simple, affordable stays—operators who win on cost control and basic cleanliness, not breakfast theater or meetings.",
    similarBrands: ["Rodeway Inn", "Suburban Studios", "Quality Inn"],
  },
  {
    name: "Rodeway Inn",
    slug: "rodeway-inn",
    segment: "economy",
    scaleLabel: "economy",
    tagline: "Economy lodging for budget-conscious travelers.",
    royaltyLabel: "5.0% royalty on gross room revenues (Rodeway FDD)",
    fddFile: "35784-202604-09.txt",
    pressKitFile: "rodeway-inn-press-kit.txt",
    positioning:
      "Economy flag alongside Econo—value ADR, lean operations, lowest loyalty attach in CHI Item 19 sample set.",
    developmentModel: "Conversion and selective new build; economy interstate and urban budget.",
    typicalUseCase:
      "Budget highway hotels, economy conversions, and owners prioritizing lowest franchise fee tier in Choice family.",
    scenarios: [
      "Economy reflag from independent or distressed midscale downgrade path.",
      "Interstate budget with minimal amenity investment.",
      "Investors targeting highest economy yield on lean cost structure.",
    ],
    bestAt: [
      "Ultra-lean operations with lowest CP contribution expectations (model ~27% loyalty per Item 19).",
      "Markets tolerating economy ADR with OTA and highway retail mix.",
      "Owners who accept economy QA with minimal franchise support burden.",
    ],
    growthThemes: ["Economy conversion", "Interstate budget", "Lean staffing model", "Lowest loyalty attach tier"],
    footprintEditorial:
      "Rodeway competes at economy with among the lowest Choice Privileges room-mix contribution in CHI disclosures—stress-test net contribution after fees and OTA mix, not headline occupancy.",
    pipelineStats: "Item 19 FY 2025: ~26.8% loyalty contribution; ~51.1% enterprise/CRS — FDD sample.",
    heroPurpose:
      "Budget stays with recognizable economy retail—operators disciplined on cost per key and realistic about economy channel economics.",
    similarBrands: ["Econo Lodge", "Suburban Studios", "Quality Inn"],
  },
  {
    name: "Suburban Studios",
    slug: "suburban-studios",
    segment: "extendedStay",
    scaleLabel: "extended-stay economy",
    tagline: "Extended-stay studios for weekly and monthly guests.",
    royaltyLabel: "6.0% royalty on gross room revenues (Suburban FDD)",
    fddFile: "35786-202604-09.txt",
    pressKitFile: "suburban-studios-press-kit.txt",
    positioning:
      "Extended-stay studio product at economy-extended price point—kitchenettes and weekly stays, not MainStay’s residential upscale extended or nightly midscale.",
    developmentModel: "Conversion and new construction; weekly rate mix; lean extended-stay operations.",
    typicalUseCase:
      "Extended-stay conversions near employment and budget extended corridors; owners seeking Choice stack at economy-extended economics.",
    scenarios: [
      "Conversion of older extended-stay to Suburban prototype.",
      "Weekly-stay demand near industrial or medical employment.",
      "Portfolio extended-stay at lower capex than MainStay.",
    ],
    bestAt: [
      "Weekly/monthly revenue mix with kitchenette opex control.",
      "Markets where economy-extended beats midscale transient conversion ROI.",
      "Operators managing reduced housekeeping frequency and utility costs.",
    ],
    growthThemes: ["Economy extended-stay", "Weekly rate mix", "Studio conversion", "Employment-center studios"],
    footprintEditorial:
      "Suburban Studios bridges economy and extended-stay—Item 19 shows moderate enterprise participation (~62%) versus midscale. Confirm studio counts and PIP in FDD.",
    pipelineStats: "Item 19 FY 2025: ~44.9% loyalty; ~61.6% enterprise — FDD sample.",
    heroPurpose:
      "Affordable extended stays with in-suite kitchenettes—operators who understand weekly billing, wear-and-tear, and lean extended-stay staffing.",
    similarBrands: ["MainStay Suites", "Everhome Suites", "Rodeway Inn"],
  },
  {
    name: "Park Inn by Radisson (Choice)",
    slug: "park-inn-by-radisson-choice",
    segment: "upperMidscale",
    scaleLabel: "upper-midscale (Radisson family)",
    tagline: "Fresh, functional, friendly hotels in the Radisson family.",
    royaltyLabel: "5.5% royalty on gross room revenues (Park Inn FDD)",
    fddFile: "35776-202604-09.txt",
    pressKitFile: null,
    positioning:
      "Radisson-family upper-midscale in Americas under Choice—functional full-service/light F&B positioning versus core Radisson upscale or Blu design-forward.",
    developmentModel: "Conversion and new build in Americas; Radisson visual identity with Choice distribution.",
    typicalUseCase:
      "Owners wanting Radisson recognition at upper-midscale economics; airport and suburban full-service conversions in CALA and North America.",
    scenarios: [
      "Radisson-family reflag with Choice Privileges in Americas.",
      "Airport or suburban conversion needing recognizable Radisson sub-brand.",
      "CALA growth alongside other Radisson-family flags under Choice.",
    ],
    bestAt: [
      "Markets where Radisson name recognition matters at upper-midscale fee levels.",
      "Conversions that cannot support Blu or core Radisson full-service capex.",
      "Owners aligning with Choice + Radisson family portfolio strategy in Americas.",
    ],
    growthThemes: [
      "Americas Radisson-family conversion",
      "Airport and suburban upper-midscale",
      "Choice + RHG brand architecture",
      "CALA expansion",
    ],
    footprintEditorial:
      "Park Inn Item 19 uses a small performance sample (5 hotels per FDD notes)—treat footprint metrics as directional. Confirm Americas pipeline with Choice development; RHG operates Park Inn outside Americas.",
    pipelineStats: "Item 19 sample: 5 hotels; ~49.5% loyalty; ~80.8% enterprise — FDD FY 2025.",
    heroPurpose:
      "Fresh, functional Radisson-family stays—operators who deliver upper-midscale service with Choice systems and realistic Radisson-family standards.",
    similarBrands: ["Country Inn & Suites by Radisson (Choice)", "Radisson (Choice)", "Comfort Inn & Suites"],
  },
  {
    name: "Country Inn & Suites by Radisson (Choice)",
    slug: "country-inn-suites-by-radisson-choice",
    segment: "upperMidscale",
    scaleLabel: "upper-midscale (Radisson family)",
    tagline: "Warm, welcoming upper-midscale in the Radisson family.",
    royaltyLabel: "6.0% royalty on gross room revenues (Country Inn FDD)",
    fddFile: "35772-202604-09.txt",
    pressKitFile: null,
    positioning:
      "Radisson-family upper-midscale with residential warmth and breakfast-led limited-service—distinct from Park Inn functional positioning and core Radisson upscale.",
    developmentModel: "Conversion and new construction; suburban and highway upper-midscale in Americas.",
    typicalUseCase:
      "Suburban upper-midscale conversions seeking Radisson-family warmth with Choice Privileges; competitor to Comfort/Quality in family-oriented corridors.",
    scenarios: [
      "Upper-midscale conversion wanting Radisson family without full-service capex.",
      "Suburban markets with leisure and family transient.",
      "Portfolio owners balancing Country Inn with other CHI upper-midscale flags.",
    ],
    bestAt: [
      "Family and leisure upper-midscale corridors with breakfast-led model.",
      "Owners leveraging Radisson name at 6% royalty with strong enterprise mix (~82% Item 19).",
      "Conversions where warm residential guestroom narrative fits building.",
    ],
    growthThemes: [
      "Suburban upper-midscale",
      "Radisson-family conversion",
      "Breakfast-led limited-service",
      "Americas Choice distribution",
    ],
    footprintEditorial:
      "Country Inn & Suites by Radisson is a core Radisson-family upper-midscale flag in Americas under Choice—confirm property counts in FDD Item 20. High enterprise participation in Item 19 sample; model net after fees.",
    pipelineStats: "Item 19 FY 2025: ~43.7% loyalty; ~81.8% enterprise — FDD sample.",
    heroPurpose:
      "Warm, residential upper-midscale stays—operators who execute breakfast and guestroom consistency with Radisson-family standards.",
    similarBrands: ["Park Inn by Radisson (Choice)", "Comfort Inn & Suites", "Quality Inn"],
  },
  {
    name: "Everhome Suites",
    slug: "everhome-suites",
    segment: "extendedStay",
    scaleLabel: "extended-stay (newer system)",
    tagline: "Residential extended-stay for longer-term guests.",
    royaltyLabel: "6% royalty on room revenue for duration of agreement (Everhome FDD)",
    fddFile: "35774-202604-09.txt",
    pressKitFile: "everhome-suites.txt",
    positioning:
      "Newer Choice extended-stay system—residential suites with kitchen; Item 19 has no past financial performance representations in current FDD.",
    developmentModel: "New construction and conversion in extended-stay markets; confirm prototype and fees in disclosure.",
    typicalUseCase:
      "Greenfield or conversion extended-stay where owners want newest Choice extended-stay brand versus MainStay or Suburban.",
    scenarios: [
      "NC extended-stay pad in employment or medical corridor.",
      "Conversion of independent extended-stay to Choice stack.",
      "Developers comparing Everhome vs MainStay fee and standards vintage.",
    ],
    bestAt: [
      "Extended-stay markets with weekly demand and kitchen-equipped suites.",
      "Owners willing to diligence newer FDD without Item 19 performance tables.",
      "Operators comparing Choice extended-stay brands on royalty and prototype fit.",
    ],
    growthThemes: ["New extended-stay system", "Residential suites", "NC and conversion", "Weekly-stay mix"],
    footprintEditorial:
      "Everhome is a developing extended-stay brand—FDD Item 19 states no financial performance representations. Rely on market study, comp extended-stay performance, and development economics rather than system averages.",
    pipelineStats: "No Item 19 financial performance table — confirm open count in Item 20 and press/dev materials.",
    heroPurpose:
      "Residential extended-stay comfort—operators who underwrite weekly stays without system Item 19 averages and who meet newer-brand standards discipline.",
    similarBrands: ["MainStay Suites", "Suburban Studios", "WoodSpring Suites"],
  },
  {
    name: "Radisson RED  (Choice)",
    slug: "radisson-red-choice",
    segment: "upscale",
    scaleLabel: "upscale select-service (urban lifestyle)",
    tagline: "Play seriously—informal, flexible, and boldly designed urban hotels.",
    royaltyLabel:
      "6.0% royalty on gross room revenues (Radisson-family CHI FDD; confirm marketing, technology, and reservation fees—no RED-specific Item 19 performance table in current disclosure)",
    fddFile: "35779-202604-10.txt",
    pressKitFile: "Radisson-Red-press-kit.txt",
    positioning:
      "Upscale select-service urban social brand under Choice in the Americas—OUIBar + KTCHN flex F&B, 24/7 fitness, bold design, and guest-controlled informal service—not core Radisson full-service, Blu Nordic Nouveau, or upper-upscale soft-collection Individuals.",
    developmentModel:
      "Urban conversion and new build in vibrant North and South American cities; playful prototype with communal bar-food hub, digiwall lobby, and Instagrammable design moments.",
    typicalUseCase:
      "Owners with urban lifestyle or select-service assets seeking Radisson RED recognition, Choice Privileges distribution, and flexible F&B without full-service kitchen complexity—Minneapolis, Miami Airport, Campinas, Miraflores-style gateways.",
    scenarios: [
      "Urban conversion or NC where guests want control, social lobby energy, and flex deli-bar F&B instead of formal restaurant operations.",
      "CALA gateway cities (Brazil, Peru) aligning with Choice Radisson portfolio growth and press-kit pipeline (4 open + 5 in development per Sep 2023 materials).",
      "Operators experienced in upscale limited-service with 24/7 fitness, high Wi-Fi expectations, and informal service culture.",
    ],
    bestAt: [
      "Vibrant urban locations where food, drink, and culture drive ADR—not suburban highway limited-service.",
      "Developers who can deliver bold design, OUIBar + KTCHN social hub, and 24/7 fitness without full-service staffing.",
      "Markets where playful RED personality differentiates versus conventional upscale boxes and Radisson core full-service.",
    ],
    growthThemes: [
      "Urban upscale select-service",
      "Americas pipeline (4 open / 5 in development per press kit)",
      "OUIBar + KTCHN flex F&B",
      "Social lobby & 24/7 fitness",
      "CALA urban gateways",
    ],
    footprintEditorial:
      "Radisson RED redefines the ordinary with playful, informal service and bold design in vibrant urban locations across the Americas. Press materials cite 4 hotels (606 rooms) open and 5 in development as of September 30, 2023. FDD Item 19 has no RED-specific financial performance table—underwrite from local urban comp set, flex F&B labor, and conversion PIP. Outside the Americas, Radisson RED is owned by Radisson Hotel Group (RHG), not Choice.",
    pipelineStats:
      "4 hotels, 606 rooms in operation; 5 hotels in development across the Americas (Choice media center, Sep 30, 2023).",
    heroPurpose:
      "Deliver a playful upscale urban stay—OUIBar social hub, 24/7 fitness, rain showers, and lightning-fast Wi-Fi—for operators who run informal select-service with design discipline and social-lobby energy.",
    similarBrands: ["Radisson (Choice)", "Radisson Blu (Choice)", "Cambria Hotels"],
    materialsCaseStudy: {
      title: "Radisson RED — urban social upscale",
      body: "Urban lifestyle · Americas · Choice portfolio\n\n4 open hotels, 606 rooms; 5 in pipeline (Sep 2023 press kit)\n\nUpscale select-service · OUIBar + KTCHN · 24/7 fitness\n\nPlayful design and informal service where guests control work/leisure blend\n\nCombine FDD Items 19–20 with local urban comp study—no RED Item 19 performance table in current CHI disclosure.",
      caseSummaryOverview:
        "Americas upscale select-service urban brand: 4 open / 5 pipeline hotels per Choice press materials; playful social positioning with flex F&B.",
      caseSummaryOwnerObjective:
        "Evaluate urban conversion or NC capex for OUIBar hub, fitness, and design-forward PIP versus full-service Radisson or soft-brand Individuals.",
      caseSummaryBrandRelevance:
        "Relevant when asset is urban lifestyle select-service—not suburban upper-midscale or collection boutique independence.",
      caseSummaryInterpretation:
        "Press-kit scale is directional; FDD shows 0 RED hotels in master sample—rely on property-level feasibility and operator tier fit.",
      caseSummaryTags: "Urban, Upscale select-service, OUIBar, Americas, Pipeline",
    },
    footprintOpenings: [
      {
        title: "Radisson RED Minneapolis",
        body: "Urban, United States, North America, OUIBar + KTCHN\n\nMinneapolis, Minnesota\n\nUpscale select-service · urban social hub\n\nReference property · flex F&B & design\n\nFlagship North American RED with OUIBar + KTCHN communal bar-food space and bold lobby experience—illustrative of playful urban positioning under Choice in the Americas.\n\nUrban RED economics hinge on flex F&B labor and social-lobby activation—not full-service kitchen capex. Model Minneapolis-style urban ADR and event/leisure mix for your market.\n\nhttps://www.choicehotels.com/minnesota/minneapolis/radisson-red-hotels",
        sort: 0,
        caseSummaryOverview:
          "Minneapolis: North American RED reference with OUIBar + KTCHN and urban social lobby—press-kit photography market.",
        caseSummaryOwnerObjective:
          "Benchmark flex deli-bar F&B and informal service model for U.S. urban conversion or NC.",
        caseSummaryBrandRelevance:
          "Shows RED playful upscale select-service in a major U.S. city—not Radisson core full-service.",
        caseSummaryInterpretation:
          "One urban reference does not replace your comp set—confirm fees, PIP, and flex F&B operating plan locally.",
        caseSummaryTags: "United States, Urban, OUIBar, Reference",
      },
      {
        title: "Radisson RED Miami Airport",
        body: "Urban gateway, United States, North America, Airport\n\nMiami, Florida (airport corridor)\n\nUpscale select-service · transient + leisure\n\nGateway urban · social lobby\n\nAirport-adjacent RED with lobby and urban design personality—illustrative of gateway transient and bleisure demand under RED informal service standards.\n\nAirport urban sites need strong lobby throughput, 24/7 fitness, and Wi-Fi infrastructure—stress-test fee stack and OTA versus member mix.\n\nhttps://www.choicehotels.com/florida/miami/radisson-red-hotels",
        sort: 1,
        caseSummaryOverview:
          "Miami Airport corridor: RED upscale select-service at a gateway—lobby-led social experience for transient guests.",
        caseSummaryOwnerObjective:
          "Compare airport urban RED against conventional upscale select-service flags on flex F&B and design PIP.",
        caseSummaryBrandRelevance:
          "Gateway urban fit for RED—not resort Blu or Individuals soft collection.",
        caseSummaryInterpretation:
          "Airport compression varies by season and airline mix—underwrite from local airport comps, not press photos alone.",
        caseSummaryTags: "United States, Airport, Urban gateway",
      },
      {
        title: "Radisson RED Campinas",
        body: "Urban, Brazil, CALA, Social F&B\n\nCampinas, São Paulo state, Brazil\n\nUpscale select-service · CALA growth\n\nLounge RØD Grainne's · urban culture\n\nCampinas RED with Lounge RØD Grainne's and local F&B personality—illustrative CALA urban social hub under Choice-affiliated Radisson RED in Brazil.\n\nCALA urban RED requires flex F&B execution and design PIP aligned to brand playfulness—confirm Choice agreement scope and RHG ownership outside Americas.\n\nhttps://www.choicehotels.com/brazil/campinas/radisson-red-hotels",
        sort: 2,
        caseSummaryOverview:
          "Campinas, Brazil: CALA RED with lounge-led social F&B—press-kit imagery for Brazil urban lifestyle positioning.",
        caseSummaryOwnerObjective:
          "Reference for Brazilian urban conversion economics and RED flex F&B versus core Radisson full-service.",
        caseSummaryBrandRelevance:
          "CALA pipeline context alongside 4 open / 5 development Americas scale cited in press materials.",
        caseSummaryInterpretation:
          "Brazil fees, labor, and FX differ from U.S. references—local counsel and comp set required.",
        caseSummaryTags: "Brazil, CALA, Urban, Social F&B",
      },
      {
        title: "Radisson RED Miraflores",
        body: "Urban, Peru, CALA, Coastal gateway\n\nMiraflores, Lima, Peru\n\nUpscale select-service · leisure + corporate\n\nCALA coastal urban · design-led\n\nMiraflores RED in Lima's primary leisure and business district—census-backed Choice-affiliated RED in Peru for urban lifestyle positioning.\n\nCoastal urban Peru needs security, F&B localization, and design PIP discipline—confirm census affiliation and franchise agreement geography.\n\nhttps://www.choicehotels.com/peru/lima/radisson-red-hotels",
        sort: 3,
        caseSummaryOverview:
          "Miraflores, Lima: Peru urban RED in a premier coastal business/leisure district—Dealality census tracks Radisson RED by Choice affiliation.",
        caseSummaryOwnerObjective:
          "Illustrates CALA urban RED outside Brazil for owners comparing Lima gateway economics.",
        caseSummaryBrandRelevance:
          "Pairs with Campinas for CALA urban RED footprint storytelling—not Individuals boutique soft brand.",
        caseSummaryInterpretation:
          "Peru market ADR and seasonality differ from U.S. gateway examples—build pro forma from Lima comps.",
        caseSummaryTags: "Peru, CALA, Miraflores, Urban",
      },
    ],
  },
  {
    name: "Radisson Individual (Choice)",
    slug: "radisson-individual-choice",
    segment: "softCollection",
    scaleLabel: "upper-upscale soft collection",
    tagline: "Explorers Welcome—hand-selected independent and boutique hotels.",
    royaltyLabel:
      "6.0% royalty on gross room revenues (Radisson-family CHI FDD; confirm marketing, technology, and reservation fees—no Item 19 performance table in current disclosure)",
    fddFile: "35779-202604-10.txt",
    pressKitFile: "Radisson-Individuals-press-kit.txt",
    positioning:
      "Upper-upscale soft brand for hand-selected independent and boutique hotels in the Americas—Characterful Encounters, Vivid Settings, and Explorer's Compass service without rinse-and-repeat chain design. Maintains property uniqueness with Choice infrastructure; not RED urban playful select-service or Blu Nordic Nouveau.",
    developmentModel:
      "Conversion of distinctive independent and boutique assets; hand-selection for bold vision and exceptional service; relaunched 2024 in Americas under Choice.",
    typicalUseCase:
      "Owners of independent or boutique upper-upscale hotels seeking collection-level character with Choice Privileges, reservations, and growth trajectory—Faranda and CALA members in Colombia, Panama, and similar markets.",
    scenarios: [
      "Boutique urban or resort independent with strong local story wanting upper-upscale distribution without losing design identity.",
      "CALA conversion where press kit cites 15 hotels and 1,732 rooms (Sep 2024) and hand-selected portfolio growth.",
      "Operators who can deliver curated local experiences and Explorer's Compass service while meeting collection compliance.",
    ],
    bestAt: [
      "Distinctive assets with local design narrative that cannot fit rigid prototype flags.",
      "Markets where upper-upscale independents compete with Autograph, Curio, and Tribute—Choice + Radisson awareness in Americas.",
      "Owners prioritizing uniqueness plus Choice systems over standardized chain formats.",
    ],
    growthThemes: [
      "Upper-upscale soft collection",
      "Hand-selected independents & boutiques",
      "15 hotels / 1,732 rooms (Sep 2024 press kit)",
      "Characterful Encounters · Vivid Settings · Explorer's Compass",
      "CALA Faranda & boutique members",
    ],
    footprintEditorial:
      "Radisson Individuals brings together independent and boutique hotels that spark curiosity for untold local stories. Press materials cite 15 hotels with 1,732 rooms across the Americas as of September 30, 2024. Item 19 has no financial performance representations—underwrite from property-level ADR, local F&B, and collection compliance costs. Outside the Americas, the brand is owned by RHG, not Choice.",
    pipelineStats:
      "15 hotels, 1,732 rooms in operation across the Americas (Choice media center, Sep 30, 2024).",
    heroPurpose:
      "Curate characterful encounters and vivid settings with Explorer's Compass service—for operators who run upper-upscale independents with local authenticity backed by Choice distribution.",
    similarBrands: ["Ascend Hotel Collection", "Radisson Collection  (Choice)", "Radisson Blu (Choice)"],
    materialsCaseStudy: {
      title: "Radisson Individuals — soft collection scale",
      body: "Upper-upscale soft brand · Americas · Choice portfolio\n\n15 hotels, 1,732 rooms in operation (Sep 2024 press kit)\n\nHand-selected independents · three experience pillars\n\nCharacterful Encounters · Vivid Settings · Explorer's Compass\n\nNo Item 19 performance table—feasibility and local comp set drive underwriting.",
      caseSummaryOverview:
        "Americas upper-upscale soft brand: 15 hotels / 1,732 rooms per Choice press kit; hand-selected boutique and independent positioning.",
      caseSummaryOwnerObjective:
        "Evaluate maintaining uniqueness while adopting Choice infrastructure, loyalty, and collection compliance.",
      caseSummaryBrandRelevance:
        "For distinctive independents—not RED urban select-service or standardized upper-midscale flags.",
      caseSummaryInterpretation:
        "Collection flexibility still carries compliance and QA—budget Explorer's Compass service and design preservation costs.",
      caseSummaryTags: "Soft collection, Boutique, Americas, Hand-selected",
    },
    footprintOpenings: [
      {
        title: "Faranda Collection Bogotá",
        body: "Urban, Colombia, CALA, Boutique collection\n\nBogotá, Colombia\n\nUpper-upscale soft brand · hand-selected\n\nCharacterful Encounters · local culture\n\nFaranda Collection Bogotá, a member of Radisson Individuals—illustrative CALA urban boutique joining Choice-affiliated soft collection with local design narrative.\n\nBoutique collection economics depend on preserving vivid settings while funding collection compliance—confirm member agreement and PIP scope.\n\nhttps://www.choicehotels.com/colombia/bogota/hotels",
        sort: 0,
        caseSummaryOverview:
          "Bogotá: Faranda Collection member of Radisson Individuals—census-backed CALA boutique in Colombia capital.",
        caseSummaryOwnerObjective:
          "Benchmark upper-upscale independent conversion with Choice distribution while keeping local character.",
        caseSummaryBrandRelevance:
          "Core Individuals fit: hand-selected boutique with Explorer's Compass service—not chain prototype.",
        caseSummaryInterpretation:
          "Member hotels vary by prior flag and PIP—do not assume Bogotá results for every boutique conversion.",
        caseSummaryTags: "Colombia, CALA, Bogotá, Boutique",
      },
      {
        title: "Hotel Casa La Factoría by Faranda Boutique",
        body: "Urban heritage, Colombia, CALA, Boutique\n\nCartagena, Colombia\n\nUpper-upscale · historic character\n\nVivid Settings · local story\n\nCasa La Factoría by Faranda Boutique, a member of Radisson Individuals—heritage urban boutique illustrating vivid settings and characterful encounters in a leisure destination.\n\nHeritage boutiques need PIP discipline that preserves story—model compliance costs alongside upper-upscale ADR in leisure compression.\n\nhttps://www.choicehotels.com/colombia/cartagena/hotels",
        sort: 1,
        caseSummaryOverview:
          "Cartagena: heritage boutique Faranda member—Individuals positioning in a high-leisure CALA market.",
        caseSummaryOwnerObjective:
          "Shows soft-brand fit for historic urban boutique with strong local narrative.",
        caseSummaryBrandRelevance:
          "Leisure boutique versus airport RED or Blu resort—collection preserves uniqueness.",
        caseSummaryInterpretation:
          "Heritage conversions often have hidden PIP—separate building systems from brand marketing story.",
        caseSummaryTags: "Colombia, CALA, Cartagena, Heritage boutique",
      },
      {
        title: "Faranda Collection Barranquilla",
        body: "Urban business, Colombia, CALA, Coastal gateway\n\nBarranquilla, Colombia\n\nUpper-upscale · meetings & leisure\n\nExplorer's Compass · corporate + events\n\nFaranda Collection Barranquilla, a member of Radisson Individuals—business-coastal gateway member illustrating collection growth on Colombia's Caribbean coast.\n\nCoastal business cities mix corporate and event demand—confirm meetings capability and collection service standards for your asset.\n\nhttps://www.choicehotels.com/colombia/barranquilla/hotels",
        sort: 2,
        caseSummaryOverview:
          "Barranquilla: Faranda Collection member on Colombia's coast—corporate and leisure blend under Individuals.",
        caseSummaryOwnerObjective:
          "Reference for CALA urban upper-upscale independent with meetings and local F&B narrative.",
        caseSummaryBrandRelevance:
          "Expands Individuals CALA story beyond Bogotá—hand-selected portfolio growth trajectory.",
        caseSummaryInterpretation:
          "Event and corporate mix varies—underwrite from Barranquilla comps, not system averages.",
        caseSummaryTags: "Colombia, CALA, Barranquilla, Urban",
      },
      {
        title: "Hotel Faranda Express Puerta del Sol",
        body: "Urban, Panama, CALA, Pool & F&B\n\nPanama City, Panama\n\nUpper-upscale · leisure amenities\n\nVivid Settings · outdoor activation\n\nFaranda Express Puerta del Sol, a member of Radisson Individuals—pool, patio, and bar-forward leisure positioning in Panama per press-kit market imagery.\n\nPool and outdoor F&B add opex but support leisure ADR—align Individuals compliance with your amenity stack.\n\nhttps://www.choicehotels.com/panama/panama-city/hotels",
        sort: 3,
        caseSummaryOverview:
          "Panama City: Faranda Express member with outdoor pool and bar—Individuals in Central America gateway.",
        caseSummaryOwnerObjective:
          "Illustrates leisure-amenity boutique under soft brand versus urban RED select-service.",
        caseSummaryBrandRelevance:
          "Panama reference for CALA pipeline alongside Colombia members.",
        caseSummaryInterpretation:
          "Outdoor F&B and pool operations need staffing plan—press imagery is not opex budget.",
        caseSummaryTags: "Panama, CALA, Leisure, Pool",
      },
      {
        title: "Hotel Bambito by Faranda Boutique",
        body: "Mountain leisure, Panama, CALA, Nature retreat\n\nBambito, Chiriquí, Panama\n\nUpper-upscale boutique · eco-leisure\n\nCharacterful Encounters · exploration\n\nHotel Bambito by Faranda Boutique, a member of Radisson Individuals—mountain boutique illustrating Explorer's Compass and nature-led character outside urban cores.\n\nResort-adjacent boutiques have seasonality—model shoulder months and access logistics, not only peak leisure ADR.\n\nhttps://www.choicehotels.com/panama/hotels",
        sort: 4,
        caseSummaryOverview:
          "Bambito, Panama: mountain boutique Faranda member—Individuals beyond city-center conversions.",
        caseSummaryOwnerObjective:
          "Reference for nature/leisure boutique joining soft collection with local exploration positioning.",
        caseSummaryBrandRelevance:
          "Shows pillar breadth (Characterful Encounters) beyond urban Bogotá/Cartagena examples.",
        caseSummaryInterpretation:
          "Mountain seasonality and access drive occupancy—feasibility must be property-specific.",
        caseSummaryTags: "Panama, CALA, Boutique, Nature leisure",
      },
    ],
  },
];

export function item19ForBrand(name) {
  return FDD_ITEM19[name] || {};
}
