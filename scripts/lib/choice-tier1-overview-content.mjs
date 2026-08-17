/**
 * Brand-specific Overview tab copy for Tier 1 CHI brands.
 * Feeds scenario titles, bestAt titles, why_value bullets, differentiators, and proof grid.
 */

/** @typedef {{ scenarioTitles: string[], scenarioBodies: string[], bestAtTitles: string[], bestAtBodies: string[], whyValue: string, ownerExperience: string, differentiatorsIdentity: string, differentiatorsCommercial: string, proofOperator: string, proof: { title: string, body: string }[] }} OverviewContent */

/** @type {Record<string, OverviewContent>} */
export const TIER1_OVERVIEW = {
  "Comfort Inn & Suites": {
    scenarioTitles: [
      "Interstate & Suburban Reflag",
      "New-Build Breakfast-Led NC",
      "Portfolio Upper-Midscale Standardization",
    ],
    scenarioBodies: [
      "Independent or tired midscale assets along interstates and suburbs where guests expect complimentary hearty breakfast, smoke-free rooms, and recognizable upper-midscale retail—not economy-only amenities.",
      "Greenfield pads in growth corridors where ~80% of the Comfort pipeline is new construction and the refreshed prototype (RAIO amenities, pillow choice, waffle bar) is the competitive story.",
      "Multi-hotel sponsors aligning several assets to one upper-midscale flag after the brand renaissance—guest and franchisee satisfaction cited near all-time highs post product transformation.",
    ],
    bestAtTitles: [
      "Smoke-Free Breakfast-Led Corridors",
      "Post-Renaissance Guest Satisfaction",
      "High Enterprise & Loyalty Mix",
    ],
    bestAtBodies: [
      "Largest smoke-free hotel brand in North America with complimentary hearty breakfast (waffles, hot proteins, yogurts)—differentiates versus bare-bones economy on the same interstate.",
      "Brand transformation from Welcome to Goodbye lifted product and satisfaction—owners benefit when execution matches the refreshed lobby, guestroom, and breakfast standards.",
      "FDD Item 19 sample: ~51.8% Choice Privileges room contribution and ~81.3% enterprise booking mix—model net contribution after 6.0% royalty and mandatory programs.",
    ],
    whyValue:
      "Why owners choose Comfort: trusted upper-midscale reliability with 30+ years of business and leisure travel recognition.\nGuest promise: refreshing stay with RAIO bath amenities, pillow choice, smoke-free rooms, and complimentary hearty breakfast.\nScale: 1,600+ U.S. hotels and 2,100+ globally; 300+ properties in pipeline (~80% new construction per press materials).\nEconomics: upper-midscale fee stack with Choice Privileges top-rated program—underwrite breakfast labor, FF&E refresh, and net channel mix.\nFit: operators who run consistent limited-service with strong breakfast QA—not full-service F&B or extended-stay weekly models.",
    ownerExperience:
      "Typical guest: business and leisure travelers who want value, convenience, and a consistent breakfast-led stay.\nOwner journey: feasibility on breakfast economics → prototype/PIP for refreshed guestroom and lobby → systems cutover → opening QA on smoke-free and breakfast execution.\nRamp: loyalty and enterprise mix build toward Item 19 sample levels—do not assume Radisson or Cambria ADR curves.\nOngoing: franchisee satisfaction momentum requires maintaining transformation standards through PIP and QA cycles.",
    differentiatorsIdentity:
      "Refreshed logo and modern look signaling brand renaissance—not legacy midscale tired retail.\n100% smoke-free positioning (largest smoke-free brand in North America per press kit).\nRAIO bath amenities and firm/soft pillow choice on every bed.\nComplimentary hearty breakfast with waffle bar, hot proteins, and healthy options—core identity touchpoint.",
    differentiatorsCommercial:
      "Choice Privileges: ~51.8% room contribution in FDD FY 2025 sample (1,581-hotel system).\nEnterprise/CRS: ~81.3% booking mix in same sample—stress-test OTA leakage versus member/direct.\n6.0% royalty on gross room revenues—confirm marketing, technology, and reservation fees in FDD.\nConversion and NC pipeline weighted to new build—align capex with prototype, not economy PIP.",
    proofOperator:
      "Operators who excel at breakfast execution, smoke-free compliance, and upper-midscale guestroom consistency—without full-service kitchen complexity or resort staffing.",
    proof: [
      {
        title: "North American Scale",
        body: "1,600+ U.S. hotels and 2,100+ properties globally (Choice internal data, press kit)—among the largest upper-midscale systems in the portfolio.",
      },
      {
        title: "Pipeline Momentum",
        body: "300+ properties in pipeline with ~80% new construction—signals brand reinvestment, not harvest-only positioning.",
      },
      {
        title: "Transformation-Led Growth",
        body: "Major product and guest-experience renaissance; guest and franchisee satisfaction near all-time highs following Welcome-to-Goodbye refresh.",
      },
      {
        title: "Interstate & Suburban Focus",
        body: "Hotels located where guests want to stay—highway, suburban business, and small-city corridors; confirm your submarket comp set.",
      },
      {
        title: "Choice Privileges Strength",
        body: "FDD sample ~51.8% loyalty room contribution; program top-rated by U.S. News and USA Today 10Best—model elite fulfillment cost.",
      },
      {
        title: "Breakfast-Led Operations",
        body: "Success depends on hearty breakfast labor, food cost control, and guestroom QA—not meetings, bar, or resort F&B theater.",
      },
    ],
  },
  "Sleep Inn": {
    scenarioTitles: [
      "Midscale New-Construction Pad",
      "Sleep + MainStay Dual-Brand Site",
      "Efficient-Footprint Highway NC",
    ],
    scenarioBodies: [
      "Developers targeting lowest build cost in midscale with simply stylish nature-inspired design and Morning Medley breakfast—5.5% royalty versus upper-midscale flags.",
      "Shared-site economics with MainStay extended stay: 10 open dual-brands and 90+ dual projects in pipeline—split back-of-house where feasible.",
      "Highway and suburban NC where efficient room layout and Zenses amenities win without upper-midscale breakfast capex.",
    ],
    bestAtTitles: [
      "Lowest Midscale Build Cost",
      "Dual-Brand With MainStay",
      "Simply Stylish NC Prototype",
    ],
    bestAtBodies: [
      "Press kit positions Sleep Inn as lowest cost to build in midscale—maximize keys per acre with efficient footprint and timeless prototype.",
      "Choice’s dual-brand concept pairs transient midscale with extended stay on one pad—underwrite shared utilities, parking, and staffing plan.",
      "Nature-inspired design, Zenses bath products, Dream Cup coffee, and Morning Medley breakfast at affordable ADR—Item 19 ~48.8% loyalty / ~78.8% enterprise.",
    ],
    whyValue:
      "Why owners choose Sleep Inn: simply stylish, timeless new construction at affordable style for guests and smart investment for developers.\nGuest promise: happy night’s rest with soothing colors, nature-inspired design, and Dream Cup coffee & tea.\nScale: 550+ hotels open or in pipeline worldwide with rapid U.S. expansion.\nEconomics: 5.5% royalty; midscale fee stack; strong enterprise mix in FDD sample—compare to Quality or Comfort before reflag.\nFit: NC-focused operators and dual-brand developers—not upscale F&B or economy-only ops without breakfast.",
    ownerExperience:
      "Typical guest: value-conscious travelers wanting reliable, affordable style—not resort or full-service expectations.\nOwner journey: prototype sign-off on efficient footprint → NC or dual-brand pad planning → pre-opening with Morning Medley launch.\nDual-brand: coordinate Sleep transient mix with MainStay weekly stays when on same site.\nOngoing: maintain Zenses and breakfast consistency; midscale QA without over-building public space.",
    differentiatorsIdentity:
      "Simply stylish, timeless design with nature-inspired furnishings and soothing palette.\nZenses bath products with essential oils; Dream Cup coffee and BE Rested tea.\nMorning Medley breakfast buffet—hot and cold options.\nWhite nature-inspired linens and dream tips throughout stay (brand personality).",
    differentiatorsCommercial:
      "5.5% royalty on gross room revenues (Sleep Inn FDD).\nItem 19 FY 2025: ~48.8% Choice Privileges contribution; ~78.8% enterprise mix.\nDual-brand pipeline with MainStay—shared-site capital efficiency.\nProven midscale performer with rapid expansion narrative in press materials.",
    proofOperator:
      "Operators skilled at midscale NC delivery, dual-brand back-of-house planning, and lean breakfast operations—without Cambria-level F&B or Radisson meetings.",
    proof: [
      { title: "Midscale NC Leader", body: "Lowest costs to build in midscale segment per press kit—efficient footprint and smart room layout." },
      { title: "550+ Open or Pipeline", body: "550+ Sleep Inn hotels worldwide; rapid U.S. expansion in progress." },
      { title: "Dual-Brand Expansion", body: "10 open Sleep + MainStay hotels; 90+ dual-brand projects in pipeline." },
      { title: "Highway & Suburban NC", body: "Designed for affordable style on pads where midscale ADR supports limited-service breakfast model." },
      { title: "Choice Privileges Mix", body: "FDD sample ~48.8% loyalty rooms; ~78.8% enterprise—model net after 5.5% royalty." },
      { title: "Lean Operating Profile", body: "Morning Medley breakfast and midscale staffing—no restaurant, rooftop bar, or meetings stack." },
    ],
  },
  "Quality Inn": {
    scenarioTitles: [
      "Conversion-Heavy Midscale Reflag",
      "Value-Q Highway Portfolio",
      "Fastest-Growing Midscale Scale",
    ],
    scenarioBodies: [
      "Tired independent or economy conversions where Quality’s conversion ROI story and 1,800+ global open hotels de-risk the flag decision.",
      "Highway and suburban assets competing on Value Qs (Q Bed, Q Breakfast, Q Shower, Q Service, Q Essentials)—not upscale public space.",
      "Owners standardizing midscale portfolio on the brand Choice was founded on—high awareness supports pricing power in midscale comp sets.",
    ],
    bestAtTitles: [
      "Conversion ROI & Scale",
      "Value Q Guest Promise",
      "Midscale Pricing Power",
    ],
    bestAtBodies: [
      "Fastest-growing midscale brand in Choice system; growth fueled by conversions delivering high ROI per press kit.",
      "Five Value Qs: Q Bed linens/pillows, Q Breakfast, Q Shower, Q Service, Q Essentials (Wi-Fi, fridge, coffee)—clear operating checklist.",
      "1,800+ hotels globally (1,600+ U.S.); Item 19 ~44.9% loyalty / ~74.2% enterprise with 5.25% royalty—underwrite vs Sleep and Comfort.",
    ],
    whyValue:
      "Why owners choose Quality: the founding Choice brand—reliable, affordable lodging with high awareness and conversion economics.\nGuest promise: Value Qs guests desire—fresh bed, hot breakfast, bright shower, responsive service, essentials included.\nScale: 1,800+ open globally; 1,600+ in U.S. alone.\nEconomics: 5.25% royalty; conversion-led growth; competitive pricing enabled by brand equity.\nFit: midscale conversion specialists—not upscale urban F&B or extended-stay weekly billing.",
    ownerExperience:
      "Typical guest: travelers who want reliable midscale value with clear amenity expectations (breakfast, shower, Wi-Fi).\nOwner journey: conversion PIP scoped to Value Qs → breakfast and guestroom alignment → Choice systems and loyalty cutover.\nCommercial: brand equity supports rate integrity in midscale corridors—validate with local ADR and OTA mix.\nOngoing: conversion pipeline remains core—plan for standards refresh at renewal.",
    differentiatorsIdentity:
      "Founding Choice Hotels brand—heritage and system scale.\nValue Q framework: bed, breakfast, shower, service, essentials—memorable operating pillars.\nHigh brand awareness and strong brand equity (press kit).\nComplementary hot breakfast and Q Shower multi-setting heads.",
    differentiatorsCommercial:
      "5.25% royalty—lowest among Tier 1 midscale trio (vs Sleep 5.5%, Comfort 6.0%).\nItem 19: ~44.9% loyalty contribution; ~74.2% enterprise sample.\nConversion-led growth with 1,800+ open hotels—pipeline de-risk for lenders.\nFranchisee benefits from industry-leading tech, distribution, and Choice Privileges.",
    proofOperator:
      "Operators with conversion discipline, midscale breakfast execution, and Value Q QA—who model 5.25% royalty and mid-tier loyalty attach.",
    proof: [
      { title: "1,800+ Global Hotels", body: "More than 1,800 Quality hotels open worldwide; 1,600+ in the U.S. (press kit)." },
      { title: "Conversion-Led Growth", body: "Fastest-growing midscale brand; growth fueled by conversions with high ROI narrative." },
      { title: "Founding Brand Equity", body: "Brand upon which Choice was founded—high awareness supports competitive pricing." },
      { title: "Value Q Operating Model", body: "Clear five-pillar guest promise—simplifies training and QA versus vague soft-brand standards." },
      { title: "Choice Privileges Mix", body: "FDD FY 2025 sample ~44.9% loyalty rooms; ~74.2% enterprise booking mix." },
      { title: "Midscale Conversion Ops", body: "Win on PIP sequencing, breakfast consistency, and conversion speed—not resort or meetings capability." },
    ],
  },
  "Cambria Hotels": {
    scenarioTitles: [
      "Urban Adaptive Reuse & NC",
      "Convention-Adjacent Upscale",
      "Locally Inspired F&B & Rooftop",
    ],
    scenarioBodies: [
      "Downtown adaptive reuse or NC where design-forward rooms, spa-inspired baths, and local décor justify upscale capex and 6% royalty.",
      "Sites near corporate campuses, convention centers, and attractions per Choice site-selection criteria—not interstate limited-service pads.",
      "Owners who can operate bar/restaurant, rooftop activation, and meetings—J.D. Power #1 upscale guest satisfaction (2023 study cited in press kit).",
    ],
    bestAtTitles: [
      "Upscale Guest Satisfaction",
      "Design-Forward F&B & Meetings",
      "Urban & Convention NC",
    ],
    bestAtBodies: [
      "#1 in upscale segment, J.D. Power 2023 N.A. Hotel Guest Satisfaction Index; Top 10 Hotel Brands construction pipeline (press citations).",
      "Rooftop bars, local craft beer, Cambria Estate Wines, customizable meeting space, spa-inspired baths with Bluetooth mirrors.",
      "Growing faster than ever—8 locations opened in 2022 cited; NC, conversion, and adaptive reuse near demand generators.",
    ],
    whyValue:
      "Why owners choose Cambria: upscale for modern travelers—contemporary essentials and approachable indulgences.\nGuest promise: maximize time away with locally inspired design, premium bedding, immersive baths, and F&B.\nProduct: rooftop/outdoor spaces, state-of-the-art fitness, 24/7 marketplace, Contactless Concierge.\nEconomics: 6% royalty; Item 19 ~50.8% loyalty / ~51.9% proprietary (3-hotel sample)—full upscale fee stack.\nFit: operators with upscale F&B and meetings depth—not midscale breakfast-only or economy highway.",
    ownerExperience:
      "Typical guest: modern business and leisure travelers who want local flavor with upscale consistency.\nOwner journey: site criteria review (corporate, convention, attractions) → design-forward PIP → F&B concept and meetings buildout → upscale opening QA.\nRamp: rate and mix must support restaurant, bar, and staffing—guest satisfaction scores are public proof point.\nOngoing: locally inspired décor refresh cycles; Cambria Estate Wines and bar program require beverage discipline.",
    differentiatorsIdentity:
      "Design-forward accommodations reflecting surrounding community personality.\nApproachable indulgences: rooftop bars, spa-inspired baths, premium bedding.\nCambria Contactless Concierge and tech-friendly features.\nRanked among top hotel brands for U.S. construction pipeline (press kit).",
    differentiatorsCommercial:
      "6% royalty on gross room revenues.\nItem 19 FY 2025: ~50.8% loyalty; ~51.9% proprietary (excludes OTA)—51-hotel sample.\nSite selection: proximity to corporate, convention, entertainment—not generic highway.\nChoice Privileges with cobrand accelerators—upscale fulfillment costs higher than midscale.",
    proofOperator:
      "Upscale operators who deliver local F&B, meetings sales, and design-forward service—budget gallery-level public space investment and QA.",
    proof: [
      { title: "J.D. Power #1 Upscale", body: "Ranked #1 upscale in J.D. Power 2023 North America Guest Satisfaction Index (press kit)." },
      { title: "Accelerating Openings", body: "8 locations opened in 2022 cited; brand growing faster than ever in upscale pipeline." },
      { title: "Urban & Convention Sites", body: "Market research targets corporate, convention, and attraction adjacency—not passive interstate." },
      { title: "Full F&B & Meetings Stack", body: "Onsite restaurants, bars, rooftop, customizable meeting/event space—upscale capex required." },
      { title: "Loyalty & Direct Mix", body: "FDD sample ~50.8% Choice Privileges contribution; proprietary mix ~51.9% (ex-OTA)." },
      { title: "Upscale Operating Depth", body: "Requires beverage, culinary, and meetings capability—midscale operators need partner or GM upgrade." },
    ],
  },
  "MainStay Suites": {
    scenarioTitles: [
      "Weekly-Stay Employment Center",
      "Sleep + MainStay Dual-Brand Pad",
      "Extended-Stay Conversion",
    ],
    scenarioBodies: [
      "Greenfield or conversion near hospitals, industrial parks, and project corridors where weekly/monthly revenue dominates transient.",
      "Dual-brand with Sleep Inn on same site—10 open dual hotels, 90+ in pipeline per Sleep press kit; shared parking and utilities.",
      "Older extended-stay product needing Choice systems, kitchen FF&E standards, and 6.0% royalty versus independent extended flags.",
    ],
    bestAtTitles: [
      "Residential Extended-Stay",
      "Dual-Brand Site Efficiency",
      "Weekly Revenue Mix",
    ],
    bestAtBodies: [
      "In-suite kitchen and residential feel for project-based and relocating guests—not nightly breakfast-led midscale.",
      "Pair with Sleep Inn to capture transient plus extended on one pad—Choice’s fastest-growing dual-brand concept.",
      "Item 19 ~50.7% loyalty / ~74.6% enterprise—6.0% royalty; model housekeeping cadence and kitchen wear reserves.",
    ],
    whyValue:
      "Why owners choose MainStay: residential-style extended stay with Choice distribution.\nGuest promise: kitchen-equipped suites for longer stays; residential comfort.\nDual-brand: natural pairing with Sleep Inn for blended demand on one development.\nEconomics: 6.0% royalty; weekly rate mix; extended-stay opex (utilities, kitchen FF&E).\nFit: extended-stay operators—not highway transient or upscale F&B.",
    ownerExperience:
      "Typical guest: relocations, projects, training assignments—weekly billing mindset.\nOwner journey: kitchen FF&E plan → extended-stay prototype approval → dual-brand coordination if applicable.\nOperations: reduced daily housekeeping versus transient; kitchen wear and utility costs in pro forma.\nOngoing: compete with MainStay, Suburban, Everhome in Choice extended tier—compare fees and standards.",
    differentiatorsIdentity:
      "Residential extended-stay positioning within Choice portfolio.\nIn-suite kitchen—core product difference from Sleep/Quality.\nDesigned to pair with Sleep Inn dual-brand sites.\nExtended-stay guest expectations: space, kitchen, weekly value.",
    differentiatorsCommercial:
      "6.0% royalty on gross room revenues.\nItem 19: ~50.7% loyalty; ~74.6% enterprise (FY 2025 sample).\nDual-brand capital efficiency with Sleep Inn (90+ pipeline dual projects).\nWeekly/monthly rate mix changes working capital and staffing model vs nightly brands.",
    proofOperator:
      "Extended-stay operators who manage kitchen FF&E, weekly billing, and optional dual-brand back-of-house with Sleep Inn.",
    proof: [
      { title: "Extended-Stay Product", body: "Residential suites with in-suite kitchens—distinct from midscale nightly brands." },
      { title: "Dual-Brand Pipeline", body: "10 open Sleep+MainStay; 90+ dual projects in pipeline (Sleep press kit)." },
      { title: "Employment-Center Fit", body: "Targets project, relocation, and medical extended demand—not resort leisure." },
      { title: "Choice Systems Access", body: "Industry-leading tech, distribution, and Choice Privileges per franchise positioning." },
      { title: "Loyalty Participation", body: "FDD sample ~50.7% loyalty room contribution; ~74.6% enterprise mix." },
      { title: "Weekly-Stay Operations", body: "Housekeeping cadence, kitchen maintenance, and utility economics drive returns—not ADR alone." },
    ],
  },
  "Ascend Hotel Collection": {
    scenarioTitles: [
      "Boutique Independent Conversion",
      "Historic Urban Repositioning",
      "Local F&B Preserved",
    ],
    scenarioBodies: [
      "Unique independents that need Choice Privileges and CRS without homogenizing local story—collection standards vs rigid prototype.",
      "Historic or design hotels where character is the asset; confirm contractually how much identity survives brand review.",
      "Markets where guests pay for authenticity; owners accept 5.0% membership fee plus marketing & reservation fees per FDD structure.",
    ],
    bestAtTitles: [
      "Local Character + Choice Stack",
      "Boutique & Historic Conversions",
      "Collection Flexibility",
    ],
    bestAtBodies: [
      "Soft collection: independent-spirited hotels united by distribution—not one-box prototype.",
      "Item 19 sample (142 hotels): ~45.0% loyalty / ~45.8% proprietary (ex-OTA)—strong direct story for independents.",
      "5.0% membership fee on gross room revenues plus separate marketing & reservation fees—read full FDD fee article.",
    ],
    whyValue:
      "Why owners choose Ascend: unique hotels with Choice backing—personality plus portfolio distribution.\nGuest promise: authentic local experience with member recognition on direct channels.\nCollection model: flexibility within standards; not Sleep/Cambria prototype homogenization.\nEconomics: 5.0% membership fee; marketing & reservation fees separate; Item 19 142-hotel sample.\nFit: boutique operators and independent owners—not economy highway or rigid NC prototype.",
    ownerExperience:
      "Typical guest: travelers seeking local character with loyalty benefits.\nOwner journey: collection fit review → negotiate preserved F&B and design elements → systems integration without losing story.\nCommercial: proprietary mix ~45.8% in sample—direct and member paths matter for independents.\nOngoing: collection QA balances uniqueness with compliance—waivers are not automatic.",
    differentiatorsIdentity:
      "Independent-spirited hotels united under one collection.\nLocal character and design preserved where agreement allows.\nNot a single prototype—each asset has distinct narrative.\nChoice Privileges access without full Cambria/Radisson upscale mandate.",
    differentiatorsCommercial:
      "5.0% membership fee (FDD) plus 3% marketing & reservation fee structure—confirm current disclosure.\nItem 19: ~45.0% loyalty; ~45.8% proprietary booking mix (142-hotel sample).\nConversion-friendly for boutique and historic assets.\nCRS and loyalty without forcing midscale breakfast prototype.",
    proofOperator:
      "Boutique operators who protect local F&B and design while meeting collection compliance and loyalty fulfillment.",
    proof: [
      { title: "142-Hotel FDD Sample", body: "Item 19 performance sample: 142 hotels (FY 2025)—collection scale, not single prototype." },
      { title: "Proprietary Booking Mix", body: "~45.8% proprietary (non-OTA) in sample—important for independent positioning." },
      { title: "Membership Fee Model", body: "5.0% membership fee on gross room revenues; marketing & reservation fees per FDD." },
      { title: "Boutique Conversions", body: "Targets unique, independent-spirited assets—not highway economy boxes." },
      { title: "Choice Privileges Access", body: "~45.0% loyalty room contribution in sample—member rates on direct paths." },
      { title: "Collection Operating Skill", body: "Success requires balancing local story with brand QA—not limited-service breakfast ops alone." },
    ],
  },
  "Clarion": {
    scenarioTitles: [
      "Meetings-Capable Midscale Conversion",
      "SMERF & Small-Group Corridors",
      "Suburban Event-Driven Assets",
    ],
    scenarioBodies: [
      "Conversions with viable meeting space needing Clarion family distribution—5.5% royalty, shared Item 19 with Clarion Pointe (155-hotel sample).",
      "Secondary markets where small groups and local events supplement transient—modest F&B without Cambria upscale capex.",
      "Portfolio owners separating meetings-capable midscale from pure limited-service Quality/Sleep assets.",
    ],
    bestAtTitles: [
      "Meetings & Event Space",
      "Clarion Family Distribution",
      "Group + Transient Mix",
    ],
    bestAtBodies: [
      "Meetings-oriented midscale—event space and group potential versus breakfast-only flags.",
      "Item 19 combined Clarion + Pointe: ~41.1% loyalty / ~75.9% enterprise (155-hotel sample).",
      "Operators need group sales and modest banquet capability—not full upscale resort F&B.",
    ],
    whyValue:
      "Why owners choose Clarion: meetings-capable midscale with Choice distribution.\nGuest promise: flexible event space and midscale lodging for groups and transient.\nShared economics: Item 19 performance combined with Clarion Pointe (155 hotels).\nEconomics: 5.5% royalty; meetings staffing and F&B scope drive returns.\nFit: assets with real meeting rooms—not economy without event space.",
    ownerExperience:
      "Typical guest: group attendees, SMERF, corporate small meetings, plus highway transient.\nOwner journey: meeting space compliance → F&B scope definition → group sales plan before opening.\nRamp: group calendar builds slower than pure transient—model banquet labor.\nOngoing: differentiate from Clarion Pointe when full meetings stack is not viable.",
    differentiatorsIdentity:
      "Meetings-capable midscale flag in Clarion family.\nFlexible event space positioning.\nDistinct from Pointe select-service and from Quality Value Q limited-service.\nGroup + transient dual revenue story.",
    differentiatorsCommercial:
      "5.5% royalty; shared Item 19 with Clarion Pointe (~41.1% loyalty, ~75.9% enterprise).\n155-hotel combined performance sample (FY 2025 FDD).\nConversion-friendly for assets with existing meeting infrastructure.\nChoice tech, distribution, and loyalty at midscale fee level.",
    proofOperator:
      "Operators with group sales, modest catering, and meeting-space ops—who can staff events without Cambria-level culinary.",
    proof: [
      { title: "Meetings-Capable Midscale", body: "Core positioning: event space and groups—not pure limited-service." },
      { title: "155-Hotel FDD Sample", body: "Combined Clarion + Clarion Pointe Item 19 sample (FY 2025)." },
      { title: "5.5% Royalty", body: "Midscale fee level—compare to Cambria 6% or Quality 5.25% before reflag." },
      { title: "Group Revenue Potential", body: "SMERF and corporate small meetings supplement transient in suburban markets." },
      { title: "Enterprise Booking Mix", body: "~75.9% enterprise/CRS in combined sample—distribution-heavy model." },
      { title: "Event Operations", body: "Proof is execution on meetings and modest F&B—not breakfast-only midscale ops." },
    ],
  },
  "Clarion Pointe": {
    scenarioTitles: [
      "Select-Service Clarion Conversion",
      "Reduced-Meetings Highway Box",
      "Clarion Family at Lower Capex",
    ],
    scenarioBodies: [
      "Assets that want Clarion recognition but cannot support full Clarion meetings/F&B—invest in Pointe prototype efficiency.",
      "Highway select-service with modest meeting room for small groups—not full banquet infrastructure.",
      "Owners trading down from full Clarion scope while staying in Clarion system economics (shared Item 19 metrics).",
    ],
    bestAtTitles: [
      "Select-Service Efficiency",
      "Clarion Family Access",
      "Modest Meeting Room",
    ],
    bestAtBodies: [
      "Streamlined Clarion expression—conversion-friendly when full Clarion capex is not viable.",
      "Shared Item 19 with Clarion: ~41.1% loyalty / ~75.9% enterprise (155-hotel combined sample).",
      "Right-size meetings and F&B to building constraints—confirm Pointe-specific standards in FDD.",
    ],
    whyValue:
      "Why owners choose Clarion Pointe: Clarion distribution with select-service economics.\nGuest promise: Clarion family recognition at efficient operating scope.\nDifferentiation vs Clarion: lighter meetings/F&B; vs Quality: Clarion retail and group option.\nEconomics: 5.5% royalty (shared FDD with Clarion); confirm Pointe-specific fees.\nFit: conversions with small meeting room—not full Clarion banquet or upscale F&B.",
    ownerExperience:
      "Typical guest: highway and suburban transient plus small local groups.\nOwner journey: confirm Pointe vs full Clarion fit → scope meeting room and F&B minimums → conversion PIP.\nCommercial: same Item 19 tables as Clarion—do not assume separate performance stats.\nOngoing: avoid over-building meetings product Pointe prototype cannot support.",
    differentiatorsIdentity:
      "Select-service expression of Clarion brand family.\nConversion-friendly efficient prototype.\nModest meeting capability versus full Clarion.\nClarion retail without full-event infrastructure.",
    differentiatorsCommercial:
      "5.5% royalty (Clarion FDD); Pointe-specific standards separate from performance tables.\nShared Item 19: ~41.1% loyalty; ~75.9% enterprise (combined 155-hotel sample).\nLower capex than full Clarion meetings stack.\nChoice distribution at midscale select-service cost structure.",
    proofOperator:
      "Select-service operators who run small meeting room and limited F&B—without full Clarion banquet staffing.",
    proof: [
      { title: "Select-Service Clarion", body: "Pointe = efficient Clarion family access—not full meetings resort." },
      { title: "Shared FDD Performance", body: "Item 19 combined with Clarion (155 hotels)—same loyalty/enterprise benchmarks." },
      { title: "Conversion Economics", body: "Designed for assets that cannot carry full Clarion public-space investment." },
      { title: "Modest Group Option", body: "Small meeting room possible—confirm prototype requirements per site." },
      { title: "5.5% Royalty Tier", body: "Midscale royalty—model separately from Pointe-specific PIP scope." },
      { title: "Right-Sized Ops", body: "Win by matching building to Pointe standards—avoid Clarion-full capex trap." },
    ],
  },
  "Econo Lodge": {
    scenarioTitles: [
      "Economy Highway Conversion",
      "Price-Led Interstate Reflag",
      "Lean-Ops Portfolio Tier",
    ],
    scenarioBodies: [
      "Budget interstate sites where 5.0% royalty and minimal amenity stack beat failed midscale repositioning ROI.",
      "Price-sensitive leisure and contractor demand—fresh coffee and clean rooms, not breakfast buffet labor.",
      "Portfolio rationalization to economy tier with Choice Privileges at lower contribution (~33.8% Item 19).",
    ],
    bestAtTitles: [
      "Lowest Amenity Economy Stack",
      "5.0% Royalty Tier",
      "Price-Sensitive Demand",
    ],
    bestAtBodies: [
      "Affordable economy lodging—minimal public space and amenity versus midscale breakfast brands.",
      "Item 19: ~33.8% loyalty / ~57.9% enterprise—plan for more OTA and highway retail mix.",
      "5.0% royalty on gross room revenues; lean staffing and QA focused on value.",
    ],
    whyValue:
      "Why owners choose Econo Lodge: affordable economy for value-conscious travelers.\nGuest promise: restful stay, great savings, travel basics covered.\nEconomics: 5.0% royalty; lowest Tier 1 midscale/economy loyalty attach in CHI sample set.\nOperations: fresh coffee, clean rooms, Wi-Fi—no hearty breakfast theater.\nFit: lean operators—not Comfort/Quality breakfast models or upscale F&B.",
    ownerExperience:
      "Typical guest: highly price-sensitive transient and contractor stays.\nOwner journey: economy PIP → minimal FF&E → low labor model validation.\nCommercial: ~33.8% loyalty rooms means less member lift—stress OTA and highway signage.\nOngoing: compete with Rodeway on economy tier; monitor net after fees.",
    differentiatorsIdentity:
      "Economy positioning—travel basics at lowest amenity level in Tier 1 set.\nFresh, clean rooms and morning coffee focus.\nPeace of mind from nationally recognized Choice backing.\nNot midscale breakfast or extended-stay kitchen.",
    differentiatorsCommercial:
      "5.0% royalty on gross room revenues.\nItem 19 FY 2025: ~33.8% loyalty; ~57.9% enterprise—lower than midscale flags.\nEconomy fee stack supports price-led ADR strategy.\nAccess to Choice tech and distribution without midscale PIP cost.",
    proofOperator:
      "Operators expert at lean economy staffing, basic QA, and price-positioned retail—accept lower loyalty attach.",
    proof: [
      { title: "Economy Tier Positioning", body: "Covers travel basics—lowest amenity burden among Tier 1 CHI brands." },
      { title: "5.0% Royalty", body: "Lowest royalty in Tier 1 economy pair (with Rodeway)—model full fee article." },
      { title: "Lower Loyalty Attach", body: "FDD sample ~33.8% Choice Privileges room contribution." },
      { title: "Highway & Budget Markets", body: "Price-sensitive corridors where economy ADR clears lean cost structure." },
      { title: "~57.9% Enterprise Mix", body: "Still majority enterprise in sample—but more OTA leakage risk than midscale." },
      { title: "Lean Execution", body: "Returns depend on cost per key and basic cleanliness—not breakfast or meetings." },
    ],
  },
  "Rodeway Inn": {
    scenarioTitles: [
      "Ultra-Lean Economy Highway",
      "Lowest Loyalty-Attach Model",
      "Budget Interstate Reflag",
    ],
    scenarioBodies: [
      "Interstate economy where Rodeway’s 600+ open hotels and national brand recognition support budget ADR.",
      "Owners modeling lowest Choice Privileges contribution in Tier 1 (~26.8% Item 19)—plan OTA and signage heavily.",
      "Failed midscale conversions downgrading to 5.0% royalty economy with fresh rooms and premium TV per press kit.",
    ],
    bestAtTitles: [
      "Economy Cost Leadership",
      "600+ Property Scale",
      "Choice Tech at Economy Fees",
    ],
    bestAtBodies: [
      "Travel basics: fresh clean rooms, premium movie channels, complimentary Wi-Fi, morning coffee (press kit).",
      "Item 19 lowest loyalty attach in Tier 1: ~26.8% rooms; ~51.1% enterprise.",
      "5.0% royalty—operators win on labor and maintenance discipline.",
    ],
    whyValue:
      "Why owners choose Rodeway: nationally recognized economy with great savings.\nGuest promise: restful stay backed by Choice; travel basics on the road.\nScale: 600+ properties worldwide (press kit).\nEconomics: 5.0% royalty; ~26.8% loyalty contribution—lowest in Tier 1 CHI Item 19 set.\nFit: pure economy operators—not midscale breakfast or extended-stay.",
    ownerExperience:
      "Typical guest: budget highway traveler; expects clean room and coffee—not breakfast buffet.\nOwner journey: minimal PIP → economy prototype → low staffing validation.\nCommercial: assume heavy highway retail and OTA; loyalty lift is modest.\nOngoing: benchmark against Econo Lodge in same corridor—compare fees and guest reviews.",
    differentiatorsIdentity:
      "Economy brand covering travel basics with national recognition.\nFresh clean rooms; premium movie channels; free Wi-Fi.\nFresh coffee every morning—minimal F&B.\n600+ properties open worldwide (press kit).",
    differentiatorsCommercial:
      "5.0% royalty on gross room revenues.\nItem 19: ~26.8% loyalty (lowest Tier 1); ~51.1% enterprise.\nIndustry-leading technology, distribution, and Choice Privileges access at economy tier.\nFranchisee benefits from Choice scale without midscale amenity capex.",
    proofOperator:
      "Disciplined economy operators who maximize occupancy at low ADR with minimal labor—realistic about ~27% loyalty mix.",
    proof: [
      { title: "600+ Open Worldwide", body: "More than 600 Rodeway Inn properties (press kit internal data)." },
      { title: "Lowest Loyalty Attach", body: "FDD FY 2025 sample ~26.8% Choice Privileges room contribution—plan accordingly." },
      { title: "5.0% Royalty Economy", body: "Same royalty tier as Econo Lodge—compare brand recognition in your market." },
      { title: "Travel Basics Focus", body: "Clean rooms, Wi-Fi, coffee—no breakfast buffet or meetings revenue." },
      { title: "~51.1% Enterprise Mix", body: "Enterprise majority but lowest among Tier 1—OTA risk matters." },
      { title: "Lean Highway Ops", body: "Operator proof is cost control and basic QA—not guest experience theater." },
    ],
  },
  "Suburban Studios": {
    scenarioTitles: [
      "Economy Extended-Stay Studio",
      "Weekly-Stay Employment Corridor",
      "Kitchenette Conversion",
    ],
    scenarioBodies: [
      "Extended-stay studios at economy-extended price versus MainStay residential upscale extended—kitchenette FF&E and weekly billing.",
      "Sites near industrial, medical, and project corridors where guests stay by the week, not the night.",
      "Conversions of older studio product needing Choice systems; Item 19 ~44.9% loyalty / ~61.6% enterprise with 6.0% royalty.",
    ],
    bestAtTitles: [
      "Studio Kitchenette Product",
      "Weekly Rate Economics",
      "Economy Extended Tier",
    ],
    bestAtBodies: [
      "Extended-stay with in-suite kitchenette—leaner than MainStay residential positioning.",
      "6.0% royalty; model utilities, wear, and reduced housekeeping versus nightly midscale.",
      "Moderate enterprise mix (~61.6%) versus MainStay (~74.6%)—more retail and weekly-rate sensitivity.",
    ],
    whyValue:
      "Why owners choose Suburban: affordable extended stay with kitchenettes.\nGuest promise: studio space for weekly and monthly guests on a budget.\nEconomics: 6.0% royalty at economy-extended positioning—compare MainStay and Everhome.\nOperations: kitchenette FF&E, weekly housekeeping cadence, utility costs.\nFit: extended-stay operators accepting economy ADR—not Cambria or Comfort transient.",
    ownerExperience:
      "Typical guest: budget extended stays—projects, training, medical.\nOwner journey: kitchenette PIP → weekly rate strategy → utility and wear reserves.\nCommercial: loyalty ~44.9% in sample—between economy nightly and MainStay extended.\nOngoing: compete with Everhome for newest extended brand; compare 6% royalty across extended trio.",
    differentiatorsIdentity:
      "Economy-extended studio positioning.\nIn-suite kitchenette for weekly guests.\nLower amenity than MainStay residential extended.\nChoice extended-stay option at leaner capex than upscale extended flags.",
    differentiatorsCommercial:
      "6.0% royalty on gross room revenues.\nItem 19: ~44.9% loyalty; ~61.6% enterprise (FY 2025).\nWeekly/monthly mix drives revenue stability versus pure transient.\nPositioned below MainStay on service level and ADR expectations.",
    proofOperator:
      "Extended-stay operators who manage kitchenettes, weekly billing, and lean staffing at economy-extended ADR.",
    proof: [
      { title: "Studio Extended-Stay", body: "Kitchenette product for weekly/monthly guests—not nightly midscale." },
      { title: "Economy-Extended Tier", body: "Lower ADR and amenity than MainStay; compare Everhome for newest system." },
      { title: "6.0% Royalty", body: "Same royalty rate as MainStay—underwrite different guest ADR and opex." },
      { title: "Weekly Demand Focus", body: "Employment and medical corridors—not convention or resort." },
      { title: "Moderate Loyalty Mix", body: "FDD sample ~44.9% loyalty; ~61.6% enterprise." },
      { title: "Kitchenette Operations", body: "Proof is wear, utilities, and housekeeping cadence—not breakfast execution." },
    ],
  },
  "Park Inn by Radisson (Choice)": {
    scenarioTitles: [
      "Radisson Family Upper-Midscale",
      "Airport & Suburban Conversion",
      "Americas RHG + Choice Stack",
    ],
    scenarioBodies: [
      "Owners wanting Radisson recognition at 5.5% royalty without core Radisson upscale or Blu design capex.",
      "Airport and suburban conversions in Americas under Choice distribution and Choice Privileges.",
      "Item 19 small sample (5 hotels): ~49.5% loyalty / ~80.8% enterprise—treat footprint metrics as directional only.",
    ],
    bestAtTitles: [
      "Fresh Functional Radisson Family",
      "Airport & Suburban Fit",
      "Choice + RHG Architecture",
    ],
    bestAtBodies: [
      "Radisson-family upper-midscale: functional, friendly positioning versus Country Inn warmth or core Radisson upscale.",
      "5.5% royalty; high enterprise mix in small FDD sample (~80.8%).",
      "CALA and North America growth under Choice—confirm RHG vs Choice standards in LOI.",
    ],
    whyValue:
      "Why owners choose Park Inn: fresh, functional, friendly hotels in Radisson family.\nGuest promise: Radisson recognition at upper-midscale economics in Americas.\nArchitecture: Choice operates Americas; RHG outside Americas—confirm program rules.\nEconomics: 5.5% royalty; Item 19 5-hotel sample ~49.5% loyalty / ~80.8% enterprise.\nFit: upper-midscale conversions—not Blu design-forward or economy highway.",
    ownerExperience:
      "Typical guest: business and leisure travelers seeking Radisson name at moderate price point.\nOwner journey: Radisson-family PIP → Choice systems cutover → clarify RHG marketing vs Choice Privileges.\nCommercial: small Item 19 sample—rely on local comps and operator references.\nOngoing: position vs Country Inn (warmth/breakfast) and core Radisson (upscale full-service).",
    differentiatorsIdentity:
      "Radisson family: fresh, functional, friendly (Americas under Choice).\nDistinct from Country Inn residential warmth and core Radisson upscale.\nRadisson visual identity with Choice distribution.\nUpper-midscale—not Park Plaza upscale or Blu design tier.",
    differentiatorsCommercial:
      "5.5% royalty on gross room revenues (Park Inn FDD).\nItem 19: 5-hotel sample ~49.5% loyalty; ~80.8% enterprise (directional).\nChoice Privileges in Americas; RHG programs outside—confirm market.\nConversion-friendly Radisson sub-brand at lower capex than core Radisson.",
    proofOperator:
      "Upper-midscale operators delivering Radisson-family standards with Choice CRS—comfortable with small-system FDD sample limits.",
    proof: [
      { title: "Radisson Family (Americas)", body: "Park Inn owned by Choice in Americas; RHG elsewhere—confirm agreement scope." },
      { title: "5-Hotel FDD Sample", body: "Item 19 uses 5-hotel sample—footprint metrics are directional, not system-wide." },
      { title: "High Enterprise Mix", body: "Sample ~80.8% enterprise/CRS booking mix—distribution-dependent model." },
      { title: "5.5% Royalty", body: "Below Country Inn 6.0%; below core Radisson upscale fee burden." },
      { title: "~49.5% Loyalty", body: "Choice Privileges contribution in small sample—validate locally." },
      { title: "Functional Upper-Midscale Ops", body: "Not Blu Nordic Nouveau or economy lean—airport/suburban service discipline." },
    ],
  },
  "Country Inn & Suites by Radisson (Choice)": {
    scenarioTitles: [
      "Warm Upper-Midscale Suburban",
      "Radisson Family Breakfast-Led",
      "High Enterprise Mix",
    ],
    scenarioBodies: [
      "Family and leisure suburban corridors competing with Comfort—Radisson warmth and residential feel.",
      "6.0% royalty with Item 19 ~43.7% loyalty / ~81.8% enterprise (strong distribution story).",
      "Conversion of upper-midscale assets wanting Radisson name without core Radisson full-service capex.",
    ],
    bestAtTitles: [
      "Residential Warmth Positioning",
      "Radisson Family Recognition",
      "Enterprise-Heavy Distribution",
    ],
    bestAtBodies: [
      "Warm, welcoming upper-midscale in Radisson family—breakfast-led limited-service versus Park Inn functional.",
      "6.0% royalty; ~81.8% enterprise in FDD sample—model net after fees.",
      "Suburban and highway upper-midscale where guests expect residential comfort cues.",
    ],
    whyValue:
      "Why owners choose Country Inn: warm, welcoming upper-midscale in Radisson family.\nGuest promise: residential comfort with Radisson recognition and Choice Privileges.\nEconomics: 6.0% royalty; ~81.8% enterprise mix in Item 19 sample.\nCompetitive set: Comfort, Quality, Park Inn—compare fee stack and guest promise.\nFit: breakfast-led upper-midscale operators in suburban/family corridors.",
    ownerExperience:
      "Typical guest: families and business travelers wanting warm, consistent upper-midscale.\nOwner journey: Radisson-family standards → breakfast and guestroom PIP → Choice loyalty cutover.\nCommercial: high enterprise participation—execute on CRS and member fulfillment.\nOngoing: differentiate from Park Inn (functional) and core Radisson (full-service).",
    differentiatorsIdentity:
      "Warm, welcoming, residential Radisson-family positioning.\nUpper-midscale breakfast-led model (versus Park Inn functional).\nRadisson name in Americas under Choice Hotels.\nSuburban and family-corridor guest experience focus.",
    differentiatorsCommercial:
      "6.0% royalty on gross room revenues.\nItem 19 FY 2025: ~43.7% loyalty; ~81.8% enterprise—high distribution mix.\nCompetes with Comfort on breakfast upper-midscale—compare 6% vs Comfort 6% and prototypes.\nChoice Privileges and enterprise channels core to economics.",
    proofOperator:
      "Upper-midscale operators who deliver warm guestroom and breakfast experience with Radisson-family QA under Choice systems.",
    proof: [
      { title: "Radisson Family Warmth", body: "Residential, welcoming positioning—distinct from Park Inn functional sub-brand." },
      { title: "6.0% Royalty", body: "Same royalty tier as Comfort—compare prototypes and PIP scope." },
      { title: "~81.8% Enterprise Mix", body: "FDD sample shows heavy enterprise/CRS participation—distribution-led model." },
      { title: "Suburban & Family Corridors", body: "Typical fit: suburban upper-midscale—not urban upscale or economy." },
      { title: "~43.7% Loyalty Rooms", body: "Choice Privileges contribution in FY 2025 sample." },
      { title: "Breakfast-Led Execution", body: "Operator proof: guestroom warmth + breakfast consistency—not meetings or resort F&B." },
    ],
  },
  "Radisson RED  (Choice)": {
    scenarioTitles: [
      "Urban Lifestyle Conversion",
      "CALA Gateway Select-Service",
      "Flex F&B Without Full Kitchen",
    ],
    scenarioBodies: [
      "Urban mid-rise or mixed-use conversions where guests want playful design, OUIBar + KTCHN social hub, and 24/7 fitness—not formal full-service dining.",
      "Brazil and Peru gateway cities (Campinas, Miraflores) aligning with Choice Radisson growth—4 open + 5 pipeline hotels per Sep 2023 press materials.",
      "Operators who can run informal upscale select-service with lightning-fast Wi-Fi, digiwall lobby, and social scene activation.",
    ],
    bestAtTitles: [
      "OUIBar + KTCHN Social Hub",
      "Bold Urban Design",
      "Playful Informal Service",
    ],
    bestAtBodies: [
      "All-day communal bar-food space for meet-ups, remote work, and local culture—flex model versus full restaurant.",
      "Instagrammable design, rain showers, RED amenities, and digiwall lobby—press kit emphasizes stand-out urban experience.",
      "Guests control work/leisure blend; 24/7 fitness and on-demand access—operator must staff informal service culture.",
    ],
    whyValue:
      "Why owners choose RED: upscale select-service urban social brand in Americas under Choice.\nGuest promise: Play seriously—informal, flexible, boldly designed stays with OUIBar energy.\nScale: 4 hotels, 606 rooms open; 5 in development (Sep 2023 press kit).\nEconomics: Radisson-family 6% royalty; no RED Item 19 table in current CHI FDD—feasibility-led.\nFit: urban lifestyle operators—not suburban breakfast-led or soft-collection boutiques.",
    ownerExperience:
      "Typical guest: urban travelers wanting control, social lobby, flex F&B, and design-forward rooms.\nOwner journey: urban feasibility → OUIBar/lobby PIP → 24/7 fitness and tech → informal service training.\nRamp: no system Item 19 averages—model from Minneapolis/Miami/Campinas comps and local ADR.\nOngoing: activate social hub and design standards; differentiate from core Radisson full-service.",
    differentiatorsIdentity:
      "Playful, informal, boldly designed urban hotels (Choice dev site).\nOUIBar + KTCHN flex deli-bar—not full-service restaurant mandate.\n24/7 fitness, rain showers, RED bath amenities, complimentary hotel-wide Wi-Fi.\nDigiwall lobby and communal work/social furniture.",
    differentiatorsCommercial:
      "6.0% royalty (Radisson-family CHI disclosure—confirm all program fees).\nNo Item 19 performance table for RED in current master FDD sample.\nChoice Privileges participation—model member discounts and fulfillment.\nAmericas owned by Choice; RHG owns brand outside Americas.",
    proofOperator:
      "Upscale select-service operators who deliver urban social lobby, flex F&B, 24/7 fitness, and design PIP without full-service kitchen complexity.",
    proof: [
      { title: "4 Open / 5 Pipeline", body: "Choice media center (Sep 30, 2023): 4 hotels, 606 rooms open; 5 in development across Americas." },
      { title: "Urban Americas Focus", body: "Vibrant North and South American cities—food, drink, and culture immersion per press kit." },
      { title: "OUIBar + KTCHN", body: "Signature flex F&B hub combining bar soul and great food for social and remote-work use." },
      { title: "No Item 19 Table", body: "Current CHI FDD cites 0 RED hotels in master sample—underwrite property-level, not system averages." },
      { title: "Playful Positioning", body: "Enjoy It! / play seriously—differentiates from Blu Nordic and Individuals boutique collection." },
      { title: "24/7 Fitness & Tech", body: "On-demand fitness and lightning-fast Wi-Fi are guest expectations—budget equipment and bandwidth." },
    ],
  },
  "Radisson Individual (Choice)": {
    scenarioTitles: [
      "Boutique Independent Conversion",
      "CALA Hand-Selected Growth",
      "Preserve Uniqueness + Choice Scale",
    ],
    scenarioBodies: [
      "Distinctive urban or resort boutique with local story seeking upper-upscale distribution without rinse-and-repeat chain design.",
      "Colombia and Panama Faranda members and similar CALA assets—15 hotels, 1,732 rooms per Sep 2024 press kit.",
      "Operators delivering Characterful Encounters, Vivid Settings, and Explorer's Compass service with collection compliance discipline.",
    ],
    bestAtTitles: [
      "Three Experience Pillars",
      "Hand-Selected Portfolio",
      "Upper-Upscale Soft Brand",
    ],
    bestAtBodies: [
      "Characterful Encounters, Vivid Settings, Explorer's Compass—immersive local culture with reliable exploration-minded team.",
      "Each property chosen for bold vision and exceptional service versus standardized prototype rollout.",
      "Maintain uniqueness while benefiting from Choice infrastructure, awareness, and 2024 relaunch momentum.",
    ],
    whyValue:
      "Why owners choose Individuals: upper-upscale soft brand for independents and boutiques in Americas.\nGuest promise: Explorers Welcome—curiosity, local stories, vivid design.\nScale: 15 hotels, 1,732 rooms (Sep 2024 press kit).\nEconomics: 6% royalty; no Item 19 performance table—property feasibility required.\nFit: distinctive assets—not RED urban playful or economy/midscale prototypes.",
    ownerExperience:
      "Typical guest: experience-led travelers seeking boutique character with loyalty and reservations platform.\nOwner journey: hand-selection narrative → collection PIP preserving design → Choice systems cutover.\nCommercial: model net after fees without system Item 19 averages.\nOngoing: balance local F&B/design freedom with collection QA and Explorer's Compass training.",
    differentiatorsIdentity:
      "Hand-selected independent and boutique hotels only.\nThree pillars: Characterful Encounters, Vivid Settings, Explorer's Compass.\nRelaunched 2024 as upper-upscale soft brand in Americas.\nStands against rinse-and-repeat chain experiences (press kit).",
    differentiatorsCommercial:
      "6.0% royalty; moderate PIP band in fee tooling—confirm FDD.\nNo Item 19 financial performance representations.\nCompetes with Autograph, Curio, JdV, Tribute (industry framing).\nChoice + Radisson awareness in Americas; RHG outside Americas.",
    proofOperator:
      "Upper-upscale independent operators who curate local experiences, preserve design identity, and meet collection compliance without expecting prototype uniformity.",
    proof: [
      { title: "15 Hotels / 1,732 Rooms", body: "Choice media center (Sep 30, 2024): Americas operating scale for Individuals." },
      { title: "2024 Relaunch", body: "Young, dynamic brand conquering new frontiers—press kit growth trajectory narrative." },
      { title: "Hand-Selected Only", body: "Properties chosen for vision and service— not open enrollment on rigid prototype." },
      { title: "Three Pillars", body: "Characterful Encounters · Vivid Settings · Explorer's Compass structure guest promise." },
      { title: "No Item 19 Table", body: "FDD: no financial performance representations—feasibility and local comps required." },
      { title: "CALA Member Examples", body: "Faranda Collection and boutique members in Colombia and Panama illustrate collection fit." },
    ],
  },
  "Everhome Suites": {
    scenarioTitles: [
      "Weekly-Stay Revenue Stability",
      "Efficient Purpose-Built Prototype",
      "Midscale Platform With Choice Scale",
    ],
    scenarioBodies: [
      "Greenfield or conversion extended-stay in markets with project, relocation, and training demand where guests stay about two weeks on average. Full kitchens and residential suites support weekly rate structures and lower guest churn than nightly midscale flags.",
      "Developers choosing Choice's newest midscale extended-stay product—designed with extended-stay operators for efficient new builds and conversions. Energy-efficient systems and modular FF&E help control capex and utility intensity versus ad hoc extended conversions.",
      "Midscale extended-stay positioning with Choice distribution, loyalty enrollment, and dedicated extended-stay opening support—modern residential product above economy extended flags without legacy prototype constraints on mature MainStay conversions.",
    ],
    bestAtTitles: [
      "Newest Choice Midscale Extended Brand",
      "Residential Weekly-Stay",
      "Developer-Friendly Prototype",
    ],
    bestAtBodies: [
      "Introduced in 2020 as the first new core midscale brand in nearly a decade—residential extended-stay with kitchens and Homebase Market.\n6% royalty on room revenue for agreement duration.\nCompare prototype and fees against MainStay (mature) and Suburban (economy extended).",
      "Kitchen-equipped suites for relocations, projects, and training assignments averaging about 15 nights—with Homebase Market, outdoor living, and fitness amenities per prototype.",
      "Turnkey development support, flexible prototypes, competitive cost per key, and area development model—ready-to-build track typically 10–12 months per brand materials.",
    ],
    whyValue:
      "Why owners choose Everhome: first new core midscale extended-stay brand from Choice in nearly a decade (2020).\nGuest promise: residential suites with kitchens, Homebase Market, and outdoor living for ~15-night stays.\nEconomics: midscale extended-stay fee tier with incremental revenue from Homebase, housekeeping upgrades, and pet fees.\nFit: developers and operators with professional third-party management and weekly-stay expertise—not nightly transient midscale.",
    ownerExperience:
      "Typical guest: weekly and monthly stays (~15 nights average)—relocations, projects, travel nurses, and training.\nOwner journey: market feasibility → prototype approval → professional management onboarding → compare vs MainStay/Suburban fees.\nCommercial: build pro forma from comp set extended properties and Homebase ancillary revenue.\nOngoing: weekly billing discipline, kitchen maintenance, and brand QA cadence.",
    differentiatorsIdentity:
      "Midscale extended-stay with fully equipped in-suite kitchens.\nSeparate spaces to sleep, work, and eat.\nSocial outdoor living with fire pits, grills, and optional pool.\nDesigned with prominent extended-stay developers and operators.",
    differentiatorsCommercial:
      "6% royalty on room revenue per franchise agreement.\nIncremental revenue from Homebase Market, upgraded housekeeping, faster Wi-Fi, and pet fees.\nChoice distribution and Choice Privileges enrollment.\nArea development model for multi-unit growth.",
    proofOperator:
      "Extended-stay operators with professional third-party management experience—strong feasibility, kitchen ops, and weekly billing discipline.",
    proof: [
      { title: "2020 Midscale Launch", body: "Introduced in 2020 as the first new core midscale brand in nearly a decade; first hotel opened in Corona, CA." },
      { title: "6% Room Revenue Royalty", body: "Royalty six percent of room revenue for agreement duration (franchise disclosure)." },
      { title: "Purpose-Built Prototype", body: "Designed with extended-stay developers; ready-to-build track typically 10–12 months." },
      { title: "Residential Suites", body: "Weekly-stay residential product with fully equipped kitchens—not nightly midscale or upscale F&B." },
      { title: "Compare MainStay/Suburban", body: "Benchmark fees and prototypes against mature extended brands in portfolio." },
      { title: "Extended-Stay Expert Bench", body: "70+ dedicated extended-stay experts support openings and operating model execution." },
    ],
  },
};

export function overviewForBrand(brandName) {
  return TIER1_OVERVIEW[brandName] || null;
}
