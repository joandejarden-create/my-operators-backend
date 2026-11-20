#!/usr/bin/env node
/** One-shot generator for Batch 14 depth packs (Accor/partner + Marriott + Wyndham). */
import { writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");
const outPath = join(ROOT, "lib/brand-explorer/factory-2/be-factory-2-depth-research-packs-batch14-v1.js");
const S = "S";
const M = "M";
const L = "L";
const N = "N";

/** @type {Array<object>} */
const brands = [
  {
    id: "recJgviX5z4tLcMom",
    name: "Sofitel",
    live: "Sofitel",
    slug: "sofitel",
    parent: "Accor",
    loyalty: "ALL - Accor Live Limitless",
    proof: "ACCOR_PROOF",
    short: "Sofitel",
    feeGeo: "Confirm current Accor development / franchise / management disclosure for Sofitel (luxury)",
    brandPage: "https://all.accor.com/brands/sofitel.en.shtml",
    positioning: "Luxury French Full-Service",
    commercial:
      "Versus Raffles ultra-luxury heritage icons, Sofitel sells contemporary French luxury full-service; versus Mövenpick upscale, Sofitel is harder luxury intensity inside ALL.",
    scenarios: [
      ["Gateway Luxury Full-Service", "Primary gateways where Sofitel French luxury and ALL clear ADR."],
      ["Resort Luxury Destinations", "Destination resorts funding Sofitel F&B and spa standards."],
      ["Conversion Into Sofitel Luxury", "Conversions only when luxury PIP and service clear Sofitel QA."],
    ],
    demand: [
      ["Luxury Urban Flagship", S, "Sofitel fits contemporary French luxury urban flags with ALL."],
      ["Resort & Destination Leisure", S, "Luxury resorts are a core Sofitel lane when capital clears."],
      ["Meetings & Group Compression", M, "Meetings fit when luxury full-service plant exists."],
      ["Conversion & Repositioning", M, "Conversions succeed only with true luxury PIP."],
      ["Suburban Business & Leisure", L, "Suburban select-service economics sit outside Sofitel luxury."],
      ["Highway & Interstate Transient", N, "Highway transient sits outside Sofitel luxury."],
    ],
    ops: "Accor luxury full-service with elevated F&B and ALL. Do not import midscale Mercure / ibis economics. Distinguish Sofitel Legend icons.",
    footprint: "Confirm live hotel counts from current Accor / Sofitel disclosures across APAC, EMEA, Americas, and CALA.",
    insight: "Sofitel fits sponsors funding French luxury with ALL; fails when Mercure or Sofitel Legend capital is assumed.",
  },
  {
    id: "recxAGLjrBt8PF3IT",
    name: "Sofitel Legend",
    live: "Sofitel Legend",
    slug: "sofitel-legend",
    parent: "Accor",
    loyalty: "ALL - Accor Live Limitless",
    proof: "ACCOR_PROOF",
    short: "Sofitel Legend",
    feeGeo: "Confirm current Accor management / development disclosure for Sofitel Legend (ultra-curated icons)",
    brandPage: "https://all.accor.com/brands/sofitel.en.shtml",
    positioning: "Ultra-Curated Heritage Luxury",
    commercial:
      "Versus Sofitel contemporary luxury, Sofitel Legend sells rare heritage icons; versus Raffles, Legend is Sofitel-branded curated landmarks inside ALL.",
    scenarios: [
      ["Heritage Icon Conversion", "Landmark assets that clear Sofitel Legend exclusivity and capital."],
      ["Gateway Ultra-Luxury Flag", "Gateways where Legend product and ALL clear ultra-luxury ADR."],
      ["Destination Legend Resort", "Resorts with heritage narrative funding Legend service intensity."],
    ],
    demand: [
      ["Luxury Urban Flagship", S, "Sofitel Legend fits rare urban heritage icons with ALL."],
      ["Resort & Destination Leisure", S, "Destination Legend resorts fit when exclusivity clears."],
      ["Conversion & Repositioning", M, "Only true landmark conversions clear Legend QA."],
      ["Meetings & Group Compression", L, "Large group compression often conflicts with Legend exclusivity."],
      ["Suburban Business & Leisure", N, "Suburban productive mix sits outside Legend."],
      ["Highway & Interstate Transient", N, "Highway transient is outside Legend."],
    ],
    ops: "Accor ultra-curated Sofitel Legend icons — management-heavy, exclusivity-gated. Do not invent mature counts for a curated collection. Preserve live name Sofitel Legend.",
    footprint: "Confirm live Legend hotel counts from current Accor disclosures—intentionally small global set.",
    insight: "Sofitel Legend fits sponsors with landmark heritage assets; fails when Sofitel contemporary economics are assumed.",
  },
  {
    id: "recjYuYOLjL4zusI6",
    name: "Raffles Hotels & Resorts",
    live: "Raffles",
    slug: "raffles-hotels-resorts",
    parent: "Accor",
    loyalty: "ALL - Accor Live Limitless",
    proof: "ACCOR_PROOF",
    short: "Raffles",
    feeGeo: "Confirm current Accor management / development disclosure for Raffles (ultra-luxury)",
    brandPage: "https://all.accor.com/brands/raffles.en.shtml",
    positioning: "Ultra-Luxury Heritage Icons",
    commercial:
      "Versus Sofitel contemporary luxury, Raffles sells legendary ultra-luxury heritage; versus Banyan Tree wellness luxury, Raffles is Accor-owned icon hospitality inside ALL.",
    scenarios: [
      ["Ultra-Luxury Heritage Flag", "Icons where Raffles legend and ALL clear ultra-luxury ADR."],
      ["Destination Ultra-Luxury Resort", "Resorts funding Raffles service and F&B intensity."],
      ["Landmark Conversion Into Raffles", "Only landmarks that clear Raffles exclusivity and PIP."],
    ],
    demand: [
      ["Luxury Urban Flagship", S, "Raffles fits legendary urban ultra-luxury flags with ALL."],
      ["Resort & Destination Leisure", S, "Ultra-luxury resorts are a core Raffles lane."],
      ["Conversion & Repositioning", M, "Conversions succeed only for true landmark assets."],
      ["Meetings & Group Compression", L, "Large groups often conflict with Raffles exclusivity."],
      ["Suburban Business & Leisure", N, "Suburban select-service sits outside Raffles."],
      ["Highway & Interstate Transient", N, "Highway transient is outside Raffles."],
    ],
    ops: "Accor ultra-luxury Raffles — management-led icons with ALL. Preserve live name Raffles. Do not invent fee percentages.",
    footprint: "Confirm live Raffles counts from current Accor disclosures—curated global ultra-luxury set across APAC, EMEA, and Americas.",
    insight: "Raffles fits sponsors funding legendary ultra-luxury; fails when Sofitel or Grand Mercure capital is assumed.",
  },
  {
    id: "recfG3hikZOMqDEb0",
    name: "Mövenpick Hotels & Resorts",
    live: "Mövenpick",
    slug: "movenpick-hotels-resorts",
    parent: "Accor",
    loyalty: "ALL - Accor Live Limitless",
    proof: "ACCOR_PROOF",
    short: "Mövenpick",
    feeGeo: "Confirm current Accor development / franchise disclosure for Mövenpick (upscale)",
    brandPage: "https://all.accor.com/brands/movenpick.en.shtml",
    positioning: "Upscale Full-Service Lifestyle",
    commercial:
      "Versus Sofitel luxury, Mövenpick sells productive upscale full-service; versus Grand Mercure mid-upscale, Mövenpick is higher lifestyle intensity inside ALL.",
    scenarios: [
      ["Urban / Suburban Upscale Full-Service", "Markets where Mövenpick ADR and ALL clear IC."],
      ["Resort Upscale Leisure", "Resorts funding Mövenpick F&B and leisure product."],
      ["Conversion Into Mövenpick", "Conversions that can fund upscale PIP and F&B."],
    ],
    demand: [
      ["Suburban Business & Leisure", S, "Mövenpick fits productive upscale suburban mix with ALL."],
      ["Meetings & Group Compression", S, "Meetings fit when full-service plant exists."],
      ["Resort & Destination Leisure", S, "Upscale leisure resorts are a core Mövenpick lane."],
      ["Conversion & Repositioning", M, "Conversions succeed when PIP funds upscale rooms and F&B."],
      ["Luxury Urban Flagship", L, "True luxury flagships sit closer to Sofitel / Raffles."],
      ["Highway & Interstate Transient", N, "Highway limited-service sits outside Mövenpick."],
    ],
    ops: "Accor upscale full-service Mövenpick with ALL. Preserve live name Mövenpick. Confirm franchise / management exhibits—do not invent royalties.",
    footprint: "Confirm live Mövenpick counts from current Accor disclosures across EMEA, APAC, Africa, and Americas.",
    insight: "Mövenpick fits upscale full-service with ALL; fails when Sofitel luxury or ibis limited-service fees are assumed.",
  },
  {
    id: "recnRlPoQjNlDFVJn",
    name: "Grand Mercure",
    live: "Grand Mercure",
    slug: "grand-mercure",
    parent: "Accor",
    loyalty: "ALL - Accor Live Limitless",
    proof: "ACCOR_PROOF",
    short: "Grand Mercure",
    feeGeo: "Confirm current Accor development / franchise disclosure for Grand Mercure (mid-upscale)",
    brandPage: "https://all.accor.com/brands/grand-mercure.en.shtml",
    positioning: "Mid-Upscale Full-Service / Apartments",
    commercial:
      "Versus Mövenpick upscale lifestyle, Grand Mercure sells mid-upscale full-service and apartments; versus Mercure classic, Grand Mercure is the elevated Mercure rung inside ALL.",
    scenarios: [
      ["Urban Mid-Upscale Full-Service", "Cities where Grand Mercure ADR clears IC versus Mercure."],
      ["Apartment / Extended Stay Mix", "Markets needing apartment stock with ALL."],
      ["Conversion Into Grand Mercure", "Conversions funding mid-upscale PIP."],
    ],
    demand: [
      ["Suburban Business & Leisure", S, "Grand Mercure fits mid-upscale suburban productive mix."],
      ["Extended-Stay Corporate", S, "Apartment product is a core Grand Mercure lane where offered."],
      ["Meetings & Group Compression", M, "Meetings fit when full-service plant exists."],
      ["Conversion & Repositioning", M, "Conversions succeed at mid-upscale capital."],
      ["Luxury Urban Flagship", L, "Luxury flags sit above Grand Mercure."],
      ["Highway & Interstate Transient", N, "Highway limited-service sits outside Grand Mercure."],
    ],
    ops: "Accor mid-upscale Grand Mercure with ALL — often hotel + apartment formats. Confirm regional standards (strong APAC presence).",
    footprint: "Confirm live Grand Mercure counts from current Accor disclosures—concentrated APAC with selective global growth.",
    insight: "Grand Mercure fits mid-upscale and apartment sponsors with ALL; fails when Sofitel luxury capital is assumed.",
  },
  {
    id: "rec7F80S01oyTADN2",
    name: "Peppers",
    live: "Peppers",
    slug: "peppers",
    parent: "Accor",
    loyalty: "ALL - Accor Live Limitless",
    proof: "ACCOR_PROOF",
    short: "Peppers",
    feeGeo: "Confirm current Accor / AAPC development disclosure for Peppers (upscale lifestyle / APAC)",
    brandPage: "https://all.accor.com/brands/peppers.en.shtml",
    positioning: "Upscale Lifestyle / Retreats APAC",
    commercial:
      "Versus Mantra midscale apartments, Peppers sells upscale lifestyle retreats; versus Sofitel luxury, Peppers is regional upscale intensity inside ALL.",
    scenarios: [
      ["APAC Lifestyle / Retreat", "Australia / NZ / Asia leisure corridors funding Peppers product."],
      ["Urban Upscale Lifestyle", "Cities where Peppers lifestyle ADR clears IC."],
      ["Conversion Into Peppers", "Conversions that clear Peppers design and service."],
    ],
    demand: [
      ["Resort & Destination Leisure", S, "Peppers fits lifestyle retreats and leisure destinations."],
      ["Suburban Business & Leisure", M, "Select suburban lifestyle sites can fit."],
      ["Conversion & Repositioning", M, "Conversions succeed when design clears Peppers."],
      ["Meetings & Group Compression", L, "Large groups are secondary to Peppers lifestyle."],
      ["Extended-Stay Corporate", L, "Extended-stay kitchens sit closer to Mantra / The Sebel."],
      ["Highway & Interstate Transient", N, "Highway transient sits outside Peppers."],
    ],
    ops: "Accor AAPC Peppers — upscale lifestyle / retreats, ALL participation. Geographic concentration in APAC. Confirm management vs franchise locally.",
    footprint: "Confirm live Peppers counts from current Accor / AAPC disclosures—APAC-weighted.",
    insight: "Peppers fits APAC lifestyle sponsors with ALL; fails when US interstate midscale economics are assumed.",
  },
  {
    id: "receSUBBbaxgbenTd",
    name: "Mantra",
    live: "Mantra",
    slug: "mantra",
    parent: "Accor",
    loyalty: "ALL - Accor Live Limitless",
    proof: "ACCOR_PROOF",
    short: "Mantra",
    feeGeo: "Confirm current Accor / AAPC development disclosure for Mantra (midscale apartments / hotels)",
    brandPage: "https://all.accor.com/brands/mantra.en.shtml",
    positioning: "Midscale Hotels & Apartments APAC",
    commercial:
      "Versus Peppers upscale lifestyle, Mantra sells midscale hotels and apartments; versus BreakFree leisure apartments, Mantra is the broader midscale apartment platform inside ALL.",
    scenarios: [
      ["APAC Midscale Apartment", "Leisure and urban markets needing Mantra apartment stock."],
      ["Urban Midscale Hotel", "Cities where Mantra hotel ADR clears IC."],
      ["Conversion Into Mantra", "Conversions funding midscale PIP and apartment setups."],
    ],
    demand: [
      ["Extended-Stay Corporate", S, "Apartment inventory is a core Mantra economics lane."],
      ["Resort & Destination Leisure", S, "Leisure apartment resorts fit Mantra well."],
      ["Suburban Business & Leisure", S, "Suburban midscale mix fits Mantra."],
      ["Conversion & Repositioning", M, "Conversions succeed at midscale apartment capital."],
      ["Luxury Urban Flagship", N, "Luxury flags sit outside Mantra."],
      ["Highway & Interstate Transient", L, "Pure highway limited-service is usually BreakFree / other."],
    ],
    ops: "Accor AAPC Mantra — midscale hotels & apartments with ALL. Housekeeping and apartment staffing differ from transient hotels. APAC concentration.",
    footprint: "Confirm live Mantra counts from current Accor / AAPC disclosures—Australia / NZ / Asia weighted.",
    insight: "Mantra fits midscale apartment sponsors in APAC; fails when Peppers upscale or US extended-stay Bonvoy economics are assumed.",
  },
  {
    id: "recjRHpf0a0ylJbXQ",
    name: "BreakFree",
    live: "BreakFree Resort",
    slug: "breakfree",
    parent: "Accor",
    loyalty: "ALL - Accor Live Limitless",
    proof: "ACCOR_PROOF",
    short: "BreakFree",
    feeGeo: "Confirm current Accor / AAPC development disclosure for BreakFree (leisure apartments)",
    brandPage: "https://all.accor.com/brands/breakfree.en.shtml",
    positioning: "Leisure Apartments APAC",
    commercial:
      "Versus Mantra broader midscale apartments, BreakFree sells leisure-first apartment resorts; versus The Sebel serviced apartments, BreakFree is more leisure / family intensity inside ALL.",
    scenarios: [
      ["APAC Leisure Apartment Resort", "Coastal / leisure corridors funding BreakFree apartments."],
      ["Family Leisure Destination", "Markets needing self-catering leisure apartments with ALL."],
      ["Conversion Into BreakFree", "Conversions that clear leisure apartment standards."],
    ],
    demand: [
      ["Resort & Destination Leisure", S, "BreakFree is built for leisure apartment destinations."],
      ["Extended-Stay Corporate", M, "Longer leisure stays fit; corporate ES is secondary."],
      ["Suburban Business & Leisure", L, "Suburban business mix is usually Mantra / Mercure."],
      ["Conversion & Repositioning", M, "Conversions succeed for leisure apartment assets."],
      ["Meetings & Group Compression", L, "Meetings are not the BreakFree core."],
      ["Luxury Urban Flagship", N, "Luxury urban flags sit outside BreakFree."],
    ],
    ops: "Accor AAPC BreakFree — leisure apartments with ALL. Preserve live name BreakFree Resort. Confirm APAC operating model; do not import US highway ES tables.",
    footprint: "Confirm live BreakFree counts from current Accor / AAPC disclosures—APAC leisure weighted.",
    insight: "BreakFree fits APAC leisure apartment sponsors; fails when Mantra urban midscale or Peppers upscale fees are assumed.",
  },
  {
    id: "recilV5oDDP3wtKJn",
    name: "The Sebel",
    live: "The Sebel",
    slug: "the-sebel",
    parent: "Accor",
    loyalty: "ALL - Accor Live Limitless",
    proof: "ACCOR_PROOF",
    short: "The Sebel",
    feeGeo: "Confirm current Accor / AAPC development disclosure for The Sebel (serviced apartments)",
    brandPage: "https://all.accor.com/brands/the-sebel.en.shtml",
    positioning: "Upscale Serviced Apartments APAC",
    commercial:
      "Versus Mantra midscale apartments, The Sebel sells upscale serviced apartments; versus Peppers lifestyle hotels, The Sebel is apartment-first inside ALL.",
    scenarios: [
      ["APAC Upscale Serviced Apartment", "Urban corridors needing Sebel apartment product."],
      ["Extended Corporate Housing", "Markets funding longer-stay upscale apartments with ALL."],
      ["Conversion Into The Sebel", "Conversions clearing upscale apartment PIP."],
    ],
    demand: [
      ["Extended-Stay Corporate", S, "The Sebel is built for upscale serviced apartments."],
      ["Suburban Business & Leisure", M, "Suburban apartment sites can fit."],
      ["Conversion & Repositioning", M, "Conversions succeed at upscale apartment capital."],
      ["Resort & Destination Leisure", L, "Pure leisure resorts sit closer to BreakFree / Peppers."],
      ["Luxury Urban Flagship", L, "True luxury hotels sit above The Sebel."],
      ["Highway & Interstate Transient", N, "Highway transient sits outside The Sebel."],
    ],
    ops: "Accor AAPC The Sebel — upscale serviced apartments with ALL. Housekeeping cadence differs from transient hotels. APAC concentration.",
    footprint: "Confirm live The Sebel counts from current Accor / AAPC disclosures—APAC urban weighted.",
    insight: "The Sebel fits upscale apartment sponsors in APAC; fails when BreakFree leisure or Sofitel hotel luxury fees are assumed.",
  },
  {
    id: "recNabpDL7arkX5mi",
    name: "Banyan Tree",
    live: "Banyan Tree",
    slug: "banyan-tree",
    parent: "Banyan Tree Holdings (Accor partner distribution)",
    loyalty: "ALL - Accor Live Limitless",
    proof: "ACCOR_PROOF",
    short: "Banyan Tree",
    feeGeo: "Confirm Banyan Tree Holdings management / development terms and Accor distribution partnership exhibits—partner-affiliated, not Accor-owned fee tables",
    brandPage: "https://www.banyantree.com/",
    positioning: "Luxury Wellness Resorts",
    commercial:
      "Versus Angsana accessible luxury wellness, Banyan Tree sells harder luxury spa / wellness; versus Raffles Accor-owned ultra-luxury, Banyan Tree is partner-owned wellness luxury with ALL distribution.",
    scenarios: [
      ["Luxury Wellness Resort", "Destinations funding Banyan Tree spa and service intensity."],
      ["Exclusive Urban Retreat", "Select urban retreats clearing Banyan Tree exclusivity."],
      ["Conversion Into Banyan Tree", "Only assets clearing luxury wellness PIP and brand QA."],
    ],
    demand: [
      ["Resort & Destination Leisure", S, "Banyan Tree is built for luxury wellness destinations."],
      ["Luxury Urban Flagship", M, "Select urban retreats can fit when exclusivity clears."],
      ["Conversion & Repositioning", M, "Conversions succeed only with true luxury wellness capital."],
      ["Meetings & Group Compression", L, "Large groups often conflict with wellness exclusivity."],
      ["Suburban Business & Leisure", N, "Suburban business mix sits outside Banyan Tree."],
      ["Highway & Interstate Transient", N, "Highway transient sits outside Banyan Tree."],
    ],
    ops: "Partner-affiliated Banyan Tree Holdings brand with Accor ALL distribution—not Accor-owned. Management-heavy wellness resorts. Do not invent Accor franchise royalties for Banyan Tree.",
    footprint: "Confirm live Banyan Tree counts from Banyan Tree Holdings / Accor partner disclosures—APAC / resort weighted with selective global growth.",
    insight: "Banyan Tree fits luxury wellness sponsors under partner management + ALL; fails when Accor-owned Sofitel franchise tables are assumed.",
  },
  {
    id: "recR1DWI8QIecWQDl",
    name: "Angsana",
    live: "Angsana",
    slug: "angsana",
    parent: "Banyan Tree Holdings (Accor partner distribution)",
    loyalty: "ALL - Accor Live Limitless",
    proof: "ACCOR_PROOF",
    short: "Angsana",
    feeGeo: "Confirm Banyan Tree Holdings management / development terms and Accor distribution partnership exhibits for Angsana—partner-affiliated",
    brandPage: "https://www.angsana.com/",
    positioning: "Accessible Luxury Wellness",
    commercial:
      "Versus Banyan Tree harder luxury wellness, Angsana sells more accessible luxury spa / leisure; versus Sofitel Accor-owned luxury, Angsana is partner wellness with ALL.",
    scenarios: [
      ["Accessible Luxury Wellness Resort", "Destinations funding Angsana spa and leisure."],
      ["Family / Leisure Wellness", "Markets wanting wellness with broader leisure mix."],
      ["Conversion Into Angsana", "Conversions clearing accessible luxury wellness PIP."],
    ],
    demand: [
      ["Resort & Destination Leisure", S, "Angsana fits accessible luxury wellness destinations."],
      ["Conversion & Repositioning", M, "Conversions succeed at accessible luxury capital."],
      ["Suburban Business & Leisure", L, "Suburban business mix is secondary."],
      ["Meetings & Group Compression", L, "Meetings are not the Angsana core."],
      ["Luxury Urban Flagship", M, "Select urban wellness sites can fit."],
      ["Highway & Interstate Transient", N, "Highway transient sits outside Angsana."],
    ],
    ops: "Partner-affiliated Angsana (Banyan Tree Holdings) with Accor ALL distribution. Distinguish from Accor-owned brands. Do not invent Accor franchise royalties.",
    footprint: "Confirm live Angsana counts from Banyan Tree Holdings / Accor partner disclosures—resort / APAC weighted.",
    insight: "Angsana fits accessible luxury wellness sponsors under partner management + ALL; fails when Banyan Tree ultra-luxury or Accor franchise tables are assumed.",
  },
  {
    id: "recmkvmV84vr5LNwi",
    name: "Art Series Hotels",
    live: "Art Series",
    slug: "art-series-hotels",
    parent: "Accor",
    loyalty: "ALL - Accor Live Limitless",
    proof: "ACCOR_PROOF",
    short: "Art Series",
    feeGeo: "Confirm current Accor / AAPC development disclosure for Art Series (lifestyle)",
    brandPage: "https://all.accor.com/brands/art-series.en.shtml",
    positioning: "Art-Led Lifestyle Hotels",
    commercial:
      "Versus Peppers upscale lifestyle, Art Series sells art-led boutique lifestyle; versus Sofitel luxury, Art Series is design-forward lifestyle intensity inside ALL.",
    scenarios: [
      ["Urban Art Lifestyle Flag", "Creative urban corridors funding Art Series product."],
      ["Lifestyle Conversion", "Assets that clear art / design PIP for Art Series."],
      ["Leisure Urban Stay", "Cities where lifestyle ADR clears IC with ALL."],
    ],
    demand: [
      ["Suburban Business & Leisure", M, "Select urban-edge lifestyle sites can fit."],
      ["Conversion & Repositioning", S, "Design-led conversions are a core Art Series path."],
      ["Luxury Urban Flagship", M, "Lifestyle flags fit; ultra-luxury does not."],
      ["Resort & Destination Leisure", L, "Pure resort destinations sit closer to Peppers / BreakFree."],
      ["Meetings & Group Compression", L, "Large meetings are secondary."],
      ["Highway & Interstate Transient", N, "Highway transient sits outside Art Series."],
    ],
    ops: "Accor AAPC Art Series — art-led lifestyle hotels with ALL. Preserve live name Art Series. Confirm APAC / selective footprint.",
    footprint: "Confirm live Art Series counts from current Accor / AAPC disclosures—Australia weighted with selective growth.",
    insight: "Art Series fits art-led lifestyle sponsors with ALL; fails when Peppers resort or Sofitel luxury fees are assumed.",
  },
  // Marriott
  {
    id: "recpJ13Fo4nxXmEzD",
    name: "citizenM",
    live: "citizenM",
    slug: "citizenm",
    parent: "Marriott International (distribution / partnership — confirm current affiliation)",
    loyalty: "Marriott Bonvoy",
    proof: "BONVOY_PROOF",
    short: "citizenM",
    feeGeo: "Confirm current Marriott / citizenM affiliation, management, and development exhibits—do not invent Bonvoy franchise royalties without disclosure",
    brandPage: "https://www.citizenm.com/",
    positioning: "Design-Led Select Urban",
    commercial:
      "Versus Element extended-stay select, citizenM sells compact design-led urban rooms; versus Four Points Flex midscale, citizenM is lifestyle select intensity inside Bonvoy where participating.",
    scenarios: [
      ["Urban Lifestyle Select", "Cities where citizenM compact design product clears ADR."],
      ["Gateway / Transit Urban", "Gateways needing efficient lifestyle rooms with Bonvoy."],
      ["Conversion / New-Build citizenM", "Sites clearing citizenM modular / design standards."],
    ],
    demand: [
      ["Suburban Business & Leisure", M, "Select urban-edge sites can fit; classic suburban ES does not."],
      ["Airport & Transportation", S, "Transit-oriented urban sites fit citizenM well."],
      ["Conversion & Repositioning", M, "Conversions succeed when modular / design PIP clears."],
      ["Luxury Urban Flagship", L, "True luxury sits above citizenM."],
      ["Extended-Stay Corporate", L, "Extended-stay kitchens sit closer to Element / MEA."],
      ["Highway & Interstate Transient", N, "Highway limited-service sits outside citizenM."],
    ],
    ops: "citizenM design-led select urban — confirm current Marriott Bonvoy participation and affiliation structure. Do not invent mature franchise fee tables. Preserve live name citizenM.",
    footprint: "Confirm live citizenM counts from current citizenM / Marriott disclosures—urban Europe / Americas / APAC gateways.",
    insight: "citizenM fits urban lifestyle select sponsors with Bonvoy; fails when Element ES or luxury full-service fees are assumed.",
  },
  {
    id: "recCtMPwNFDoE3e9m",
    name: "Element Hotels",
    live: "Element by Westin",
    slug: "element-hotels",
    parent: "Marriott International",
    loyalty: "Marriott Bonvoy",
    proof: "BONVOY_PROOF",
    short: "Element",
    feeGeo: "Confirm current Marriott FDD / development disclosure for Element (extended-stay select)",
    brandPage: "https://element-hotels.marriott.com/",
    positioning: "Upscale Extended-Stay Select",
    commercial:
      "Versus Residence Inn classic ES, Element sells eco / wellness-tilted upscale ES; versus Four Points Flex midscale transient, Element is kitchen-led extended stay inside Bonvoy.",
    scenarios: [
      ["Corporate Extended-Stay Corridor", "Markets funding Element kitchens and Bonvoy ES demand."],
      ["Suburban / Urban-Edge ES", "Sites where Element ADR clears IC versus Residence Inn."],
      ["Conversion Into Element", "Conversions funding Element kitchen and wellness PIP."],
    ],
    demand: [
      ["Extended-Stay Corporate", S, "Element is built for upscale extended-stay with Bonvoy."],
      ["Suburban Business & Leisure", S, "Suburban / urban-edge ES corridors fit Element."],
      ["Conversion & Repositioning", M, "Conversions succeed when kitchen PIP clears."],
      ["Airport & Transportation", M, "Airport-adjacent ES can fit."],
      ["Meetings & Group Compression", L, "Large meetings sit outside Element select ES."],
      ["Highway & Interstate Transient", N, "Pure highway transient sits outside Element."],
    ],
    ops: "Marriott Element — upscale extended-stay select with Bonvoy. Preserve live name Element by Westin. Confirm FDD exhibits; kitchen / housekeeping ES economics ≠ transient select-service.",
    footprint: "Confirm live Element counts from current Marriott disclosures—Americas weighted with selective global growth.",
    insight: "Element fits upscale ES sponsors with Bonvoy; fails when Four Points Flex transient or MEA apartment fees are assumed.",
  },
  {
    id: "rec5SgAtIKEAC9Jgr",
    name: "Protea Hotels by Marriott",
    live: "Protea Hotels by Marriott",
    slug: "protea-hotels-by-marriott",
    parent: "Marriott International",
    loyalty: "Marriott Bonvoy",
    proof: "BONVOY_PROOF",
    short: "Protea",
    feeGeo: "Confirm current Marriott Africa development / franchise disclosure for Protea Hotels by Marriott",
    brandPage: "https://www.marriott.com/en-us/hotels/brands/protea-hotels/",
    positioning: "Regional Full / Select Service Africa",
    commercial:
      "Versus Four Points Flex midscale global, Protea sells Africa-regional full and select service; versus Element ES, Protea is transient regional hospitality inside Bonvoy.",
    scenarios: [
      ["Africa Urban / Gateway Hotel", "African cities where Protea and Bonvoy clear ADR."],
      ["Regional Meetings / Corporate", "Markets funding Protea meetings and corporate mix."],
      ["Conversion Into Protea", "Conversions clearing Protea regional standards."],
    ],
    demand: [
      ["Suburban Business & Leisure", S, "Protea fits productive African urban / suburban mix."],
      ["Meetings & Group Compression", S, "Meetings are a core Protea lane where plant exists."],
      ["Airport & Transportation", S, "Gateway / airport markets fit Protea."],
      ["Conversion & Repositioning", M, "Conversions succeed at regional capital levels."],
      ["Luxury Urban Flagship", L, "True luxury sits above Protea."],
      ["Highway & Interstate Transient", L, "Pure highway limited-service is usually not Protea's core."],
    ],
    ops: "Marriott Protea — Africa-regional brands with Bonvoy. Confirm local franchise / management exhibits. Do not import US-only FDD tables blindly.",
    footprint: "Confirm live Protea counts from current Marriott Africa disclosures—Africa-concentrated.",
    insight: "Protea fits Africa-regional sponsors with Bonvoy; fails when US Element ES or global luxury fees are assumed.",
  },
  {
    id: "recgaMzDn2GKkpUsi",
    name: "Four Points Flex by Sheraton",
    live: "Four Points Flex by Sheraton",
    slug: "four-points-flex-by-sheraton",
    parent: "Marriott International",
    loyalty: "Marriott Bonvoy",
    proof: "BONVOY_PROOF",
    short: "Four Points Flex",
    feeGeo: "Confirm current Marriott FDD / development disclosure for Four Points Flex by Sheraton (midscale). Do not invent mature operating metrics for a newer brand.",
    brandPage: "https://four-points.marriott.com/",
    positioning: "Midscale Select / Flexible Conversion",
    commercial:
      "Versus classic Four Points upscale-leaning select, Four Points Flex sells lower-capital midscale conversion; versus Element ES, Flex is transient midscale inside Bonvoy.",
    scenarios: [
      ["Midscale Conversion Corridor", "Assets where Flex PIP clears versus higher Four Points capital."],
      ["Suburban / Secondary Market Select", "Secondary markets funding Flex with Bonvoy."],
      ["New Midscale Build", "Sites sized for Flex midscale select economics."],
    ],
    demand: [
      ["Conversion & Repositioning", S, "Four Points Flex is built for conversion flexibility."],
      ["Suburban Business & Leisure", S, "Suburban / secondary midscale mix fits Flex."],
      ["Airport & Transportation", M, "Airport midscale can fit."],
      ["Highway & Interstate Transient", M, "Select highway corridors can fit midscale Flex."],
      ["Extended-Stay Corporate", L, "Kitchens sit closer to Element."],
      ["Luxury Urban Flagship", N, "Luxury flags sit outside Flex."],
    ],
    ops: "Marriott Four Points Flex — newer midscale / conversion-friendly Sheraton family brand with Bonvoy. Do not invent mature hotel counts or fee percentages. Confirm current FDD exhibits.",
    footprint: "Confirm live Four Points Flex counts from current Marriott disclosures—early-stage / growing footprint; do not invent mature global counts.",
    insight: "Four Points Flex fits conversion midscale sponsors with Bonvoy; fails when classic Four Points or Element ES capital is assumed. Newer-brand maturity caution.",
  },
  {
    id: "NEW_marriott-executive-apartments",
    name: "Marriott Executive Apartments",
    live: "Marriott Executive Apartments",
    slug: "marriott-executive-apartments",
    parent: "Marriott International",
    loyalty: "Marriott Bonvoy",
    proof: "BONVOY_PROOF",
    short: "Marriott Executive Apartments",
    researchClass: "NEW_BRAND_CANDIDATE",
    feeGeo: "Confirm current Marriott development / management disclosure for Marriott Executive Apartments (serviced apartments)",
    brandPage: "https://www.marriott.com/en-us/hotels/brands/marriott-executive-apartments/",
    positioning: "Upscale Serviced Apartments",
    commercial:
      "Versus Element kitchen-led ES hotels, Marriott Executive Apartments sell longer-stay apartment living; versus Protea regional hotels, MEA is apartment-first inside Bonvoy.",
    scenarios: [
      ["Urban Serviced Apartment Corridor", "Cities funding MEA apartment stays with Bonvoy."],
      ["Long-Stay Corporate Housing", "Markets needing upscale apartment inventory."],
      ["Conversion Into MEA", "Assets clearing apartment PIP and Bonvoy delivery."],
    ],
    demand: [
      ["Extended-Stay Corporate", S, "MEA is built for upscale serviced apartments with Bonvoy."],
      ["Suburban Business & Leisure", M, "Suburban apartment corridors can fit."],
      ["Conversion & Repositioning", M, "Conversions succeed at apartment capital."],
      ["Luxury Urban Flagship", L, "True luxury hotels sit above MEA apartments."],
      ["Meetings & Group Compression", L, "Large meetings are not MEA's core."],
      ["Highway & Interstate Transient", N, "Highway transient sits outside MEA."],
    ],
    ops: "Marriott Executive Apartments — upscale serviced apartments with Bonvoy. NEW Brand Basics create requires founder auth. Apartment housekeeping ≠ transient hotel economics.",
    footprint: "Confirm live MEA counts from current Marriott disclosures—selective global apartment footprint.",
    insight: "MEA fits upscale apartment sponsors with Bonvoy; fails when Element hotel ES or Four Points Flex transient fees are assumed.",
  },
  // Wyndham
  {
    id: "NEW_echo-suites",
    name: "ECHO Suites Extended Stay by Wyndham",
    live: "ECHO Suites Extended Stay by Wyndham",
    slug: "echo-suites-extended-stay-by-wyndham",
    parent: "Wyndham Hotels & Resorts",
    loyalty: "Wyndham Rewards",
    proof: "WYNDHAM_PROOF",
    short: "ECHO Suites",
    researchClass: "NEW_BRAND_CANDIDATE",
    feeGeo: "Confirm current Wyndham FDD / development disclosure for ECHO Suites Extended Stay. Do not invent mature operating metrics for a newer brand.",
    brandPage: "https://www.wyndhamhotels.com/echo-suites",
    positioning: "New Midscale Extended Stay",
    commercial:
      "Versus WaterWalk premium ES, ECHO Suites sells new-build midscale extended stay; versus Wyndham Alltra AI resorts, ECHO is kitchen-led ES inside Wyndham Rewards.",
    scenarios: [
      ["New-Build Midscale ES Corridor", "US corridors sized for ECHO new-build ES economics."],
      ["Suburban Corporate ES", "Markets funding midscale kitchens with Wyndham Rewards."],
      ["Conversion Caution", "Prefer new-build / purpose-built; conversions only when standards clear."],
    ],
    demand: [
      ["Extended-Stay Corporate", S, "ECHO Suites is built for midscale extended stay."],
      ["Suburban Business & Leisure", S, "Suburban ES corridors fit ECHO."],
      ["Highway & Interstate Transient", M, "Interstate ES corridors can fit."],
      ["Conversion & Repositioning", L, "Brand is new-build biased—conversions need proof."],
      ["Meetings & Group Compression", N, "Large meetings sit outside midscale ES."],
      ["Luxury Urban Flagship", N, "Luxury flags sit outside ECHO."],
    ],
    ops: "Wyndham ECHO Suites — newer midscale extended-stay franchise with Wyndham Rewards. Do not invent mature hotel counts or fee percentages. NEW Basics create requires founder auth.",
    footprint: "Confirm live ECHO Suites counts from current Wyndham disclosures—early-stage US growth; do not invent mature footprint.",
    insight: "ECHO Suites fits new midscale ES sponsors with Wyndham Rewards; fails when WaterWalk premium or Alltra AI economics are assumed. Newer-brand maturity caution.",
  },
  {
    id: "NEW_waterwalk",
    name: "WaterWalk Extended Stay by Wyndham",
    live: "WaterWalk Extended Stay by Wyndham",
    slug: "waterwalk-extended-stay-by-wyndham",
    parent: "Wyndham Hotels & Resorts",
    loyalty: "Wyndham Rewards",
    proof: "WYNDHAM_PROOF",
    short: "WaterWalk",
    researchClass: "NEW_BRAND_CANDIDATE",
    feeGeo: "Confirm current Wyndham FDD / development disclosure for WaterWalk Extended Stay. Do not invent mature operating metrics for a newer brand.",
    brandPage: "https://www.wyndhamhotels.com/waterwalk",
    positioning: "Upscale Extended Stay",
    commercial:
      "Versus ECHO Suites midscale ES, WaterWalk sells higher upscale ES product; versus Alltra AI resorts, WaterWalk is kitchen-led extended stay inside Wyndham Rewards.",
    scenarios: [
      ["Upscale Corporate ES Corridor", "Markets funding WaterWalk upscale kitchens and Rewards."],
      ["Urban / Suburban Upscale ES", "Sites where WaterWalk ADR clears versus ECHO."],
      ["Conversion / New-Build WaterWalk", "Assets clearing WaterWalk upscale ES standards."],
    ],
    demand: [
      ["Extended-Stay Corporate", S, "WaterWalk is built for upscale extended stay."],
      ["Suburban Business & Leisure", S, "Suburban upscale ES corridors fit WaterWalk."],
      ["Conversion & Repositioning", M, "Conversions succeed when upscale ES PIP clears."],
      ["Airport & Transportation", M, "Airport-adjacent upscale ES can fit."],
      ["Highway & Interstate Transient", L, "Pure highway midscale sits closer to ECHO."],
      ["Luxury Urban Flagship", N, "Luxury hotels sit outside WaterWalk ES."],
    ],
    ops: "Wyndham WaterWalk — newer upscale extended-stay franchise with Wyndham Rewards. Do not invent mature counts or royalties. NEW Basics create requires founder auth.",
    footprint: "Confirm live WaterWalk counts from current Wyndham disclosures—early-stage growth; do not invent mature footprint.",
    insight: "WaterWalk fits upscale ES sponsors with Wyndham Rewards; fails when ECHO midscale or Alltra AI fees are assumed. Newer-brand maturity caution.",
  },
  {
    id: "NEW_wyndham-alltra",
    name: "Wyndham Alltra",
    live: "Wyndham Alltra",
    slug: "wyndham-alltra",
    parent: "Wyndham Hotels & Resorts",
    loyalty: "Wyndham Rewards",
    proof: "WYNDHAM_PROOF",
    short: "Wyndham Alltra",
    researchClass: "NEW_BRAND_CANDIDATE",
    feeGeo: "Confirm current Wyndham disclosure for Alltra all-inclusive resorts—AI exhibits, not transient hotel royalty tables",
    brandPage: "https://www.wyndhamhotels.com/alltra",
    positioning: "All-Inclusive Resorts",
    commercial:
      "Versus ECHO / WaterWalk extended stay, Alltra sells all-inclusive resort economics; versus Hyatt Inclusive Collection peers, Alltra is Wyndham Rewards AI distribution.",
    scenarios: [
      ["All-Inclusive Leisure Destination", "CALA / leisure destinations funding Alltra AI product."],
      ["Family / Couples AI Mix", "Markets where Alltra AI package ADR clears IC."],
      ["Conversion Into Alltra AI", "Resorts that can fund AI F&B and amenity intensity."],
    ],
    demand: [
      ["Resort & Destination Leisure", S, "Alltra is built for all-inclusive leisure destinations."],
      ["Conversion & Repositioning", M, "Conversions succeed only with true AI capital and ops."],
      ["Meetings & Group Compression", M, "Groups / weddings can fit when AI plant supports."],
      ["Extended-Stay Corporate", N, "Corporate ES kitchens sit outside Alltra AI."],
      ["Suburban Business & Leisure", N, "Suburban transient hotels sit outside Alltra."],
      ["Highway & Interstate Transient", N, "Highway transient sits outside Alltra."],
    ],
    ops: "Wyndham Alltra — all-inclusive resorts with Wyndham Rewards. AI F&B / labor intensity ≠ transient ES franchise tables. NEW Basics create requires founder auth. Do not invent mature counts.",
    footprint: "Confirm live Alltra counts from current Wyndham disclosures—CALA / leisure weighted early footprint.",
    insight: "Alltra fits AI resort sponsors with Wyndham Rewards; fails when ECHO / WaterWalk ES economics are assumed. Adjust underwriting for all-inclusive ops.",
  },
];

function esc(s) {
  return String(s).replace(/\\/g, "\\\\").replace(/`/g, "\\`").replace(/\$/g, "\\$");
}

function demandFit(code) {
  if (code === "S") return "S";
  if (code === "M") return "M";
  if (code === "L") return "L";
  return "N";
}

const lines = [];
lines.push(`/**`);
lines.push(` * Factory 2.0 Batch 14 — Accor/partner + Marriott + Wyndham.`);
lines.push(` * NEW_BRAND_CANDIDATE packs use provisional NEW_<slug> keys — never invent Airtable rec IDs.`);
lines.push(` * Partner brands (Banyan Tree / Angsana) ≠ Accor-owned fee tables. Newer brands: no invented mature metrics.`);
lines.push(` * All-inclusive (Alltra) economics ≠ transient extended-stay franchise tables.`);
lines.push(` */`);
lines.push(`import { DEMAND_FIT_LABELS } from "../be-factory-permanent-demand-scenarios-six-v1.js";`);
lines.push(`import { ACCOR_PROOF, BONVOY_PROOF, WYNDHAM_PROOF } from "./be-factory-2-depth-loyalty-proofs-v1.js";`);
lines.push(``);
lines.push(`function pack(base) {`);
lines.push(`  return Object.freeze({ researchSpendUsd: 0, paidCalls: 0, loyaltyProofRequired: 6, batchTag: "batch-14", ...base });`);
lines.push(`}`);
lines.push(`function demand(rows) { return rows.map((r) => ({ title: r.title, fit: r.fit, explanation: r.explanation })); }`);
lines.push(`function gallery(brand, url) {`);
lines.push(`  return [`);
lines.push(`    { title: \`\${brand} — Exterior / Arrival\`, body: "Exterior presence · confirm property photography before publication", source: url },`);
lines.push(`    { title: \`\${brand} — Guest Room / Suite\`, body: "Guest-room, suite, or apartment product typical of this ladder rung", source: url },`);
lines.push(`    { title: \`\${brand} — Lobby / Public Space\`, body: "Public space supporting the brand promise", source: url },`);
lines.push(`    { title: \`\${brand} — F&B / Amenity Cue\`, body: "F&B, spa, kitchen, or amenity cues where required by standards", source: url },`);
lines.push(`    { title: \`\${brand} — Meetings / Pool / Flexibility Cue\`, body: "Meetings, pool, apartment, or AI amenity cues typical of this product", source: url },`);
lines.push(`    { title: \`\${brand} — Context / Setting\`, body: "Urban, resort, destination, or corridor context where typical", source: url },`);
lines.push(`  ];`);
lines.push(`}`);
lines.push(`export const FACTORY_2_BATCH_14_V1 = "FACTORY_2_BATCH_14_V1";`);
lines.push(`const FDD = "Confirm current franchise disclosure / FDD / partnership exhibits for application, royalty, marketing, loyalty, and PIP. Distinguish historical figures from the live agreement. Amounts are not staged without a verified disclosure. For partner-affiliated brands, confirm partner management terms—do not import Accor-owned franchise tables blindly. For all-inclusive / newer brands, do not invent mature operating metrics.";`);
lines.push(`const S = DEMAND_FIT_LABELS.STRONG_FIT;`);
lines.push(`const M = DEMAND_FIT_LABELS.MODERATE_FIT;`);
lines.push(`const L = DEMAND_FIT_LABELS.LIMITED_FIT;`);
lines.push(`const N = DEMAND_FIT_LABELS.NOT_A_FIT;`);
lines.push(`export const BATCH_14_IDS = Object.freeze([`);
for (const b of brands) lines.push(`  "${b.id}", // ${b.name}`);
lines.push(`]);`);
lines.push(`export const BATCH_14_PACKS_BY_ID = Object.freeze({`);

for (const b of brands) {
  const researchClass = b.researchClass || "EXISTING_CANONICAL";
  lines.push(`  "${b.id}": pack({`);
  lines.push(`    brandId: "${b.id}",`);
  lines.push(`    brandName: ${JSON.stringify(b.name)},`);
  lines.push(`    liveBrandName: ${JSON.stringify(b.live)},`);
  lines.push(`    slug: "${b.slug}",`);
  lines.push(`    parentCompany: ${JSON.stringify(b.parent)},`);
  lines.push(`    loyaltyProgram: ${JSON.stringify(b.loyalty)},`);
  lines.push(`    researchClass: ${JSON.stringify(researchClass)},`);
  lines.push(`    feeGeography: ${JSON.stringify(b.feeGeo)},`);
  lines.push(`    sources: { brandPage: ${JSON.stringify(b.brandPage)} },`);
  lines.push(`    positioning: ${JSON.stringify(b.positioning)},`);
  lines.push(`    commercialDifferentiator: ${JSON.stringify(b.commercial)},`);
  lines.push(`    scenarios: [`);
  for (const [t, body] of b.scenarios) lines.push(`      { title: ${JSON.stringify(t)}, body: ${JSON.stringify(body)} },`);
  lines.push(`    ],`);
  lines.push(`    demandScenarios: demand([`);
  for (const [t, fit, expl] of b.demand) lines.push(`      { title: ${JSON.stringify(t)}, fit: ${demandFit(fit)}, explanation: ${JSON.stringify(expl)} },`);
  lines.push(`    ]),`);
  lines.push(`    loyaltyProof: ${b.proof}(${JSON.stringify(b.short)}),`);
  lines.push(`    feeNote: \`${esc(b.short)} application, royalty, marketing, loyalty, and PIP — \${FDD}\`,`);
  lines.push(`    operatingModel: ${JSON.stringify(b.ops)},`);
  lines.push(`    footprint: ${JSON.stringify(b.footprint)},`);
  lines.push(`    insight: ${JSON.stringify(b.insight)},`);
  lines.push(`    standards: [`);
  lines.push(`      { title: "Brand Product Standards", body: "Rooms / suites / apartments / resort product must clear ${esc(b.short)} standards for the operating region." },`);
  lines.push(`      { title: "Conversion / PIP Discipline", body: "PIP or partnership quality scope must match the brand promise—not under-scoped capital." },`);
  lines.push(`      { title: "Loyalty / Network Integration", body: "Confirm ${esc(b.loyalty)} delivery obligations for this brand before IC." },`);
  lines.push(`    ],`);
  lines.push(`    valueOwnerScenarios: [`);
  lines.push(`      { title: "Primary Owner Corridor", body: "Sites where ${esc(b.short)} ADR / contribution clears IC versus local comps." },`);
  lines.push(`      { title: "Conversion / Affiliation Path", body: "Assets where ${esc(b.short)} affiliation clears IC versus sibling alternatives." },`);
  lines.push(`      { title: "Disclosure Fee Gate", body: "Model only from current disclosure / partnership / AI exhibits for ${esc(b.short)}." },`);
  lines.push(`      { title: "Sibling Clarity", body: "Diligence versus sibling flags / partner networks before IC." },`);
  lines.push(`    ],`);
  lines.push(`    gallery: gallery(${JSON.stringify(b.short)}, ${JSON.stringify(b.brandPage)}),`);
  lines.push(`  }),`);
  lines.push(``);
}
lines.push(`});`);
lines.push(`export function listBatch14() { return BATCH_14_IDS.map((id) => BATCH_14_PACKS_BY_ID[id]).filter(Boolean); }`);
lines.push(`export function getBatch14Pack(brandId) { return BATCH_14_PACKS_BY_ID[brandId] || null; }`);
lines.push(``);

writeFileSync(outPath, lines.join("\n"));
console.log(`Wrote ${outPath} (${brands.length} packs)`);
