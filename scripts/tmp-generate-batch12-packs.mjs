#!/usr/bin/env node
/** One-shot generator for Batch 12 depth packs (Hilton / Accor-Ennismore / Choice). */
import { writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");
const outPath = join(ROOT, "lib/brand-explorer/factory-2/be-factory-2-depth-research-packs-batch12-v1.js");

const S = "S";
const M = "M";
const L = "L";
const N = "N";

/** @type {Array<object>} */
const brands = [
  {
    id: "recQxUKKgH0Pq1VgR",
    name: "Graduate by Hilton",
    live: "Graduate Hotels",
    slug: "graduate-by-hilton",
    parent: "Hilton Worldwide",
    loyalty: "Hilton Honors",
    proof: "HONORS_PROOF",
    short: "Graduate",
    feeGeo: "Confirm current Hilton franchise / affiliation disclosure for Graduate by Hilton (college-town lifestyle)",
    brandPage: "https://www.hilton.com/en/hotels/brands/graduate-by-hilton/",
    positioning: "College Town Lifestyle",
    commercial:
      "Versus Canopy neighborhood lifestyle, Graduate sells campus-town lifestyle identity; versus Embassy Suites, it is lifestyle transient rather than suite product inside Hilton Honors.",
    scenarios: [
      ["College-Town Lifestyle Urban", "Campus-adjacent markets where Graduate identity and Honors clear lifestyle ADR."],
      ["Adaptive Reuse Campus Corridors", "Conversions that can fund Graduate design narrative near universities."],
      ["Sibling Clarity Versus Canopy", "Owners comparing Graduate campus lifestyle versus Canopy neighborhood lifestyle."],
    ],
    demand: [
      ["Urban Lifestyle & Design", S, "Graduate core is college-town lifestyle with Hilton Honors."],
      ["University & Medical", S, "Campus and academic demand is a primary Graduate pocket."],
      ["Conversion & Repositioning", M, "Conversions succeed when design PIP clears Graduate standards."],
      ["Suburban Business & Leisure", L, "Suburban select-service economics usually miss Graduate lifestyle intensity."],
      ["Meetings & Group Compression", L, "Large meetings plant is Embassy / Hilton full-service territory."],
      ["Highway & Interstate Transient", N, "Highway limited-service transient sits outside Graduate lifestyle."],
    ],
    ops: "Hilton lifestyle brand focused on college towns with Hilton Honors. Confirm franchise/affiliation path—do not invent royalty percentages. Preserve live name Graduate Hotels.",
    footprint: "Confirm live hotel counts from current Hilton / Graduate disclosures—campus-town footprint; do not underwrite from launch media alone.",
    insight: "Graduate fits campus-town lifestyle assets with Honors; fails when Embassy suite economics or Canopy neighborhood fees are blindly assumed.",
  },
  {
    id: "NEW_outset-collection",
    name: "Outset Collection by Hilton",
    live: "Outset Collection by Hilton",
    slug: "outset-collection-by-hilton",
    parent: "Hilton Worldwide",
    loyalty: "Hilton Honors",
    proof: "HONORS_PROOF",
    short: "Outset Collection",
    feeGeo: "Confirm current Hilton soft-collection / affiliation exhibits for Outset AFTER Brand Basics create",
    brandPage: "https://www.hilton.com/",
    positioning: "Independent Collection",
    commercial:
      "Versus Tapestry / Curio soft collections, Outset (if a distinct Hilton soft lane) needs verified ladder position before IC; stage research only—do not invent fee tables.",
    scenarios: [
      ["Soft-Collection Diligence Gate", "Sites where sponsors must confirm Outset versus Curio / Tapestry before assuming a distinct owner use case."],
      ["Independent Character Conversion", "Distinctive independents needing Honors—validate brand standards after Basics create."],
      ["Conversion Into Outset", "Conversions only after authorized Brand Basics create and current disclosure exhibits."],
    ],
    demand: [
      ["Conversion & Repositioning", M, "Provisional: soft-collection conversions may fit—confirm after Basics create."],
      ["Urban Lifestyle & Design", M, "Provisional: design-led independents may fit Outset standards."],
      ["Resort & Destination Leisure", M, "Provisional: destination character hotels may fit when curation holds."],
      ["Suburban Business & Leisure", L, "Suburban select-service economics usually sit below soft-collection upscale."],
      ["Airport & Transportation", L, "Airport hard-prototype is not the soft-collection lane."],
      ["Highway & Interstate Transient", N, "Highway transient sits outside independent collection."],
    ],
    ops: "Staged NEW_BRAND_CANDIDATE — no live Brand Basics match for Outset Collection by Hilton. Do not invent rec ID. Soft-collection diligence after authorized create.",
    footprint: "No Airtable inventory yet. Confirm existence and counts from Hilton disclosures before underwriting.",
    insight: "Outset Collection is NEW_BRAND_CANDIDATE—founder must authorize Basics create only after brand existence and ladder position are verified versus Curio / Tapestry.",
    researchClass: "NEW_BRAND_CANDIDATE",
  },
  {
    id: "recVAnsCsz4ifaAw2",
    name: "LivSmart Studios by Hilton",
    live: "LivSmart Studios by Hilton",
    slug: "livsmart-studios-by-hilton",
    parent: "Hilton Worldwide",
    loyalty: "Hilton Honors",
    proof: "HONORS_PROOF",
    short: "LivSmart Studios",
    feeGeo: "Confirm current Hilton franchise / development disclosure for LivSmart Studios (extended-stay)",
    brandPage: "https://www.hilton.com/en/hotels/brands/livsmart-studios-by-hilton/",
    positioning: "Midscale Extended-Stay",
    commercial:
      "Versus Homewood / Home2 extended-stay, LivSmart is Hilton's newer extended-stay ladder rung; versus Embassy Suites, it is length-of-stay kitchenette economics inside Hilton Honors.",
    scenarios: [
      ["Extended-Stay Corporate", "Markets with multi-week demand needing kitchenettes with Hilton Honors."],
      ["Project / Medical Longer Stay", "Project and medical corridors needing suite product."],
      ["New-Build Extended-Stay Prototype", "New-builds optimized for LivSmart prototypes—confirm disclosure exhibits."],
    ],
    demand: [
      ["Extended-Stay Corporate", S, "LivSmart thesis is extended-stay with kitchens and Hilton Honors."],
      ["Project & Workforce Demand", S, "Project/workforce weekly stays are a primary extended-stay pocket."],
      ["University & Medical", M, "Medical/university longer stays can fit suite kitchenette product."],
      ["Suburban Business & Leisure", M, "Suburban midweek extended-stay works; pure weekend leisure is secondary."],
      ["Highway & Interstate Transient", L, "One-night highway transient is Hampton / Hilton Garden territory."],
      ["Luxury Urban Flagship", N, "Luxury urban flagship sits outside LivSmart extended-stay."],
    ],
    ops: "Hilton midscale extended-stay with kitchenettes and Hilton Honors. Housekeeping cadence differs from transient—confirm FDD exhibits.",
    footprint: "Confirm live hotel counts from current Hilton / LivSmart disclosures—growth brand; do not underwrite from launch media alone.",
    insight: "LivSmart fits sponsors with genuine extended-stay demand and suite capital; fails when underwritten like Embassy Suites transient without kitchen PIP.",
  },
  {
    id: "recjvESTY8tYZjlAJ",
    name: "Embassy Suites by Hilton",
    live: "Embassy Suites by Hilton",
    slug: "embassy-suites-by-hilton",
    parent: "Hilton Worldwide",
    loyalty: "Hilton Honors",
    proof: "HONORS_PROOF",
    short: "Embassy Suites",
    feeGeo: "Confirm current Hilton franchise disclosure for Embassy Suites (all-suite upscale)",
    brandPage: "https://www.hilton.com/en/embassy-suites/",
    positioning: "Upscale All-Suite",
    commercial:
      "Versus Hilton Hotels full-service, Embassy sells all-suite product with atrium / cooked-to-order breakfast cues; versus LivSmart extended-stay, it is suite transient rather than kitchenette weekly stay inside Hilton Honors.",
    scenarios: [
      ["Upscale All-Suite Urban / Suburban", "Markets where suite product and Honors clear upscale ADR."],
      ["Meetings-Capable Suite Hotels", "Assets with atrium / meetings plant that can fund Embassy standards."],
      ["Conversion Into Embassy Suites", "Conversions that can deliver suite and atrium PIP."],
    ],
    demand: [
      ["Suburban Business & Leisure", S, "Embassy core includes suburban upscale suite business/leisure with Honors."],
      ["Meetings & Group Compression", S, "Meetings-capable atrium plant is an Embassy strength when present."],
      ["Airport & Transportation", S, "Airport suite hubs fit when product and Honors defend share."],
      ["Conversion & Repositioning", M, "Conversions succeed when PIP funds suites and public space."],
      ["Extended-Stay Corporate", L, "True weekly kitchenette extended-stay is LivSmart / Homewood territory."],
      ["Highway & Interstate Transient", N, "Highway limited-service transient sits outside Embassy upscale suites."],
    ],
    ops: "Hilton upscale all-suite with cooked-to-order breakfast / evening reception cues and Hilton Honors. Confirm franchise exhibits—do not invent royalty percentages.",
    footprint: "Confirm live hotel counts from current Hilton / Embassy Suites disclosures—large North America footprint with international presence.",
    insight: "Embassy Suites fits owners funding upscale suite product with Honors; fails when LivSmart extended-stay or select-service capital is assumed.",
  },
  {
    id: "recqsodgJLUzsdCED",
    name: "NoMad Hotels",
    live: "NoMad",
    slug: "nomad-hotels",
    parent: "Hilton Worldwide",
    loyalty: "Hilton Honors",
    proof: "HONORS_PROOF",
    short: "NoMad",
    feeGeo: "Confirm current Hilton / NoMad affiliation disclosure (luxury lifestyle soft brand)",
    brandPage: "https://www.hilton.com/en/hotels/brands/nomad/",
    positioning: "Luxury Lifestyle",
    commercial:
      "Versus Waldorf / Conrad hard luxury, NoMad sells lifestyle luxury individuality; versus Curio soft collection, NoMad is a defined lifestyle luxury node with Hilton Honors—confirm affiliation path carefully.",
    scenarios: [
      ["Luxury Lifestyle Urban", "Gateway lifestyle districts where NoMad design and Honors clear luxury ADR."],
      ["Destination Design Luxury", "Destination assets that can fund design authorship and F&B character."],
      ["Affiliation Into NoMad", "Affiliations that can meet NoMad luxury lifestyle standards."],
    ],
    demand: [
      ["Urban Lifestyle & Design", S, "NoMad core is luxury lifestyle urban design with Hilton Honors."],
      ["Luxury Urban Flagship", S, "Luxury flagship lifestyle fits when product clears NoMad standards."],
      ["Conversion & Repositioning", M, "Conversions succeed only when luxury lifestyle PIP is funded."],
      ["Resort & Destination Leisure", M, "Select destination resorts can work when design authorship is credible."],
      ["Suburban Business & Leisure", L, "Suburban select-service economics sit outside NoMad luxury lifestyle."],
      ["Highway & Interstate Transient", N, "Highway transient sits outside luxury lifestyle."],
    ],
    ops: "Hilton luxury lifestyle brand (live name: NoMad). Luxury-band capital and operating intensity—do not import select-service economics. Confirm Honors delivery and affiliation exhibits.",
    footprint: "Confirm live hotel counts from current Hilton / NoMad disclosures—select curated footprint.",
    insight: "NoMad fits sponsors who can fund luxury lifestyle delivery with Honors; wrong when capital is upper-upscale only. Preserve live name NoMad.",
  },
  {
    id: "recF9TQzTd3wwEiKa",
    name: "ibis Styles",
    live: "ibis Styles",
    slug: "ibis-styles",
    parent: "Accor",
    loyalty: "ALL - Accor Live Limitless",
    proof: "ACCOR_PROOF",
    short: "ibis Styles",
    feeGeo: "Confirm current Accor franchise / development disclosure for ibis Styles (design midscale)",
    brandPage: "https://all.accor.com/brands/ibis-styles.en.shtml",
    positioning: "Design Midscale",
    commercial:
      "Versus classic ibis economy, ibis Styles sells design-led midscale; versus Mercure fuller midscale, Styles keeps leaner capital with ALL - Accor Live Limitless.",
    scenarios: [
      ["Design Midscale Urban / Corridor", "Markets where design midscale and ALL clear ADR without Mercure capital."],
      ["Conversion Into ibis Styles", "Conversions that can fund Styles design standards."],
      ["Sibling Clarity Versus ibis / ibis budget", "Owners comparing Styles versus classic ibis and ibis budget."],
    ],
    demand: [
      ["Urban Lifestyle & Design", S, "ibis Styles core is design-led midscale with ALL."],
      ["Suburban Business & Leisure", S, "Suburban midscale mix fits when product clears Styles standards."],
      ["Airport & Transportation", M, "Airport-edge design midscale can fit Styles."],
      ["Conversion & Repositioning", M, "Conversions succeed when design PIP clears Styles—not economy-only scopes."],
      ["Highway & Interstate Transient", L, "Hard economy highway transient is ibis budget / hotelF1 territory."],
      ["Luxury Urban Flagship", N, "Luxury flagship sits outside ibis Styles midscale."],
    ],
    ops: "Accor design midscale limited-service with ALL. Confirm ibis family exhibits—do not invent royalty percentages or confuse with classic ibis / ibis budget.",
    footprint: "Confirm live hotel counts from current Accor / ibis Styles disclosures across EMEA, APAC, and Americas.",
    insight: "ibis Styles fits design midscale with ALL; fails when classic ibis economy or Mercure midscale fees are assumed.",
  },
  {
    id: "rec0lQ4Imf614F8jo",
    name: "ibis budget",
    live: "ibis budget",
    slug: "ibis-budget",
    parent: "Accor",
    loyalty: "ALL - Accor Live Limitless",
    proof: "ACCOR_PROOF",
    short: "ibis budget",
    feeGeo: "Confirm current Accor franchise / development disclosure for ibis budget (economy / budget)",
    brandPage: "https://all.accor.com/brands/ibis-budget.en.shtml",
    positioning: "Budget Economy",
    commercial:
      "Versus classic ibis, ibis budget is leaner budget economy; versus hotelF1 ultra-budget, it sits above the hardest Accor budget rung with ALL participation.",
    scenarios: [
      ["Budget Economy Corridor", "Highway and value corridors needing Accor budget product and ALL."],
      ["Airport & Gateway Value", "Airport-edge value stays without midscale capital."],
      ["Conversion Into ibis budget", "Conversions that can fund budget standards—not Styles design scopes."],
    ],
    demand: [
      ["Highway & Interstate Transient", S, "ibis budget is built for value transient corridors with ALL."],
      ["Regional & Secondary Markets", S, "Secondary markets are a core budget economy use case."],
      ["Airport & Transportation", S, "Airport-edge budget stays fit when product clears ibis budget standards."],
      ["Conversion & Repositioning", M, "Conversions succeed when PIP funds budget—not midscale Styles capital."],
      ["Suburban Business & Leisure", M, "Suburban value mix can work at budget ADR."],
      ["Luxury Urban Flagship", N, "Luxury flagship sits outside ibis budget."],
    ],
    ops: "Accor budget economy limited-service with lean operations and ALL. Confirm exhibits—do not invent royalty percentages.",
    footprint: "Confirm live hotel counts from current Accor / ibis budget disclosures.",
    insight: "ibis budget fits Accor budget economy with ALL; fails when Styles design or Mercure midscale fees are assumed.",
  },
  {
    id: "recGJctYTmJiwwXKG",
    name: "greet",
    live: "Greet",
    slug: "greet",
    parent: "Accor",
    loyalty: "ALL - Accor Live Limitless",
    proof: "ACCOR_PROOF",
    short: "greet",
    feeGeo: "Confirm current Accor franchise / development disclosure for greet (eco midscale)",
    brandPage: "https://all.accor.com/brands/greet.en.shtml",
    positioning: "Eco Midscale",
    commercial:
      "Versus ibis Styles design midscale, greet sells eco / responsible midscale cues; versus Mercure, it keeps leaner midscale capital with ALL.",
    scenarios: [
      ["Eco Midscale Urban / Corridor", "Markets where responsible midscale positioning and ALL clear ADR."],
      ["Conversion Into greet", "Conversions that can fund greet eco product standards."],
      ["Sibling Clarity Versus ibis Styles", "Owners comparing greet versus Styles design midscale."],
    ],
    demand: [
      ["Suburban Business & Leisure", S, "greet fits midscale business/leisure with eco cues and ALL."],
      ["Conversion & Repositioning", S, "Conversions are a core greet use case when eco standards clear."],
      ["Regional & Secondary Markets", M, "Secondary markets fit when ADR supports greet fees."],
      ["Airport & Transportation", M, "Airport-edge midscale can fit greet."],
      ["Urban Lifestyle & Design", L, "Pure lifestyle design intensity is more Styles / TRIBE."],
      ["Luxury Urban Flagship", N, "Luxury flagship sits outside greet midscale."],
    ],
    ops: "Accor eco / responsible midscale with ALL. Confirm franchise exhibits—do not invent royalty percentages. Preserve live name Greet.",
    footprint: "Confirm live hotel counts from current Accor / greet disclosures.",
    insight: "greet fits owners funding eco midscale with ALL; fails when Styles or Mercure fees are assumed.",
  },
  {
    id: "rec9Q89yzK4QZKnal",
    name: "hotelF1",
    live: "hotelF1",
    slug: "hotelf1",
    parent: "Accor",
    loyalty: "ALL - Accor Live Limitless",
    proof: "ACCOR_PROOF",
    short: "hotelF1",
    feeGeo: "Confirm current Accor franchise / development disclosure for hotelF1 (ultra-budget)",
    brandPage: "https://all.accor.com/brands/hotelf1.en.shtml",
    positioning: "Ultra Budget",
    commercial:
      "Versus ibis budget, hotelF1 is Accor's hardest ultra-budget rung; versus classic ibis, it is deliberately leaner capital and product with ALL participation where applicable.",
    scenarios: [
      ["Ultra-Budget Corridor", "Value corridors needing Accor ultra-budget product."],
      ["Highway Transient Value", "Highway sites optimized for ultra-budget ADR."],
      ["Conversion Into hotelF1", "Conversions that can fund ultra-budget standards only."],
    ],
    demand: [
      ["Highway & Interstate Transient", S, "hotelF1 is built for ultra-budget transient corridors."],
      ["Regional & Secondary Markets", S, "Secondary markets needing Accor ultra-budget."],
      ["Airport & Transportation", M, "Airport-edge ultra-budget can fit when product clears hotelF1."],
      ["Conversion & Repositioning", M, "Conversions succeed only with ultra-budget capital scopes."],
      ["Suburban Business & Leisure", L, "Suburban midscale mix usually sits above hotelF1."],
      ["Luxury Urban Flagship", N, "Luxury flagship sits outside hotelF1."],
    ],
    ops: "Accor ultra-budget limited-service with leanest Accor operating stack and ALL where delivered. Confirm exhibits—do not invent royalty percentages.",
    footprint: "Confirm live hotel counts from current Accor / hotelF1 disclosures—primarily EMEA.",
    insight: "hotelF1 fits ultra-budget Accor economics; fails when ibis budget or midscale fees are assumed.",
  },
  {
    id: "rechyrQmHmeR1ovme",
    name: "TRIBE",
    live: "TRIBE",
    slug: "tribe",
    parent: "Accor",
    loyalty: "ALL - Accor Live Limitless",
    proof: "ACCOR_PROOF",
    short: "TRIBE",
    feeGeo: "Confirm current Accor franchise / development disclosure for TRIBE (upscale lifestyle)",
    brandPage: "https://all.accor.com/brands/tribe.en.shtml",
    positioning: "Upscale Lifestyle",
    commercial:
      "Versus ibis Styles midscale design, TRIBE sells upscale lifestyle; versus SO/ Accor lifestyle, confirm sibling clarity on upscale lifestyle intensity inside ALL.",
    scenarios: [
      ["Upscale Lifestyle Urban", "City sites where TRIBE lifestyle and ALL lift rates."],
      ["Conversion Into TRIBE", "Conversions that can fund lifestyle public-space standards."],
      ["Sibling Clarity Versus SO/ / Mama Shelter", "Owners comparing TRIBE versus other Accor lifestyle lanes."],
    ],
    demand: [
      ["Urban Lifestyle & Design", S, "TRIBE core is upscale urban lifestyle with ALL."],
      ["Conversion & Repositioning", M, "Conversions succeed when design PIP clears lifestyle standards."],
      ["Suburban Business & Leisure", L, "Suburban select-service economics usually miss TRIBE lifestyle intensity."],
      ["Airport & Transportation", L, "Airport hard-prototype is not TRIBE's core lane."],
      ["Meetings & Group Compression", L, "Large meetings compression is Pullman / Novotel territory."],
      ["Highway & Interstate Transient", N, "Highway transient sits outside upscale lifestyle."],
    ],
    ops: "Accor upscale lifestyle brand with design-forward public space and ALL. Confirm franchise exhibits—do not invent royalty percentages.",
    footprint: "Confirm live hotel counts from current Accor / TRIBE disclosures.",
    insight: "TRIBE fits upscale lifestyle with ALL; fails when midscale Styles or ultra-budget fees are assumed.",
  },
  {
    id: "reck9Gq60El13HG5Z",
    name: "Adagio",
    live: "Adagio Aparthotel",
    slug: "adagio",
    parent: "Accor",
    loyalty: "ALL - Accor Live Limitless",
    proof: "ACCOR_PROOF",
    short: "Adagio",
    feeGeo: "Confirm current Accor franchise / development disclosure for Adagio (aparthotel midscale+)",
    brandPage: "https://all.accor.com/brands/adagio.en.shtml",
    positioning: "Aparthotel Midscale",
    commercial:
      "Versus Adagio Access leaner aparthotel, Adagio sells fuller aparthotel product; versus Novotel transient midscale, it is length-of-stay apartment economics with ALL.",
    scenarios: [
      ["Aparthotel Extended Stay", "Urban markets with multi-night and weekly demand needing kitchens."],
      ["Urban Aparthotel Conversion", "Conversions that can fund Adagio apartment standards."],
      ["Sibling Clarity Versus Adagio Access", "Owners comparing Adagio versus Access ladder."],
    ],
    demand: [
      ["Extended-Stay Corporate", S, "Adagio core is aparthotel extended-stay with ALL."],
      ["Urban Lifestyle & Design", M, "Urban aparthotel can carry lifestyle cues when product clears Adagio."],
      ["Suburban Business & Leisure", M, "Suburban midweek extended-stay works when kitchens are real."],
      ["Conversion & Repositioning", M, "Conversions succeed when apartment PIP is funded."],
      ["Highway & Interstate Transient", L, "One-night highway transient is ibis territory."],
      ["Luxury Urban Flagship", N, "Luxury flagship sits outside Adagio aparthotel."],
    ],
    ops: "Accor aparthotel (live: Adagio Aparthotel) with kitchenette product and ALL. Distinct from transient midscale—confirm exhibits.",
    footprint: "Confirm live hotel counts from current Accor / Adagio disclosures across EMEA and beyond.",
    insight: "Adagio fits aparthotel demand with ALL; fails when Adagio Access lean capital or Novotel transient fees are assumed. Preserve live name Adagio Aparthotel.",
  },
  {
    id: "recSAVQWVAnn0NKYg",
    name: "Adagio Access",
    live: "Aparthotel Adagio Access",
    slug: "adagio-access",
    parent: "Accor",
    loyalty: "ALL - Accor Live Limitless",
    proof: "ACCOR_PROOF",
    short: "Adagio Access",
    feeGeo: "Confirm current Accor franchise / development disclosure for Adagio Access (economy aparthotel)",
    brandPage: "https://all.accor.com/brands/adagio-access.en.shtml",
    positioning: "Economy Aparthotel",
    commercial:
      "Versus Adagio fuller aparthotel, Access is leaner economy aparthotel; versus ibis budget transient, it is length-of-stay kitchenette economics with ALL.",
    scenarios: [
      ["Economy Aparthotel Extended Stay", "Markets needing kitchenettes at economy capital with ALL."],
      ["Conversion Into Adagio Access", "Conversions that can fund Access apartment standards."],
      ["Sibling Clarity Versus Adagio", "Owners comparing Access versus fuller Adagio."],
    ],
    demand: [
      ["Extended-Stay Corporate", S, "Adagio Access thesis is economy aparthotel extended-stay with ALL."],
      ["Project & Workforce Demand", S, "Project/workforce weekly stays fit Access kitchenette product."],
      ["University & Medical", M, "Medical/university longer stays can fit when rates clear economy aparthotel IC."],
      ["Conversion & Repositioning", M, "Conversions succeed when PIP funds Access—not fuller Adagio scopes."],
      ["Highway & Interstate Transient", L, "One-night highway transient is ibis budget territory."],
      ["Luxury Urban Flagship", N, "Luxury flagship sits outside Adagio Access."],
    ],
    ops: "Accor economy aparthotel (live: Aparthotel Adagio Access) with leaner product than Adagio and ALL. Confirm exhibits.",
    footprint: "Confirm live hotel counts from current Accor / Adagio Access disclosures.",
    insight: "Adagio Access fits lean aparthotel demand with ALL; fails when fuller Adagio capital is assumed. Preserve live name Aparthotel Adagio Access.",
  },
  {
    id: "recKPpUCq9EZlSSYx",
    name: "25hours Hotels",
    live: "25Hours Hotel",
    slug: "25hours-hotels",
    parent: "Accor / Ennismore",
    loyalty: "ALL - Accor Live Limitless",
    proof: "ACCOR_PROOF",
    short: "25hours",
    feeGeo: "Confirm current Accor / Ennismore affiliation disclosure for 25hours (lifestyle)",
    brandPage: "https://www.25hours-hotels.com/",
    positioning: "Lifestyle Design Hotels",
    commercial:
      "Versus The Hoxton lifestyle, 25hours sells playful design authorship; versus Mondrian luxury lifestyle, it is more accessible lifestyle intensity inside ALL / Ennismore.",
    scenarios: [
      ["Lifestyle Urban Design", "Cities where 25hours design narrative and ALL clear lifestyle ADR."],
      ["Conversion Into 25hours", "Conversions that can fund design authorship and F&B character."],
      ["Sibling Clarity Versus Hoxton / Mondrian", "Owners comparing 25hours versus other Ennismore lifestyle lanes."],
    ],
    demand: [
      ["Urban Lifestyle & Design", S, "25hours core is lifestyle design urban hotels with ALL."],
      ["Conversion & Repositioning", M, "Conversions succeed when design PIP clears 25hours standards."],
      ["Resort & Destination Leisure", M, "Select destination lifestyle hotels can fit."],
      ["Suburban Business & Leisure", L, "Suburban select-service economics miss 25hours lifestyle intensity."],
      ["Meetings & Group Compression", L, "Large meetings plant is not the 25hours lane."],
      ["Highway & Interstate Transient", N, "Highway transient sits outside lifestyle design."],
    ],
    ops: "Accor / Ennismore lifestyle brand (live: 25Hours Hotel). Soft/lifestyle diligence—not hard-brand prototype economics. Confirm ALL delivery.",
    footprint: "Confirm live hotel counts from current 25hours / Accor disclosures—curated lifestyle footprint.",
    insight: "25hours fits design lifestyle assets with ALL; fails when midscale Accor fees are assumed. Preserve live name 25Hours Hotel.",
  },
  {
    id: "recXmadOxgzU6Ug5l",
    name: "The Hoxton",
    live: "The Hoxton",
    slug: "the-hoxton",
    parent: "Accor / Ennismore",
    loyalty: "ALL - Accor Live Limitless",
    proof: "ACCOR_PROOF",
    short: "The Hoxton",
    feeGeo: "Confirm current Accor / Ennismore affiliation disclosure for The Hoxton (lifestyle)",
    brandPage: "https://thehoxton.com/",
    positioning: "Urban Lifestyle",
    commercial:
      "Versus 25hours playful design, Hoxton sells neighborhood lifestyle hospitality; versus Mondrian luxury lifestyle, Hoxton is more accessible urban lifestyle inside ALL / Ennismore.",
    scenarios: [
      ["Neighborhood Lifestyle Urban", "Walkable urban neighborhoods where Hoxton and ALL lift lifestyle rates."],
      ["Adaptive Reuse Lifestyle", "Conversions that can fund Hoxton public-space and F&B character."],
      ["Sibling Clarity Versus 25hours / Mondrian", "Owners comparing Hoxton versus other Ennismore lanes."],
    ],
    demand: [
      ["Urban Lifestyle & Design", S, "The Hoxton core is neighborhood urban lifestyle with ALL."],
      ["Conversion & Repositioning", S, "Conversions fit when individuality and Accor systems both clear IC."],
      ["Resort & Destination Leisure", L, "Destination resort is secondary to urban neighborhood product."],
      ["Suburban Business & Leisure", L, "Suburban select-service economics miss Hoxton lifestyle."],
      ["Airport & Transportation", L, "Airport hard-prototype is not the Hoxton lane."],
      ["Highway & Interstate Transient", N, "Highway transient sits outside Hoxton lifestyle."],
    ],
    ops: "Accor / Ennismore urban lifestyle brand with ALL. Soft/lifestyle diligence—confirm affiliation exhibits.",
    footprint: "Confirm live hotel counts from current Hoxton / Accor disclosures—urban lifestyle footprint.",
    insight: "The Hoxton fits neighborhood lifestyle assets with ALL; fails when midscale Accor or Mondrian luxury fees are assumed.",
  },
  {
    id: "recPF5NM9kDZwkNmS",
    name: "Mondrian",
    live: "Mondrian",
    slug: "mondrian",
    parent: "Accor / Ennismore",
    loyalty: "ALL - Accor Live Limitless",
    proof: "ACCOR_PROOF",
    short: "Mondrian",
    feeGeo: "Confirm current Accor / Ennismore affiliation disclosure for Mondrian (luxury lifestyle)",
    brandPage: "https://www.sbe.com/hotels/mondrian",
    positioning: "Luxury Lifestyle",
    commercial:
      "Versus Hyde / SLS Ennismore luxury lifestyle siblings, Mondrian sells design luxury lifestyle; versus Swissôtel classic upscale, Mondrian is lifestyle luxury intensity inside ALL.",
    scenarios: [
      ["Luxury Lifestyle Urban", "Gateway lifestyle districts where Mondrian and ALL clear luxury ADR."],
      ["Destination Design Luxury", "Destination assets that can fund luxury lifestyle F&B and design."],
      ["Sibling Clarity Versus Hyde / SLS", "Owners comparing Mondrian versus Hyde and SLS."],
    ],
    demand: [
      ["Urban Lifestyle & Design", S, "Mondrian core is luxury lifestyle urban design with ALL."],
      ["Luxury Urban Flagship", S, "Luxury flagship lifestyle fits when product clears Mondrian."],
      ["Resort & Destination Leisure", M, "Select destination resorts can work when design authorship holds."],
      ["Conversion & Repositioning", M, "Conversions succeed only with luxury lifestyle PIP."],
      ["Suburban Business & Leisure", L, "Suburban select-service sits outside Mondrian luxury lifestyle."],
      ["Highway & Interstate Transient", N, "Highway transient sits outside luxury lifestyle."],
    ],
    ops: "Accor / Ennismore luxury lifestyle brand with ALL. Luxury-band capital—do not import midscale Accor economics.",
    footprint: "Confirm live hotel counts from current Mondrian / Accor disclosures—curated luxury lifestyle footprint.",
    insight: "Mondrian fits luxury lifestyle with ALL; fails when Hoxton accessible lifestyle or Swissôtel classic fees are assumed.",
  },
  {
    id: "recharEz6EaOg9AiP",
    name: "Hyde Hotels",
    live: "Hyde",
    slug: "hyde-hotels",
    parent: "Accor / Ennismore",
    loyalty: "ALL - Accor Live Limitless",
    proof: "ACCOR_PROOF",
    short: "Hyde",
    feeGeo: "Confirm current Accor / Ennismore affiliation disclosure for Hyde (luxury lifestyle)",
    brandPage: "https://www.sbe.com/hotels/hyde",
    positioning: "Luxury Lifestyle",
    commercial:
      "Versus Mondrian design luxury lifestyle, Hyde sells nightlife-forward luxury lifestyle; versus SLS, confirm sibling clarity inside Ennismore / ALL.",
    scenarios: [
      ["Luxury Lifestyle Nightlife Urban", "Lifestyle districts where Hyde energy and ALL clear luxury ADR."],
      ["Destination Party / Resort Energy", "Destination assets that can sustain nightlife and F&B intensity."],
      ["Sibling Clarity Versus Mondrian / SLS", "Owners comparing Hyde versus Mondrian and SLS."],
    ],
    demand: [
      ["Urban Lifestyle & Design", S, "Hyde core is luxury lifestyle urban energy with ALL."],
      ["Luxury Urban Flagship", S, "Luxury lifestyle flagship fits when product clears Hyde."],
      ["Resort & Destination Leisure", M, "Destination resorts can work when entertainment intensity is funded."],
      ["Conversion & Repositioning", M, "Conversions succeed only with luxury lifestyle PIP."],
      ["Suburban Business & Leisure", L, "Suburban select-service sits outside Hyde."],
      ["Highway & Interstate Transient", N, "Highway transient sits outside luxury lifestyle."],
    ],
    ops: "Accor / Ennismore luxury lifestyle brand (live: Hyde) with ALL. Luxury-band operating intensity—confirm affiliation exhibits.",
    footprint: "Confirm live hotel counts from current Hyde / Accor disclosures.",
    insight: "Hyde fits luxury lifestyle energy with ALL; fails when midscale Accor capital is assumed. Preserve live name Hyde.",
  },
  {
    id: "recrJtboQyjYGsZZm",
    name: "SLS Hotels",
    live: "SLS",
    slug: "sls-hotels",
    parent: "Accor / Ennismore",
    loyalty: "ALL - Accor Live Limitless",
    proof: "ACCOR_PROOF",
    short: "SLS",
    feeGeo: "Confirm current Accor / Ennismore affiliation disclosure for SLS (luxury lifestyle)",
    brandPage: "https://www.sbe.com/hotels/sls",
    positioning: "Luxury Lifestyle",
    commercial:
      "Versus Mondrian / Hyde Ennismore siblings, SLS sells luxury lifestyle with strong F&B / nightlife cues; versus Swissôtel classic upscale, SLS is lifestyle luxury intensity inside ALL.",
    scenarios: [
      ["Luxury Lifestyle Urban Flagship", "Gateway districts where SLS and ALL clear luxury ADR."],
      ["Destination Luxury Lifestyle", "Destination assets that can fund SLS F&B and design."],
      ["Sibling Clarity Versus Mondrian / Hyde", "Owners comparing SLS versus Mondrian and Hyde."],
    ],
    demand: [
      ["Luxury Urban Flagship", S, "SLS fits luxury lifestyle flagship with ALL when product clears standards."],
      ["Urban Lifestyle & Design", S, "Design and nightlife-forward urban lifestyle is an SLS strength."],
      ["Resort & Destination Leisure", M, "Select destination resorts can work when luxury lifestyle is funded."],
      ["Conversion & Repositioning", M, "Conversions succeed only with luxury lifestyle PIP."],
      ["Suburban Business & Leisure", L, "Suburban select-service sits outside SLS."],
      ["Highway & Interstate Transient", N, "Highway transient sits outside luxury lifestyle."],
    ],
    ops: "Accor / Ennismore luxury lifestyle brand (live: SLS) with ALL. Luxury-band capital—confirm affiliation exhibits.",
    footprint: "Confirm live hotel counts from current SLS / Accor disclosures—curated footprint.",
    insight: "SLS fits luxury lifestyle with ALL; fails when Swissôtel classic or midscale Accor fees are assumed. Preserve live name SLS.",
  },
  {
    id: "rect7iv1y3mGFadqL",
    name: "Swissôtel",
    live: "Swissotel",
    slug: "swissotel",
    parent: "Accor",
    loyalty: "ALL - Accor Live Limitless",
    proof: "ACCOR_PROOF",
    short: "Swissôtel",
    feeGeo: "Confirm current Accor franchise / development disclosure for Swissôtel (upscale full-service)",
    brandPage: "https://all.accor.com/brands/swissotel.en.shtml",
    positioning: "Upscale Full-Service",
    commercial:
      "Versus Pullman premium meetings, Swissôtel sells classic upscale full-service; versus Mondrian lifestyle luxury, Swissôtel is classic Accor upscale with ALL.",
    scenarios: [
      ["Upscale Full-Service Urban", "Gateway and secondary cities where Swissôtel and ALL clear upscale ADR."],
      ["Meetings-Capable Upscale", "Assets with meetings plant that can fund Swissôtel standards."],
      ["Conversion Into Swissôtel", "Conversions that can deliver upscale full-service PIP."],
    ],
    demand: [
      ["Suburban Business & Leisure", S, "Swissôtel fits upscale business/leisure with ALL when product clears standards."],
      ["Meetings & Group Compression", S, "Meetings-capable plant is a Swissôtel strength."],
      ["Airport & Transportation", M, "Airport upscale hubs can fit Swissôtel."],
      ["Conversion & Repositioning", M, "Conversions succeed when PIP funds upscale full-service."],
      ["Urban Lifestyle & Design", L, "Pure lifestyle design is Ennismore / TRIBE territory."],
      ["Highway & Interstate Transient", N, "Highway limited-service sits outside Swissôtel upscale full-service."],
    ],
    ops: "Accor upscale full-service (live: Swissotel) with meetings capability and ALL. Confirm franchise exhibits—do not invent royalty percentages.",
    footprint: "Confirm live hotel counts from current Accor / Swissôtel disclosures across EMEA, APAC, and Americas.",
    insight: "Swissôtel fits classic upscale full-service with ALL; fails when Ennismore lifestyle or ibis midscale fees are assumed. Preserve live name Swissotel.",
  },
  {
    id: "reclu1Fgv5Leq25qn",
    name: "Park Plaza",
    live: "Park Plaza",
    slug: "park-plaza",
    parent: "Choice Hotels International",
    loyalty: "Choice Privileges",
    proof: "CHOICE_PROOF",
    short: "Park Plaza",
    feeGeo: "Confirm current Choice Hotels FDD / Radisson portfolio exhibits for Park Plaza (upscale)",
    brandPage: "https://www.choicehotels.com/park-plaza",
    positioning: "Upscale Full-Service",
    commercial:
      "Versus Park Inn by Radisson midscale, Park Plaza sells higher upscale full-service; versus Radisson Blu, confirm sibling clarity inside Choice Privileges / Radisson portfolio.",
    scenarios: [
      ["Upscale Full-Service Urban", "Markets where Park Plaza product and Privileges clear upscale ADR."],
      ["Meetings-Capable Upscale", "Assets with meetings plant that can fund Park Plaza standards."],
      ["Conversion Into Park Plaza", "Conversions that can deliver upscale full-service PIP."],
    ],
    demand: [
      ["Suburban Business & Leisure", S, "Park Plaza fits upscale business/leisure with Privileges when product clears standards."],
      ["Meetings & Group Compression", S, "Meetings-capable plant is a Park Plaza strength."],
      ["Airport & Transportation", M, "Airport upscale hubs can fit Park Plaza."],
      ["Conversion & Repositioning", M, "Conversions succeed when PIP funds upscale full-service."],
      ["Urban Lifestyle & Design", L, "Pure lifestyle design is more Radisson RED territory."],
      ["Highway & Interstate Transient", N, "Highway limited-service sits outside Park Plaza upscale."],
    ],
    ops: "Choice / Radisson portfolio upscale full-service with Choice Privileges. Live Airtable parent may still read Radisson Hotel Group—confirm Choice commercial path. Do not invent royalty percentages.",
    footprint: "Confirm live hotel counts from current Choice / Park Plaza disclosures—primarily EMEA.",
    insight: "Park Plaza fits upscale full-service with Privileges; fails when Park Inn midscale fees are assumed. Airtable parent currently Radisson Hotel Group (Draft).",
  },
  {
    id: "recyI1O61Or7LXwXl",
    name: "Park Inn by Radisson",
    live: "Park Inn by Radisson",
    slug: "park-inn-by-radisson",
    parent: "Choice Hotels International",
    loyalty: "Choice Privileges",
    proof: "CHOICE_PROOF",
    short: "Park Inn",
    feeGeo: "Confirm current Choice Hotels FDD / Radisson portfolio exhibits for Park Inn by Radisson (midscale)",
    brandPage: "https://www.choicehotels.com/park-inn",
    positioning: "Midscale Full-Service",
    commercial:
      "Versus Park Plaza upscale, Park Inn is midscale full-service; versus Comfort limited-service, Park Inn keeps fuller midscale product inside Choice Privileges.",
    scenarios: [
      ["Midscale Full-Service Urban / Suburban", "Markets where Park Inn product and Privileges clear midscale ADR."],
      ["Meetings-Capable Midscale", "Secondary cities with midscale meetings plant."],
      ["Conversion Into Park Inn", "Conversions that can fund midscale full-service PIP."],
    ],
    demand: [
      ["Suburban Business & Leisure", S, "Park Inn fits midscale business/leisure with Privileges."],
      ["Meetings & Group Compression", M, "Meetings plant can fit when midscale full-service is present."],
      ["Airport & Transportation", M, "Airport midscale hubs can fit Park Inn."],
      ["Conversion & Repositioning", M, "Conversions succeed when PIP funds midscale full-service."],
      ["Highway & Interstate Transient", L, "Hard limited-service highway transient is Comfort / Quality territory."],
      ["Luxury Urban Flagship", N, "Luxury flagship sits outside Park Inn midscale."],
    ],
    ops: "Choice / Radisson portfolio midscale full-service with Choice Privileges. Live Airtable parent may still read Radisson Hotel Group—confirm Choice commercial path.",
    footprint: "Confirm live hotel counts from current Choice / Park Inn disclosures.",
    insight: "Park Inn fits midscale full-service with Privileges; fails when Park Plaza upscale fees are assumed. Airtable status Draft.",
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
lines.push(` * Factory 2.0 Batch 12 — Hilton / Accor-Ennismore / Choice.`);
lines.push(` * NEW_BRAND_CANDIDATE packs use provisional staging keys (NEW_<slug>) — never invent Airtable rec IDs.`);
lines.push(` * Franchise fee amounts never invented — FDD / disclosure confirmation only.`);
lines.push(` * Demand Scenario Fit: brand-specific six (not mechanical repeats).`);
lines.push(` */`);
lines.push(`import { DEMAND_FIT_LABELS } from "../be-factory-permanent-demand-scenarios-six-v1.js";`);
lines.push(`import {`);
lines.push(`  HONORS_PROOF,`);
lines.push(`  ACCOR_PROOF,`);
lines.push(`  CHOICE_PROOF,`);
lines.push(`} from "./be-factory-2-depth-loyalty-proofs-v1.js";`);
lines.push(``);
lines.push(`function pack(base) {`);
lines.push(`  return Object.freeze({`);
lines.push(`    researchSpendUsd: 0,`);
lines.push(`    paidCalls: 0,`);
lines.push(`    loyaltyProofRequired: 6,`);
lines.push(`    batchTag: "batch-12",`);
lines.push(`    ...base,`);
lines.push(`  });`);
lines.push(`}`);
lines.push(``);
lines.push(`function demand(rows) {`);
lines.push(`  return rows.map((r) => ({`);
lines.push(`    title: r.title,`);
lines.push(`    fit: r.fit,`);
lines.push(`    explanation: r.explanation,`);
lines.push(`  }));`);
lines.push(`}`);
lines.push(``);
lines.push(`function gallery(brand, url) {`);
lines.push(`  return [`);
lines.push(`    { title: \`\${brand} — Exterior / Arrival\`, body: "Exterior presence · confirm property photography before publication", source: url },`);
lines.push(`    { title: \`\${brand} — Guest Room\`, body: "Guest-room product typical of this ladder rung", source: url },`);
lines.push(`    { title: \`\${brand} — Lobby / Public Space\`, body: "Public space supporting the brand promise", source: url },`);
lines.push(`    { title: \`\${brand} — F&B / Amenity Cue\`, body: "F&B or amenity cues where required by standards", source: url },`);
lines.push(`    { title: \`\${brand} — Meetings / Flexibility Cue\`, body: "Meetings, kitchen, or efficiency cues typical of this product", source: url },`);
lines.push(`    { title: \`\${brand} — Context / Setting\`, body: "Urban, suburban, airport, or destination context where typical", source: url },`);
lines.push(`  ];`);
lines.push(`}`);
lines.push(``);
lines.push(`export const FACTORY_2_BATCH_12_V1 = "FACTORY_2_BATCH_12_V1";`);
lines.push(``);
lines.push(`const FDD =`);
lines.push(`  "Confirm current franchise disclosure / FDD exhibits for application, royalty, marketing, loyalty, and PIP. Distinguish historical FDD figures from the live agreement. Amounts are not staged without a verified disclosure.";`);
lines.push(``);
lines.push(`const S = DEMAND_FIT_LABELS.STRONG_FIT;`);
lines.push(`const M = DEMAND_FIT_LABELS.MODERATE_FIT;`);
lines.push(`const L = DEMAND_FIT_LABELS.LIMITED_FIT;`);
lines.push(`const N = DEMAND_FIT_LABELS.NOT_A_FIT;`);
lines.push(``);
lines.push(`export const BATCH_12_IDS = Object.freeze([`);
for (const b of brands) lines.push(`  "${b.id}", // ${b.name}`);
lines.push(`]);`);
lines.push(``);
lines.push(`export const BATCH_12_PACKS_BY_ID = Object.freeze({`);

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
  lines.push(`    sources: {`);
  lines.push(`      brandPage: ${JSON.stringify(b.brandPage)},`);
  lines.push(`    },`);
  lines.push(`    positioning: ${JSON.stringify(b.positioning)},`);
  lines.push(`    commercialDifferentiator: ${JSON.stringify(b.commercial)},`);
  lines.push(`    scenarios: [`);
  for (const [t, body] of b.scenarios) {
    lines.push(`      { title: ${JSON.stringify(t)}, body: ${JSON.stringify(body)} },`);
  }
  lines.push(`    ],`);
  lines.push(`    demandScenarios: demand([`);
  for (const [t, fit, expl] of b.demand) {
    lines.push(`      { title: ${JSON.stringify(t)}, fit: ${demandFit(fit)}, explanation: ${JSON.stringify(expl)} },`);
  }
  lines.push(`    ]),`);
  lines.push(`    loyaltyProof: ${b.proof}(${JSON.stringify(b.short)}),`);
  lines.push(`    feeNote: \`${esc(b.short)} application, royalty, marketing, loyalty, and PIP — \${FDD}\`,`);
  lines.push(`    operatingModel: ${JSON.stringify(b.ops)},`);
  lines.push(`    footprint: ${JSON.stringify(b.footprint)},`);
  lines.push(`    insight: ${JSON.stringify(b.insight)},`);
  lines.push(`    standards: [`);
  lines.push(`      { title: "Brand Product Standards", body: "Rooms and public space must clear ${esc(b.short)} standards for the operating region." },`);
  lines.push(`      { title: "Conversion / PIP Discipline", body: "PIP or membership quality scope must match the brand promise—not under-scoped capital." },`);
  lines.push(`      { title: "Loyalty / Network Integration", body: "Confirm ${esc(b.loyalty)} delivery obligations for this brand before IC." },`);
  lines.push(`    ],`);
  lines.push(`    valueOwnerScenarios: [`);
  lines.push(`      { title: "Primary Owner Corridor", body: "Sites where ${esc(b.short)} ADR clears IC versus local comps." },`);
  lines.push(`      { title: "Conversion / Membership Path", body: "Assets where ${esc(b.short)} affiliation clears IC versus sibling alternatives." },`);
  lines.push(`      { title: "Disclosure Fee Gate", body: "Model only from current disclosure / membership exhibits for ${esc(b.short)}." },`);
  lines.push(`      { title: "Sibling Clarity", body: "Diligence versus sibling flags / networks before IC." },`);
  lines.push(`    ],`);
  lines.push(`    gallery: gallery(${JSON.stringify(b.short)}, ${JSON.stringify(b.brandPage)}),`);
  lines.push(`  }),`);
  lines.push(``);
}

lines.push(`});`);
lines.push(``);
lines.push(`export function listBatch12() {`);
lines.push(`  return BATCH_12_IDS.map((id) => BATCH_12_PACKS_BY_ID[id]).filter(Boolean);`);
lines.push(`}`);
lines.push(``);
lines.push(`export function getBatch12Pack(brandId) {`);
lines.push(`  return BATCH_12_PACKS_BY_ID[brandId] || null;`);
lines.push(`}`);
lines.push(``);

writeFileSync(outPath, lines.join("\n"));
console.log(`Wrote ${outPath} (${brands.length} packs)`);
