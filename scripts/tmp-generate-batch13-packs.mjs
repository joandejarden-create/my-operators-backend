#!/usr/bin/env node
/** One-shot generator for Batch 13 depth packs (IHG + Hyatt AI/wellness). */
import { writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");
const outPath = join(ROOT, "lib/brand-explorer/factory-2/be-factory-2-depth-research-packs-batch13-v1.js");
const S = "S";
const M = "M";
const L = "L";
const N = "N";

/** @type {Array<object>} */
const brands = [
  {
    id: "reckR3Txp5OzwVl5q",
    name: "Regent Hotels & Resorts",
    live: "Regent Hotels & Resorts",
    slug: "regent-hotels-resorts",
    parent: "InterContinental Hotels Group",
    loyalty: "IHG One Rewards",
    proof: "IHG_PROOF",
    short: "Regent",
    feeGeo: "Confirm current IHG development / franchise disclosure for Regent (luxury)",
    brandPage: "https://www.ihg.com/regent/hotels/us/en/reservation",
    positioning: "Luxury Full-Service",
    commercial:
      "Versus InterContinental classic luxury, Regent sells contemporary luxury full-service; versus Kimpton lifestyle, Regent is harder luxury intensity inside IHG One Rewards.",
    scenarios: [
      ["Gateway Luxury Full-Service", "Primary gateways where Regent product and One Rewards clear luxury ADR."],
      ["Resort Luxury Destinations", "Destination resorts that can fund spa and F&B at Regent standards."],
      ["Conversion Into Regent Luxury", "Conversions only when luxury PIP and service clear Regent QA."],
    ],
    demand: [
      ["Luxury Urban Flagship", S, "Regent fits contemporary luxury urban flagships with One Rewards."],
      ["Resort & Destination Leisure", S, "Luxury resorts are a core Regent lane when capital clears."],
      ["Meetings & Group Compression", M, "Meetings can fit when luxury full-service plant exists."],
      ["Conversion & Repositioning", M, "Conversions succeed only with true luxury PIP."],
      ["Suburban Business & Leisure", L, "Suburban select-service economics sit outside Regent luxury."],
      ["Highway & Interstate Transient", N, "Highway transient sits outside luxury Regent."],
    ],
    ops: "IHG luxury full-service with elevated F&B and One Rewards. Do not import midscale Holiday Inn economics.",
    footprint: "Confirm live hotel counts from current IHG / Regent disclosures—curated luxury footprint across APAC, EMEA, and Americas.",
    insight: "Regent fits sponsors funding contemporary luxury with One Rewards; fails when Crowne Plaza or Holiday Inn capital is assumed.",
  },
  {
    id: "recaPiDEZ7K7IXhcZ",
    name: "Crowne Plaza Hotels & Resorts",
    live: "Crowne Plaza",
    slug: "crowne-plaza",
    parent: "InterContinental Hotels Group",
    loyalty: "IHG One Rewards",
    proof: "IHG_PROOF",
    short: "Crowne Plaza",
    feeGeo: "Confirm current IHG development / franchise disclosure for Crowne Plaza (upscale meetings)",
    brandPage: "https://www.ihg.com/crowneplaza/hotels/us/en/reservation",
    positioning: "Upscale Meetings Full-Service",
    commercial:
      "Versus Holiday Inn midscale full-service, Crowne Plaza sells higher meetings intensity; versus Regent luxury, it is productive upscale rather than luxury inside One Rewards.",
    scenarios: [
      ["Urban / Suburban Meetings Full-Service", "Markets where meetings ADR clears Crowne Plaza IC."],
      ["Airport Upscale Hub", "Airport hubs needing Crowne Plaza meetings product and One Rewards."],
      ["Conversion Into Crowne Plaza", "Conversions that can fund meetings plant and upscale PIP."],
    ],
    demand: [
      ["Meetings & Group Compression", S, "Crowne Plaza is built for productive meetings with One Rewards."],
      ["Airport & Transportation", S, "Airport upscale hubs are a core Crowne Plaza use case."],
      ["Suburban Business & Leisure", S, "Suburban productive mix fits Crowne Plaza."],
      ["Conversion & Repositioning", M, "Conversions succeed when PIP funds meetings and upscale rooms."],
      ["Extended-Stay Corporate", L, "Extended-stay kitchens are Staybridge / Candlewood territory."],
      ["Highway & Interstate Transient", N, "Highway limited-service sits outside Crowne Plaza upscale."],
    ],
    ops: "IHG upscale full-service meetings brand with One Rewards. Confirm franchise exhibits—do not invent royalty percentages. Preserve live name Crowne Plaza.",
    footprint: "Confirm live hotel counts from current IHG / Crowne Plaza disclosures globally.",
    insight: "Crowne Plaza fits meetings-capable upscale assets with One Rewards; fails when Holiday Inn midscale or Regent luxury fees are assumed.",
  },
  {
    id: "reccQERSltr3CcDgs",
    name: "HUALUXE Hotels & Resorts",
    live: "HUALUXE Hotels and Resorts",
    slug: "hualuxe-hotels-resorts",
    parent: "InterContinental Hotels Group",
    loyalty: "IHG One Rewards",
    proof: "IHG_PROOF",
    short: "HUALUXE",
    feeGeo: "Confirm current IHG development / franchise disclosure for HUALUXE (China / Greater China luxury)",
    brandPage: "https://www.ihg.com/hualuxe/hotels/us/en/reservation",
    positioning: "Luxury Chinese Hospitality",
    commercial:
      "Versus Regent global luxury, HUALUXE is IHG's China-focused luxury hospitality brand; versus Crowne Plaza upscale, it sits higher on luxury intensity inside One Rewards for Greater China corridors.",
    scenarios: [
      ["Greater China Luxury Urban", "China / Greater China gateways where HUALUXE and One Rewards clear luxury ADR."],
      ["Destination Luxury China", "Destination resorts that can fund HUALUXE luxury standards."],
      ["Sibling Clarity Versus Regent", "Owners comparing HUALUXE China luxury versus Regent global luxury."],
    ],
    demand: [
      ["Luxury Urban Flagship", S, "HUALUXE fits Greater China luxury urban flagships with One Rewards."],
      ["Regional & Secondary Markets", M, "Select secondary China markets can fit when luxury ADR clears."],
      ["Resort & Destination Leisure", M, "Destination luxury can work when HUALUXE standards clear."],
      ["Conversion & Repositioning", M, "Conversions succeed only with luxury PIP."],
      ["Suburban Business & Leisure", L, "Suburban select-service sits outside HUALUXE luxury."],
      ["Highway & Interstate Transient", N, "Highway transient sits outside HUALUXE."],
    ],
    ops: "IHG luxury brand focused on Chinese hospitality cues with One Rewards. Confirm geographic rights and disclosure exhibits. Preserve live name HUALUXE Hotels and Resorts.",
    footprint: "Confirm live hotel counts from current IHG / HUALUXE disclosures—primarily Greater China / APAC.",
    insight: "HUALUXE fits Greater China luxury with One Rewards; fails when Crowne Plaza or Holiday Inn capital is assumed.",
  },
  {
    id: "NEW_ruby-hotels",
    name: "Ruby Hotels",
    live: "Ruby Hotels",
    slug: "ruby-hotels",
    parent: "InterContinental Hotels Group",
    loyalty: "IHG One Rewards",
    proof: "IHG_PROOF",
    short: "Ruby",
    feeGeo: "Confirm current IHG disclosure for Ruby Hotels AFTER Brand Basics create",
    brandPage: "https://www.ihg.com/",
    positioning: "Lifestyle Midscale",
    commercial:
      "Versus Hotel Indigo boutique, Ruby (IHG partnership / lifestyle midscale) needs verified ladder position before IC; stage research only—do not invent fee tables.",
    scenarios: [
      ["Lifestyle Midscale Diligence Gate", "Confirm Ruby versus Indigo / voco before assuming distinct owner use case."],
      ["Urban Lifestyle Midscale", "Urban sites needing One Rewards—validate standards after Basics create."],
      ["Conversion Into Ruby", "Conversions only after authorized Brand Basics create and disclosure exhibits."],
    ],
    demand: [
      ["Urban Lifestyle & Design", M, "Provisional: lifestyle midscale may fit—confirm after Basics create."],
      ["Conversion & Repositioning", M, "Conversions gated on authorized Basics create."],
      ["Suburban Business & Leisure", M, "Provisional: suburban mix may fit."],
      ["Airport & Transportation", L, "Confirm airport fit after live standards."],
      ["Meetings & Group Compression", L, "Large meetings usually Crowne Plaza territory."],
      ["Luxury Urban Flagship", N, "Luxury flagship sits outside Ruby midscale lifestyle."],
    ],
    ops: "Staged NEW_BRAND_CANDIDATE — no live Brand Basics for Ruby Hotels. Do not invent rec ID. Confirm IHG partnership / franchise path after authorized create.",
    footprint: "No Airtable inventory yet. Confirm existence and counts from IHG disclosures before underwriting.",
    insight: "Ruby Hotels is NEW_BRAND_CANDIDATE—authorize Basics create only after brand existence and ladder position versus Indigo / voco are verified.",
    researchClass: "NEW_BRAND_CANDIDATE",
  },
  {
    id: "NEW_noted-collection",
    name: "Noted Collection",
    live: "Noted Collection",
    slug: "noted-collection",
    parent: "InterContinental Hotels Group",
    loyalty: "IHG One Rewards",
    proof: "IHG_PROOF",
    short: "Noted Collection",
    feeGeo: "Confirm current IHG soft-collection / affiliation exhibits for Noted AFTER Brand Basics create",
    brandPage: "https://www.ihg.com/",
    positioning: "Independent Collection",
    commercial:
      "Versus Vignette soft collection, Noted (if a distinct IHG soft lane) needs verified ladder position before IC; stage research only.",
    scenarios: [
      ["Soft-Collection Diligence Gate", "Confirm Noted versus Vignette / Kimpton before assuming identical use case."],
      ["Independent Character Conversion", "Distinctive independents needing One Rewards—validate after Basics create."],
      ["Conversion Into Noted", "Conversions only after authorized Basics create and disclosure exhibits."],
    ],
    demand: [
      ["Conversion & Repositioning", M, "Provisional: soft-collection conversions may fit."],
      ["Urban Lifestyle & Design", M, "Provisional: design-led independents may fit."],
      ["Resort & Destination Leisure", M, "Provisional: destination character hotels may fit."],
      ["Suburban Business & Leisure", L, "Suburban select-service usually sits below soft-collection upscale."],
      ["Airport & Transportation", L, "Airport hard-prototype is not soft-collection lane."],
      ["Highway & Interstate Transient", N, "Highway transient sits outside independent collection."],
    ],
    ops: "Staged NEW_BRAND_CANDIDATE — no live Brand Basics for Noted Collection. Soft-collection diligence after authorized create.",
    footprint: "No Airtable inventory yet. Confirm from IHG disclosures before underwriting.",
    insight: "Noted Collection is NEW_BRAND_CANDIDATE—authorize Basics create only after existence and position versus Vignette are verified.",
    researchClass: "NEW_BRAND_CANDIDATE",
  },
  {
    id: "rechOYMLJ3tZI0y1P",
    name: "Staybridge Suites",
    live: "Staybridge Suites",
    slug: "staybridge-suites",
    parent: "InterContinental Hotels Group",
    loyalty: "IHG One Rewards",
    proof: "IHG_PROOF",
    short: "Staybridge",
    feeGeo: "Confirm current IHG FDD for Staybridge Suites (upscale extended-stay)",
    brandPage: "https://www.ihg.com/staybridge/hotels/us/en/reservation",
    positioning: "Upscale Extended-Stay",
    commercial:
      "Versus Candlewood economy extended-stay, Staybridge sells fuller suite / kitchen product; versus Holiday Inn transient, it is length-of-stay economics inside One Rewards.",
    scenarios: [
      ["Upscale Extended-Stay Corporate", "Markets with multi-week demand needing kitchens and One Rewards."],
      ["Project / Medical Longer Stay", "Project and medical corridors needing suite product."],
      ["Conversion Into Staybridge", "Conversions that can fund suite/kitchen PIP."],
    ],
    demand: [
      ["Extended-Stay Corporate", S, "Staybridge core is upscale extended-stay with kitchens and One Rewards."],
      ["Project & Workforce Demand", S, "Project/workforce weekly stays are a primary Staybridge pocket."],
      ["University & Medical", S, "Medical/university longer stays fit suite kitchenette product."],
      ["Suburban Business & Leisure", M, "Suburban midweek extended-stay works; pure weekend leisure secondary."],
      ["Highway & Interstate Transient", L, "One-night highway transient is Holiday Inn Express territory."],
      ["Luxury Urban Flagship", N, "Luxury flagship sits outside Staybridge extended-stay."],
    ],
    ops: "IHG upscale extended-stay with kitchenettes and One Rewards. Housekeeping cadence differs from transient—confirm FDD exhibits.",
    footprint: "Confirm live hotel counts from current IHG / Staybridge disclosures—primarily Americas with international presence.",
    insight: "Staybridge fits genuine extended-stay demand with One Rewards; fails when Candlewood lean capital or Holiday Inn transient fees are assumed.",
  },
  {
    id: "recTGrNzTA6DRe0LL",
    name: "Candlewood Suites",
    live: "Candlewood Suites",
    slug: "candlewood-suites",
    parent: "InterContinental Hotels Group",
    loyalty: "IHG One Rewards",
    proof: "IHG_PROOF",
    short: "Candlewood",
    feeGeo: "Confirm current IHG FDD for Candlewood Suites (midscale / economy extended-stay)",
    brandPage: "https://www.ihg.com/candlewood/hotels/us/en/reservation",
    positioning: "Economy Extended-Stay",
    commercial:
      "Versus Staybridge upscale extended-stay, Candlewood sells leaner kitchenette economics; versus Atwell suites midscale, Candlewood is longer-stay focused inside One Rewards.",
    scenarios: [
      ["Economy Extended-Stay Corporate", "Markets needing kitchenettes at leaner capital with One Rewards."],
      ["Project / Workforce Longer Stay", "Project and workforce corridors needing value extended-stay."],
      ["Conversion Into Candlewood", "Conversions that can fund suite/kitchen PIP at Candlewood standards."],
    ],
    demand: [
      ["Extended-Stay Corporate", S, "Candlewood core is leaner extended-stay with kitchens and One Rewards."],
      ["Project & Workforce Demand", S, "Project/workforce weekly stays are a primary Candlewood pocket."],
      ["University & Medical", M, "Medical/university longer stays can fit when rates clear economy extended-stay IC."],
      ["Suburban Business & Leisure", M, "Suburban midweek extended-stay works; weekend leisure secondary."],
      ["Highway & Interstate Transient", L, "One-night highway transient is Express territory."],
      ["Luxury Urban Flagship", N, "Luxury flagship sits outside Candlewood."],
    ],
    ops: "IHG economy / midscale extended-stay with kitchenettes and One Rewards. Confirm FDD exhibits—do not invent royalty percentages.",
    footprint: "Confirm live hotel counts from current IHG / Candlewood disclosures.",
    insight: "Candlewood fits lean extended-stay demand with One Rewards; fails when Staybridge upscale capital is assumed.",
  },
  {
    id: "rec2lJCTSYIwsQYMc",
    name: "Atwell Suites",
    live: "Atwell Suites",
    slug: "atwell-suites",
    parent: "InterContinental Hotels Group",
    loyalty: "IHG One Rewards",
    proof: "IHG_PROOF",
    short: "Atwell Suites",
    feeGeo: "Confirm current IHG FDD for Atwell Suites (all-suite midscale)",
    brandPage: "https://www.ihg.com/atwellsuites/hotels/us/en/reservation",
    positioning: "Midscale All-Suite",
    commercial:
      "Versus Staybridge extended-stay, Atwell is suite transient / short-stay midscale; versus Holiday Inn, it sells all-suite product inside One Rewards—confirm disclosure maturity for newer brand.",
    scenarios: [
      ["Midscale All-Suite Urban / Suburban", "Markets where suite product and One Rewards clear midscale ADR."],
      ["New-Build Atwell Prototype", "New-builds optimized for Atwell suites—underwrite development cost carefully."],
      ["Sibling Clarity Versus Staybridge", "Owners comparing Atwell suite transient versus Staybridge extended-stay."],
    ],
    demand: [
      ["Suburban Business & Leisure", S, "Atwell fits midscale suite business/leisure with One Rewards."],
      ["Airport & Transportation", M, "Airport-edge suite midscale can fit Atwell."],
      ["Conversion & Repositioning", M, "Conversions succeed when suite PIP clears Atwell standards."],
      ["Extended-Stay Corporate", L, "True weekly kitchenette extended-stay is Staybridge / Candlewood territory."],
      ["Regional & Secondary Markets", M, "Secondary markets need ADR that supports Atwell fees."],
      ["Luxury Urban Flagship", N, "Luxury flagship sits outside Atwell midscale suites."],
    ],
    ops: "IHG midscale all-suite brand (newer ladder rung) with One Rewards. Do not invent mature operating metrics from launch media alone—confirm current disclosures.",
    footprint: "Confirm live hotel counts and pipeline from current IHG / Atwell disclosures—growth brand; do not underwrite from headlines alone.",
    insight: "Atwell fits midscale suite product with One Rewards; fails when Staybridge extended-stay or invented launch metrics are assumed.",
  },
  {
    id: "recokU1aiTKb0bhgE",
    name: "Holiday Inn",
    live: "Holiday Inn",
    slug: "holiday-inn",
    parent: "InterContinental Hotels Group",
    loyalty: "IHG One Rewards",
    proof: "IHG_PROOF",
    short: "Holiday Inn",
    feeGeo: "Confirm current IHG FDD for Holiday Inn (midscale full-service) — not Express",
    brandPage: "https://www.ihg.com/holidayinn/hotels/us/en/reservation",
    positioning: "Midscale Full-Service",
    commercial:
      "Versus Holiday Inn Express limited-service, classic Holiday Inn sells fuller midscale F&B / meetings; versus Crowne Plaza upscale, it is a lower full-service ladder inside One Rewards.",
    scenarios: [
      ["Midscale Full-Service Urban / Suburban", "Markets where Holiday Inn product and One Rewards clear midscale ADR."],
      ["Meetings-Capable Midscale", "Secondary cities with midscale meetings plant."],
      ["Conversion Into Holiday Inn", "Conversions that can fund midscale full-service PIP—not Express scopes."],
    ],
    demand: [
      ["Suburban Business & Leisure", S, "Holiday Inn fits midscale business/leisure with One Rewards."],
      ["Meetings & Group Compression", M, "Meetings plant can fit when midscale full-service is present."],
      ["Airport & Transportation", M, "Airport midscale hubs can fit Holiday Inn."],
      ["Conversion & Repositioning", M, "Conversions succeed when PIP funds full-service—not Express limited-service."],
      ["Highway & Interstate Transient", L, "Hard limited-service highway transient is Express territory."],
      ["Luxury Urban Flagship", N, "Luxury flagship sits outside Holiday Inn midscale."],
    ],
    ops: "IHG midscale full-service with One Rewards. Do not confuse with Holiday Inn Express limited-service fee or staffing assumptions.",
    footprint: "Confirm live hotel counts from current IHG / Holiday Inn disclosures—global midscale full-service footprint.",
    insight: "Holiday Inn fits midscale full-service with One Rewards; fails when Express limited-service or Crowne Plaza upscale fees are assumed.",
  },
  {
    id: "NEW_holiday-inn-club-vacations",
    name: "Holiday Inn Club Vacations",
    live: "Holiday Inn Club Vacations",
    slug: "holiday-inn-club-vacations",
    parent: "InterContinental Hotels Group",
    loyalty: "IHG One Rewards",
    proof: "IHG_PROOF",
    short: "HICV",
    feeGeo: "Confirm current IHG / HICV vacation ownership disclosure AFTER Brand Basics create",
    brandPage: "https://www.ihg.com/",
    positioning: "Vacation Ownership",
    commercial:
      "Versus Holiday Inn transient hotel, Club Vacations is vacation ownership / timeshare economics—do not apply conventional hotel franchise royalty tables without disclosure confirmation.",
    scenarios: [
      ["Vacation Ownership Diligence Gate", "Confirm HICV ownership / operating model versus transient Holiday Inn before IC."],
      ["Leisure Destination Ownership", "Destination leisure sites—validate ownership vs hotel franchise path after Basics create."],
      ["Conversion Into Club Vacations", "Only after authorized Basics create and current ownership disclosure exhibits."],
    ],
    demand: [
      ["Resort & Destination Leisure", M, "Provisional: destination leisure may fit ownership product—confirm after Basics create."],
      ["Conversion & Repositioning", L, "Ownership conversions differ from hotel reflags—confirm path."],
      ["Suburban Business & Leisure", L, "Corporate midweek transient is Holiday Inn / Express territory."],
      ["Meetings & Group Compression", L, "Group meetings usually transient full-service."],
      ["Extended-Stay Corporate", L, "Corporate extended-stay is Staybridge / Candlewood."],
      ["Highway & Interstate Transient", N, "Highway transient sits outside vacation ownership."],
    ],
    ops: "Staged NEW_BRAND_CANDIDATE — vacation ownership / club product. Do not invent hotel franchise fee tables. Confirm One Rewards participation and ownership exhibits after authorized Basics create.",
    footprint: "No Airtable inventory yet. Confirm HICV property counts from current disclosures—not hotel room keys alone.",
    insight: "Holiday Inn Club Vacations is NEW_BRAND_CANDIDATE with ownership economics—authorize Basics create only after ownership vs hotel franchise path is verified.",
    researchClass: "NEW_BRAND_CANDIDATE",
  },
  // ——— Hyatt all-inclusive & wellness ———
  {
    id: "rec0sfR7SrVZUBm18",
    name: "Hyatt Ziva",
    live: "Hyatt Ziva",
    slug: "hyatt-ziva",
    parent: "Hyatt Hotels Corporation",
    loyalty: "World of Hyatt",
    proof: "HYATT_PROOF",
    short: "Hyatt Ziva",
    feeGeo: "Confirm current Hyatt / all-inclusive disclosure for Hyatt Ziva (family AI)",
    brandPage: "https://www.hyatt.com/hyatt-ziva",
    positioning: "Family All-Inclusive Resort",
    commercial:
      "Versus Hyatt Zilara adults-only AI, Ziva sells family all-inclusive; versus Secrets adults-only Hyatt Inclusive Collection, Ziva is family AI with World of Hyatt—do not underwrite as transient select-service.",
    scenarios: [
      ["Family All-Inclusive Destination", "CALA / leisure destinations where family AI and World of Hyatt clear resort contribution."],
      ["Group & Family Leisure Mix", "Markets needing family AI with group leisure potential."],
      ["Sibling Clarity Versus Zilara", "Owners comparing Ziva family AI versus Zilara adults-only."],
    ],
    demand: [
      ["Resort & Destination Leisure", S, "Hyatt Ziva core is family all-inclusive resort leisure with World of Hyatt."],
      ["Group Incentives & Weddings", M, "Group leisure can fit when AI plant and World of Hyatt support mix."],
      ["Conversion & Repositioning", L, "AI conversions require F&B / amenity intensity—not soft reflags."],
      ["Suburban Business & Leisure", N, "Suburban transient business hotels sit outside Ziva AI."],
      ["Airport & Transportation", L, "Airport transient is not Ziva's core lane."],
      ["Highway & Interstate Transient", N, "Highway transient sits outside all-inclusive resorts."],
    ],
    ops: "Hyatt family all-inclusive resort with inclusive F&B intensity and World of Hyatt. Do not apply conventional transient-room franchise economics without AI disclosure adjustment.",
    footprint: "Confirm live resort counts from current Hyatt / Ziva disclosures—primarily CALA / leisure destinations.",
    insight: "Ziva fits family AI resort sponsors with World of Hyatt; fails when Zilara adults-only or Hyatt Place transient fees are assumed.",
  },
  {
    id: "rechIjRoVmm4bo8g1",
    name: "Hyatt Zilara",
    live: "Hyatt Zilara",
    slug: "hyatt-zilara",
    parent: "Hyatt Hotels Corporation",
    loyalty: "World of Hyatt",
    proof: "HYATT_PROOF",
    short: "Hyatt Zilara",
    feeGeo: "Confirm current Hyatt / all-inclusive disclosure for Hyatt Zilara (adults-only AI)",
    brandPage: "https://www.hyatt.com/hyatt-zilara",
    positioning: "Adults-Only All-Inclusive Resort",
    commercial:
      "Versus Hyatt Ziva family AI, Zilara sells adults-only all-inclusive; versus Secrets adults-only Inclusive Collection, confirm sibling clarity inside World of Hyatt.",
    scenarios: [
      ["Adults-Only All-Inclusive Destination", "Leisure destinations where adults-only AI and World of Hyatt clear contribution."],
      ["Couples & Leisure AI Mix", "Markets needing adults-only AI without family product."],
      ["Sibling Clarity Versus Ziva / Secrets", "Owners comparing Zilara versus Ziva and Secrets."],
    ],
    demand: [
      ["Resort & Destination Leisure", S, "Hyatt Zilara core is adults-only all-inclusive with World of Hyatt."],
      ["Group Incentives & Weddings", M, "Adults-only group leisure / weddings can fit when AI plant supports."],
      ["Conversion & Repositioning", L, "AI conversions require F&B intensity—not soft reflags."],
      ["Suburban Business & Leisure", N, "Suburban transient sits outside Zilara AI."],
      ["Airport & Transportation", L, "Airport transient is not Zilara's core lane."],
      ["Highway & Interstate Transient", N, "Highway transient sits outside all-inclusive resorts."],
    ],
    ops: "Hyatt adults-only all-inclusive resort with inclusive F&B intensity and World of Hyatt. Adjust underwriting for AI operating costs versus transient hotels.",
    footprint: "Confirm live resort counts from current Hyatt / Zilara disclosures—CALA / leisure destinations.",
    insight: "Zilara fits adults-only AI with World of Hyatt; fails when Ziva family AI or select-service capital is assumed.",
  },
  {
    id: "recNMxUhbOaIq3so5",
    name: "Secrets Resorts & Spas",
    live: "Secrets Resorts & Spas",
    slug: "secrets-resorts-spas",
    parent: "Hyatt Hotels Corporation",
    loyalty: "World of Hyatt",
    proof: "HYATT_PROOF",
    short: "Secrets",
    feeGeo: "Confirm current Hyatt Inclusive Collection / Secrets disclosure (adults-only AI)",
    brandPage: "https://www.hyatt.com/secrets",
    positioning: "Adults-Only All-Inclusive Resort",
    commercial:
      "Versus Dreams family AI, Secrets sells adults-only Inclusive Collection; versus Breathless nightlife AI, Secrets is couples-forward adults-only with World of Hyatt.",
    scenarios: [
      ["Adults-Only Inclusive Collection", "CALA destinations where Secrets adults-only AI and World of Hyatt clear contribution."],
      ["Weddings & Couples Leisure", "Markets needing adults-only AI with wedding / couples mix."],
      ["Sibling Clarity Versus Dreams / Breathless", "Owners comparing Secrets versus Dreams and Breathless."],
    ],
    demand: [
      ["Resort & Destination Leisure", S, "Secrets core is adults-only all-inclusive resort leisure with World of Hyatt."],
      ["Group Incentives & Weddings", S, "Weddings and couples groups are a Secrets strength when AI plant supports."],
      ["Conversion & Repositioning", L, "AI conversions require F&B / spa intensity."],
      ["Suburban Business & Leisure", N, "Suburban transient sits outside Secrets AI."],
      ["Airport & Transportation", L, "Airport transient is not Secrets' core lane."],
      ["Highway & Interstate Transient", N, "Highway transient sits outside all-inclusive resorts."],
    ],
    ops: "Hyatt Inclusive Collection adults-only all-inclusive with spa / F&B intensity and World of Hyatt. Do not apply transient franchise economics without AI adjustment.",
    footprint: "Confirm live resort counts from current Hyatt / Secrets disclosures—primarily CALA / Caribbean / Mexico.",
    insight: "Secrets fits adults-only AI with World of Hyatt; fails when Dreams family AI or Hyatt Place transient fees are assumed.",
  },
  {
    id: "rec6iaZERtyk8Zxen",
    name: "Dreams Resorts & Spas",
    live: "Dreams Resorts & Spas",
    slug: "dreams-resorts-spas",
    parent: "Hyatt Hotels Corporation",
    loyalty: "World of Hyatt",
    proof: "HYATT_PROOF",
    short: "Dreams",
    feeGeo: "Confirm current Hyatt Inclusive Collection / Dreams disclosure (family AI)",
    brandPage: "https://www.hyatt.com/dreams",
    positioning: "Family All-Inclusive Resort",
    commercial:
      "Versus Secrets adults-only, Dreams sells family Inclusive Collection AI; versus Sunscape value family AI, Dreams sits higher on product intensity inside World of Hyatt.",
    scenarios: [
      ["Family Inclusive Collection", "Destinations where Dreams family AI and World of Hyatt clear contribution."],
      ["Family Leisure & Groups", "Markets needing family AI with group leisure potential."],
      ["Sibling Clarity Versus Secrets / Sunscape", "Owners comparing Dreams versus Secrets and Sunscape."],
    ],
    demand: [
      ["Resort & Destination Leisure", S, "Dreams core is family all-inclusive with World of Hyatt."],
      ["Group Incentives & Weddings", M, "Family groups and leisure incentives can fit when AI plant supports."],
      ["Conversion & Repositioning", L, "AI conversions require F&B intensity."],
      ["Suburban Business & Leisure", N, "Suburban transient sits outside Dreams AI."],
      ["Airport & Transportation", L, "Airport transient is not Dreams' core lane."],
      ["Highway & Interstate Transient", N, "Highway transient sits outside all-inclusive resorts."],
    ],
    ops: "Hyatt Inclusive Collection family all-inclusive with World of Hyatt. Adjust underwriting for inclusive F&B operating intensity.",
    footprint: "Confirm live resort counts from current Hyatt / Dreams disclosures—CALA / leisure destinations.",
    insight: "Dreams fits family AI with World of Hyatt; fails when Secrets adults-only or transient select-service fees are assumed.",
  },
  {
    id: "NEW_zoetry",
    name: "Zoëtry Wellness & Spa Resorts",
    live: "Zoëtry Wellness & Spa Resorts",
    slug: "zoetry-wellness-spa-resorts",
    parent: "Hyatt Hotels Corporation",
    loyalty: "World of Hyatt",
    proof: "HYATT_PROOF",
    short: "Zoëtry",
    feeGeo: "Confirm current Hyatt Inclusive Collection / Zoëtry disclosure AFTER Brand Basics create",
    brandPage: "https://www.hyatt.com/",
    positioning: "Luxury Wellness All-Inclusive",
    commercial:
      "Versus Secrets adults-only AI, Zoëtry sells boutique wellness luxury AI; versus Miraval wellness (non-AI spa resort), Zoëtry is Inclusive Collection wellness AI—confirm after Basics create.",
    scenarios: [
      ["Wellness AI Diligence Gate", "Confirm Zoëtry versus Secrets / Miraval before assuming identical economics."],
      ["Boutique Wellness All-Inclusive", "Destinations needing wellness AI with World of Hyatt—validate after Basics create."],
      ["Conversion Into Zoëtry", "Only after authorized Basics create and AI wellness disclosure exhibits."],
    ],
    demand: [
      ["Resort & Destination Leisure", M, "Provisional: wellness AI leisure may fit—confirm after Basics create."],
      ["Group Incentives & Weddings", L, "Boutique wellness scale may limit large groups—confirm."],
      ["Conversion & Repositioning", L, "Wellness AI conversions require spa / F&B intensity."],
      ["Suburban Business & Leisure", N, "Suburban transient sits outside Zoëtry."],
      ["Airport & Transportation", L, "Airport transient is not Zoëtry's lane."],
      ["Highway & Interstate Transient", N, "Highway transient sits outside wellness AI resorts."],
    ],
    ops: "Staged NEW_BRAND_CANDIDATE — Hyatt Inclusive Collection wellness AI. Do not invent rec ID. Confirm World of Hyatt and AI economics after authorized Basics create.",
    footprint: "No Airtable inventory yet. Confirm from Hyatt Inclusive Collection disclosures before underwriting.",
    insight: "Zoëtry is NEW_BRAND_CANDIDATE—authorize Basics create only after wellness AI ladder position versus Secrets / Miraval is verified.",
    researchClass: "NEW_BRAND_CANDIDATE",
  },
  {
    id: "recY7MP8WBRiLyddc",
    name: "Breathless Resorts & Spas",
    live: "Breathless Resorts & Spas",
    slug: "breathless-resorts-spas",
    parent: "Hyatt Hotels Corporation",
    loyalty: "World of Hyatt",
    proof: "HYATT_PROOF",
    short: "Breathless",
    feeGeo: "Confirm current Hyatt Inclusive Collection / Breathless disclosure (adults-only nightlife AI)",
    brandPage: "https://www.hyatt.com/breathless",
    positioning: "Adults-Only Nightlife All-Inclusive",
    commercial:
      "Versus Secrets couples AI, Breathless sells nightlife-forward adults-only AI; versus Hyatt Zilara, confirm sibling clarity inside World of Hyatt Inclusive Collection.",
    scenarios: [
      ["Nightlife Adults-Only AI", "Destinations where Breathless energy and World of Hyatt clear AI contribution."],
      ["Adults-Only Leisure Mix", "Markets needing nightlife AI without family product."],
      ["Sibling Clarity Versus Secrets / Zilara", "Owners comparing Breathless versus Secrets and Zilara."],
    ],
    demand: [
      ["Resort & Destination Leisure", S, "Breathless core is adults-only nightlife all-inclusive with World of Hyatt."],
      ["Group Incentives & Weddings", M, "Adults-only groups can fit when nightlife AI plant supports."],
      ["Conversion & Repositioning", L, "AI conversions require F&B / nightlife intensity."],
      ["Suburban Business & Leisure", N, "Suburban transient sits outside Breathless AI."],
      ["Airport & Transportation", L, "Airport transient is not Breathless' core lane."],
      ["Highway & Interstate Transient", N, "Highway transient sits outside all-inclusive resorts."],
    ],
    ops: "Hyatt Inclusive Collection adults-only nightlife AI with World of Hyatt. Adjust underwriting for inclusive F&B and entertainment intensity.",
    footprint: "Confirm live resort counts from current Hyatt / Breathless disclosures—CALA leisure destinations.",
    insight: "Breathless fits nightlife adults-only AI with World of Hyatt; fails when Dreams family AI or transient fees are assumed.",
  },
  {
    id: "recI2FAEkY46LSx3m",
    name: "Sunscape Resorts & Spas",
    live: "Sunscape Resorts & Spas",
    slug: "sunscape-resorts-spas",
    parent: "Hyatt Hotels Corporation",
    loyalty: "World of Hyatt",
    proof: "HYATT_PROOF",
    short: "Sunscape",
    feeGeo: "Confirm current Hyatt Inclusive Collection / Sunscape disclosure (value family AI)",
    brandPage: "https://www.hyatt.com/sunscape",
    positioning: "Value Family All-Inclusive",
    commercial:
      "Versus Dreams higher Inclusive Collection family AI, Sunscape sells value family AI; versus Hyatt Ziva, confirm sibling clarity inside World of Hyatt.",
    scenarios: [
      ["Value Family All-Inclusive", "Destinations where Sunscape value family AI and World of Hyatt clear contribution."],
      ["Family Leisure Value Mix", "Markets needing accessible family AI."],
      ["Sibling Clarity Versus Dreams / Ziva", "Owners comparing Sunscape versus Dreams and Ziva."],
    ],
    demand: [
      ["Resort & Destination Leisure", S, "Sunscape core is value family all-inclusive with World of Hyatt."],
      ["Group Incentives & Weddings", M, "Family groups can fit when AI plant supports."],
      ["Conversion & Repositioning", L, "AI conversions still require F&B intensity—even at value ladder."],
      ["Suburban Business & Leisure", N, "Suburban transient sits outside Sunscape AI."],
      ["Airport & Transportation", L, "Airport transient is not Sunscape's core lane."],
      ["Highway & Interstate Transient", N, "Highway transient sits outside all-inclusive resorts."],
    ],
    ops: "Hyatt Inclusive Collection value family all-inclusive with World of Hyatt. Still AI operating intensity—do not import select-service economics.",
    footprint: "Confirm live resort counts from current Hyatt / Sunscape disclosures—CALA leisure destinations.",
    insight: "Sunscape fits value family AI with World of Hyatt; fails when Dreams higher AI fees or transient capital is assumed.",
  },
  {
    id: "recpl5pAKwA6buOat",
    name: "Hyatt Vivid Hotels & Resorts",
    live: "Hyatt Vivid",
    slug: "hyatt-vivid",
    parent: "Hyatt Hotels Corporation",
    loyalty: "World of Hyatt",
    proof: "HYATT_PROOF",
    short: "Hyatt Vivid",
    feeGeo: "Confirm current Hyatt disclosure for Hyatt Vivid (all-inclusive / lifestyle AI growth brand)",
    brandPage: "https://www.hyatt.com/hyatt-vivid",
    positioning: "All-Inclusive Lifestyle Resort",
    commercial:
      "Versus Hyatt Ziva / Zilara established AI, Vivid is a newer Hyatt AI lifestyle ladder rung—do not invent mature operating metrics from launch media; confirm disclosure exhibits.",
    scenarios: [
      ["New AI Lifestyle Diligence", "Destinations where Vivid AI lifestyle and World of Hyatt may clear contribution—validate standards maturity."],
      ["Sibling Clarity Versus Ziva / Inclusive Collection", "Owners comparing Vivid versus Ziva/Zilara and Secrets/Dreams."],
      ["Ground-Up AI Development", "New-builds only when AI capital and World of Hyatt path clear IC."],
    ],
    demand: [
      ["Resort & Destination Leisure", M, "Provisional-to-moderate: Vivid AI leisure may fit when product clears—confirm live standards."],
      ["Group Incentives & Weddings", M, "Groups may fit when AI plant matures."],
      ["Conversion & Repositioning", L, "New brand conversions need verified PIP standards."],
      ["Suburban Business & Leisure", N, "Suburban transient sits outside Vivid AI."],
      ["Airport & Transportation", L, "Airport transient is not Vivid's lane."],
      ["Highway & Interstate Transient", N, "Highway transient sits outside all-inclusive resorts."],
    ],
    ops: "Hyatt all-inclusive lifestyle growth brand (live: Hyatt Vivid) with World of Hyatt. Do not invent mature KPIs from launch media alone.",
    footprint: "Confirm live resort counts and pipeline from current Hyatt / Vivid disclosures—growth brand.",
    insight: "Hyatt Vivid fits AI lifestyle sponsors who validate standards maturity; fails when invented launch metrics or Ziva mature fees are blindly assumed. Preserve live name Hyatt Vivid.",
  },
  {
    id: "NEW_alua",
    name: "Alua Hotels & Resorts",
    live: "Alua Hotels & Resorts",
    slug: "alua-hotels-resorts",
    parent: "Hyatt Hotels Corporation",
    loyalty: "World of Hyatt",
    proof: "HYATT_PROOF",
    short: "Alua",
    feeGeo: "Confirm current Hyatt / Alua disclosure (EMEA leisure / AI-adjacent) AFTER Brand Basics create",
    brandPage: "https://www.hyatt.com/",
    positioning: "Leisure Resort EMEA",
    commercial:
      "Versus Hyatt Inclusive Collection CALA AI brands, Alua is Hyatt's EMEA leisure / resort platform—confirm AI vs European leisure operating model after Basics create; do not invent fee tables.",
    scenarios: [
      ["EMEA Leisure Diligence Gate", "Confirm Alua versus Inclusive Collection CALA AI before assuming identical economics."],
      ["European Leisure Resort", "EMEA leisure destinations—validate World of Hyatt path after Basics create."],
      ["Conversion Into Alua", "Only after authorized Basics create and current disclosure exhibits."],
    ],
    demand: [
      ["Resort & Destination Leisure", M, "Provisional: EMEA leisure resorts may fit—confirm after Basics create."],
      ["Group Incentives & Weddings", M, "Leisure groups may fit when plant supports."],
      ["Conversion & Repositioning", L, "Confirm conversion path after standards are live."],
      ["Suburban Business & Leisure", L, "Suburban transient usually not Alua's core."],
      ["Airport & Transportation", L, "Airport transient secondary."],
      ["Highway & Interstate Transient", N, "Highway transient sits outside leisure resorts."],
    ],
    ops: "Staged NEW_BRAND_CANDIDATE — Hyatt EMEA leisure / Alua platform. Do not invent rec ID. Confirm World of Hyatt and AI-vs-leisure model after authorized Basics create.",
    footprint: "No Airtable inventory yet. Confirm from Hyatt / Alua disclosures—primarily EMEA.",
    insight: "Alua is NEW_BRAND_CANDIDATE—authorize Basics create only after EMEA leisure ladder position versus Inclusive Collection is verified.",
    researchClass: "NEW_BRAND_CANDIDATE",
  },
  {
    id: "recdgXCQU6GIc0l5Z",
    name: "Miraval Resorts & Spas",
    live: "Miraval",
    slug: "miraval-resorts-spas",
    parent: "Hyatt Hotels Corporation",
    loyalty: "World of Hyatt",
    proof: "HYATT_PROOF",
    short: "Miraval",
    feeGeo: "Confirm current Hyatt / Miraval affiliation disclosure (wellness destination resorts)",
    brandPage: "https://www.miravalresorts.com/",
    positioning: "Luxury Wellness Destination",
    commercial:
      "Versus Zoëtry wellness AI, Miraval sells destination wellness spa resort (not Inclusive Collection AI); versus Park Hyatt intimate luxury, Miraval is wellness-program intensive with World of Hyatt.",
    scenarios: [
      ["Luxury Wellness Destination", "Destinations where Miraval wellness programming and World of Hyatt clear contribution."],
      ["Wellness Retreat & Spa", "Assets that can fund spa / wellness intensity matching Miraval."],
      ["Sibling Clarity Versus Zoëtry / Park Hyatt", "Owners comparing Miraval wellness versus AI wellness and Park Hyatt luxury."],
    ],
    demand: [
      ["Resort & Destination Leisure", S, "Miraval core is destination wellness leisure with World of Hyatt."],
      ["Conversion & Repositioning", L, "Wellness destination conversions require spa / program intensity—not soft reflags."],
      ["Group Incentives & Weddings", M, "Wellness groups / retreats can fit at Miraval scale."],
      ["Suburban Business & Leisure", N, "Suburban transient sits outside Miraval wellness destination."],
      ["Airport & Transportation", L, "Airport transient is not Miraval's lane."],
      ["Highway & Interstate Transient", N, "Highway transient sits outside wellness destination resorts."],
    ],
    ops: "Hyatt luxury wellness destination resorts (live: Miraval) with World of Hyatt. Not all-inclusive Inclusive Collection economics—confirm wellness operating model and affiliation exhibits.",
    footprint: "Confirm live resort counts from current Miraval / Hyatt disclosures—select curated wellness destinations.",
    insight: "Miraval fits wellness destination sponsors with World of Hyatt; fails when AI Inclusive Collection or select-service fees are assumed. Preserve live name Miraval.",
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
lines.push(` * Factory 2.0 Batch 13 — IHG premium/suites + Hyatt all-inclusive & wellness.`);
lines.push(` * NEW_BRAND_CANDIDATE packs use provisional NEW_<slug> keys — never invent Airtable rec IDs.`);
lines.push(` * AI resort economics ≠ transient hotel franchise tables. Fee amounts never invented.`);
lines.push(` */`);
lines.push(`import { DEMAND_FIT_LABELS } from "../be-factory-permanent-demand-scenarios-six-v1.js";`);
lines.push(`import { IHG_PROOF, HYATT_PROOF } from "./be-factory-2-depth-loyalty-proofs-v1.js";`);
lines.push(``);
lines.push(`function pack(base) {`);
lines.push(`  return Object.freeze({ researchSpendUsd: 0, paidCalls: 0, loyaltyProofRequired: 6, batchTag: "batch-13", ...base });`);
lines.push(`}`);
lines.push(`function demand(rows) { return rows.map((r) => ({ title: r.title, fit: r.fit, explanation: r.explanation })); }`);
lines.push(`function gallery(brand, url) {`);
lines.push(`  return [`);
lines.push(`    { title: \`\${brand} — Exterior / Arrival\`, body: "Exterior presence · confirm property photography before publication", source: url },`);
lines.push(`    { title: \`\${brand} — Guest Room / Suite\`, body: "Guest-room or suite product typical of this ladder rung", source: url },`);
lines.push(`    { title: \`\${brand} — Lobby / Public Space\`, body: "Public space supporting the brand promise", source: url },`);
lines.push(`    { title: \`\${brand} — F&B / Amenity Cue\`, body: "F&B, spa, or amenity cues where required by standards", source: url },`);
lines.push(`    { title: \`\${brand} — Meetings / Pool / Flexibility Cue\`, body: "Meetings, pool, kitchen, or AI amenity cues typical of this product", source: url },`);
lines.push(`    { title: \`\${brand} — Context / Setting\`, body: "Urban, resort, destination, or corridor context where typical", source: url },`);
lines.push(`  ];`);
lines.push(`}`);
lines.push(`export const FACTORY_2_BATCH_13_V1 = "FACTORY_2_BATCH_13_V1";`);
lines.push(`const FDD = "Confirm current franchise disclosure / FDD exhibits for application, royalty, marketing, loyalty, and PIP. Distinguish historical FDD figures from the live agreement. Amounts are not staged without a verified disclosure. For all-inclusive / vacation ownership, confirm AI or ownership exhibits—do not import transient hotel royalty tables blindly.";`);
lines.push(`const S = DEMAND_FIT_LABELS.STRONG_FIT;`);
lines.push(`const M = DEMAND_FIT_LABELS.MODERATE_FIT;`);
lines.push(`const L = DEMAND_FIT_LABELS.LIMITED_FIT;`);
lines.push(`const N = DEMAND_FIT_LABELS.NOT_A_FIT;`);
lines.push(`export const BATCH_13_IDS = Object.freeze([`);
for (const b of brands) lines.push(`  "${b.id}", // ${b.name}`);
lines.push(`]);`);
lines.push(`export const BATCH_13_PACKS_BY_ID = Object.freeze({`);

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
  lines.push(`      { title: "Brand Product Standards", body: "Rooms / suites / resort product must clear ${esc(b.short)} standards for the operating region." },`);
  lines.push(`      { title: "Conversion / PIP Discipline", body: "PIP or membership quality scope must match the brand promise—not under-scoped capital." },`);
  lines.push(`      { title: "Loyalty / Network Integration", body: "Confirm ${esc(b.loyalty)} delivery obligations for this brand before IC." },`);
  lines.push(`    ],`);
  lines.push(`    valueOwnerScenarios: [`);
  lines.push(`      { title: "Primary Owner Corridor", body: "Sites where ${esc(b.short)} ADR / contribution clears IC versus local comps." },`);
  lines.push(`      { title: "Conversion / Affiliation Path", body: "Assets where ${esc(b.short)} affiliation clears IC versus sibling alternatives." },`);
  lines.push(`      { title: "Disclosure Fee Gate", body: "Model only from current disclosure / AI / ownership exhibits for ${esc(b.short)}." },`);
  lines.push(`      { title: "Sibling Clarity", body: "Diligence versus sibling flags / networks before IC." },`);
  lines.push(`    ],`);
  lines.push(`    gallery: gallery(${JSON.stringify(b.short)}, ${JSON.stringify(b.brandPage)}),`);
  lines.push(`  }),`);
  lines.push(``);
}
lines.push(`});`);
lines.push(`export function listBatch13() { return BATCH_13_IDS.map((id) => BATCH_13_PACKS_BY_ID[id]).filter(Boolean); }`);
lines.push(`export function getBatch13Pack(brandId) { return BATCH_13_PACKS_BY_ID[brandId] || null; }`);
lines.push(``);

writeFileSync(outPath, lines.join("\n"));
console.log(`Wrote ${outPath} (${brands.length} packs)`);
