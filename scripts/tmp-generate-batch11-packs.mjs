#!/usr/bin/env node
/**
 * One-shot generator for Batch 11 depth packs.
 */
import { writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");
const outPath = join(
  ROOT,
  "lib/brand-explorer/factory-2/be-factory-2-depth-research-packs-batch11-v1.js"
);

const S = "S";
const M = "M";
const L = "L";
const N = "N";

/** @type {Array<object>} */
const brands = [
  // ——— Part A: five EXISTING Active (original inventory remainder) ———
  {
    id: "recWPEvxBQxVVzSq3",
    name: "Radisson Blu by Choice",
    live: "Radisson Blu by Choice",
    slug: "radisson-blu-by-choice",
    parent: "Choice Hotels International",
    loyalty: "Choice Privileges",
    proof: "CHOICE_PROOF",
    short: "Radisson Blu",
    feeGeo: "Confirm current Choice Hotels FDD / development exhibits for Radisson Blu (upscale full-service)",
    brandPage: "https://www.choicehotels.com/radisson-blu",
    positioning: "Upscale Full-Service",
    commercial:
      "Versus Radisson by Choice classic upscale, Blu sells a more design-forward upscale full-service lane; versus Radisson RED lifestyle, Blu keeps fuller service and meetings capability inside Choice Privileges.",
    scenarios: [
      ["Upscale Full-Service Urban", "Gateway and secondary cities where Blu product and Privileges clear upscale ADR."],
      ["Meetings-Capable Upscale", "Assets with credible meetings plant that can fund Blu standards."],
      ["Conversion Into Radisson Blu", "Conversions that can deliver upscale public space without RED-only lifestyle underwriting."],
    ],
    demand: [
      ["Suburban Business & Leisure", S, "Radisson Blu fits upscale business/leisure mix with Choice Privileges when product clears Blu standards."],
      ["Meetings & Group Compression", S, "Meetings-capable plant is a Blu strength versus RED lifestyle."],
      ["Airport & Transportation", M, "Airport upscale hubs can fit when service intensity matches Blu."],
      ["Conversion & Repositioning", M, "Conversions succeed when PIP funds upscale full-service—not midscale paint."],
      ["Urban Lifestyle & Design", L, "Pure lifestyle design intensity is more RED than Blu."],
      ["Highway & Interstate Transient", N, "Highway limited-service transient sits outside Radisson Blu upscale full-service."],
    ],
    ops: "Choice upscale full-service Radisson Blu with meetings capability and Choice Privileges. Confirm franchise vs management path from current exhibits—do not invent royalty percentages.",
    footprint: "Confirm live hotel counts from current Choice Hotels / Radisson Blu disclosures—do not underwrite from launch media alone.",
    insight: "Radisson Blu fits owners funding upscale full-service with Privileges; fails when RED lifestyle or midscale Comfort economics are assumed.",
  },
  {
    id: "recOzH5iAE1xEjyD0",
    name: "Comfort Inn & Suites",
    live: "Comfort Inn & Suites",
    slug: "comfort-inn-suites",
    parent: "Choice Hotels International",
    loyalty: "Choice Privileges",
    proof: "CHOICE_PROOF",
    short: "Comfort Inn & Suites",
    feeGeo: "Confirm current Choice Hotels FDD for Comfort Inn & Suites (midscale limited-service / suites mix)",
    brandPage: "https://www.choicehotels.com/comfort-inn",
    positioning: "Midscale Limited-Service",
    commercial:
      "Versus Quality Inn midscale, Comfort Inn & Suites is the Choice midscale workhorse with suite-capable product; versus Cambria upscale, it keeps leaner capital and breakfast-led limited-service economics.",
    scenarios: [
      ["Corridor Midscale Limited-Service", "Highway and suburban corridors where Comfort product and Privileges compete on value."],
      ["Airport & Gateway Value", "Airport-adjacent value stays needing Choice distribution without upscale capital."],
      ["Conversion Into Comfort Midscale", "Conversions that can fund Comfort standards and breakfast delivery."],
    ],
    demand: [
      ["Highway & Interstate Transient", S, "Comfort Inn & Suites is built for corridor midscale transient with Privileges."],
      ["Suburban Business & Leisure", S, "Suburban midweek/weekend mix is a core Comfort use case."],
      ["Airport & Transportation", S, "Airport-edge value midscale fits when product clears Comfort standards."],
      ["Conversion & Repositioning", M, "Conversions succeed when PIP funds midscale rooms and breakfast—not economy-only scopes."],
      ["Extended-Stay Corporate", L, "True extended-stay kitchens are WoodSpring / Suburban territory, not Comfort suites marketing alone."],
      ["Luxury Urban Flagship", N, "Luxury urban flagship sits outside Comfort midscale limited-service."],
    ],
    ops: "Choice midscale limited-service with breakfast and Choice Privileges. Confirm FDD exhibits—do not invent royalty percentages.",
    footprint: "Confirm live hotel counts from current Choice Hotels disclosures—Comfort is a scaled midscale flag.",
    insight: "Comfort Inn & Suites fits efficient midscale limited-service with Privileges; wrong when sponsors underwrite Cambria upscale or WoodSpring extended-stay economics.",
  },
  {
    id: "recRyvM8OmLlDj9G7",
    name: "Radisson Individuals by Choice",
    live: "Radisson Individuals by Choice",
    slug: "radisson-individuals-by-choice",
    parent: "Choice Hotels International",
    loyalty: "Choice Privileges",
    proof: "CHOICE_PROOF",
    short: "Radisson Individuals",
    feeGeo: "Confirm current Choice Hotels soft-collection / affiliation exhibits for Radisson Individuals",
    brandPage: "https://www.choicehotels.com/radisson-individuals",
    positioning: "Upscale Independent Collection",
    commercial:
      "Versus Radisson Blu hard upscale brand, Individuals sells independent character inside Choice; versus Preferred / SLH membership networks, it rides Choice Privileges rather than pure membership distribution.",
    scenarios: [
      ["Independent Character Conversion", "Distinctive upscale independents needing Privileges without hard Radisson prototyping."],
      ["Soft-Collection Diligence", "Owners comparing Individuals versus Blu / RED hard brands and other soft lanes."],
      ["Heritage & Adaptive Reuse", "Heritage assets where individuality is the product and Choice systems expand distribution."],
    ],
    demand: [
      ["Conversion & Repositioning", S, "Radisson Individuals is built for character conversions that keep independent story with Privileges."],
      ["Urban Lifestyle & Design", S, "Design-led independents fit when collection standards clear."],
      ["Suburban Business & Leisure", M, "Suburban mix can work when product clears soft-collection standards—not economy capital."],
      ["Meetings & Group Compression", L, "Large meetings compression is usually a hard Radisson Blu / Radisson lane."],
      ["Airport & Transportation", L, "Airport hard-prototype midscale/upscale is not the soft-collection lane."],
      ["Highway & Interstate Transient", N, "Highway limited-service transient sits outside upscale independent collection."],
    ],
    ops: "Choice soft / independent collection — distinctive upscale conversions with Choice Privileges. Do not underwrite as a hard Radisson Blu prototype franchise.",
    footprint: "Confirm live hotel counts from current Choice / Radisson Individuals disclosures—curated versus mass Choice midscale flags.",
    insight: "Radisson Individuals fits owners who need Choice soft-collection distribution for distinctive upscale assets; fails when Blu hard-brand fees or invented royalty percentages are assumed.",
  },
  {
    id: "recaayt9u7YYg8h7Y",
    name: "Country Inn & Suites by Choice",
    live: "Country Inn & Suites by Choice",
    slug: "country-inn-suites-by-choice",
    parent: "Choice Hotels International",
    loyalty: "Choice Privileges",
    proof: "CHOICE_PROOF",
    short: "Country Inn & Suites",
    feeGeo: "Confirm current Choice Hotels FDD for Country Inn & Suites (midscale limited-service)",
    brandPage: "https://www.choicehotels.com/country-inn",
    positioning: "Midscale Limited-Service",
    commercial:
      "Versus Comfort Inn & Suites, Country Inn leans warmer residential midscale cues; versus Clarion full-service midscale, it keeps limited-service capital inside Choice Privileges.",
    scenarios: [
      ["Suburban Midscale Limited-Service", "Suburban corridors where Country Inn product and Privileges clear midscale ADR."],
      ["Family & Leisure Midscale", "Markets where residential midscale cues support leisure mix."],
      ["Conversion Into Country Inn", "Conversions that can fund Country Inn standards without Clarion full-service capital."],
    ],
    demand: [
      ["Suburban Business & Leisure", S, "Country Inn core is suburban midscale business/leisure with Privileges."],
      ["Highway & Interstate Transient", S, "Corridor midscale transient fits when product clears Country Inn standards."],
      ["Regional & Secondary Markets", S, "Secondary markets fit when Privileges and midscale product defend ADR."],
      ["Conversion & Repositioning", M, "Conversions succeed when PIP funds midscale—not economy-only scopes."],
      ["Meetings & Group Compression", L, "Large meetings plant is more Clarion / Radisson territory."],
      ["Luxury Urban Flagship", N, "Luxury urban flagship sits outside Country Inn midscale."],
    ],
    ops: "Choice midscale limited-service with breakfast and Choice Privileges. Confirm FDD exhibits—do not invent royalty percentages.",
    footprint: "Confirm live hotel counts from current Choice Hotels disclosures.",
    insight: "Country Inn & Suites fits residential midscale limited-service with Privileges; wrong when Clarion full-service or Cambria upscale capital is assumed.",
  },
  {
    id: "recsOd51NzRPYsMko",
    name: "WoodSpring Suites",
    live: "WoodSpring Suites",
    slug: "woodspring-suites",
    parent: "Choice Hotels International",
    loyalty: "Choice Privileges",
    proof: "CHOICE_PROOF",
    short: "WoodSpring",
    feeGeo: "Confirm current Choice Hotels FDD for WoodSpring Suites (economy / midscale extended-stay)",
    brandPage: "https://www.choicehotels.com/woodspring-suites",
    positioning: "Economy Extended-Stay",
    commercial:
      "Versus Suburban Studios / Everhome midscale extended-stay, WoodSpring sells leaner economy extended-stay; versus Comfort transient midscale, it is length-of-stay kitchenette economics inside Choice Privileges.",
    scenarios: [
      ["Economy Extended-Stay Corporate", "Markets with multi-week demand needing kitchenettes at economy capital."],
      ["Project / Workforce Longer Stay", "Project and workforce corridors needing value extended-stay."],
      ["Conversion Into WoodSpring Suites", "Conversions that can fund suite/kitchen PIP at WoodSpring standards."],
    ],
    demand: [
      ["Extended-Stay Corporate", S, "WoodSpring core is economy extended-stay with kitchens and Privileges."],
      ["Project & Workforce Demand", S, "Project/workforce weekly stays are a primary WoodSpring pocket."],
      ["University & Medical", M, "Medical/university longer stays can fit when rates clear economy extended-stay IC."],
      ["Suburban Business & Leisure", M, "Suburban midweek extended-stay works; pure weekend leisure is secondary."],
      ["Highway & Interstate Transient", L, "One-night highway transient is a weak fit versus Comfort / Quality."],
      ["Luxury Urban Flagship", N, "Luxury urban flagship sits outside WoodSpring economy extended-stay."],
    ],
    ops: "Choice economy extended-stay with kitchenettes and Choice Privileges. Housekeeping cadence differs from transient midscale—confirm FDD exhibits.",
    footprint: "Confirm live hotel counts from current Choice Hotels / WoodSpring disclosures.",
    insight: "WoodSpring fits sponsors with genuine extended-stay demand and lean kitchenette capital; fails when underwritten like Comfort transient without suite PIP.",
  },

  // ——— Part B: existing Under Review / Draft ———
  {
    id: "recAT3p3d7LAZZNkb",
    name: "Cambria Hotels",
    live: "Cambria Hotels",
    slug: "cambria-hotels",
    parent: "Choice Hotels International",
    loyalty: "Choice Privileges",
    proof: "CHOICE_PROOF",
    short: "Cambria",
    feeGeo: "Confirm current Choice Hotels FDD for Cambria Hotels (upscale select-service)",
    brandPage: "https://www.choicehotels.com/cambria",
    positioning: "Upscale Select-Service",
    commercial:
      "Versus Comfort midscale limited-service, Cambria sells upscale select-service design and F&B lite; versus Radisson Blu full-service, it keeps tighter capital inside Choice Privileges.",
    scenarios: [
      ["Urban Upscale Select-Service", "Urban and mixed-use sites where Cambria design and Privileges lift select-service rates."],
      ["Airport & Gateway Upscale Select", "Airport-adjacent assets needing upscale select-service without full-service capital."],
      ["Conversion Into Cambria", "Conversions that can fund Cambria public-space and room standards."],
    ],
    demand: [
      ["Suburban Business & Leisure", S, "Cambria fits upscale select-service business/leisure with Privileges."],
      ["Airport & Transportation", S, "Airport-edge upscale select-service is a common Cambria use case."],
      ["Urban Lifestyle & Design", S, "Design-led urban select-service is a Cambria strength."],
      ["Conversion & Repositioning", M, "Conversions succeed when PIP funds upscale select-service—not midscale Comfort scopes."],
      ["Meetings & Group Compression", L, "Large meetings compression is limited versus full-service Radisson Blu."],
      ["Highway & Interstate Transient", N, "Hard highway midscale transient sits below Cambria upscale select-service."],
    ],
    ops: "Choice upscale select-service with design-forward public space and Choice Privileges. Confirm FDD exhibits—do not invent royalty percentages.",
    footprint: "Confirm live hotel counts from current Choice Hotels / Cambria disclosures.",
    insight: "Cambria fits owners funding upscale select-service with Privileges; wrong when Comfort midscale capital or Radisson full-service fees are assumed.",
  },
  {
    id: "recq3vCeOn8lQdY4P",
    name: "Clarion Pointe",
    live: "Clarion Pointe",
    slug: "clarion-pointe",
    parent: "Choice Hotels International",
    loyalty: "Choice Privileges",
    proof: "CHOICE_PROOF",
    short: "Clarion Pointe",
    feeGeo: "Confirm current Choice Hotels FDD for Clarion Pointe (midscale limited-service)",
    brandPage: "https://www.choicehotels.com/clarion-pointe",
    positioning: "Midscale Limited-Service",
    commercial:
      "Versus Clarion full-service midscale, Clarion Pointe is the leaner limited-service Clarion ladder; versus Comfort, it is Clarion-branded midscale limited-service inside Privileges.",
    scenarios: [
      ["Corridor Midscale Limited-Service", "Highway and suburban corridors needing Clarion Pointe product and Privileges."],
      ["Conversion Into Clarion Pointe", "Conversions that can fund Pointe standards without Clarion full-service capital."],
      ["Sibling Clarity Versus Clarion", "Owners comparing Pointe limited-service versus Clarion full-service."],
    ],
    demand: [
      ["Highway & Interstate Transient", S, "Clarion Pointe is built for corridor midscale limited-service transient."],
      ["Suburban Business & Leisure", S, "Suburban midweek/weekend mix fits Pointe when product clears standards."],
      ["Regional & Secondary Markets", S, "Secondary markets fit when Privileges defend midscale ADR."],
      ["Conversion & Repositioning", M, "Conversions succeed when PIP funds midscale limited-service."],
      ["Meetings & Group Compression", L, "Meetings plant is Clarion full-service territory."],
      ["Luxury Urban Flagship", N, "Luxury urban flagship sits outside Clarion Pointe."],
    ],
    ops: "Choice midscale limited-service Clarion Pointe with breakfast and Choice Privileges. Do not import Clarion full-service fee or staffing assumptions.",
    footprint: "Confirm live hotel counts from current Choice Hotels disclosures.",
    insight: "Clarion Pointe fits lean midscale limited-service with Privileges; fails when Clarion full-service meetings economics are assumed.",
  },
  {
    id: "recsbxr7pMGMN0N6R",
    name: "Clarion",
    live: "Clarion",
    slug: "clarion",
    parent: "Choice Hotels International",
    loyalty: "Choice Privileges",
    proof: "CHOICE_PROOF",
    short: "Clarion",
    feeGeo: "Confirm current Choice Hotels FDD for Clarion (midscale full-service / meetings-capable)",
    brandPage: "https://www.choicehotels.com/clarion",
    positioning: "Midscale Full-Service",
    commercial:
      "Versus Clarion Pointe limited-service, Clarion sells midscale full-service and meetings; versus Radisson Blu upscale, it is a lower full-service ladder inside Choice Privileges.",
    scenarios: [
      ["Midscale Full-Service Meetings", "Secondary cities where meetings ADR clears Clarion full-service IC."],
      ["Suburban Full-Service Utility", "Suburban assets needing F&B and meetings without upscale Radisson capital."],
      ["Conversion Into Clarion Full-Service", "Conversions that can fund Clarion public-space and meetings PIP."],
    ],
    demand: [
      ["Meetings & Group Compression", S, "Clarion is built for midscale meetings and full-service utility with Privileges."],
      ["Suburban Business & Leisure", S, "Suburban full-service mix is a core Clarion use case."],
      ["Airport & Transportation", M, "Airport midscale full-service hubs can fit Clarion."],
      ["Conversion & Repositioning", M, "Conversions succeed when PIP funds full-service—not Pointe limited-service scopes."],
      ["Highway & Interstate Transient", L, "Hard limited-service highway transient is Pointe / Comfort territory."],
      ["Luxury Urban Flagship", N, "Luxury urban flagship sits outside Clarion midscale full-service."],
    ],
    ops: "Choice midscale full-service with meetings capability and Choice Privileges. Confirm FDD exhibits—do not invent royalty percentages.",
    footprint: "Confirm live hotel counts from current Choice Hotels / Clarion disclosures.",
    insight: "Clarion fits midscale full-service meetings assets with Privileges; wrong when Clarion Pointe limited-service capital is assumed.",
  },
  {
    id: "recWpSnsKxoUTcWs2",
    name: "Wyndham Grand",
    live: "Wyndham Grand",
    slug: "wyndham-grand",
    parent: "Wyndham Hotels & Resorts",
    loyalty: "Wyndham Rewards",
    proof: "WYNDHAM_PROOF",
    short: "Wyndham Grand",
    feeGeo: "Confirm current Wyndham franchise / development disclosure for Wyndham Grand (upscale full-service)",
    brandPage: "https://www.wyndhamhotels.com/wyndham-grand",
    positioning: "Upscale Full-Service",
    commercial:
      "Versus Trademark soft-brand collection, Wyndham Grand is a hard upscale full-service brand; versus Dolce meetings-intensive, Grand sells broader upscale full-service with Wyndham Rewards.",
    scenarios: [
      ["Upscale Full-Service Urban", "Gateway and secondary cities where Grand product and Rewards clear upscale ADR."],
      ["Meetings-Capable Upscale", "Assets with meetings plant that can fund Wyndham Grand standards."],
      ["Conversion Into Wyndham Grand", "Conversions that can deliver upscale full-service PIP."],
    ],
    demand: [
      ["Suburban Business & Leisure", S, "Wyndham Grand fits upscale business/leisure with Rewards when product clears Grand standards."],
      ["Meetings & Group Compression", S, "Meetings-capable plant is a Grand strength."],
      ["Airport & Transportation", M, "Airport upscale hubs can fit Wyndham Grand."],
      ["Conversion & Repositioning", M, "Conversions succeed when PIP funds upscale full-service."],
      ["Urban Lifestyle & Design", L, "Pure lifestyle design is more TRYP / Dazzler territory."],
      ["Highway & Interstate Transient", N, "Highway limited-service transient sits outside Wyndham Grand."],
    ],
    ops: "Wyndham upscale full-service with meetings capability and Wyndham Rewards. Confirm franchise exhibits—do not invent royalty percentages.",
    footprint: "Confirm live hotel counts from current Wyndham Hotels disclosures.",
    insight: "Wyndham Grand fits owners funding upscale full-service with Rewards; fails when Trademark soft-brand or midscale fees are assumed.",
  },
  {
    id: "rec4w2XsL3yDUsv2C",
    name: "Dolce Hotels & Resorts by Wyndham",
    live: "Dolce Hotels and Resorts by Wyndham",
    slug: "dolce-hotels-resorts-by-wyndham",
    parent: "Wyndham Hotels & Resorts",
    loyalty: "Wyndham Rewards",
    proof: "WYNDHAM_PROOF",
    short: "Dolce",
    feeGeo: "Confirm current Wyndham franchise / development disclosure for Dolce (meetings-intensive upscale)",
    brandPage: "https://www.wyndhamhotels.com/dolce",
    positioning: "Meetings Intensive Upscale",
    commercial:
      "Versus Wyndham Grand broader upscale full-service, Dolce sells meetings- and conference-intensive product; versus TRYP lifestyle, it is group-compression economics inside Wyndham Rewards.",
    scenarios: [
      ["Conference & Meetings Destination", "Sites where conference plant and Rewards underwrite Dolce contribution."],
      ["Corporate Campus Meetings", "Campus-adjacent assets needing meetings intensity without luxury capital."],
      ["Conversion Into Dolce Meetings", "Conversions that can fund meetings plant and Dolce standards."],
    ],
    demand: [
      ["Meetings & Group Compression", S, "Dolce is built for meetings and conference compression with Wyndham Rewards."],
      ["Suburban Business & Leisure", M, "Suburban mix works when meetings plant is real—not hotel-as-rooms-only."],
      ["Airport & Transportation", M, "Airport-edge conference hotels can fit Dolce."],
      ["Conversion & Repositioning", M, "Conversions succeed when PIP funds meetings capacity."],
      ["Urban Lifestyle & Design", L, "Lifestyle design intensity is TRYP / Dazzler territory."],
      ["Highway & Interstate Transient", N, "Highway transient sits outside Dolce meetings-intensive upscale."],
    ],
    ops: "Wyndham meetings-intensive upscale / conference hotel brand with Wyndham Rewards. Confirm franchise exhibits—do not invent royalty percentages.",
    footprint: "Confirm live hotel counts from current Wyndham / Dolce disclosures—portfolio is meetings-curated versus mass midscale flags.",
    insight: "Dolce fits sponsors with genuine meetings plant and Rewards demand; fails when underwritten like TRYP lifestyle or limited-service transient.",
  },
  {
    id: "recXIwHYrvJ89a1yK",
    name: "TRYP by Wyndham",
    live: "TRYP by Wyndham",
    slug: "tryp-by-wyndham",
    parent: "Wyndham Hotels & Resorts",
    loyalty: "Wyndham Rewards",
    proof: "WYNDHAM_PROOF",
    short: "TRYP",
    feeGeo: "Confirm current Wyndham franchise / development disclosure for TRYP (upscale lifestyle)",
    brandPage: "https://www.wyndhamhotels.com/tryp",
    positioning: "Upscale Lifestyle",
    commercial:
      "Versus Wyndham Grand full-service, TRYP sells urban lifestyle design; versus Dazzler / Esplendor regional lifestyle, TRYP is the global Wyndham lifestyle node with Rewards.",
    scenarios: [
      ["Urban Lifestyle Upscale", "City sites where lifestyle design and Rewards lift TRYP rates."],
      ["Gateway Lifestyle Conversion", "Conversions that can fund TRYP public-space and design standards."],
      ["Sibling Clarity Versus Grand / Dolce", "Owners comparing lifestyle TRYP versus Grand full-service or Dolce meetings."],
    ],
    demand: [
      ["Urban Lifestyle & Design", S, "TRYP core is upscale urban lifestyle with Wyndham Rewards."],
      ["Conversion & Repositioning", M, "Conversions succeed when design PIP clears lifestyle standards."],
      ["Suburban Business & Leisure", L, "Suburban select-service economics usually miss TRYP lifestyle intensity."],
      ["Airport & Transportation", L, "Airport hard-prototype is not TRYP's core lane."],
      ["Meetings & Group Compression", L, "Large meetings compression is Dolce / Grand territory."],
      ["Highway & Interstate Transient", N, "Highway transient sits outside upscale lifestyle."],
    ],
    ops: "Wyndham upscale lifestyle brand with design-forward public space and Wyndham Rewards. Confirm franchise exhibits—do not invent royalty percentages.",
    footprint: "Confirm live hotel counts from current Wyndham / TRYP disclosures across Americas, EMEA, and APAC.",
    insight: "TRYP fits owners funding urban lifestyle with Rewards; fails when Grand full-service or midscale fees are assumed.",
  },
  {
    id: "recE5LoJH1EaszkqG",
    name: "Esplendor by Wyndham",
    live: "Esplendor by Wyndham",
    slug: "esplendor-by-wyndham",
    parent: "Wyndham Hotels & Resorts",
    loyalty: "Wyndham Rewards",
    proof: "WYNDHAM_PROOF",
    short: "Esplendor",
    feeGeo: "Confirm current Wyndham franchise / development disclosure for Esplendor (LATAM / CALA upscale lifestyle)",
    brandPage: "https://www.wyndhamhotels.com/esplendor",
    positioning: "Upscale Lifestyle LATAM",
    commercial:
      "Versus TRYP global lifestyle, Esplendor is the LATAM / CALA upscale lifestyle node; versus Dazzler, confirm sibling clarity on regional lifestyle positioning inside Wyndham Rewards.",
    scenarios: [
      ["LATAM Urban Lifestyle", "CALA / LATAM urban markets where Esplendor design and Rewards clear upscale ADR."],
      ["Gateway Lifestyle Conversion", "Conversions that can fund Esplendor standards in regional gateways."],
      ["Sibling Clarity Versus TRYP / Dazzler", "Owners comparing Esplendor versus TRYP and Dazzler lifestyle lanes."],
    ],
    demand: [
      ["Urban Lifestyle & Design", S, "Esplendor core is LATAM / CALA upscale lifestyle with Wyndham Rewards."],
      ["Regional & Secondary Markets", S, "Regional gateways in CALA / LATAM fit when product clears Esplendor standards."],
      ["Conversion & Repositioning", M, "Conversions succeed when design PIP clears lifestyle standards."],
      ["Suburban Business & Leisure", L, "Suburban select-service economics usually miss Esplendor lifestyle intensity."],
      ["Meetings & Group Compression", L, "Large meetings compression is Dolce / Grand territory."],
      ["Highway & Interstate Transient", N, "Highway transient sits outside upscale lifestyle."],
    ],
    ops: "Wyndham upscale lifestyle brand focused on LATAM / CALA with Wyndham Rewards. Confirm franchise exhibits—do not invent royalty percentages.",
    footprint: "Confirm live hotel counts from current Wyndham / Esplendor disclosures—primarily CALA / LATAM.",
    insight: "Esplendor fits LATAM / CALA lifestyle assets with Rewards; fails when North American midscale or Dolce meetings economics are assumed.",
  },
  {
    id: "rec72kWHpqp5awuZY",
    name: "Registry Collection Hotels",
    live: "Registry Collection Hotels",
    slug: "registry-collection-hotels",
    parent: "Wyndham Hotels & Resorts",
    loyalty: "Wyndham Rewards",
    proof: "WYNDHAM_PROOF",
    short: "Registry Collection",
    feeGeo: "Confirm current Wyndham soft-collection / affiliation exhibits for Registry Collection",
    brandPage: "https://www.wyndhamhotels.com/registry-collection",
    positioning: "Upscale Independent Collection",
    commercial:
      "Versus Trademark soft-brand collection, Registry Collection sits higher on upscale/luxury-adjacent independent character; versus Wyndham Grand hard brand, it preserves individuality with Wyndham Rewards.",
    scenarios: [
      ["Independent Upscale Conversion", "Distinctive upscale independents needing Rewards without hard Grand prototyping."],
      ["Soft-Collection Diligence", "Owners comparing Registry versus Trademark and hard Wyndham brands."],
      ["Destination Character Hotels", "Destination assets where individuality drives rate with Rewards distribution."],
    ],
    demand: [
      ["Conversion & Repositioning", S, "Registry Collection is built for distinctive upscale conversions with Rewards."],
      ["Urban Lifestyle & Design", S, "Design-led independents fit when collection standards clear."],
      ["Resort & Destination Leisure", M, "Select destination resorts can work when individuality is credible."],
      ["Suburban Business & Leisure", L, "Suburban select-service economics usually sit below Registry collection."],
      ["Airport & Transportation", L, "Airport hard-prototype is not the soft-collection lane."],
      ["Highway & Interstate Transient", N, "Highway transient sits outside upscale independent collection."],
    ],
    ops: "Wyndham soft / independent collection — distinctive upscale conversions with Wyndham Rewards. Do not underwrite as a hard Wyndham Grand prototype franchise.",
    footprint: "Confirm live hotel counts from current Wyndham / Registry Collection disclosures—curated versus mass midscale flags.",
    insight: "Registry Collection fits owners needing Wyndham soft-collection distribution for distinctive upscale assets; fails when Grand hard-brand fees are invented.",
  },
  {
    id: "recSgIQ0bhRpPXZbU",
    name: "Caption by Hyatt",
    live: "Caption by Hyatt",
    slug: "caption-by-hyatt",
    parent: "Hyatt Hotels Corporation",
    loyalty: "World of Hyatt",
    proof: "HYATT_PROOF",
    short: "Caption",
    feeGeo: "Confirm current Hyatt franchise / development disclosure for Caption by Hyatt (select-service lifestyle)",
    brandPage: "https://www.hyatt.com/caption",
    positioning: "Select-Service Lifestyle",
    commercial:
      "Versus Hyatt Place conventional select-service, Caption sells lifestyle-forward select-service; versus Hyatt Centric explorer lifestyle, Caption keeps leaner select-service capital inside World of Hyatt.",
    scenarios: [
      ["Urban Select-Service Lifestyle", "City and mixed-use sites where Caption lifestyle cues lift select-service rates with World of Hyatt."],
      ["Conversion Into Caption", "Conversions that can fund Caption public-space standards without Centric full lifestyle capital."],
      ["Sibling Clarity Versus Hyatt Place", "Owners comparing Caption lifestyle select-service versus Hyatt Place utility."],
    ],
    demand: [
      ["Urban Lifestyle & Design", S, "Caption core is lifestyle-forward select-service with World of Hyatt."],
      ["Suburban Business & Leisure", S, "Suburban mix can work when lifestyle product clears Caption standards."],
      ["Airport & Transportation", M, "Airport-edge lifestyle select-service can fit Caption."],
      ["Conversion & Repositioning", M, "Conversions succeed when PIP funds lifestyle select-service—not Place-only scopes."],
      ["Meetings & Group Compression", L, "Large meetings compression is Hyatt Regency / Grand Hyatt territory."],
      ["Luxury Urban Flagship", N, "Luxury urban flagship sits outside Caption select-service lifestyle."],
    ],
    ops: "Hyatt select-service lifestyle with leaner staffing than full-service Hyatt brands and World of Hyatt systems. Do not import Park Hyatt / Andaz luxury economics.",
    footprint: "Confirm live hotel counts from current Hyatt / Caption disclosures—growth brand; do not underwrite from launch media alone.",
    insight: "Caption fits owners wanting lifestyle select-service with World of Hyatt; wrong when Hyatt Place utility capital or Centric explorer fees are blindly assumed.",
  },
  {
    id: "NEW_hyatt-studios",
    name: "Hyatt Studios",
    live: "Hyatt Studios",
    slug: "hyatt-studios",
    parent: "Hyatt Hotels Corporation",
    loyalty: "World of Hyatt",
    proof: "HYATT_PROOF",
    short: "Hyatt Studios",
    feeGeo: "Confirm current Hyatt franchise / development disclosure for Hyatt Studios (extended-stay) AFTER Brand Basics create",
    brandPage: "https://www.hyatt.com/",
    positioning: "Midscale Extended-Stay",
    commercial:
      "Versus Hyatt House upper-midscale extended-stay, Hyatt Studios is the leaner extended-stay ladder; versus Hyatt Place transient select-service, it is length-of-stay kitchenette economics inside World of Hyatt.",
    scenarios: [
      ["Extended-Stay Corporate", "Markets with multi-week demand needing kitchenettes at midscale capital with World of Hyatt."],
      ["Project / Medical Longer Stay", "Project and medical corridors needing suite product."],
      ["New-Build Extended-Stay Prototype", "New-builds optimized for Hyatt Studios prototypes—confirm disclosure after Basics create."],
    ],
    demand: [
      ["Extended-Stay Corporate", S, "Hyatt Studios thesis is extended-stay with kitchens and World of Hyatt (confirm live brand standards)."],
      ["Project & Workforce Demand", S, "Project/workforce weekly stays are a primary extended-stay pocket."],
      ["University & Medical", M, "Medical/university longer stays can fit suite kitchenette product."],
      ["Suburban Business & Leisure", M, "Suburban midweek extended-stay works; pure weekend leisure is secondary."],
      ["Highway & Interstate Transient", L, "One-night highway transient is Hyatt Place territory."],
      ["Luxury Urban Flagship", N, "Luxury urban flagship sits outside Hyatt Studios extended-stay."],
    ],
    ops: "Hyatt midscale extended-stay (staged NEW_BRAND_CANDIDATE — no Brand Basics ID yet). Confirm franchise path and Travel Pass / World of Hyatt delivery after authorized Basics create.",
    footprint: "Confirm live hotel counts from current Hyatt disclosures after Brand Basics create—do not invent inventory from media alone.",
    insight: "Hyatt Studios is staged as NEW_BRAND_CANDIDATE: research pack ready; Brand Basics create requires separate founder authorization—no invented rec ID.",
    researchClass: "NEW_BRAND_CANDIDATE",
  },
  {
    id: "rec4oSoJsqHtGfFYL",
    name: "JdV by Hyatt",
    live: "Joie de Vivre Hotels",
    slug: "jdv-by-hyatt",
    parent: "Hyatt Hotels Corporation",
    loyalty: "World of Hyatt",
    proof: "HYATT_PROOF",
    short: "JdV",
    feeGeo: "Confirm current Hyatt / JdV affiliation disclosure for Joie de Vivre (independent lifestyle collection)",
    brandPage: "https://www.jdvhotels.com/",
    positioning: "Independent Lifestyle Collection",
    commercial:
      "Versus The Unbound Collection luxury soft collection, JdV sells independent lifestyle character at upper-upscale; versus Caption select-service lifestyle, JdV is fuller independent lifestyle with World of Hyatt.",
    scenarios: [
      ["Independent Lifestyle Conversion", "Distinctive lifestyle independents needing World of Hyatt without hard Hyatt prototyping."],
      ["Urban Character Hotels", "Urban assets where individuality and place narrative drive rate."],
      ["Sibling Clarity Versus Unbound / Centric", "Owners comparing JdV versus Unbound Collection and Hyatt Centric."],
    ],
    demand: [
      ["Urban Lifestyle & Design", S, "JdV core is independent lifestyle character with World of Hyatt."],
      ["Conversion & Repositioning", S, "Conversions fit when individuality and Hyatt systems both clear IC."],
      ["Resort & Destination Leisure", M, "Select destination lifestyle hotels can fit JdV."],
      ["Suburban Business & Leisure", L, "Suburban select-service economics usually miss JdV lifestyle intensity."],
      ["Airport & Transportation", L, "Airport hard-prototype is not the independent lifestyle lane."],
      ["Highway & Interstate Transient", N, "Highway transient sits outside JdV independent lifestyle."],
    ],
    ops: "Hyatt independent lifestyle collection (live Brand Basics name: Joie de Vivre Hotels). Soft-collection diligence—not hard-brand prototype economics. World of Hyatt participation.",
    footprint: "Confirm live hotel counts from current Hyatt / JdV disclosures.",
    insight: "JdV fits distinctive lifestyle independents needing World of Hyatt; fails when Caption select-service or Unbound luxury fees are blindly assumed. Preserve live name Joie de Vivre Hotels.",
  },
  {
    id: "recDQwcMFontK2CSP",
    name: "The Unbound Collection by Hyatt",
    live: "Unbound Collection by Hyatt",
    slug: "unbound-collection-by-hyatt",
    parent: "Hyatt Hotels Corporation",
    loyalty: "World of Hyatt",
    proof: "HYATT_PROOF",
    short: "Unbound Collection",
    feeGeo: "Confirm current Hyatt soft-collection / affiliation exhibits for The Unbound Collection (luxury)",
    brandPage: "https://www.hyatt.com/unbound-collection",
    positioning: "Luxury Independent Collection",
    commercial:
      "Versus Park Hyatt intimate hard luxury, Unbound sells soft-collection individuality; versus JdV lifestyle collection, Unbound sits higher on luxury intensity inside World of Hyatt.",
    scenarios: [
      ["Independent Luxury Character", "Assets with strong design authorship needing World of Hyatt without Park Hyatt prototyping."],
      ["Destination Luxury Soft Collection", "Destination luxury hotels where individuality is the product."],
      ["Affiliation Into Unbound", "Affiliations that can meet Unbound luxury standards while preserving character."],
    ],
    demand: [
      ["Luxury Urban Flagship", S, "Unbound fits curated luxury independents that clear soft-collection standards with World of Hyatt."],
      ["Resort & Destination Leisure", S, "Destination luxury resorts are a core Unbound lane when curation holds."],
      ["Conversion & Repositioning", M, "Repositionings fit only when true luxury product clears Unbound."],
      ["Urban Lifestyle & Design", M, "Design-led luxury lifestyle can fit when QA matches Unbound."],
      ["Suburban Business & Leisure", L, "Suburban select-service economics sit outside Unbound luxury."],
      ["Highway & Interstate Transient", N, "Highway transient is not an Unbound use case."],
    ],
    ops: "Hyatt luxury soft collection — property individuality with World of Hyatt. Operating and design capital remain luxury-band—do not confuse with Caption select-service.",
    footprint: "Confirm live hotel counts from current Hyatt / Unbound disclosures—intentionally curated.",
    insight: "Unbound works when individuality and luxury delivery are both real; fails when sponsors want World of Hyatt without luxury capital. Preserve live name Unbound Collection by Hyatt.",
  },
  {
    id: "NEW_hyatt-select",
    name: "Hyatt Select",
    live: "Hyatt Select",
    slug: "hyatt-select",
    parent: "Hyatt Hotels Corporation",
    loyalty: "World of Hyatt",
    proof: "HYATT_PROOF",
    short: "Hyatt Select",
    feeGeo: "Confirm current Hyatt franchise / development disclosure for Hyatt Select AFTER Brand Basics create",
    brandPage: "https://www.hyatt.com/",
    positioning: "Select-Service",
    commercial:
      "Versus Hyatt Place upper-midscale select-service, Hyatt Select (if launched as a distinct ladder rung) needs verified positioning versus Place / Caption before IC; stage research only—do not invent fee tables.",
    scenarios: [
      ["Select-Service Diligence Gate", "Sites where sponsors must confirm Hyatt Select versus Hyatt Place / Caption before assuming a distinct owner use case."],
      ["Corridor Select-Service", "Corridor assets needing World of Hyatt—validate brand standards after Basics create."],
      ["Conversion Into Hyatt Select", "Conversions only after authorized Brand Basics create and current disclosure exhibits."],
    ],
    demand: [
      ["Suburban Business & Leisure", M, "Provisional: suburban select-service may fit if Hyatt Select standards clear—confirm after Basics create."],
      ["Airport & Transportation", M, "Provisional: airport-edge select-service may fit—confirm live brand standards."],
      ["Conversion & Repositioning", M, "Conversions gated on authorized Basics create and disclosure exhibits."],
      ["Highway & Interstate Transient", L, "Highway midscale may overlap Place / other Hyatt select-service—confirm differentiation."],
      ["Urban Lifestyle & Design", L, "Lifestyle intensity may be Caption territory—confirm sibling clarity."],
      ["Luxury Urban Flagship", N, "Luxury urban flagship sits outside select-service."],
    ],
    ops: "Staged NEW_BRAND_CANDIDATE — no live Brand Basics match for Hyatt Select. Do not invent rec ID. Research pack only; franchise path and World of Hyatt delivery confirmed after authorized create.",
    footprint: "No Airtable inventory yet. Confirm existence and counts from Hyatt disclosures before underwriting.",
    insight: "Hyatt Select is NEW_BRAND_CANDIDATE with weak public inventory signal—founder must authorize Basics create only after brand existence and ladder position are verified versus Hyatt Place / Caption.",
    researchClass: "NEW_BRAND_CANDIDATE",
  },
  {
    id: "recmK2PWDTHW6Y6tx",
    name: "Sonesta Simply Suites",
    live: "Sonesta Simply Suites",
    slug: "sonesta-simply-suites",
    parent: "Sonesta International Hotels Corporation",
    loyalty: "Sonesta Travel Pass",
    proof: "SONESTA_PROOF",
    short: "Sonesta Simply Suites",
    feeGeo: "Confirm current Sonesta franchise / development disclosure for Sonesta Simply Suites (economy / midscale extended-stay)",
    brandPage: "https://www.sonesta.com/sonesta-simply-suites",
    positioning: "Economy Extended-Stay",
    commercial:
      "Versus Sonesta ES Suites midscale extended-stay, Simply Suites sells leaner economy extended-stay; versus Sonesta Select transient, it is length-of-stay kitchenette economics inside Sonesta Travel Pass.",
    scenarios: [
      ["Economy Extended-Stay Corporate", "Markets with multi-week demand needing kitchenettes at economy capital."],
      ["Project / Workforce Longer Stay", "Project and workforce corridors needing value extended-stay."],
      ["Conversion Into Simply Suites", "Conversions that can fund suite/kitchen PIP at Simply Suites standards."],
    ],
    demand: [
      ["Extended-Stay Corporate", S, "Simply Suites core is economy extended-stay with kitchens and Travel Pass."],
      ["Project & Workforce Demand", S, "Project/workforce weekly stays are a primary Simply Suites pocket."],
      ["University & Medical", M, "Medical/university longer stays can fit when rates clear economy extended-stay IC."],
      ["Suburban Business & Leisure", M, "Suburban midweek extended-stay works; pure weekend leisure is secondary."],
      ["Highway & Interstate Transient", L, "One-night highway transient is Select / Red Lion territory."],
      ["Luxury Urban Flagship", N, "Luxury urban flagship sits outside Simply Suites economy extended-stay."],
    ],
    ops: "Sonesta economy extended-stay with kitchenettes and Sonesta Travel Pass. Housekeeping cadence differs from transient select-service—confirm disclosure exhibits.",
    footprint: "Confirm live hotel counts from current Sonesta disclosures (Brand Basics status: Draft).",
    insight: "Sonesta Simply Suites fits lean extended-stay demand with Travel Pass; fails when underwritten like Sonesta Select transient or ES Suites midscale without kitchen PIP.",
  },
  {
    id: "NEW_sonesta-essential",
    name: "Sonesta Essential Hotels",
    live: "Sonesta Essential Hotels",
    slug: "sonesta-essential-hotels",
    parent: "Sonesta International Hotels Corporation",
    loyalty: "Sonesta Travel Pass",
    proof: "SONESTA_PROOF",
    short: "Sonesta Essential",
    feeGeo: "Confirm current Sonesta franchise / development disclosure for Sonesta Essential AFTER Brand Basics create",
    brandPage: "https://www.sonesta.com/",
    positioning: "Essential Midscale",
    commercial:
      "Versus Sonesta Select upper-midscale select-service, Essential (if a distinct ladder rung) needs verified positioning versus Select / Red Lion before IC; stage research only—do not invent fee tables.",
    scenarios: [
      ["Essential Midscale Diligence Gate", "Sites where sponsors must confirm Essential versus Select / Red Lion before assuming a distinct owner use case."],
      ["Corridor Essential Midscale", "Corridor assets needing Travel Pass—validate brand standards after Basics create."],
      ["Conversion Into Sonesta Essential", "Conversions only after authorized Brand Basics create and current disclosure exhibits."],
    ],
    demand: [
      ["Highway & Interstate Transient", M, "Provisional: corridor essential midscale may fit—confirm after Basics create."],
      ["Suburban Business & Leisure", M, "Provisional: suburban mix may fit—confirm live brand standards."],
      ["Airport & Transportation", M, "Provisional: airport-edge value stays may fit."],
      ["Conversion & Repositioning", M, "Conversions gated on authorized Basics create and disclosure exhibits."],
      ["Extended-Stay Corporate", L, "Extended-stay is Simply Suites / ES Suites territory."],
      ["Luxury Urban Flagship", N, "Luxury urban flagship sits outside essential midscale."],
    ],
    ops: "Staged NEW_BRAND_CANDIDATE — no live Brand Basics match for Sonesta Essential Hotels. Do not invent rec ID. Research pack only; franchise path and Travel Pass delivery confirmed after authorized create.",
    footprint: "No Airtable inventory yet. Confirm existence and counts from Sonesta disclosures before underwriting.",
    insight: "Sonesta Essential Hotels is NEW_BRAND_CANDIDATE—founder must authorize Basics create only after brand existence and ladder position are verified versus Sonesta Select / Red Lion.",
    researchClass: "NEW_BRAND_CANDIDATE",
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
lines.push(` * Factory 2.0 Batch 11 — Part A five EXISTING Active (original inventory remainder)`);
lines.push(` * + Part B fifteen Choice/Wyndham/Hyatt/Sonesta candidates.`);
lines.push(` * NEW_BRAND_CANDIDATE packs use provisional staging keys (NEW_<slug>) — never invent Airtable rec IDs.`);
lines.push(` * Franchise fee amounts never invented — FDD / disclosure confirmation only.`);
lines.push(` * Demand Scenario Fit: brand-specific six (not mechanical repeats).`);
lines.push(` */`);
lines.push(`import { DEMAND_FIT_LABELS } from "../be-factory-permanent-demand-scenarios-six-v1.js";`);
lines.push(`import {`);
lines.push(`  CHOICE_PROOF,`);
lines.push(`  WYNDHAM_PROOF,`);
lines.push(`  HYATT_PROOF,`);
lines.push(`  SONESTA_PROOF,`);
lines.push(`} from "./be-factory-2-depth-loyalty-proofs-v1.js";`);
lines.push(``);
lines.push(`function pack(base) {`);
lines.push(`  return Object.freeze({`);
lines.push(`    researchSpendUsd: 0,`);
lines.push(`    paidCalls: 0,`);
lines.push(`    loyaltyProofRequired: 6,`);
lines.push(`    batchTag: "batch-11",`);
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
lines.push(`export const FACTORY_2_BATCH_11_V1 = "FACTORY_2_BATCH_11_V1";`);
lines.push(``);
lines.push(`const FDD =`);
lines.push(`  "Confirm current franchise disclosure / FDD exhibits for application, royalty, marketing, loyalty, and PIP. Distinguish historical FDD figures from the live agreement. Amounts are not staged without a verified disclosure.";`);
lines.push(``);
lines.push(`const S = DEMAND_FIT_LABELS.STRONG_FIT;`);
lines.push(`const M = DEMAND_FIT_LABELS.MODERATE_FIT;`);
lines.push(`const L = DEMAND_FIT_LABELS.LIMITED_FIT;`);
lines.push(`const N = DEMAND_FIT_LABELS.NOT_A_FIT;`);
lines.push(``);
lines.push(`export const BATCH_11_IDS = Object.freeze([`);
for (const b of brands) {
  lines.push(`  "${b.id}", // ${b.name}`);
}
lines.push(`]);`);
lines.push(``);
lines.push(`export const BATCH_11_PACKS_BY_ID = Object.freeze({`);

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
lines.push(`export function listBatch11() {`);
lines.push(`  return BATCH_11_IDS.map((id) => BATCH_11_PACKS_BY_ID[id]).filter(Boolean);`);
lines.push(`}`);
lines.push(``);
lines.push(`export function getBatch11Pack(brandId) {`);
lines.push(`  return BATCH_11_PACKS_BY_ID[brandId] || null;`);
lines.push(`}`);
lines.push(``);

writeFileSync(outPath, lines.join("\n"));
console.log(`Wrote ${outPath} (${brands.length} packs)`);
