/**
 * Editorial owner-value packages for Brand Explorer overview.scenario.1–3.
 *
 * Gold bar: Kimpton / Curio / Design Hotels — Proper Case titles,
 * distinct owner-economics bodies (~48–75 words) with underwrite / conversion /
 * PIP / weaker-when / capital / affiliation cues. No identical diligence closer.
 *
 * Apply via brand-explorer-scenario-owner-value-editorial-remediation.js (Title/Body only).
 */

export const EDITORIAL_SCENARIO_PACKAGES_VERSION =
  "scenario-owner-value-editorial-packages-v1";

/**
 * @typedef {{ title: string, body: string }} EditorialScenario
 * @typedef {{ brandName: string, scenarios: [EditorialScenario, EditorialScenario, EditorialScenario] }} EditorialScenarioPackage
 */

/** @type {Readonly<Record<string, EditorialScenarioPackage>>} */
export const EDITORIAL_SCENARIO_PACKAGES = Object.freeze({
  ascend: Object.freeze({
    brandName: "Ascend Hotel Collection",
    scenarios: Object.freeze([
      Object.freeze({
        title: "Boutique Independent Conversion",
        body:
          "Ascend creates owner value on distinctive independents that need Choice distribution without a hard-brand prototype. Underwrite conversion PIP against lobby, guestroom, and systems scope so affiliation lifts rate without erasing the asset’s story. Confirm operator capacity for soft-collection QA. Weaker when capital plans assume a light cosmetic reflag while standards still require material public-space work.",
      }),
      Object.freeze({
        title: "Historic Urban Repositioning",
        body:
          "Historic urban shells fit Ascend when architecture and neighborhood identity are the product and Choice commercial tools can monetize them. Diligence heritage constraints, conversion capital, and labor intensity before modeling ADR from newer peers. Owner value holds when the PIP preserves character while meeting affiliation thresholds. Weaker when adaptive-reuse costs outrun underwriting for the specific corridor.",
      }),
      Object.freeze({
        title: "Local F&B Preserved",
        body:
          "Owners keep local F&B as a demand driver when Ascend affiliation adds loyalty and sales without forcing a corporate restaurant program. Underwrite kitchen labor, lease structure, and conversion PIP so dining stays profitable after cutover. Confirm whether the brand path treats F&B as optional or expected. Weaker when capital and staffing cannot sustain the outlet the asset already markets.",
      }),
    ]),
  }),

  "bw-premier-collection": Object.freeze({
    brandName: "BW Premier Collection",
    scenarios: Object.freeze([
      Object.freeze({
        title: "Design-Led Independent Conversion",
        body:
          "BW Premier Collection creates owner value on design-led independents that need Best Western commercial reach without full hard-brand standardization. Underwrite conversion PIP for guestrooms, lobby finish, and systems so affiliation is earned, not assumed. Confirm fee stack and operator depth before light-reflag underwriting. Weaker when capital plans ignore upper-midscale presentation expectations.",
      }),
      Object.freeze({
        title: "Commercial Platform And Lobby Experience",
        body:
          "Lobby experience and commercial platform matter when BW Premier is used to lift transient and loyalty mix on an otherwise independent stay. Diligence public-space capital, staffing, and conversion scope against Best Western standards. Owner value fits when the lobby can carry brand presentation without overbuilding F&B. Weaker when underwriting treats lobby work as optional while affiliation still requires it.",
      }),
      Object.freeze({
        title: "Destination Lifestyle Stay Experience",
        body:
          "Destination and lifestyle stays create owner value with BW Premier when the asset already sells place and design, and affiliation mainly sharpens distribution. Confirm conversion PIP, capital reserves, and operator capacity for lifestyle service before modeling peer ADR. Weaker when markets cannot support the finish level Best Western Premier presentation implies after affiliation.",
      }),
    ]),
  }),

  "bw-signature-collection": Object.freeze({
    brandName: "BW Signature Collection",
    scenarios: Object.freeze([
      Object.freeze({
        title: "Design-Led Independent Conversion",
        body:
          "BW Signature Collection fits distinctive independents that want Best Western affiliation while keeping a signature design narrative. Underwrite conversion PIP and capital for guestrooms and public space so the soft brand does not become a hidden hard conversion. Confirm standards and operator readiness before assuming a light reflag. Weaker when the asset cannot carry Signature presentation economics.",
      }),
      Object.freeze({
        title: "Commercial Platform And Lobby Experience",
        body:
          "Commercial platform value shows up when Signature affiliation improves booking mix and the lobby can host brand-grade first impressions. Diligence lobby capital, conversion scope, and labor against Best Western expectations. Owner value holds when public space investment is priced into underwriting. Weaker when affiliation is modeled as distribution-only while PIP still forces material lobby work.",
      }),
      Object.freeze({
        title: "Destination Lifestyle Stay Experience",
        body:
          "Lifestyle destination assets create owner value with BW Signature when place identity is real and Best Western systems monetize it. Confirm conversion capital, F&B or lounge intensity, and operator capacity before cutting over. Weaker when underwriting assumes soft-collection flexibility while standards and capital needs behave like a full repositioning.",
      }),
    ]),
  }),

  "comfort-inn-suites": Object.freeze({
    brandName: "Comfort Inn & Suites",
    scenarios: Object.freeze([
      Object.freeze({
        title: "Conversion-Ready Midscale Reflag",
        body:
          "Comfort Inn & Suites creates owner value on midscale conversions where Choice distribution and a clear guest promise can stabilize occupancy without full-service capital. Underwrite conversion PIP for guestrooms, breakfast, and systems against realistic ADR. Confirm operator capacity for midscale QA. Weaker when capital plans treat the reflag as cosmetic while Comfort standards still require meaningful scope.",
      }),
      Object.freeze({
        title: "Highway And Suburban Corridor Portfolio",
        body:
          "Highway and suburban corridors fit Comfort when transient demand is reliable and suite mix can capture longer or family stays. Diligence corridor competition, conversion capital, and labor before modeling Choice loyalty lift. Owner value is strongest when affiliation costs stay inside underwriting for the corridor. Weaker when markets cannot support midscale rates after PIP.",
      }),
      Object.freeze({
        title: "Midscale Suite Mix With Choice Scale",
        body:
          "Suite-heavy Comfort assets create owner value when the physical product already supports extended or family demand and Choice affiliation sharpens distribution. Confirm conversion PIP, breakfast labor, and capital reserves before assuming portfolio standardization is cheap. Weaker when underwriting ignores suite refresh costs or operator limits after affiliation cutover.",
      }),
    ]),
  }),

  "country-inn-suites": Object.freeze({
    brandName: "Country Inn & Suites by Choice",
    scenarios: Object.freeze([
      Object.freeze({
        title: "Midscale Conversion With Residential Feel",
        body:
          "Country Inn & Suites creates owner value on midscale conversions that can deliver a residential, welcoming stay without upper-upscale capital. Underwrite conversion PIP for guestrooms, lobby warmth, and breakfast operations. Confirm Choice affiliation fees and operator depth before light-reflag assumptions. Weaker when finish expectations outrun the corridor’s ADR support after cutover.",
      }),
      Object.freeze({
        title: "Family And Leisure Corridor Stay",
        body:
          "Family and leisure corridors fit Country when suite mix, breakfast, and approachable design drive repeat demand. Diligence conversion capital, labor intensity, and competitive set before modeling loyalty contribution. Owner value holds when affiliation lifts mix without forcing full-service F&B. Weaker when underwriting ignores breakfast staffing or PIP scope on the asset.",
      }),
      Object.freeze({
        title: "Choice Midscale Portfolio Standardization",
        body:
          "Multi-asset sponsors use Country to standardize midscale presentation across Choice portfolios. Confirm conversion PIP consistency, capital pacing, and operator capacity across assets before treating affiliation as uniform or cheap. Weaker when one underwriting template is applied to assets that still need material repositioning capital after the affiliation cutover.",
      }),
    ]),
  }),

  "everhome-suites": Object.freeze({
    brandName: "Everhome Suites",
    scenarios: Object.freeze([
      Object.freeze({
        title: "Weekly-Stay Revenue Stability",
        body:
          "Everhome Suites creates owner value where weekly-stay demand can stabilize occupancy versus pure transient midscale. Underwrite length-of-stay mix, kitchenette utility costs, and conversion or prototype capital against Choice extended-stay peers. Confirm operator capacity for weekly housekeeping rhythms. Weaker when affiliation is modeled on transient ADR that the product will not earn.",
      }),
      Object.freeze({
        title: "Efficient Purpose-Built Prototype",
        body:
          "Purpose-built Everhome prototypes fit when capital can deliver an efficient extended-stay box without overbuilding public space. Diligence prototype PIP, construction capital, and labor model before comparing to conversion deals. Owner value is strongest when underwriting stays disciplined on rooms and kitchens. Weaker when sponsors add full-service scope the brand path does not need.",
      }),
      Object.freeze({
        title: "Midscale Platform With Choice Scale",
        body:
          "Choice scale helps Everhome when distribution and loyalty can fill weekly and midweek demand in employment corridors. Confirm affiliation economics, conversion or new-build capital, and operator readiness for extended-stay standards on the asset. Weaker when markets lack durable weekly demand and underwriting assumes transient peak pricing after cutover.",
      }),
    ]),
  }),

  "hotel-indigo": Object.freeze({
    brandName: "Hotel Indigo",
    scenarios: Object.freeze([
      Object.freeze({
        title: "Local Storytelling In Urban Gateways",
        body:
          "Hotel Indigo creates owner value in urban gateways where neighborhood storytelling and design can support upscale rates with IHG distribution. Underwrite conversion PIP for lobby, guestrooms, and local narrative delivery—not a generic select-service reflag. Confirm F&B or lounge intensity and operator capacity. Weaker when capital plans strip storytelling budget while affiliation still expects Indigo presentation.",
      }),
      Object.freeze({
        title: "Independent Repositioning With Global Reach",
        body:
          "Independents repositioning into Hotel Indigo gain IHG reach when the asset already has character and the PIP can make that character brand-legible. Diligence conversion capital, design narrative, and labor before modeling peer ADR. Owner value fits when affiliation sharpens commercial tools without erasing local identity. Weaker when underwriting assumes soft-brand flexibility while Indigo standards still drive material scope.",
      }),
      Object.freeze({
        title: "Mixed-Demand City Hotel",
        body:
          "Mixed-demand city hotels fit Indigo when leisure storytelling and weekday corporate demand can share one design-led box. Confirm conversion PIP, capital reserves, and operator depth for lifestyle service and IHG systems after cutover. Weaker when markets cannot support the design and public-space investment Hotel Indigo presentation requires after affiliation.",
      }),
    ]),
  }),

  kimpton: Object.freeze({
    brandName: "Kimpton Hotels",
    scenarios: Object.freeze([
      Object.freeze({
        title: "Urban Lifestyle Conversion",
        body:
          "Kimpton creates owner value on urban lifestyle conversions where guests pay for personality, F&B, and neighborhood character—not limited-service commodity. Underwrite conversion PIP for design, restaurant, and social spaces against upper-upscale economics and IHG distribution. Confirm operator capacity for lifestyle service. Weaker when capital plans treat Kimpton as a light cosmetic reflag.",
      }),
      Object.freeze({
        title: "Gateway New-Build or Adaptive Reuse",
        body:
          "Gateway new-build or adaptive reuse fits Kimpton when design-led lobby, restaurant, and social space can justify upper-upscale ADR with IHG One Rewards. Diligence construction or conversion capital, F&B labor, and heritage constraints before modeling returns. Owner value holds when underwriting prices lifestyle intensity honestly. Weaker when markets will not support restaurant-forward investment after affiliation.",
      }),
      Object.freeze({
        title: "Portfolio Lifestyle Standardization",
        body:
          "Multi-asset sponsors create owner value by aligning several lifestyle conversions to one Kimpton / IHG flag with consistent design narrative, F&B discipline, and One Rewards participation. Underwrite portfolio conversion PIP, capital pacing, and operator depth across assets—not a single-asset light reflag. Confirm lifestyle standards remain deliverable at scale. Weaker when one underwriting template ignores assets that still need material restaurant and design capital after affiliation.",
      }),
    ]),
  }),

  "mgallery-collection": Object.freeze({
    brandName: "MGallery Collection",
    scenarios: Object.freeze([
      Object.freeze({
        title: "Distinctive Hotel With a Strong Story",
        body:
          "MGallery creates owner value on distinctive hotels whose story and design can support Accor collection positioning without hard-brand sameness. Underwrite conversion PIP for guestrooms, public space, and narrative delivery. Confirm affiliation fees and operator capacity before assuming a soft reflag. Weaker when capital cannot fund the story the brand presentation requires.",
      }),
      Object.freeze({
        title: "Repositioning an Established Independent",
        body:
          "Established independents reposition into MGallery when Accor distribution can lift demand while the asset keeps its identity. Diligence conversion capital, design standards, and labor intensity before modeling peer ADR. Owner value fits when affiliation sharpens commercial reach without erasing the independent’s edge. Weaker when underwriting ignores PIP scope Accor still expects.",
      }),
      Object.freeze({
        title: "CALA City or Destination Context",
        body:
          "CALA city and destination contexts fit MGallery when place authenticity and upscale demand can carry collection economics. Confirm conversion PIP, capital reserves, and operator readiness for Accor systems and lifestyle service after cutover. Weaker when markets cannot support the design and service investment MGallery presentation implies after affiliation.",
      }),
    ]),
  }),

  "preferred-hotels-and-resorts": Object.freeze({
    brandName: "Preferred Hotels & Resorts",
    scenarios: Object.freeze([
      Object.freeze({
        title: "Design-Led Independent Conversion",
        body:
          "Preferred creates owner value on design-led independents that need global sales and recognition without a hard-brand prototype. Underwrite conversion or membership capital against lobby, guestroom, and service scope so affiliation lifts rate honestly. Confirm operator capacity for Preferred standards. Weaker when capital plans assume distribution-only upside while presentation still requires material investment.",
      }),
      Object.freeze({
        title: "Commercial Platform And Lobby Experience",
        body:
          "Commercial platform and lobby experience matter when Preferred affiliation is used to sharpen first impressions and global booking tools. Diligence lobby capital, conversion scope, and labor before modeling loyalty or GDS lift. Owner value holds when public-space investment is inside underwriting. Weaker when affiliation is treated as a light reflag while lobby work remains mandatory.",
      }),
      Object.freeze({
        title: "Destination Lifestyle Stay Experience",
        body:
          "Destination lifestyle stays fit Preferred when place and design already sell, and affiliation mainly extends recognition and sales. Confirm capital reserves, conversion PIP if any, and operator depth for lifestyle service before cutover. Weaker when underwriting imports hard-brand ADR comps the independent product cannot sustain after affiliation cutover.",
      }),
    ]),
  }),

  "quality-inn": Object.freeze({
    brandName: "Quality Inn",
    scenarios: Object.freeze([
      Object.freeze({
        title: "Conversion-Heavy Midscale Reflag",
        body:
          "Quality Inn creates owner value on conversion-heavy midscale reflags where Choice distribution can stabilize occupancy without full-service capital. Underwrite conversion PIP for guestrooms, breakfast, and systems against realistic corridor ADR. Confirm operator capacity for midscale QA. Weaker when capital plans treat the reflag as cosmetic while Quality standards still require meaningful scope.",
      }),
      Object.freeze({
        title: "Value-Q Highway Portfolio",
        body:
          "Highway and value-Q portfolios fit Quality when transient demand is durable and affiliation costs stay inside underwriting. Diligence conversion capital, labor, and competitive set before modeling Choice loyalty lift for the corridor. Owner value is strongest when PIP is priced honestly. Weaker when markets cannot support midscale rates after affiliation.",
      }),
      Object.freeze({
        title: "Midscale Portfolio Standardization",
        body:
          "Sponsors standardize midscale presentation across Quality assets when conversion PIP, capital pacing, and operator capacity can be repeated. Confirm affiliation economics and QA depth before treating every asset as a light reflag. Weaker when one underwriting template ignores properties that still need material repositioning capital after affiliation cutover.",
      }),
    ]),
  }),

  radisson: Object.freeze({
    brandName: "Radisson by Choice",
    scenarios: Object.freeze([
      Object.freeze({
        title: "Full-Service Urban Conversion",
        body:
          "Radisson by Choice creates owner value on full-service urban conversions that need Choice distribution with a recognizable upscale flag. Underwrite conversion PIP for guestrooms, lobby, and meeting or F&B scope against realistic ADR. Confirm operator capacity for full-service labor. Weaker when capital plans assume a select-service light reflag while Radisson presentation still expects fuller public space.",
      }),
      Object.freeze({
        title: "Gateway Meeting And Transient Hotel",
        body:
          "Gateway meeting and transient hotels fit Radisson when weekday group and corporate demand can share one Choice-backed box. Diligence meeting-space capital, conversion scope, and labor before modeling peer rates. Owner value holds when affiliation costs and PIP stay inside underwriting. Weaker when markets lack durable meeting demand after cutover.",
      }),
      Object.freeze({
        title: "Choice Upscale Flagship Standardization",
        body:
          "Portfolio sponsors use Radisson by Choice to standardize an upscale Choice flagship across markets. Confirm conversion PIP consistency, capital pacing, and operator depth across assets before treating affiliation as uniform or inexpensive. Weaker when underwriting ignores assets that still need material F&B or meeting capital after affiliation cutover.",
      }),
    ]),
  }),

  "radisson-blu": Object.freeze({
    brandName: "Radisson Blu by Choice",
    scenarios: Object.freeze([
      Object.freeze({
        title: "Iconic Urban Flagship",
        body:
          "Radisson Blu creates owner value as an iconic urban flagship when architecture, lobby, and service can support upper-upscale rates with Choice distribution. Underwrite conversion or new-build capital and PIP for public space and guestrooms honestly. Confirm operator capacity for Blu service intensity. Weaker when capital plans strip flagship presentation while affiliation still expects it.",
      }),
      Object.freeze({
        title: "Resort And Leisure Destination",
        body:
          "Resort and leisure destinations fit Radisson Blu when destination authenticity and amenity depth can carry premium ADR. Diligence conversion PIP, F&B labor, and capital reserves before modeling peer resorts. Owner value holds when affiliation monetizes demand without overbuilding. Weaker when underwriting ignores amenity and labor intensity Blu presentation requires.",
      }),
      Object.freeze({
        title: "Adaptive Reuse And Conversion",
        body:
          "Adaptive reuse and conversion create owner value with Radisson Blu when landmark or character buildings can meet Blu standards without destroying sense of place. Confirm conversion capital, heritage constraints, and operator depth before light-reflag assumptions. Weaker when PIP and capital needs outrun underwriting for the specific asset after affiliation.",
      }),
    ]),
  }),

  "radisson-individuals-by-choice": Object.freeze({
    brandName: "Radisson Individuals by Choice",
    scenarios: Object.freeze([
      Object.freeze({
        title: "Distinctive Boutique Conversion",
        body:
          "Radisson Individuals creates owner value on distinctive boutique conversions that need Choice scale without hard-brand sameness. Underwrite conversion PIP for design narrative, guestrooms, and systems so uniqueness survives affiliation. Confirm operator capacity for soft-collection QA. Weaker when capital plans assume a cosmetic reflag while presentation still requires material work.",
      }),
      Object.freeze({
        title: "CALA Soft-Collection Growth",
        body:
          "CALA soft-collection growth fits Radisson Individuals when place identity and upscale demand can carry a flexible Choice path. Diligence conversion capital, affiliation fees, and labor before modeling peer soft brands. Owner value holds when underwriting prices uniqueness investment honestly. Weaker when markets cannot support boutique finish after cutover.",
      }),
      Object.freeze({
        title: "Uniqueness With Choice Scale",
        body:
          "Uniqueness with Choice scale is the Individuals thesis: keep the asset’s edge while adding distribution and loyalty. Confirm conversion PIP, capital reserves, and operator readiness so affiliation does not flatten the boutique product. Weaker when underwriting imports hard-brand ADR comps the boutique cannot sustain after the affiliation cutover.",
      }),
    ]),
  }),

  "radisson-red": Object.freeze({
    brandName: "Radisson RED by Choice",
    scenarios: Object.freeze([
      Object.freeze({
        title: "Urban Lifestyle Conversion",
        body:
          "Radisson RED creates owner value on urban lifestyle conversions that want bold design and social energy without full-kitchen F&B capital. Underwrite conversion PIP for guestrooms, lobby, and flex F&B against select-service economics. Confirm operator capacity for lifestyle service. Weaker when capital plans treat RED as a light cosmetic reflag while design standards still drive scope.",
      }),
      Object.freeze({
        title: "CALA Gateway Select-Service",
        body:
          "CALA gateway select-service assets fit Radisson RED when lifestyle design can support rate with Choice distribution and leaner public space. Diligence conversion capital, affiliation costs, and labor before modeling full-service peers. Owner value holds when underwriting stays select-service honest. Weaker when sponsors add kitchen intensity the brand path does not require.",
      }),
      Object.freeze({
        title: "Flex F&B Without Full Kitchen",
        body:
          "Flex F&B without a full kitchen is a RED owner-value lever when social food-and-beverage can sell lifestyle without restaurant P&L risk. Confirm conversion PIP, capital for bar or grab-and-go, and operator capacity before assuming zero F&B labor. Weaker when underwriting ignores even lean F&B staffing after affiliation cutover.",
      }),
    ]),
  }),

  "small-luxury-hotels-of-the-world": Object.freeze({
    brandName: "Small Luxury Hotels of the World",
    scenarios: Object.freeze([
      Object.freeze({
        title: "Independent Luxury With a Defined Identity",
        body:
          "SLH creates owner value on independent luxury hotels with a defined identity that need global recognition without hard-brand standardization. Underwrite membership and conversion capital for service, finish, and story delivery. Confirm operator capacity for luxury QA. Weaker when capital plans assume distribution-only upside while presentation still requires material investment.",
      }),
      Object.freeze({
        title: "Distribution And Recognition for an Independent",
        body:
          "Distribution and recognition matter when an independent already delivers luxury product and SLH mainly extends sales reach. Diligence affiliation economics, capital reserves, and labor intensity before modeling peer luxury ADR. Owner value fits when underwriting stays asset-specific. Weaker when sponsors treat SLH as a light reflag while service standards still demand depth.",
      }),
      Object.freeze({
        title: "Destination-Led Luxury Stay",
        body:
          "Destination-led luxury stays fit SLH when place authenticity drives ADR and affiliation monetizes that demand. Confirm conversion or refresh PIP, capital pacing, and operator readiness for luxury service after affiliation. Weaker when markets cannot support the finish and staffing Small Luxury Hotels presentation implies for the specific asset.",
      }),
    ]),
  }),

  "suburban-studios": Object.freeze({
    brandName: "Suburban Studios",
    scenarios: Object.freeze([
      Object.freeze({
        title: "Economy Extended-Stay Studio",
        body:
          "Suburban Studios creates owner value as an economy extended-stay studio where weekly demand can outrun pure transient economy ADR. Underwrite conversion or prototype capital, kitchenette utilities, and labor against Choice extended-stay peers. Confirm operator capacity for weekly rhythms. Weaker when affiliation is modeled on rates the studio product will not earn.",
      }),
      Object.freeze({
        title: "Weekly-Stay Employment Corridor",
        body:
          "Employment corridors fit Suburban when weekly-stay demand from contractors and relocating workers is durable. Diligence conversion capital, competitive set, and labor before modeling Choice loyalty lift for the corridor. Owner value holds when underwriting prices economy extended-stay honestly for the asset. Weaker when markets lack lasting weekly demand after affiliation.",
      }),
      Object.freeze({
        title: "Kitchenette Conversion",
        body:
          "Kitchenette conversions create owner value when the physical product can support longer stays without full-service capital. Confirm conversion PIP, capital for kitchens and laundry, and operator readiness for extended-stay standards after cutover. Weaker when underwriting ignores kitchenette refresh costs or treats affiliation as only a cosmetic light reflag.",
      }),
    ]),
  }),

  "trademark-collection-by-wyndham": Object.freeze({
    brandName: "Trademark Collection by Wyndham",
    scenarios: Object.freeze([
      Object.freeze({
        title: "Independent Hotel Conversion",
        body:
          "Trademark Collection creates owner value on independent hotel conversions that need Wyndham distribution without hard-brand sameness. Underwrite conversion PIP for guestrooms, lobby, and systems so affiliation lifts rate without erasing character. Confirm operator capacity for soft-collection QA. Weaker when capital plans assume a light cosmetic reflag while standards still require material scope.",
      }),
      Object.freeze({
        title: "Boutique or Historic Property Repositioning",
        body:
          "Boutique or historic properties reposition into Trademark when architecture and story are the product and Wyndham tools can monetize them. Diligence heritage constraints, conversion capital, and labor before modeling peer ADR. Owner value holds when the PIP preserves identity through cutover. Weaker when adaptive-reuse costs outrun underwriting after affiliation.",
      }),
      Object.freeze({
        title: "Secondary-Market Independent Repositioning",
        body:
          "Secondary-market independents fit Trademark when local demand can support upscale-leaning soft branding without gateway capital intensity. Confirm conversion PIP, capital reserves, and operator depth before treating affiliation as distribution-only upside for sponsors. Weaker when underwriting imports primary-market ADR comps the secondary-market asset cannot sustain after the affiliation cutover.",
      }),
    ]),
  }),

  "vignette-collection": Object.freeze({
    brandName: "Vignette Collection",
    scenarios: Object.freeze([
      Object.freeze({
        title: "Independent Luxury-Leaning Affiliation",
        body:
          "Vignette Collection creates owner value on independent luxury-leaning hotels that want IHG affiliation without restaurant-forward Kimpton intensity. Underwrite conversion PIP for design, guestrooms, and systems against realistic upper-upscale economics. Confirm operator capacity for collection standards on the asset. Weaker when capital plans treat Vignette as a zero-PIP reflag after cutover.",
      }),
      Object.freeze({
        title: "Distinctive Boutique Without a Restaurant-Forward Mandate",
        body:
          "Distinctive boutiques fit Vignette when the asset sells design and place without needing a full restaurant program. Diligence conversion capital, F&B optionality, and labor before modeling lifestyle peers. Owner value holds when underwriting stays honest about what affiliation still requires. Weaker when sponsors import restaurant-forward capital the brand path does not mandate.",
      }),
      Object.freeze({
        title: "Heritage or Character Asset Seeking Minimal Standardization",
        body:
          "Heritage and character assets create owner value with Vignette when minimal standardization preserves identity while IHG tools extend reach. Confirm conversion PIP, capital for heritage constraints, and operator readiness before light-reflag assumptions on the asset. Weaker when underwriting ignores even soft-collection presentation and systems capital after the affiliation.",
      }),
    ]),
  }),

  "woodspring-suites": Object.freeze({
    brandName: "WoodSpring Suites",
    scenarios: Object.freeze([
      Object.freeze({
        title: "Extended-Stay Corridor Conversion",
        body:
          "WoodSpring Suites creates owner value on extended-stay corridor conversions where weekly demand can stabilize economy-to-midscale occupancy. Underwrite conversion PIP for studios, kitchenettes, and laundry against Choice or peer extended-stay comps. Confirm operator capacity for weekly rhythms. Weaker when affiliation is modeled on transient peak ADR the product will not earn.",
      }),
      Object.freeze({
        title: "Weekly-Demand Growth Market",
        body:
          "Weekly-demand growth markets fit WoodSpring when employment and project-driven stays are durable enough to carry lean extended-stay economics. Diligence conversion capital, competitive set, and labor before modeling loyalty lift for the corridor. Owner value holds when underwriting stays lean and honest. Weaker when markets lack lasting weekly demand after affiliation.",
      }),
      Object.freeze({
        title: "Lean Extended-Stay Standardization",
        body:
          "Lean extended-stay standardization creates owner value when sponsors can repeat an efficient WoodSpring box without overbuilding public space. Confirm conversion or prototype capital, PIP consistency, and operator depth across assets before cutover. Weaker when underwriting adds full-service scope or ignores kitchenette refresh costs after the affiliation cutover on portfolio assets.",
      }),
    ]),
  }),
});

export function listEditorialScenarioPackageSlugs() {
  return Object.keys(EDITORIAL_SCENARIO_PACKAGES);
}

export function getEditorialScenarioPackage(slug) {
  return EDITORIAL_SCENARIO_PACKAGES[String(slug || "").trim().toLowerCase()] || null;
}
