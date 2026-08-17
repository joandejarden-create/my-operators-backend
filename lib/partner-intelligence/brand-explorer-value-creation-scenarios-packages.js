/**
 * Editorial packages for Brand Explorer "Value Creation Scenarios"
 * (`valueOwners.scenario.1–4`).
 *
 * Gold bar: Ascend Hotel Collection (founder screenshot) — four Proper Case
 * titles + short owner-value paragraphs (~26–45 words). Each body names the
 * brand (or short name) and explains when it creates value for owners/projects.
 */

export const VALUE_CREATION_SCENARIOS_PACKAGES_VERSION =
  "value-creation-scenarios-packages-v1";

/** Ascend-gold short-paragraph band for package bodies. */
export const VALUE_CREATION_PACKAGE_MIN_BODY_WORDS = 26;
export const VALUE_CREATION_PACKAGE_MAX_BODY_WORDS = 45;

/**
 * @typedef {{ title: string, body: string }} ValueCreationScenario
 * @typedef {{
 *   brandName: string,
 *   scenarios: [
 *     ValueCreationScenario,
 *     ValueCreationScenario,
 *     ValueCreationScenario,
 *     ValueCreationScenario
 *   ]
 * }} ValueCreationScenarioPackage
 */

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function words(s) {
  return nz(s).split(/\s+/).filter(Boolean).length;
}

function freezeScenario(title, body) {
  return Object.freeze({ title, body });
}

function freezePackage(brandName, scenarios) {
  return Object.freeze({
    brandName,
    scenarios: Object.freeze(scenarios.map((s) => freezeScenario(s.title, s.body))),
  });
}

/** @type {Readonly<Record<string, ValueCreationScenarioPackage>>} */
export const VALUE_CREATION_SCENARIO_PACKAGES = Object.freeze({
  "ac-hotels-by-marriott": freezePackage("AC Hotels by Marriott", [
    {
      title: "Urban Conversion With Design Clarity",
      body:
        "AC Hotels creates owner value on urban conversions where a clean European-inspired product can lift ADR without full-service capital intensity—Bonvoy distribution fits when the asset already reads contemporary and sponsors underwrite a disciplined public-space and guestroom PIP.",
    },
    {
      title: "Gateway And Corridor Development",
      body:
        "Gateway and strong corridor sites suit AC when new-build or adaptive projects need Marriott lifestyle positioning with efficient F&B—owners gain Bonvoy transient pull when design, fitness, and lobby standards can be delivered without overbuilding banquet or resort scope.",
    },
    {
      title: "Lifestyle Portfolio Standardization",
      body:
        "Multi-asset sponsors use AC Hotels to standardize an upper-select lifestyle box across markets—value holds when capital plans, operator depth, and Bonvoy cutover are priced honestly so affiliation lift is not mistaken for a light cosmetic reflag.",
    },
    {
      title: "Third-Party Operator-Led",
      body:
        "AC suits assets run by operators who can execute Marriott brand standards and lifestyle service rhythms—sponsors need Bonvoy scale and financing credibility while keeping conversion scope, labor, and design discipline inside underwriting for the specific urban asset.",
    },
  ]),

  ascend: freezePackage("Ascend Hotel Collection", [
    {
      title: "Independent Boutique Reflag",
      body:
        "Independent boutiques that need Choice Privileges, CRS access, and corporate transient without full prototype homogenization—Ascend fits when local story, F&B identity, and design character are assets owners want to preserve.",
    },
    {
      title: "Historic Design-Forward Conversion",
      body:
        "Historic or design-forward hotels where character is the asset—Ascend works when public spaces and guest rooms can meet collection standards while contractually protecting how much local identity survives brand review.",
    },
    {
      title: "Markets With Member Channel Gap",
      body:
        "Corridors where independents under-index on loyalty and direct booking—Ascend competes when the asset can deliver a credible guest experience and owners need enterprise channels without surrendering uniqueness.",
    },
    {
      title: "Third-Party Operator-Led",
      body:
        "Assets run by experienced operators who can balance collection QA with local execution—Ascend suits sponsors who need affiliation lift, financing credibility, and Choice Privileges scale while keeping the property's story intact.",
    },
  ]),

  "autograph-collection": freezePackage("Autograph Collection", [
    {
      title: "Independent Soft-Brand Conversion",
      body:
        "Autograph creates owner value on distinctive independents that need Bonvoy distribution without a hard-brand prototype—affiliation fits when local story, design character, and F&B identity are assets owners want to preserve while meeting collection presentation standards.",
    },
    {
      title: "Heritage Adaptive Reuse",
      body:
        "Historic or character buildings suit Autograph when architecture is the product and Marriott commercial tools can monetize it—owners underwrite conversion capital and brand review carefully so local identity survives while rooms and public spaces meet collection thresholds.",
    },
    {
      title: "Resort or Experiential Repositioning",
      body:
        "Experiential or resort assets fit Autograph when place and programming already sell, and Bonvoy mainly sharpens demand—value holds when PIP, labor, and F&B intensity stay inside underwriting rather than assuming a light soft-brand cutover.",
    },
    {
      title: "Third-Party Operator-Led",
      body:
        "Autograph suits sponsors using experienced operators who can balance Marriott collection QA with local execution—affiliation lift and financing credibility matter when the property story stays intact and conversion scope is priced honestly for the asset.",
    },
  ]),

  "avid-hotels": freezePackage("avid hotels", [
    {
      title: "Efficient Midscale Conversion",
      body:
        "avid hotels creates owner value on midscale conversions where an efficient prototype and IHG distribution can stabilize occupancy without full-service capital—sponsors underwrite guestroom, lobby, and systems PIP so affiliation is earned rather than assumed as a cosmetic reflag.",
    },
    {
      title: "Highway And Suburban Corridor Portfolio",
      body:
        "Highway and suburban corridors fit avid when transient demand is reliable and capital can deliver a consistent midscale box—owners gain IHG channel scale when labor, breakfast, and brand standards stay inside corridor underwriting after cutover.",
    },
    {
      title: "Purpose-Built New Development",
      body:
        "New-build avid projects suit sponsors who want a disciplined midscale prototype rather than overbuilt public space—value holds when construction capital, operator readiness, and IHG affiliation economics are modeled against realistic ADR for the corridor.",
    },
    {
      title: "Third-Party Operator-Led",
      body:
        "avid works with operators who can run efficient midscale QA and IHG systems cutover—sponsors need affiliation lift and financing credibility while keeping conversion or prototype scope, staffing, and brand presentation inside underwriting for each asset.",
    },
  ]),

  "bunkhouse-hotels": freezePackage("Bunkhouse Hotels", [
    {
      title: "Independent Lifestyle Soft Brand",
      body:
        "Bunkhouse creates owner value on design-led independents that need soft-brand credibility without full hard-brand homogenization—affiliation fits when local story, F&B identity, and neighborhood character are assets owners want to preserve while meeting collection presentation expectations.",
    },
    {
      title: "Neighborhood Experiential Conversion",
      body:
        "Neighborhood or experiential conversions suit Bunkhouse when place and culture are the product—owners underwrite public-space and guestroom capital carefully so authenticity survives brand review while the stay still meets soft-collection guest expectations.",
    },
    {
      title: "Creative Corridor Portfolio Play",
      body:
        "Creative corridors fit Bunkhouse when sponsors assemble a lifestyle portfolio that sells local narrative—value holds when operator depth, conversion PIP, and commercial platform lift are priced honestly rather than treating soft affiliation as distribution-only.",
    },
    {
      title: "Third-Party Operator-Led",
      body:
        "Bunkhouse suits assets run by operators who can balance soft-collection QA with local execution—sponsors need affiliation lift and financing credibility while keeping the property's story, F&B identity, and design character intact after cutover.",
    },
  ]),

  "bw-premier-collection": freezePackage("BW Premier Collection", [
    {
      title: "Independent Soft-Brand Conversion",
      body:
        "BW Premier Collection creates owner value on design-led independents that need Best Western commercial reach without full hard-brand standardization—affiliation fits when local story and finish quality can meet Premier presentation while owners preserve distinctive character.",
    },
    {
      title: "Upper-Midscale Character Repositioning",
      body:
        "Character assets suit BW Premier when public spaces and guestrooms can carry upper-midscale presentation—owners underwrite conversion capital and brand standards carefully so affiliation lift is not mistaken for a light cosmetic reflag in competitive corridors.",
    },
    {
      title: "Markets With Member Channel Gap",
      body:
        "Corridors where independents under-index on loyalty and direct booking fit BW Premier—value holds when the asset can deliver a credible guest experience and owners need Best Western enterprise channels without surrendering the property's uniqueness.",
    },
    {
      title: "Third-Party Operator-Led",
      body:
        "BW Premier suits sponsors using experienced operators who balance collection QA with local execution—affiliation lift, financing credibility, and Best Western platform scale matter when the asset's story and design identity remain intact after cutover.",
    },
  ]),

  "bw-signature-collection": freezePackage("BW Signature Collection", [
    {
      title: "Independent Soft-Brand Conversion",
      body:
        "BW Signature Collection fits distinctive independents that want Best Western affiliation while keeping a signature design narrative—owners underwrite conversion PIP for guestrooms and public space so soft branding does not hide a hard conversion.",
    },
    {
      title: "Design-Forward Lifestyle Stay",
      body:
        "Design-forward lifestyle stays create owner value with BW Signature when place identity is real and Best Western systems monetize it—confirm capital, lobby finish, and operator capacity before assuming soft-collection flexibility on standards.",
    },
    {
      title: "Destination Character Repositioning",
      body:
        "Destination character assets suit Signature when architecture and neighborhood story are the product—owners gain Best Western commercial tools when brand review protects enough local identity while rooms and public spaces meet collection thresholds.",
    },
    {
      title: "Third-Party Operator-Led",
      body:
        "BW Signature works with operators who can execute collection QA without erasing local story—sponsors need affiliation lift and financing credibility while conversion scope, labor, and Signature presentation stay inside underwriting for the asset.",
    },
  ]),

  "canopy-by-hilton": freezePackage("Canopy by Hilton", [
    {
      title: "Urban Lifestyle Conversion",
      body:
        "Canopy creates owner value on urban conversions where neighborhood energy and a design-led lifestyle product can lift ADR—Hilton Honors distribution fits when lobby, F&B, and guestroom standards can be delivered without overbuilding full-service banquet scope.",
    },
    {
      title: "Gateway Development Opportunity",
      body:
        "Gateway and high-visibility urban sites suit Canopy when new-build or adaptive projects need Hilton lifestyle positioning—owners underwrite capital, labor, and brand presentation carefully so affiliation lift matches the corridor's realistic rate support.",
    },
    {
      title: "Lifestyle Portfolio Standardization",
      body:
        "Multi-asset sponsors use Canopy to standardize a lifestyle box across cities—value holds when operator depth, conversion or prototype capital, and Hilton systems cutover are priced honestly rather than treated as a light reflag.",
    },
    {
      title: "Third-Party Operator-Led",
      body:
        "Canopy suits assets run by operators who can balance Hilton brand standards with local neighborhood execution—sponsors need Honors scale and financing credibility while keeping design character and service intensity inside underwriting.",
    },
  ]),

  "city-express-by-marriott": freezePackage("City Express by Marriott", [
    {
      title: "Midscale Conversion Reflag",
      body:
        "City Express creates owner value on midscale conversions where Bonvoy distribution and a clear guest promise can stabilize occupancy without full-service capital—sponsors underwrite guestroom, lobby, and systems PIP against realistic corridor ADR after affiliation.",
    },
    {
      title: "Urban And Suburban Corridor Portfolio",
      body:
        "Urban and suburban corridors fit City Express when transient demand is reliable and capital can deliver a consistent midscale product—owners gain Marriott channel scale when labor and brand standards stay inside underwriting for the market.",
    },
    {
      title: "Purpose-Built New Development",
      body:
        "New-build City Express projects suit sponsors who want an efficient midscale box rather than overbuilt public space—value holds when construction capital, operator readiness, and Bonvoy affiliation economics are modeled to corridor peers.",
    },
    {
      title: "Third-Party Operator-Led",
      body:
        "City Express works with operators who can run midscale QA and Marriott systems cutover—sponsors need affiliation lift and financing credibility while keeping conversion or prototype scope and staffing inside underwriting for each asset.",
    },
  ]),

  "comfort-inn-suites": freezePackage("Comfort Inn & Suites", [
    {
      title: "Conversion-Ready Midscale Reflag",
      body:
        "Comfort Inn & Suites creates owner value on midscale conversions where Choice distribution and a clear guest promise stabilize occupancy without full-service capital—underwrite guestroom, breakfast, and systems PIP so affiliation is not treated as cosmetic.",
    },
    {
      title: "Highway And Suburban Corridor Portfolio",
      body:
        "Highway and suburban corridors fit Comfort when transient demand is reliable and suite mix can capture family or longer stays—owners gain Choice channel scale when labor, breakfast, and brand standards stay inside corridor underwriting.",
    },
    {
      title: "Purpose-Built Midscale New Build",
      body:
        "New-build Comfort projects suit sponsors who want a disciplined midscale prototype—value holds when construction capital, operator readiness, and Choice affiliation economics are modeled against realistic ADR rather than peer upper-upscale comps.",
    },
    {
      title: "Third-Party Operator-Led",
      body:
        "Comfort suits assets run by operators who can execute midscale QA and Choice systems cutover—sponsors need affiliation lift and financing credibility while conversion scope, breakfast labor, and presentation stay inside underwriting.",
    },
  ]),

  "country-inn-suites": freezePackage("Country Inn & Suites", [
    {
      title: "Midscale Conversion With Residential Feel",
      body:
        "Country Inn & Suites creates owner value on midscale conversions that deliver a residential, welcoming stay without upper-upscale capital—underwrite guestroom, lobby warmth, and breakfast operations so Choice affiliation is earned after PIP.",
    },
    {
      title: "Family And Leisure Corridor Stay",
      body:
        "Family and leisure corridors fit Country when suite mix, breakfast, and approachable design drive repeat demand—owners gain Choice distribution when labor intensity and conversion capital stay inside underwriting for the competitive set.",
    },
    {
      title: "Choice Midscale Portfolio Standardization",
      body:
        "Multi-asset sponsors use Country to standardize midscale presentation across Choice portfolios—value holds when conversion PIP consistency, capital pacing, and operator capacity are confirmed before treating affiliation as uniform or cheap.",
    },
    {
      title: "Third-Party Operator-Led",
      body:
        "Country Inn suits operators who can balance midscale QA with residential service cues—sponsors need Choice Privileges scale and financing credibility while keeping breakfast labor, finish expectations, and conversion scope inside underwriting.",
    },
  ]),

  "courtyard-by-marriott": freezePackage("Courtyard by Marriott", [
    {
      title: "Select-Service Conversion Reflag",
      body:
        "Courtyard creates owner value on select-service conversions where Bonvoy distribution and a proven business-travel product can stabilize demand—sponsors underwrite lobby, F&B, and guestroom PIP so affiliation is not modeled as a light cosmetic reflag.",
    },
    {
      title: "Corporate Corridor Portfolio Play",
      body:
        "Corporate and airport corridors fit Courtyard when transient demand is durable and capital can deliver consistent select-service standards—owners gain Marriott channel scale when labor and brand presentation stay inside corridor underwriting.",
    },
    {
      title: "Purpose-Built New Development",
      body:
        "New-build Courtyard projects suit sponsors who want a disciplined select-service prototype rather than full-service capital—value holds when construction costs, operator readiness, and Bonvoy economics are modeled to realistic ADR peers.",
    },
    {
      title: "Third-Party Operator-Led",
      body:
        "Courtyard works with operators who can execute Marriott brand standards and select-service rhythms—sponsors need Bonvoy lift and financing credibility while conversion or prototype scope, staffing, and F&B intensity stay inside underwriting.",
    },
  ]),

  "curio-collection": freezePackage("Curio Collection by Hilton", [
    {
      title: "Independent Soft-Brand Conversion",
      body:
        "Curio creates owner value on distinctive independents that need Hilton Honors distribution without a hard-brand prototype—affiliation fits when local story, design character, and F&B identity are assets owners want to preserve while meeting collection standards.",
    },
    {
      title: "Resort or Experiential Repositioning",
      body:
        "Resort or experiential assets suit Curio when place and programming already sell, and Hilton mainly sharpens demand—owners underwrite PIP, labor, and F&B intensity carefully so soft-brand cutover is not mistaken for light capital work.",
    },
    {
      title: "Heritage Adaptive Reuse",
      body:
        "Historic or character buildings fit Curio when architecture is the product and Hilton commercial tools can monetize it—sponsors protect how much local identity survives brand review while rooms and public spaces meet collection thresholds.",
    },
    {
      title: "Third-Party Operator-Led",
      body:
        "Curio suits assets run by experienced operators who balance Hilton collection QA with local execution—sponsors need affiliation lift, financing credibility, and Honors scale while keeping the property's story intact after cutover.",
    },
  ]),

  "dazzler-by-wyndham": freezePackage("Dazzler by Wyndham", [
    {
      title: "Independent Soft-Brand Conversion",
      body:
        "Dazzler creates owner value on distinctive independents that need Wyndham Rewards reach without full hard-brand homogenization—affiliation fits when local design and guest experience can meet soft-collection presentation while owners preserve character.",
    },
    {
      title: "Urban Lifestyle Character Stay",
      body:
        "Urban lifestyle assets suit Dazzler when neighborhood energy is the product and Wyndham systems monetize it—owners underwrite conversion capital and brand standards carefully so affiliation lift matches realistic ADR support in the corridor.",
    },
    {
      title: "Markets With Member Channel Gap",
      body:
        "Corridors where independents under-index on loyalty and direct booking fit Dazzler—value holds when the asset can deliver a credible stay and owners need Wyndham enterprise channels without surrendering uniqueness after cutover.",
    },
    {
      title: "Third-Party Operator-Led",
      body:
        "Dazzler works with operators who can balance soft-collection QA with local execution—sponsors need affiliation lift and financing credibility while keeping the property story, design identity, and conversion scope inside underwriting.",
    },
  ]),

  "design-hotels": freezePackage("Design Hotels", [
    {
      title: "Independent Design-Led Soft Brand",
      body:
        "Design Hotels creates owner value on design-led independents that need soft-brand credibility and Marriott-linked commercial tools without prototype homogenization—affiliation fits when architecture, F&B identity, and local story are assets owners want to preserve.",
    },
    {
      title: "Heritage Adaptive Reuse",
      body:
        "Historic or character buildings suit Design Hotels when design authorship is the product—owners underwrite conversion capital and brand review carefully so local identity survives while rooms and public spaces meet collection guest expectations.",
    },
    {
      title: "Destination Experiential Repositioning",
      body:
        "Destination experiential assets fit Design Hotels when place and cultural programming already sell—value holds when PIP, labor, and F&B intensity are priced honestly rather than treating soft affiliation as distribution-only capital light work.",
    },
    {
      title: "Third-Party Operator-Led",
      body:
        "Design Hotels suits sponsors using operators who can balance collection QA with design-forward execution—affiliation lift and financing credibility matter when the property's creative story stays intact after commercial platform cutover.",
    },
  ]),

  "even-hotels": freezePackage("EVEN Hotels", [
    {
      title: "Wellness-Led Urban Conversion",
      body:
        "EVEN Hotels creates owner value on urban conversions where wellness programming and an efficient lifestyle product can differentiate—IHG distribution fits when fitness, guestroom, and public-space standards can be delivered without full-service banquet capital.",
    },
    {
      title: "Gateway Lifestyle Development",
      body:
        "Gateway and employment-corridor sites suit EVEN when new-build or adaptive projects need IHG lifestyle positioning with wellness clarity—owners underwrite capital and labor carefully so affiliation lift matches realistic ADR for the market.",
    },
    {
      title: "Lifestyle Portfolio Standardization",
      body:
        "Multi-asset sponsors use EVEN to standardize a wellness lifestyle box—value holds when operator depth, conversion or prototype capital, and IHG systems cutover are priced honestly rather than assumed as a cosmetic reflag.",
    },
    {
      title: "Third-Party Operator-Led",
      body:
        "EVEN suits assets run by operators who can execute wellness service rhythms and IHG brand standards—sponsors need affiliation lift and financing credibility while keeping fitness programming and conversion scope inside underwriting.",
    },
  ]),

  "everhome-suites": freezePackage("Everhome Suites", [
    {
      title: "Weekly-Stay Midscale Conversion",
      body:
        "Everhome Suites creates owner value where weekly-stay demand can stabilize occupancy versus pure transient midscale—underwrite length-of-stay mix, kitchenette utility, and conversion capital against Choice extended-stay peers before modeling affiliation lift.",
    },
    {
      title: "Employment Corridor Portfolio Play",
      body:
        "Employment corridors fit Everhome when durable weekly and midweek demand can fill an efficient extended-stay box—owners gain Choice channel scale when labor rhythms and brand standards stay inside underwriting for the corridor.",
    },
    {
      title: "Purpose-Built Prototype Development",
      body:
        "Purpose-built Everhome prototypes suit sponsors who want disciplined extended-stay capital without overbuilding public space—value holds when construction costs, operator readiness, and Choice economics are modeled to weekly-stay ADR, not transient peaks.",
    },
    {
      title: "Third-Party Operator-Led",
      body:
        "Everhome works with operators who can run weekly housekeeping rhythms and Choice systems cutover—sponsors need affiliation lift and financing credibility while keeping kitchenette utility, conversion scope, and staffing inside underwriting.",
    },
  ]),

  "handwritten-collection": freezePackage("Handwritten Collection", [
    {
      title: "Independent Soft-Brand Conversion",
      body:
        "Handwritten Collection creates owner value on distinctive independents that need soft-brand credibility without full hard-brand homogenization—affiliation fits when local story, design character, and guest experience are assets owners want to preserve.",
    },
    {
      title: "Boutique Character Repositioning",
      body:
        "Boutique character assets suit Handwritten when architecture and neighborhood narrative are the product—owners underwrite conversion capital and brand review carefully so uniqueness survives while rooms and public spaces meet collection thresholds.",
    },
    {
      title: "Markets With Member Channel Gap",
      body:
        "Corridors where independents under-index on loyalty and direct booking fit Handwritten—value holds when the asset can deliver a credible stay and owners need enterprise channels without surrendering the property's distinctive identity.",
    },
    {
      title: "Third-Party Operator-Led",
      body:
        "Handwritten suits sponsors using operators who balance soft-collection QA with local execution—affiliation lift and financing credibility matter when the property story stays intact and conversion scope is priced honestly for the asset.",
    },
  ]),

  "holiday-inn-express": freezePackage("Holiday Inn Express", [
    {
      title: "Select-Service Conversion Reflag",
      body:
        "Holiday Inn Express creates owner value on select-service conversions where IHG distribution and a clear limited-service promise stabilize occupancy—sponsors underwrite guestroom, breakfast, and lobby PIP so affiliation is not treated as a cosmetic reflag.",
    },
    {
      title: "Highway And Suburban Corridor Portfolio",
      body:
        "Highway and suburban corridors fit Express when transient demand is reliable and capital can deliver a consistent midscale box—owners gain IHG channel scale when breakfast labor and brand standards stay inside corridor underwriting.",
    },
    {
      title: "Purpose-Built New Development",
      body:
        "New-build Holiday Inn Express projects suit sponsors who want a disciplined select-service prototype—value holds when construction capital, operator readiness, and IHG affiliation economics are modeled against realistic ADR peers for the market.",
    },
    {
      title: "Third-Party Operator-Led",
      body:
        "Express works with operators who can execute midscale QA and IHG systems cutover—sponsors need affiliation lift and financing credibility while keeping conversion or prototype scope, staffing, and breakfast operations inside underwriting.",
    },
  ]),

  "hotel-indigo": freezePackage("Hotel Indigo", [
    {
      title: "Neighborhood Urban Conversion",
      body:
        "Hotel Indigo creates owner value on urban conversions where neighborhood narrative and design-led lifestyle product can lift ADR—IHG distribution fits when lobby, F&B, and guestroom standards can be delivered without overbuilding full-service banquet scope.",
    },
    {
      title: "Gateway Lifestyle Development",
      body:
        "Gateway and cultural-corridor sites suit Indigo when new-build or adaptive projects need IHG lifestyle positioning rooted in place—owners underwrite capital and labor carefully so affiliation lift matches realistic rate support after cutover.",
    },
    {
      title: "Lifestyle Portfolio Standardization",
      body:
        "Multi-asset sponsors use Hotel Indigo to standardize neighborhood lifestyle stays across cities—value holds when operator depth, conversion capital, and IHG systems cutover are priced honestly rather than assumed as a light reflag.",
    },
    {
      title: "Third-Party Operator-Led",
      body:
        "Indigo suits assets run by operators who can balance IHG brand standards with local neighborhood storytelling—sponsors need affiliation lift and financing credibility while keeping design character and service intensity inside underwriting.",
    },
  ]),

  kimpton: freezePackage("Kimpton Hotels & Restaurants", [
    {
      title: "Urban Lifestyle Conversion",
      body:
        "Kimpton creates owner value on urban conversions where lifestyle design and destination F&B can lift ADR—IHG distribution fits when public spaces, restaurants, and guestrooms can meet brand expectations without forcing a generic hard-brand prototype.",
    },
    {
      title: "Gateway Development With F&B",
      body:
        "Gateway and high-energy urban sites suit Kimpton when sponsors can capitalize distinctive F&B and lifestyle service—owners underwrite labor, conversion capital, and brand standards carefully so affiliation lift matches corridor rate support.",
    },
    {
      title: "Lifestyle Portfolio Standardization",
      body:
        "Multi-asset sponsors use Kimpton to assemble a lifestyle portfolio with IHG commercial scale—value holds when operator depth, F&B intensity, and conversion PIP are priced honestly rather than treating soft lifestyle cues as capital-light work.",
    },
    {
      title: "Third-Party Operator-Led",
      body:
        "Kimpton suits assets run by operators who can execute lifestyle service and IHG brand QA—sponsors need affiliation lift and financing credibility while keeping F&B identity, design character, and conversion scope inside underwriting.",
    },
  ]),

  "mgallery-collection": freezePackage("MGallery Collection", [
    {
      title: "Independent Soft-Brand Conversion",
      body:
        "MGallery creates owner value on distinctive independents that need Accor Live Limitless reach without full hard-brand homogenization—affiliation fits when local story, design character, and F&B identity are assets owners want to preserve.",
    },
    {
      title: "Heritage Adaptive Reuse",
      body:
        "Historic or character buildings suit MGallery when architecture is the product and Accor commercial tools can monetize it—owners underwrite conversion capital and brand review carefully so local identity survives collection standards.",
    },
    {
      title: "Destination Experiential Repositioning",
      body:
        "Destination experiential assets fit MGallery when place and cultural programming already sell—value holds when PIP, labor, and F&B intensity stay inside underwriting rather than assuming a light soft-brand cutover for Accor distribution.",
    },
    {
      title: "Third-Party Operator-Led",
      body:
        "MGallery suits sponsors using operators who balance collection QA with local execution—affiliation lift, financing credibility, and Accor platform scale matter when the property's story stays intact after cutover.",
    },
  ]),

  "motto-by-hilton": freezePackage("Motto by Hilton", [
    {
      title: "Compact Urban Conversion",
      body:
        "Motto creates owner value on compact urban conversions where efficient rooms and lifestyle public space can lift ADR—Hilton Honors distribution fits when sponsors underwrite a disciplined guestroom and lobby PIP without full-service banquet capital.",
    },
    {
      title: "Gateway Micro-Footprint Development",
      body:
        "Gateway and dense urban sites suit Motto when new-build or adaptive projects need Hilton lifestyle positioning on a tighter footprint—owners price construction capital and labor carefully against realistic urban ADR after affiliation.",
    },
    {
      title: "Lifestyle Portfolio Standardization",
      body:
        "Multi-asset sponsors use Motto to standardize a compact lifestyle box across cities—value holds when operator depth, prototype or conversion capital, and Hilton systems cutover are modeled honestly rather than as a cosmetic reflag.",
    },
    {
      title: "Third-Party Operator-Led",
      body:
        "Motto suits assets run by operators who can execute Hilton brand standards in compact urban formats—sponsors need Honors scale and financing credibility while keeping room efficiency and public-space intensity inside underwriting.",
    },
  ]),

  "moxy-hotels": freezePackage("Moxy Hotels", [
    {
      title: "Urban Lifestyle Conversion",
      body:
        "Moxy creates owner value on urban conversions where playful lifestyle design and social public space can lift ADR—Bonvoy distribution fits when sponsors deliver guestroom and lobby standards without overbuilding full-service banquet or resort scope.",
    },
    {
      title: "Gateway Development Opportunity",
      body:
        "Gateway and nightlife-adjacent corridors suit Moxy when new-build or adaptive projects need Marriott lifestyle positioning for younger transient demand—owners underwrite capital and labor carefully so affiliation lift matches market rate support.",
    },
    {
      title: "Lifestyle Portfolio Standardization",
      body:
        "Multi-asset sponsors use Moxy to standardize a social lifestyle box across cities—value holds when operator depth, conversion or prototype capital, and Bonvoy cutover are priced honestly rather than treated as a light reflag.",
    },
    {
      title: "Third-Party Operator-Led",
      body:
        "Moxy suits assets run by operators who can balance Marriott brand standards with energetic lifestyle service—sponsors need Bonvoy scale and financing credibility while keeping design character and labor intensity inside underwriting.",
    },
  ]),

  "preferred-hotels-and-resorts": freezePackage("Preferred Hotels & Resorts", [
    {
      title: "Independent Soft-Brand Conversion",
      body:
        "Preferred creates owner value on distinctive independents that need soft-brand credibility and global sales reach without hard-brand homogenization—affiliation fits when local story, design character, and service identity are assets owners want to preserve.",
    },
    {
      title: "Luxury Boutique Character Stay",
      body:
        "Luxury boutique assets suit Preferred when architecture and guest experience are already the product—owners underwrite brand presentation and commercial platform lift carefully so affiliation sharpens demand without forcing prototype standardization.",
    },
    {
      title: "Destination Resort Soft Affiliation",
      body:
        "Destination resorts fit Preferred when place and programming already sell, and the soft brand mainly adds global channels—value holds when PIP, labor, and F&B intensity stay inside underwriting rather than assuming capital-light cutover.",
    },
    {
      title: "Third-Party Operator-Led",
      body:
        "Preferred suits sponsors using operators who balance soft-collection QA with local luxury execution—affiliation lift and financing credibility matter when the property story stays intact and conversion scope is priced for the asset.",
    },
  ]),

  "quality-inn": freezePackage("Quality Inn", [
    {
      title: "Midscale Conversion Reflag",
      body:
        "Quality Inn creates owner value on midscale conversions where Choice distribution and a clear guest promise can stabilize occupancy without full-service capital—underwrite guestroom, lobby, and systems PIP so affiliation is not treated as cosmetic.",
    },
    {
      title: "Highway And Suburban Corridor Portfolio",
      body:
        "Highway and suburban corridors fit Quality when transient demand is reliable and capital can deliver a consistent midscale product—owners gain Choice channel scale when labor and brand standards stay inside corridor underwriting after cutover.",
    },
    {
      title: "Purpose-Built Midscale New Build",
      body:
        "New-build Quality Inn projects suit sponsors who want a disciplined midscale prototype—value holds when construction capital, operator readiness, and Choice affiliation economics are modeled against realistic ADR for the competitive corridor.",
    },
    {
      title: "Third-Party Operator-Led",
      body:
        "Quality works with operators who can execute midscale QA and Choice systems cutover—sponsors need affiliation lift and financing credibility while keeping conversion or prototype scope, staffing, and presentation inside underwriting.",
    },
  ]),

  radisson: freezePackage("Radisson", [
    {
      title: "Full-Service Urban Conversion",
      body:
        "Radisson creates owner value on full-service conversions where Choice Privileges and enterprise channels can lift transient mix—sponsors underwrite lobby, F&B, and guestroom PIP carefully so affiliation is not modeled as a light cosmetic reflag.",
    },
    {
      title: "Gateway And Meeting Corridor Play",
      body:
        "Gateway and meeting-oriented corridors fit Radisson when capital can support credible full-service presentation—owners gain Choice distribution when labor intensity and brand standards stay inside underwriting for realistic ADR peers.",
    },
    {
      title: "Portfolio Full-Service Standardization",
      body:
        "Multi-asset sponsors use Radisson to standardize full-service presentation across Choice portfolios—value holds when conversion capital pacing, operator depth, and systems cutover are confirmed before treating affiliation as uniform or cheap.",
    },
    {
      title: "Third-Party Operator-Led",
      body:
        "Radisson suits assets run by operators who can balance full-service QA with Choice systems execution—sponsors need affiliation lift and financing credibility while keeping F&B labor and conversion scope inside underwriting.",
    },
  ]),

  "radisson-blu": freezePackage("Radisson Blu", [
    {
      title: "Upscale Urban Conversion",
      body:
        "Radisson Blu creates owner value on upscale urban conversions where Choice Privileges and stronger design presentation can lift ADR—sponsors underwrite public space, F&B, and guestroom PIP so affiliation is earned rather than assumed as cosmetic.",
    },
    {
      title: "Gateway Lifestyle Development",
      body:
        "Gateway and high-visibility sites suit Radisson Blu when new-build or adaptive projects need upscale Choice positioning—owners price construction capital and labor carefully against realistic corridor ADR after affiliation cutover.",
    },
    {
      title: "Upscale Portfolio Standardization",
      body:
        "Multi-asset sponsors use Radisson Blu to standardize an upscale box across markets—value holds when operator depth, conversion capital, and Choice systems cutover are modeled honestly rather than treated as a light reflag.",
    },
    {
      title: "Third-Party Operator-Led",
      body:
        "Radisson Blu works with operators who can execute upscale service and Choice brand QA—sponsors need affiliation lift and financing credibility while keeping design finish, F&B intensity, and conversion scope inside underwriting.",
    },
  ]),

  "radisson-individuals-by-choice": freezePackage("Radisson Individuals by Choice", [
    {
      title: "Independent Soft-Brand Conversion",
      body:
        "Radisson Individuals creates owner value on distinctive independents that need Choice Privileges without full hard-brand homogenization—affiliation fits when local story, design character, and F&B identity are assets owners want to preserve.",
    },
    {
      title: "Heritage Adaptive Reuse",
      body:
        "Historic or character buildings suit Radisson Individuals when architecture is the product and Choice commercial tools can monetize it—owners underwrite conversion capital and brand review so local identity survives collection standards.",
    },
    {
      title: "Markets With Member Channel Gap",
      body:
        "Corridors where independents under-index on loyalty and direct booking fit Radisson Individuals—value holds when the asset can deliver a credible stay and owners need Choice enterprise channels without surrendering uniqueness.",
    },
    {
      title: "Third-Party Operator-Led",
      body:
        "Radisson Individuals suits sponsors using operators who balance soft-collection QA with local execution—affiliation lift, financing credibility, and Choice Privileges scale matter when the property story stays intact after cutover.",
    },
  ]),

  "radisson-red": freezePackage("Radisson RED", [
    {
      title: "Urban Lifestyle Conversion",
      body:
        "Radisson RED creates owner value on urban conversions where bold lifestyle design and social public space can lift ADR—Choice Privileges fits when sponsors deliver guestroom and lobby standards without overbuilding full-service banquet capital.",
    },
    {
      title: "Gateway Lifestyle Development",
      body:
        "Gateway and nightlife-adjacent corridors suit Radisson RED when new-build or adaptive projects need Choice lifestyle positioning—owners underwrite capital and labor carefully so affiliation lift matches realistic urban ADR after cutover.",
    },
    {
      title: "Lifestyle Portfolio Standardization",
      body:
        "Multi-asset sponsors use Radisson RED to standardize an energetic lifestyle box—value holds when operator depth, conversion or prototype capital, and Choice systems cutover are priced honestly rather than as a cosmetic reflag.",
    },
    {
      title: "Third-Party Operator-Led",
      body:
        "Radisson RED suits assets run by operators who can balance Choice brand standards with lifestyle service energy—sponsors need affiliation lift and financing credibility while keeping design character and labor intensity inside underwriting.",
    },
  ]),

  "small-luxury-hotels-of-the-world": freezePackage("Small Luxury Hotels of the World", [
    {
      title: "Independent Luxury Soft Brand",
      body:
        "SLH creates owner value on distinctive luxury independents that need global soft-brand credibility without hard-brand homogenization—affiliation fits when local story, design authorship, and service identity are assets owners want to preserve.",
    },
    {
      title: "Heritage Luxury Adaptive Reuse",
      body:
        "Historic luxury shells suit SLH when architecture and place are the product—owners underwrite conversion capital and membership expectations carefully so uniqueness survives while rooms and public spaces meet luxury guest thresholds.",
    },
    {
      title: "Destination Resort Soft Affiliation",
      body:
        "Destination resorts fit SLH when experiential programming already sells and the soft brand mainly adds global channels—value holds when PIP, labor, and F&B intensity stay inside underwriting rather than assuming capital-light cutover.",
    },
    {
      title: "Third-Party Operator-Led",
      body:
        "SLH suits sponsors using operators who balance soft-collection QA with luxury local execution—affiliation lift and financing credibility matter when the property story stays intact and conversion scope is priced for the asset.",
    },
  ]),

  "suburban-studios": freezePackage("Suburban Studios", [
    {
      title: "Extended-Stay Midscale Conversion",
      body:
        "Suburban Studios creates owner value where extended-stay demand can stabilize occupancy versus pure transient midscale—underwrite length-of-stay mix, kitchenette utility, and conversion capital against Choice extended-stay peers before modeling affiliation lift.",
    },
    {
      title: "Employment Corridor Portfolio Play",
      body:
        "Employment corridors fit Suburban when durable weekly and midweek demand can fill an efficient extended-stay box—owners gain Choice channel scale when labor rhythms and brand standards stay inside underwriting for the corridor.",
    },
    {
      title: "Purpose-Built Prototype Development",
      body:
        "Purpose-built Suburban prototypes suit sponsors who want disciplined extended-stay capital without overbuilding public space—value holds when construction costs, operator readiness, and Choice economics are modeled to weekly-stay ADR peers.",
    },
    {
      title: "Third-Party Operator-Led",
      body:
        "Suburban works with operators who can run extended-stay housekeeping rhythms and Choice systems cutover—sponsors need affiliation lift and financing credibility while keeping kitchenette utility, conversion scope, and staffing inside underwriting.",
    },
  ]),

  "tapestry-collection-by-hilton": freezePackage("Tapestry Collection by Hilton", [
    {
      title: "Independent Soft-Brand Conversion",
      body:
        "Tapestry creates owner value on distinctive independents that need Hilton Honors distribution without a hard-brand prototype—affiliation fits when local story, design character, and F&B identity are assets owners want to preserve while meeting collection standards.",
    },
    {
      title: "Heritage Adaptive Reuse",
      body:
        "Historic or character buildings suit Tapestry when architecture is the product and Hilton commercial tools can monetize it—owners underwrite conversion capital and brand review carefully so local identity survives collection thresholds.",
    },
    {
      title: "Markets With Member Channel Gap",
      body:
        "Corridors where independents under-index on loyalty and direct booking fit Tapestry—value holds when the asset can deliver a credible guest experience and owners need Hilton enterprise channels without surrendering uniqueness.",
    },
    {
      title: "Third-Party Operator-Led",
      body:
        "Tapestry suits sponsors using operators who balance Hilton collection QA with local execution—affiliation lift, financing credibility, and Honors scale matter when the property's story stays intact after cutover.",
    },
  ]),

  "tempo-by-hilton": freezePackage("Tempo by Hilton", [
    {
      title: "Urban Lifestyle Conversion",
      body:
        "Tempo creates owner value on urban conversions where lifestyle design and social public space can lift ADR—Hilton Honors distribution fits when sponsors deliver guestroom and lobby standards without overbuilding full-service banquet capital.",
    },
    {
      title: "Gateway Lifestyle Development",
      body:
        "Gateway and high-energy urban sites suit Tempo when new-build or adaptive projects need Hilton lifestyle positioning—owners underwrite capital and labor carefully so affiliation lift matches realistic corridor ADR after cutover.",
    },
    {
      title: "Lifestyle Portfolio Standardization",
      body:
        "Multi-asset sponsors use Tempo to standardize a contemporary lifestyle box across cities—value holds when operator depth, conversion or prototype capital, and Hilton systems cutover are priced honestly rather than as a cosmetic reflag.",
    },
    {
      title: "Third-Party Operator-Led",
      body:
        "Tempo suits assets run by operators who can balance Hilton brand standards with lifestyle service rhythms—sponsors need Honors scale and financing credibility while keeping design character and labor intensity inside underwriting.",
    },
  ]),

  "trademark-collection-by-wyndham": freezePackage("Trademark Collection by Wyndham", [
    {
      title: "Independent Soft-Brand Conversion",
      body:
        "Trademark creates owner value on distinctive independents that need Wyndham Rewards reach without full hard-brand homogenization—affiliation fits when local story, design character, and guest experience are assets owners want to preserve.",
    },
    {
      title: "Boutique Character Repositioning",
      body:
        "Boutique character assets suit Trademark when architecture and neighborhood narrative are the product—owners underwrite conversion capital and brand review carefully so uniqueness survives while rooms and public spaces meet collection thresholds.",
    },
    {
      title: "Markets With Member Channel Gap",
      body:
        "Corridors where independents under-index on loyalty and direct booking fit Trademark—value holds when the asset can deliver a credible stay and owners need Wyndham enterprise channels without surrendering uniqueness after cutover.",
    },
    {
      title: "Third-Party Operator-Led",
      body:
        "Trademark suits sponsors using operators who balance soft-collection QA with local execution—affiliation lift, financing credibility, and Wyndham platform scale matter when the property story stays intact after cutover.",
    },
  ]),

  "tribute-portfolio": freezePackage("Tribute Portfolio", [
    {
      title: "Independent Soft-Brand Conversion",
      body:
        "Tribute creates owner value on distinctive independents that need Bonvoy distribution without a hard-brand prototype—affiliation fits when local story, design character, and F&B identity are assets owners want to preserve while meeting collection standards.",
    },
    {
      title: "Heritage Adaptive Reuse",
      body:
        "Historic or character buildings suit Tribute when architecture is the product and Marriott commercial tools can monetize it—owners underwrite conversion capital and brand review carefully so local identity survives collection thresholds.",
    },
    {
      title: "Resort or Experiential Repositioning",
      body:
        "Resort or experiential assets fit Tribute when place and programming already sell, and Bonvoy mainly sharpens demand—value holds when PIP, labor, and F&B intensity stay inside underwriting rather than assuming a light soft-brand cutover.",
    },
    {
      title: "Third-Party Operator-Led",
      body:
        "Tribute suits sponsors using operators who balance Marriott collection QA with local execution—affiliation lift, financing credibility, and Bonvoy scale matter when the property's story stays intact after cutover.",
    },
  ]),

  "vignette-collection": freezePackage("Vignette Collection", [
    {
      title: "Independent Soft-Brand Conversion",
      body:
        "Vignette creates owner value on distinctive independents that need IHG soft-brand credibility without full hard-brand homogenization—affiliation fits when local story, design character, and F&B identity are assets owners want to preserve.",
    },
    {
      title: "Heritage Adaptive Reuse",
      body:
        "Historic or character buildings suit Vignette when architecture is the product and IHG commercial tools can monetize it—owners underwrite conversion capital and brand review carefully so local identity survives collection standards.",
    },
    {
      title: "Destination Experiential Repositioning",
      body:
        "Destination experiential assets fit Vignette when place and cultural programming already sell—value holds when PIP, labor, and F&B intensity stay inside underwriting rather than treating soft affiliation as capital-light distribution only.",
    },
    {
      title: "Third-Party Operator-Led",
      body:
        "Vignette suits sponsors using operators who balance soft-collection QA with local execution—affiliation lift, financing credibility, and IHG platform scale matter when the property story stays intact after cutover.",
    },
  ]),

  "voco-hotels": freezePackage("voco Hotels", [
    {
      title: "Urban Lifestyle Conversion",
      body:
        "voco creates owner value on urban conversions where lifestyle design and approachable full-service cues can lift ADR—IHG distribution fits when sponsors deliver lobby, F&B, and guestroom standards without forcing a rigid hard-brand prototype.",
    },
    {
      title: "Gateway Development Opportunity",
      body:
        "Gateway and high-visibility sites suit voco when new-build or adaptive projects need IHG lifestyle positioning—owners underwrite capital and labor carefully so affiliation lift matches realistic corridor ADR after systems cutover.",
    },
    {
      title: "Lifestyle Portfolio Standardization",
      body:
        "Multi-asset sponsors use voco to standardize a lifestyle box across markets—value holds when operator depth, conversion or prototype capital, and IHG cutover are priced honestly rather than assumed as a cosmetic reflag.",
    },
    {
      title: "Third-Party Operator-Led",
      body:
        "voco suits assets run by operators who can balance IHG brand standards with local lifestyle execution—sponsors need affiliation lift and financing credibility while keeping design character and service intensity inside underwriting.",
    },
  ]),

  "woodspring-suites": freezePackage("WoodSpring Suites", [
    {
      title: "Extended-Stay Economy Conversion",
      body:
        "WoodSpring Suites creates owner value where extended-stay demand can stabilize occupancy at economy capital intensity—underwrite length-of-stay mix, kitchenette utility, and conversion capital against Choice extended-stay peers before modeling affiliation lift.",
    },
    {
      title: "Employment Corridor Portfolio Play",
      body:
        "Employment corridors fit WoodSpring when durable weekly demand can fill an efficient extended-stay box—owners gain Choice channel scale when labor rhythms and brand standards stay inside underwriting for the competitive corridor.",
    },
    {
      title: "Purpose-Built Prototype Development",
      body:
        "Purpose-built WoodSpring prototypes suit sponsors who want disciplined extended-stay capital without overbuilding public space—value holds when construction costs, operator readiness, and Choice economics are modeled to weekly-stay ADR peers.",
    },
    {
      title: "Third-Party Operator-Led",
      body:
        "WoodSpring works with operators who can run extended-stay housekeeping rhythms and Choice systems cutover—sponsors need affiliation lift and financing credibility while keeping kitchenette utility, conversion scope, and staffing inside underwriting.",
    },
  ]),
});

/**
 * @param {string} slug
 * @returns {ValueCreationScenarioPackage | null}
 */
export function getValueCreationScenarioPackage(slug) {
  const key = nz(slug);
  if (!key) return null;
  return VALUE_CREATION_SCENARIO_PACKAGES[key] || null;
}

/**
 * Validate package body word counts against the Ascend short-paragraph band.
 * Used by tests.
 *
 * @param {ValueCreationScenarioPackage} pkg
 * @param {{ min?: number, max?: number }} [opts]
 * @returns {{
 *   pass: boolean,
 *   brandName: string,
 *   min: number,
 *   max: number,
 *   scenarioCount: number,
 *   counts: number[],
 *   failures: string[]
 * }}
 */
export function assertPackageWordCounts(pkg, opts = {}) {
  const min = Number(opts.min ?? VALUE_CREATION_PACKAGE_MIN_BODY_WORDS);
  const max = Number(opts.max ?? VALUE_CREATION_PACKAGE_MAX_BODY_WORDS);
  const brandName = nz(pkg?.brandName);
  const scenarios = Array.isArray(pkg?.scenarios) ? pkg.scenarios : [];
  const counts = [];
  const failures = [];

  if (scenarios.length !== 4) {
    failures.push(`scenario_count:${scenarios.length}_of_4`);
  }

  for (let i = 0; i < scenarios.length; i++) {
    const body = nz(scenarios[i]?.body);
    const title = nz(scenarios[i]?.title);
    const wordCount = words(body);
    counts.push(wordCount);
    const slot = `scenario.${i + 1}`;
    if (!title) failures.push(`missing_title_${slot}`);
    if (!body) failures.push(`blank_body_${slot}`);
    else if (wordCount < min) failures.push(`thin_body_${slot}:${wordCount}`);
    else if (wordCount > max) failures.push(`long_body_${slot}:${wordCount}`);
  }

  return {
    pass: failures.length === 0,
    brandName,
    min,
    max,
    scenarioCount: scenarios.length,
    counts,
    failures,
  };
}
