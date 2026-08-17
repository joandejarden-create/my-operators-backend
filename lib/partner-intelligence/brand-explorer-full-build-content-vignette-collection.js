/**
 * Brand Explorer Tab Factory — full build content pack: Vignette Collection.
 *
 * True-incomplete brand (see brand-explorer-built-blocked-content.js →
 * BUILT_BLOCKED_TRUE_INCOMPLETE). IHG's independent-hotel soft/luxury-lifestyle
 * collection — the lightest-touch IHG affiliation path, for owners who want to keep
 * their property fully independent while joining IHG One Rewards.
 *
 * Copy rules:
 * - Directional, owner-facing. No invented fees, ADR, FDD, Item 19, pipeline counts,
 *   or performance guarantees.
 * - Brand-specific — avoids IHG-umbrella boilerplate as the brand story.
 * - No Company Validated claims.
 * - No raw https:// URLs in any Body field (PVQL fails on raw URLs in owner-facing copy).
 * - Distinguishes Vignette from Hotel Indigo (IHG lifestyle full brand with a more
 *   standardized neighborhood-storytelling prototype) and Kimpton (IHG boutique luxury
 *   lifestyle full brand with a restaurant-forward, more standardized guest promise).
 */

const BRAND_SLUG = "vignette-collection";
const BRAND_NAME = "Vignette Collection";
const PARENT_COMPANY = "InterContinental Hotels Group";
const RECORD_ID = "recDwzv86TWnz2gGB";

function row(slotKey, title, body, sortOrder, extra = {}) {
  return {
    slotKey,
    title: title || "",
    body,
    sortOrder,
    ...(extra.caseSummaryOverview ? { caseSummaryOverview: extra.caseSummaryOverview } : {}),
    ...(extra.caseSummaryBrandRelevance
      ? { caseSummaryBrandRelevance: extra.caseSummaryBrandRelevance }
      : {}),
    ...(extra.caseSummaryOwnerObjective
      ? { caseSummaryOwnerObjective: extra.caseSummaryOwnerObjective }
      : {}),
    ...(extra.caseSummaryInterpretation
      ? { caseSummaryInterpretation: extra.caseSummaryInterpretation }
      : {}),
    ...(extra.caseSummaryTags ? { caseSummaryTags: extra.caseSummaryTags } : {}),
  };
}

const PRESENTATION = [
  // --- Positioning (Basics-backed slots) ---
  row(
    "Brand Positioning",
    "",
    "IHG's lightest-touch independent-hotel collection—Vignette lets a distinctive luxury-leaning or upper-upscale property keep its own identity and operating independence while joining IHG One Rewards, with less prototype standardization than Hotel Indigo or Kimpton.",
    10
  ),
  row(
    "Guest Psychographics Description",
    "",
    "Independent-minded upper-upscale and luxury-leaning travelers who want a genuinely distinctive, one-of-a-kind stay and still value IHG One Rewards participation—guests who would resist a hotel that felt like a standardized chain property.",
    11
  ),

  // --- Overview ---
  row(
    "overview.typical_use_case",
    "",
    "Independent luxury-leaning or upper-upscale hotels with an established identity and operating track record—owners seeking IHG One Rewards distribution and commercial systems while preserving full independence, without adopting Hotel Indigo's neighborhood prototype or Kimpton's restaurant-forward full-brand model.",
    20
  ),
  row(
    "overview.development_model",
    "",
    "Affiliation-led rather than conversion-heavy—Vignette suits properties that already operate at a credible independent standard and need distribution more than physical repositioning. Sponsors should confirm acceptance criteria and any residual PIP directly rather than assuming this is a standardized franchise conversion.",
    21
  ),
  row(
    "overview.relative_positioning",
    "Relative Positioning",
    "Vignette Collection is IHG's lightest-touch independent-hotel affiliation—less standardized than Hotel Indigo's neighborhood-storytelling full brand and Kimpton's restaurant-forward boutique luxury full brand. Owners should compare operating autonomy, design-review intensity, and F&B expectations across all three before selecting an IHG path.",
    22
  ),
  row(
    "overview.scenario.1",
    "Independent Luxury-Leaning Affiliation",
    "An established independent hotel or resort with a credible luxury-leaning or upper-upscale guest experience seeking IHG One Rewards distribution without converting to a standardized prototype. Vignette fits when the property's independence is the value proposition—confirm acceptance criteria and any residual PIP directly with IHG development.",
    30
  ),
  row(
    "overview.scenario.2",
    "Distinctive Boutique Without A Restaurant-Forward Mandate",
    "A boutique or design-led hotel that does not need Kimpton's restaurant-forward, pet-friendly full-brand promise but still wants IHG distribution and loyalty participation. Owners should diligence Vignette's lighter operating requirements, acceptance criteria, and systems-integration scope against Kimpton's more standardized guest-experience mandate before assuming either path is interchangeable.",
    31
  ),
  row(
    "overview.scenario.3",
    "Heritage Or Character Asset Seeking Minimal Standardization",
    "A heritage building or character-driven independent hotel where ownership wants to preserve maximum operating and design autonomy while still gaining IHG commercial reach. Vignette fits better than Hotel Indigo's more prototype-driven neighborhood-lifestyle model in this scenario—confirm acceptance criteria, design-review scope, and residual PIP directly with IHG development before committing capital.",
    32
  ),
  row(
    "overview.why_value",
    "Why Value Is Strongest",
    "Value concentrates where the property is already a credible, established independent luxury-leaning or upper-upscale hotel that wants IHG One Rewards distribution without giving up operating autonomy. Weakest fit is an asset that actually needs a standardized prototype (Hotel Indigo) or a restaurant-forward full-brand promise (Kimpton) to compete.",
    33
  ),
  row(
    "overview.proof.1",
    "Independence-First Affiliation",
    "Vignette Collection markets itself around preserving a hotel's independent identity rather than imposing a design or service prototype—owners should treat this as the lightest-touch IHG collection option and confirm current acceptance criteria rather than assuming Hotel Indigo or Kimpton-level standardization applies.",
    40
  ),
  row(
    "overview.proof.2",
    "IHG One Rewards Distribution",
    "Brand materials position IHG One Rewards loyalty participation and IHG's commercial systems as the core affiliation value—loyalty earn/redeem and distribution reach layered onto an independently operated hotel. Confirm systems integration scope directly with IHG development for the specific asset.",
    41
  ),
  row(
    "overview.proof.3",
    "Lighter Standardization Than Indigo Or Kimpton",
    "Vignette's design and operating review is generally lighter than Hotel Indigo's neighborhood-storytelling prototype and Kimpton's restaurant-forward full-brand mandate. Owners should confirm current acceptance criteria directly with IHG development rather than assuming Indigo- or Kimpton-level capital, staffing, and F&B standards automatically apply to a Vignette affiliation.",
    42
  ),
  row(
    "overview.proof.4",
    "Established Independent Track Record Expected",
    "Vignette generally fits properties with an already-credible independent operating track record rather than ground-up conversions needing significant repositioning. Owners should confirm whether their asset's current condition, service culture, and operating history meet acceptance expectations before assuming affiliation alone will elevate a weaker property's positioning.",
    43
  ),
  row(
    "overview.featured_application",
    "Independent luxury-leaning affiliation",
    "An established independent luxury-leaning or upper-upscale hotel can use Vignette Collection to gain IHG One Rewards distribution while preserving its own identity and operating autonomy—without adopting Hotel Indigo's neighborhood prototype or Kimpton's restaurant-forward full-brand mandate. Owners should confirm acceptance criteria and any residual PIP directly with IHG development.",
    44,
    {
      caseSummaryOverview:
        "Featured path for established independent luxury-leaning or upper-upscale assets seeking IHG distribution under Vignette Collection.",
      caseSummaryBrandRelevance:
        "Matches Vignette's independence-first lane—lighter standardization than Hotel Indigo and Kimpton within IHG's portfolio.",
      caseSummaryOwnerObjective:
        "Gain IHG One Rewards distribution and commercial systems without giving up operating and design autonomy.",
      caseSummaryInterpretation:
        "Use as an affiliation-fit lens—confirm acceptance criteria and agreement terms directly with IHG development; not a performance forecast.",
      caseSummaryTags: "soft-brand, independent-character, luxury-leaning, IHG, affiliation",
    }
  ),
  row(
    "overview.differentiators.identity",
    "Experience & Identity",
    [
      "Independence-first collection—no shared design or service prototype",
      "Lighter standardization than Hotel Indigo's neighborhood-lifestyle model",
      "No restaurant-forward full-brand mandate unlike Kimpton",
      "Established independent character preserved after IHG affiliation",
    ].join("\n"),
    45
  ),
  row(
    "overview.differentiators.commercial",
    "Commercial & Distribution",
    [
      "IHG One Rewards loyalty earn/redeem participation",
      "IHG global sales and commercial systems access",
      "Distribution reach without a standardized franchise prototype",
      "Confirm specific commercial participation terms directly with IHG development",
    ].join("\n"),
    46
  ),
  row(
    "overview.bestAt.1",
    "Preserving Full Independence",
    "Protecting an established independent hotel's own identity and operating autonomy while adding IHG One Rewards reach—Vignette's core value versus Hotel Indigo's more prototype-driven approach.",
    47
  ),
  row(
    "overview.bestAt.2",
    "Lightest-Touch IHG Affiliation",
    "Offering the least standardized IHG collection option—owners should benchmark to comparable independent luxury-leaning properties, not Hotel Indigo or Kimpton's more structured guest-experience mandates.",
    48
  ),
  row(
    "overview.bestAt.3",
    "IHG Systems Without A Restaurant-Forward Mandate",
    "Delivering IHG commercial infrastructure and One Rewards participation without requiring Kimpton's restaurant-forward, pet-friendly full-brand promise—distinct from both Kimpton and Hotel Indigo.",
    49
  ),
  row(
    "overview.portfolio_context",
    "Portfolio Context",
    "Within IHG's lifestyle and independent-character portfolio, Vignette Collection is the lightest-touch affiliation path—less standardized than Hotel Indigo's neighborhood-storytelling full brand and Kimpton's restaurant-forward boutique luxury full brand. Owners should compare operating autonomy, design-review intensity, and F&B expectations across those siblings before selecting an IHG path.",
    50
  ),
  row(
    "footprint.portfolio_context",
    "Portfolio Context",
    "Vignette Collection anchors the independence-first end of IHG's lifestyle and luxury-leaning portfolio—less standardized than Hotel Indigo and Kimpton and closer to a pure soft-collection model than either. Owners should weigh operating autonomy against desired brand-driven guest-experience structure before choosing among the three.",
    51
  ),
  row(
    "valueOwners.watchouts",
    "",
    [
      "Vignette generally suits an already-credible independent hotel—confirm acceptance criteria before assuming affiliation alone will elevate a weaker asset",
      "Do not assume Hotel Indigo or Kimpton-level standardization, capital, or F&B mandates apply—confirm current requirements directly",
      "Operating autonomy is real, but IHG systems, loyalty, and reporting participation still apply—confirm scope during diligence",
      "Confirm current remediation and ongoing review expectations directly—do not assume affiliation value is permanent",
    ].join("\n"),
    52
  ),

  // --- Value to Owners: lifecycle ---
  row(
    "valueOwners.lifecycle.1",
    "Evaluation",
    "Confirm the property already operates at a credible independent luxury-leaning or upper-upscale standard worth Vignette's independence-first positioning, not whether it merely wants an IHG flag. Assess whether Vignette, Hotel Indigo, or Kimpton best fits the asset's operating model and guest experience before committing to a path.",
    300
  ),
  row(
    "valueOwners.lifecycle.2",
    "Conversion Design",
    "Shape any affiliation-driven design work around preserving the property's existing identity—Vignette requires less standardized public-space or room-prototype conversion than Hotel Indigo or Kimpton. Confirm acceptance-critical items directly rather than assuming a light or heavy conversion scope.",
    301
  ),
  row(
    "valueOwners.lifecycle.3",
    "Pre-Opening",
    "Coordinate IHG systems cutover, One Rewards integration, staffing, and training with any acceptance-driven readiness work. Confirm owner versus operator responsibilities for commercial launch, and budget time for collection-specific orientation rather than a Hotel Indigo or Kimpton opening checklist.",
    302
  ),
  row(
    "valueOwners.lifecycle.4",
    "Opening",
    "Launch with the property's existing independent identity intact while IHG systems and One Rewards participation activate. Opening support typically centers on systems stabilization rather than a standardized full-brand punch list—confirm support scope directly with IHG development.",
    303
  ),
  row(
    "valueOwners.lifecycle.5",
    "Ramp-Up",
    "During ramp-up, calibrate rate positioning and distribution contribution from IHG One Rewards channels against the property's existing independent guest-experience quality, not only occupancy headlines. Watch whether affiliation is genuinely adding distribution value versus diluting the independent story.",
    304
  ),
  row(
    "valueOwners.lifecycle.6",
    "Ongoing",
    "On an ongoing basis, maintain the property's independent character while meeting IHG systems and reporting obligations, and reassess affiliation value as the property, operator, and market evolve. Confirm renewal and review expectations with IHG development before major changes.",
    305
  ),

  // --- Operations & Standards: model ---
  row(
    "operations.model.primary_model",
    "",
    "Independence-first soft-brand affiliation within IHG, delivered through arrangements that owners must confirm for the specific market and asset—lighter-touch than Hotel Indigo or Kimpton franchise structures.",
    100
  ),
  row(
    "operations.model.management_option",
    "",
    "Independent, owner-operated management is common; third-party operators must still preserve the property's own identity rather than imposing a standardized IHG lifestyle prototype.",
    101
  ),
  row(
    "operations.model.typical_ownership",
    "",
    "Owners of established independent luxury-leaning or upper-upscale hotels who want IHG One Rewards distribution without converting into Hotel Indigo's or Kimpton's more standardized models.",
    102
  ),
  row(
    "operations.model.brand_involvement",
    "",
    "IHG development and acceptance review typically focus on the property's independent identity and operating track record rather than a fixed design or service prototype. Confirm the current review process directly.",
    103
  ),
  row(
    "operations.model.systems_integration",
    "",
    "Vignette participates in IHG's One Rewards loyalty and commercial systems ecosystem. Owners should validate PMS/CRS cutover, training, and commercial systems requirements for the specific deal.",
    104
  ),
  row(
    "operations.model.pre_opening",
    "",
    "Expect acceptance review and readiness work generally lighter in scope than a Hotel Indigo or Kimpton conversion. Sequence any residual PIP and operating setup with financing and operator capacity.",
    105
  ),
  row(
    "operations.model.staffing_intensity",
    "",
    "Staffing intensity follows the property's existing independent operating model—Vignette does not impose a standardized service level beyond the property's own established standard.",
    106
  ),
  row(
    "operations.model.fb_complexity",
    "",
    "F&B complexity follows the property's existing independent concept—Vignette has no restaurant-forward mandate like Kimpton's. Owners should maintain, not redesign, F&B around a brand template.",
    107
  ),
  row(
    "operations.model.training",
    "",
    "IHG and Vignette onboarding for systems and loyalty participation should be confirmed during pre-opening planning; training scope is generally lighter than Hotel Indigo or Kimpton's full-brand programs.",
    108
  ),
  row(
    "operations.model.reporting_discipline",
    "",
    "IHG reporting and revenue-management cadence typically apply once affiliated. Confirm owner reporting expectations and system participation in diligence.",
    109
  ),
  row(
    "operations.model.qa_rhythm",
    "",
    "Acceptance and periodic review focus on whether the property maintains its independent identity and guest-experience quality—confirm review cadence and remediation expectations before treating affiliation as durable value.",
    110
  ),
  row(
    "operations.model.technology",
    "",
    "IHG technology and One Rewards participation are diligence items beyond the brand flag alone. Confirm systems, digital, and loyalty integration requirements for the asset.",
    111
  ),
  row(
    "operations.standards_philosophy",
    "",
    "Vignette Collection standards protect a property's independent identity while requiring a credible, established guest experience—lighter-touch than Hotel Indigo's neighborhood prototype or Kimpton's restaurant-forward mandate. Owners should underwrite to the property's existing operating quality, not a new brand-driven redesign.\nAcceptance detail: Vignette generally suits already-strong independents, not repositioning projects.\nF&B: No restaurant-forward mandate; maintain the property's existing concept.\nDifferentiation: Do not assume Indigo or Kimpton-level standardization or capital applies.",
    112
  ),
  row(
    "operations.operator_compat.summary",
    "",
    "Operators need to sustain an already-credible independent hotel identity inside IHG's Vignette Collection—minimal standardization required versus Hotel Indigo or Kimpton, but genuine independent operating quality still expected.",
    113
  ),
  row(
    "operations.operator_compat.fit",
    "",
    "Best fit: operators already delivering strong independent luxury-leaning or upper-upscale operations. Weaker fit: operators expecting a standardized IHG lifestyle prototype or restaurant-forward program to elevate a weaker asset.",
    114
  ),
  row(
    "operations.operator_compat.tags",
    "",
    "IHG soft collection\nIndependence-first\nLuxury-leaning\nMinimal standardization",
    115
  ),

  // --- Operations & Standards: flexibility indicators ---
  row(
    "operations.flexibility.design",
    "",
    "Very high",
    200
  ),
  row(
    "operations.flexibility.conversion",
    "",
    "Moderate",
    201
  ),
  row(
    "operations.flexibility.localization",
    "",
    "Very high",
    202
  ),
  row(
    "operations.flexibility.operational_rigidity",
    "",
    "Low",
    203
  ),
  row(
    "operations.flexibility.pip",
    "",
    "Low",
    204
  ),
  row(
    "operations.flexibility.prototype",
    "",
    "Minimal",
    205
  ),

  // --- Operations & Standards: compliance ---
  row(
    "operations.compliance.qa_cadence",
    "",
    "Acceptance and periodic review focus on whether the property sustains its independent identity and guest-experience quality—lighter cadence than Hotel Indigo or Kimpton. Owners should confirm current review scope and who owns corrective action.",
    210
  ),
  row(
    "operations.compliance.training_rigor",
    "",
    "IHG and Vignette onboarding centers on systems and loyalty participation rather than a full-brand service program. Confirm property-specific training scope and timing during pre-opening planning.",
    211
  ),
  row(
    "operations.compliance.reporting",
    "",
    "IHG reporting, revenue-management, and loyalty participation expectations typically apply once affiliated. Owners should confirm ownership reporting cadence, operator versus owner data responsibilities, and system participation for the specific deal.",
    212
  ),
  row(
    "operations.compliance.brand_interaction",
    "",
    "Development touchpoints usually cover acceptance review and systems integration, generally lighter cadence than Hotel Indigo or Kimpton. Confirm how often brand and owner teams meet during affiliation and stabilized operations.",
    213
  ),

  // --- Economics & Obligations: opening path ---
  row(
    "economics.opening.step.1",
    "Application & Feasibility",
    "Submit the asset for IHG development review with market context, ownership structure, and a candid read on the property's independent identity and operating track record for Vignette Collection. Confirm feasibility of affiliation and whether Vignette, Hotel Indigo, or Kimpton best fits the asset.",
    400
  ),
  row(
    "economics.opening.step.2",
    "Design & Standards",
    "Complete Vignette acceptance review with IHG—focused on confirming the property's independent identity and guest-experience quality rather than a design prototype rebuild. Treat this as an affiliation-fit review, lighter than Hotel Indigo or Kimpton design standards.",
    401
  ),
  row(
    "economics.opening.step.3",
    "Pre-Opening Planning",
    "Build pre-opening budgets for IHG systems, One Rewards integration, and any residual PIP identified during acceptance review. Confirm operator responsibilities, timeline, and milestone approvals with brand development and your advisors.",
    402
  ),
  row(
    "economics.opening.step.4",
    "Opening Support",
    "Coordinate systems go-live and One Rewards commercial activation with the operator while preserving the property's existing independent guest experience. Confirm support scope, staffing coverage, and launch-week responsibilities directly with IHG development before affiliation launch to avoid last-minute systems surprises.",
    403
  ),
  row(
    "economics.opening.step.5",
    "Stabilization",
    "Stabilize commercial participation and IHG revenue-management rhythm while maintaining the property's independent identity. Use early performance to validate distribution value from affiliation—not as a substitute for agreement-level economics review or confirmed One Rewards contribution data.",
    404
  ),

  // --- Footprint & Growth ---
  row(
    "footprint.momentum",
    "Independent Luxury-Leaning Affiliation Signals",
    "IHG owner and development materials continue to position Vignette Collection as a growth path for established independent luxury-leaning and upper-upscale hotels seeking One Rewards distribution. Treat this as directional collection momentum rather than a property-level pipeline disclosure—confirm current activity directly with IHG development.",
    450
  ),
  row(
    "footprint.momentum",
    "Complement To Hotel Indigo And Kimpton Within IHG's Lifestyle Portfolio",
    "IHG development materials describe Vignette as the lightest-touch complement to Hotel Indigo and Kimpton within the broader lifestyle and independent-character portfolio. Confirm which of the three collections fits your asset's operating model directly with IHG development.",
    451
  ),
  row(
    "footprint.momentum",
    "Heritage And Character-Asset Interest",
    "IHG owner-facing materials describe continued interest in heritage and character-driven independent hotels for Vignette given its minimal standardization requirements. Owners with distinctive existing operations should read this as directional interest—not confirmation of specific incentives for any given market.",
    452
  ),
  row(
    "footprint.portfolio_mix",
    "Portfolio mix",
    "Independent luxury-leaning hotels\nUpper-upscale independents\nMinimal standardization\nAffiliation-led (not conversion-heavy)",
    460
  ),
  row(
    "footprint.geo_intro",
    "Geographic footprint",
    "Vignette Collection has presence across U.S. and international gateway and resort markets where established independent luxury-leaning hotels seek IHG One Rewards distribution, with growing CALA representation. Owners should underwrite mainstream One Rewards commercial participation—not assume uniform density across every region.",
    470
  ),
  row(
    "footprint.region.am",
    "Americas",
    "The Americas provide the clearest comp set for Vignette's independence-first positioning under IHG—established independent luxury-leaning hotels anchor acceptance-review expectations. Confirm local comps and development interest for the specific market.",
    471
  ),
  row(
    "footprint.region.cala",
    "CALA",
    "CALA representation continues to grow as owners of established independent luxury-leaning properties seek IHG One Rewards distribution without a standardized prototype. Owners should confirm authorized geography and acceptance criteria locally.",
    472
  ),
  row(
    "footprint.region.eu",
    "Europe",
    "Europe contributes independent luxury-leaning reference points for IHG's broader lifestyle portfolio, more concentrated in Hotel Indigo and Kimpton than Vignette specifically at this stage. Americas or CALA owners can use these as directional context only.",
    473
  ),
  row(
    "footprint.region.mea",
    "MEA",
    "MEA exposure is market-specific and not a current focus area for Vignette relative to IHG's broader brand portfolio. Confirm authorization and development interest directly rather than assuming interchangeable footprint with other regions.",
    474
  ),
  row(
    "footprint.region.apac",
    "APAC",
    "APAC contributes to IHG's broader independent-character brand recognition more through other collections than Vignette at this stage. For Americas or CALA deals, treat APAC as brand-recognition context only.",
    475
  ),
  row(
    "footprint.growth_themes",
    "",
    "Independent luxury-leaning affiliation\nMinimal-standardization collection growth\nComplement to Hotel Indigo and Kimpton\nHeritage and character-asset interest",
    480
  ),
  row(
    "footprint.growth_editorial",
    "",
    "Vignette Collection compounds when owners bring an already-credible independent luxury-leaning or upper-upscale hotel and IHG One Rewards distribution amplifies it without imposing a standardized prototype. Named collection growth themes are directional context—still underwrite local comps and agreement terms independently.",
    481
  ),
  row(
    "footprint.growth_fit",
    "",
    "Best growth fit: owners of established independent luxury-leaning or upper-upscale hotels who want IHG distribution without a standardized prototype. Weaker fit: assets needing significant repositioning, which may fit Hotel Indigo or Kimpton better, or generic properties with no independent operating track record.",
    482
  ),

  // --- Owner Considerations ---
  row(
    "standards.intro",
    "",
    "Vignette Collection standards center on preserving a property's independent identity and confirming an already-credible guest-experience quality—lighter-touch than Hotel Indigo or Kimpton. Confirm current standard detail and acceptance criteria directly with IHG development for the specific asset.",
    600
  ),
  row(
    "standards.requirement",
    "Independent identity & acceptance review",
    "IHG acceptance review evaluates whether the property's independent identity and operating track record already meet a credible upper-upscale-to-luxury-leaning bar—confirm scope and timeline for your asset.",
    601
  ),
  row(
    "standards.requirement",
    "IHG One Rewards systems participation",
    "PMS/CRS cutover, One Rewards loyalty integration, and commercial systems participation are typically required. Confirm technical scope and timeline with IHG development and your systems integrator.",
    602
  ),
  row(
    "standards.requirement",
    "F&B and public-space continuity",
    "F&B and public spaces should reflect the property's existing concept—Vignette does not require a Kimpton-style restaurant-forward redesign. Confirm any acceptance-driven adjustments directly.",
    603
  ),
  row(
    "standards.requirement",
    "Guest-room standards",
    "Guest rooms should reflect the property's existing independent identity within IHG's baseline guest-experience expectations. Confirm any required baseline amenities for the asset.",
    604
  ),
  row(
    "standards.requirement",
    "Training and systems onboarding",
    "Onboarding should reinforce IHG systems and loyalty participation without imposing a new service program on an already-credible independent operation. Confirm training scope, timing, and cost during pre-opening planning.",
    605
  ),
  row(
    "standards.requirement",
    "Ongoing identity and QA review",
    "Independent identity and guest-experience quality are reviewed periodically after affiliation, generally at a lighter cadence than Hotel Indigo or Kimpton. Confirm current review cadence and remediation expectations before underwriting affiliation value as permanent.",
    606
  ),
  row(
    "standards.conversion",
    "",
    "Affiliation suitability depends on whether the property is already a credible independent luxury-leaning or upper-upscale hotel—not whether it needs significant repositioning, which may fit Hotel Indigo or Kimpton better. Confirm acceptance-review scope before committing capital.",
    607
  ),
  row(
    "standards.questions",
    "Questions owners should ask",
    [
      "What specific independence and identity criteria does IHG expect our property to already meet for Vignette acceptance?",
      "What is the current acceptance-review timeline and remediation process if elements fall short?",
      "What IHG One Rewards systems and commercial participation requirements apply to this specific asset?",
      "How does Vignette's standardization and capital expectation compare to Hotel Indigo and Kimpton for a property like ours?",
      "What ongoing review cadence should we expect after affiliation, and who owns corrective action?",
    ].join("\n"),
    608
  ),

  // --- Dealality Insight: similar brands ---
  row(
    "insight.similar.1",
    "Hotel Indigo",
    "IHG lifestyle full brand with a more standardized neighborhood-storytelling prototype than Vignette's independence-first collection—compare design-review intensity and operating autonomy.",
    700
  ),
  row(
    "insight.similar.2",
    "Kimpton Hotels & Restaurants",
    "IHG boutique luxury lifestyle full brand with a restaurant-forward, more standardized guest promise than Vignette—compare F&B mandates and operating structure.",
    701
  ),
  row(
    "insight.similar.3",
    "Autograph Collection",
    "Marriott soft-collection peer for independently distinctive upper-upscale-to-luxury-leaning hotels—compare design-review rigor and loyalty participation outside IHG.",
    702
  ),
];

export const BRAND_FULL_BUILD_CONTENT = Object.freeze({
  brandSlug: BRAND_SLUG,
  sourcePack: Object.freeze({
    canonicalSite: "ihg.com/vignettecollection",
    developmentPage: "development.ihg.com/brand/vignette-collection",
    propertyPages:
      "Individual property pages live under ihg.com/vignettecollection/hotels/ — use for identity/story context per asset; do not embed raw property URLs in owner-facing Body copy.",
    parentContextPages: [
      "ihg.com — IHG brand family context",
      "development.ihg.com — IHG development brand materials (collection growth themes only, no invented counts)",
    ],
    imageSources:
      "ihg.com/vignettecollection brand and property galleries; use for identity/character reference only — confirm licensing before any external use.",
    domains: ["ihg.com", "development.ihg.com"],
  }),
  brandLens: Object.freeze({
    brandModel:
      "IHG independence-first soft/lifestyle collection — established independent luxury-leaning and upper-upscale hotels keep their own identity and operating autonomy while joining IHG One Rewards, with less standardization than Hotel Indigo or Kimpton.",
    ownerFit:
      "Owners of already-credible independent luxury-leaning or upper-upscale hotels who want IHG distribution without adopting a standardized prototype or restaurant-forward full-brand mandate.",
    propertyFit:
      "Established independent hotels and resorts with a credible operating track record; affiliation-led rather than heavy conversion or repositioning.",
    conversionLogic:
      "Acceptance review confirms the property already meets a credible independent standard—capital needs are generally lighter than Hotel Indigo or Kimpton; confirm any residual PIP directly rather than assuming none is required.",
    operatingImplications:
      "Staffing and F&B intensity follow the property's existing independent operating model; IHG systems, One Rewards loyalty, and reporting participation apply regardless of design flexibility.",
    standardsRequirements:
      "Independent identity and guest-experience quality reviewed at acceptance and periodically thereafter, generally lighter cadence and scope than Hotel Indigo or Kimpton; confirm current acceptance criteria directly with IHG development.",
    sourceLimitations:
      "Public brand materials describe collection positioning and growth themes only—no property-level counts, fees, or performance data. Confirm agreement-specific terms directly with IHG development.",
    distinguishFrom:
      "Hotel Indigo (IHG lifestyle full brand with a more standardized neighborhood-storytelling prototype) and Kimpton Hotels & Restaurants (IHG boutique luxury lifestyle full brand with a restaurant-forward, more standardized guest promise than Vignette's independence-first collection).",
  }),
  presentation: PRESENTATION,
});

export default BRAND_FULL_BUILD_CONTENT;
