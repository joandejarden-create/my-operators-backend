/**
 * Brand Explorer Tab Factory — full build content pack: Autograph Collection.
 *
 * True-incomplete brand (see brand-explorer-built-blocked-content.js →
 * BUILT_BLOCKED_TRUE_INCOMPLETE). Marriott soft-brand collection of independently
 * distinctive upper-upscale / luxury-leaning hotels — every property keeps its own
 * identity while participating in Marriott Bonvoy and Marriott commercial systems.
 *
 * Copy rules:
 * - Directional, owner-facing. No invented fees, ADR, FDD, Item 19, pipeline counts,
 *   or performance guarantees.
 * - Brand-specific — avoids Marriott-umbrella boilerplate as the brand story.
 * - No Company Validated claims.
 * - No raw https:// URLs in any Body field (PVQL fails on raw URLs in owner-facing copy).
 * - Distinguishes Autograph from Tribute Portfolio (more lifestyle/leisure-conversion
 *   leaning) and Design Hotels (curation/affiliation platform, lighter-touch standards).
 */

const BRAND_SLUG = "autograph-collection";
const BRAND_NAME = "Autograph Collection";
const PARENT_COMPANY = "Marriott International, Inc.";
const RECORD_ID = "recEJCTDj1zrsjPM6";

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
  // --- Positioning (Basics-backed slots, matches built-blocked convention) ---
  row(
    "Brand Positioning",
    "",
    "Independently distinctive upper-upscale and luxury-leaning hotels united only by individual character—Autograph Collection gives each property its own design story, F&B identity, and guest narrative while adding Marriott Bonvoy distribution and commercial systems.",
    10
  ),
  row(
    "Guest Psychographics Description",
    "",
    "Design-curious upper-upscale and luxury travelers who want a distinctive, story-led stay with dependable Marriott systems underneath—guests choosing character over standardization, but not willing to trade away loyalty earn/redeem or consistent service infrastructure.",
    11
  ),

  // --- Overview ---
  row(
    "overview.typical_use_case",
    "",
    "Upper-upscale or luxury-leaning hotels with a genuine design or heritage story—independent boutiques, historic conversions, or architecturally distinctive new-build—where owners want Marriott Bonvoy distribution and commercial infrastructure without erasing the property's individual identity into a standardized prototype.",
    20
  ),
  row(
    "overview.development_model",
    "",
    "Conversion, adaptive reuse, or purpose-built development anchored on a credible design or narrative concept rather than a fixed room-and-corridor prototype. Sponsors should model design review timelines, public-space and F&B capital intensity, and Marriott systems integration before assuming a light reflag will satisfy collection standards.",
    21
  ),
  row(
    "overview.relative_positioning",
    "Relative Positioning",
    "Autograph Collection sits at the upper-upscale-to-luxury-leaning tier of Marriott's independent-character family—broader in segment range than Tribute Portfolio's more lifestyle/leisure-conversion focus, and structurally different from Design Hotels, which is a lighter-touch curation and affiliation platform rather than a standards-driven soft-brand collection.",
    22
  ),
  row(
    "overview.scenario.1",
    "Design-Led Independent Conversion",
    "A historic building, adaptive-reuse asset, or architecturally distinctive new-build where the story is strong enough to anchor an upper-upscale independent stay. Autograph fits when owners want to keep that story intact while gaining Marriott Bonvoy reach—confirm design review scope and public-space capital before assuming a cosmetic reflag will pass collection standards.",
    30
  ),
  row(
    "overview.scenario.2",
    "Luxury-Leaning Independent Repositioning",
    "An independent hotel or resort with strong service culture and physical product edging toward luxury, where ownership wants global distribution and Marriott commercial systems without a full luxury-flag conversion or standardized prototype rebuild. Owners should diligence collection acceptance criteria, F&B and public-space capital intensity, and design-review timelines against the asset's current guest experience before assuming a light affiliation will suffice.",
    31
  ),
  row(
    "overview.scenario.3",
    "Urban Or Resort Gateway Repositioning",
    "Gateway-city or resort-destination hotels where an independent narrative differentiates the asset from big-box competitors nearby. Autograph fits when the story and physical product can sustain upper-upscale-to-luxury-leaning expectations under Marriott systems—confirm PIP scope, design sign-off, F&B intensity, and operator capability before committing capital or assuming affiliation alone will lift the asset's positioning.",
    32
  ),
  row(
    "overview.why_value",
    "Why Value Is Strongest",
    "Value concentrates where the property already has a genuine story worth protecting, the physical product can sustain upper-upscale-to-luxury-leaning expectations, and ownership wants Marriott Bonvoy distribution without a standardized prototype. Weakest fit is a generic asset seeking a flag with no design narrative to curate.",
    33
  ),
  row(
    "overview.proof.1",
    "Individual Character, Not A Prototype",
    "Autograph Collection markets itself as a portfolio of independently distinctive hotels—no two properties share a room prototype or public-space template. Owners should treat this as a design and narrative bar to clear, not a standardized construction spec, and budget design review accordingly rather than assuming a light PIP will suffice.",
    40
  ),
  row(
    "overview.proof.2",
    "Marriott Bonvoy Distribution",
    "Brand materials position Marriott Bonvoy participation and Marriott's commercial systems as the core affiliation value—loyalty earn/redeem, global sales, and distribution reach layered onto an independently branded stay. Confirm systems integration scope and commercial participation requirements directly with Marriott development for the specific asset.",
    41
  ),
  row(
    "overview.proof.3",
    "Upper-Upscale To Luxury-Leaning Range",
    "Autograph spans a wider segment range than most single-tier soft brands—upper-upscale urban hotels through luxury-leaning resorts share the collection. Owners should benchmark comp sets and F&B intensity against properties at their own segment level rather than the collection's luxury-leaning outliers.",
    42
  ),
  row(
    "overview.proof.4",
    "Design Review As A Gate, Not A Checklist",
    "Collection acceptance and ongoing standards center on design coherence, guest-experience quality, and story authenticity rather than a fixed prototype checklist. Owners should expect qualitative design review at conversion and periodically thereafter—confirm current review cadence and remediation expectations before underwriting affiliation value as permanent.",
    43
  ),
  row(
    "overview.featured_application",
    "Design-led independent conversion or new-build",
    "An upper-upscale or luxury-leaning independent hotel with a genuine design or heritage story can use Autograph Collection to gain Marriott Bonvoy distribution and commercial systems while keeping its own identity intact. Owners should underwrite design review scope, public-space and F&B capital, and Marriott systems integration—confirming acceptance criteria directly rather than assuming a light reflag covers collection standards.",
    44,
    {
      caseSummaryOverview:
        "Featured path for independent-character upper-upscale or luxury-leaning assets seeking Marriott distribution under Autograph Collection.",
      caseSummaryBrandRelevance:
        "Matches Autograph's design-led independent lane—broader segment range than Tribute, more standards-driven than Design Hotels' curation model.",
      caseSummaryOwnerObjective:
        "Fund design review, public-space/F&B capital, and Marriott systems integration without erasing the property's own story.",
      caseSummaryInterpretation:
        "Use as a conversion-fit lens—confirm acceptance criteria and agreement terms directly with Marriott development; not a performance forecast.",
      caseSummaryTags: "soft-brand, independent-character, upper-upscale, Marriott, conversion",
    }
  ),
  row(
    "overview.differentiators.identity",
    "Experience & Identity",
    [
      "Independently distinctive hotels—no shared room or public-space prototype",
      "Upper-upscale to luxury-leaning segment range within one collection",
      "Design and narrative authenticity as the acceptance bar, not a construction spec",
      "Story-led guest experience preserved after Marriott affiliation",
    ].join("\n"),
    45
  ),
  row(
    "overview.differentiators.commercial",
    "Commercial & Distribution",
    [
      "Marriott Bonvoy loyalty earn/redeem participation",
      "Marriott global sales and commercial systems access",
      "Distribution reach without a standardized franchise prototype",
      "Confirm specific commercial participation terms directly with Marriott development",
    ].join("\n"),
    46
  ),
  row(
    "overview.bestAt.1",
    "Design-Led Independent Character",
    "Protecting a genuine design or heritage story at upper-upscale-to-luxury-leaning intensity while adding Marriott Bonvoy reach—Autograph's core value versus a standardized hard-brand conversion.",
    47
  ),
  row(
    "overview.bestAt.2",
    "Broad Segment Range Under One Collection",
    "Spanning upper-upscale urban hotels through luxury-leaning resorts under one soft-brand umbrella—owners should benchmark to their own segment rather than the collection's outliers.",
    48
  ),
  row(
    "overview.bestAt.3",
    "Marriott Systems Without A Fixed Prototype",
    "Delivering Marriott commercial infrastructure and loyalty participation while preserving property-specific design and F&B identity—distinct from Tribute's lifestyle-conversion lean and Design Hotels' lighter-touch curation.",
    49
  ),
  row(
    "overview.portfolio_context",
    "Portfolio Context",
    "Within Marriott's independent-character family, Autograph Collection is the broadest upper-upscale-to-luxury-leaning soft brand—more standards-driven than Design Hotels' curation platform and broader in segment range than Tribute Portfolio's lifestyle/leisure-conversion focus. Owners should compare design review intensity, F&B expectations, and Bonvoy participation across those siblings before selecting a collection.",
    50
  ),
  row(
    "footprint.portfolio_context",
    "Portfolio Context",
    "Autograph Collection anchors the design-led, standards-reviewed end of Marriott's independent-character portfolio—positioned above Tribute Portfolio's more lifestyle/leisure-conversion focus and structurally distinct from Design Hotels' lighter-touch affiliation model. Owners should weigh design review rigor against desired operating flexibility before choosing a Marriott soft-brand path.",
    51
  ),
  row(
    "valueOwners.watchouts",
    "",
    [
      "Design review is qualitative, not a fixed checklist—budget time and capital for genuine narrative and public-space work",
      "Segment range within the collection is wide; benchmark to comparable Autograph properties, not the luxury-leaning outliers",
      "Confirm current acceptance criteria and remediation expectations directly—do not assume affiliation value is permanent",
      "F&B and public-space intensity can be material; do not underwrite as a light limited-service conversion",
    ].join("\n"),
    52
  ),

  // --- Value to Owners: lifecycle ---
  row(
    "valueOwners.lifecycle.1",
    "Evaluation",
    "Confirm the property already has—or can credibly build—a design or narrative story worth Autograph Collection's independent-character positioning, not whether it merely wants a Marriott flag. Assess segment fit (upper-upscale versus luxury-leaning), physical product condition, and Marriott development interest before committing design capital or comparing to Tribute/Design Hotels alternatives.",
    300
  ),
  row(
    "valueOwners.lifecycle.2",
    "Conversion Design",
    "Shape conversion or new-build design around the property's own story—public spaces, guest rooms, and F&B should read as distinctive, not templated. Sequence design review milestones with financing and operator selection; treat acceptance-critical work as priority spend over decorative changes that do not improve guest-facing substance.",
    301
  ),
  row(
    "valueOwners.lifecycle.3",
    "Pre-Opening",
    "Coordinate Marriott systems cutover, Bonvoy integration, staffing, and training with design sign-off and opening readiness. Confirm owner versus operator responsibilities for commercial launch, and budget time for collection-specific design and guest-experience orientation rather than a generic hard-brand opening checklist.",
    302
  ),
  row(
    "valueOwners.lifecycle.4",
    "Opening",
    "Launch with consistent story-led service and design presentation across every guest touchpoint. Opening support typically centers on guest-experience coherence and Marriott systems stabilization rather than a standardized prototype punch list—confirm support scope and staffing coverage directly with Marriott before launch week.",
    303
  ),
  row(
    "valueOwners.lifecycle.5",
    "Ramp-Up",
    "During ramp-up, calibrate rate positioning and F&B/public-space programming against guest-review themes tied to the property's narrative, not only occupancy headlines. Watch labor intensity and F&B complexity—story-led hotels can carry more operating complexity than a comparable prototype-driven flag.",
    304
  ),
  row(
    "valueOwners.lifecycle.6",
    "Ongoing",
    "On an ongoing basis, refresh design and programming within collection guardrails and reassess affiliation value as the property, operator, and market evolve. Confirm renewal, review, and remediation expectations with Marriott before major repositioning or operator transitions so collection participation remains intentional.",
    305
  ),

  // --- Operations & Standards: model ---
  row(
    "operations.model.primary_model",
    "",
    "Soft-brand affiliation within Marriott's independent-character collection, delivered through franchise or management arrangements that owners must confirm for the specific market and asset.",
    100
  ),
  row(
    "operations.model.management_option",
    "",
    "Third-party management is common for full-service upper-upscale-to-luxury-leaning assets; owner-operated paths require credible design storytelling and service capability that meets collection review.",
    101
  ),
  row(
    "operations.model.typical_ownership",
    "",
    "Owners of design-forward, heritage, or architecturally distinctive hotels who want Marriott Bonvoy reach without converting into a standardized hard-brand prototype.",
    102
  ),
  row(
    "operations.model.brand_involvement",
    "",
    "Marriott development and design review typically touch narrative, public-space design, guest-experience standards, and opening readiness. Confirm the current review process and touchpoint frequency directly.",
    103
  ),
  row(
    "operations.model.systems_integration",
    "",
    "Autograph participates in Marriott's Bonvoy loyalty and commercial systems ecosystem. Owners should validate PMS/CRS cutover, training, and commercial systems requirements for the specific deal.",
    104
  ),
  row(
    "operations.model.pre_opening",
    "",
    "Expect design and brand sign-off, training, and opening readiness work before soft opening. Sequence PIP and operating setup with financing and operator capacity.",
    105
  ),
  row(
    "operations.model.staffing_intensity",
    "",
    "Full-service upper-upscale-to-luxury-leaning staffing across front office, housekeeping, and F&B. Underwrite labor to the intended design narrative, not a select-service skeleton.",
    106
  ),
  row(
    "operations.model.fb_complexity",
    "",
    "Public spaces and F&B often carry part of the property's story. Outlet mix, kitchen scope, and service rhythm are material diligence items, particularly at the luxury-leaning end of the collection.",
    107
  ),
  row(
    "operations.model.training",
    "",
    "Marriott and Autograph opening/service training should be confirmed as part of pre-opening planning. Budget time and cost against the agreement path and the property's specific guest-experience narrative.",
    108
  ),
  row(
    "operations.model.reporting_discipline",
    "",
    "Marriott reporting and revenue-management cadence typically apply. Confirm owner reporting expectations and system participation in diligence.",
    109
  ),
  row(
    "operations.model.qa_rhythm",
    "",
    "Design and guest-experience QA apply at opening and periodically thereafter. Confirm review cadence, scoring focus, and remediation expectations before treating affiliation as durable value.",
    110
  ),
  row(
    "operations.model.technology",
    "",
    "Marriott technology participation is a diligence item beyond the brand flag alone. Confirm systems, digital, and loyalty integration requirements for the asset.",
    111
  ),
  row(
    "operations.standards_philosophy",
    "",
    "Autograph Collection standards protect individual property character while requiring a coherent, design-forward guest experience at upper-upscale-to-luxury-leaning intensity. Owners should underwrite to design narrative, public-space activation, and service delivery—not marketing language alone.\nDesign and conversion detail: Adaptive reuse and distinctive new-build can fit when the building and story support a coherent guest journey.\nPIP / lifecycle capital: Confirm opening and conversion scope directly; do not assume a light refresh is enough.\nSegment range: Benchmark to comparable Autograph properties at your own segment level, not the collection's luxury-leaning outliers.",
    112
  ),
  row(
    "operations.operator_compat.summary",
    "",
    "Operators need to deliver a design-led, story-consistent upper-upscale-to-luxury-leaning stay with credible public spaces and F&B while operating inside Marriott systems and Bonvoy participation requirements.",
    113
  ),
  row(
    "operations.operator_compat.fit",
    "",
    "Best fit: operators experienced with independent, boutique, or luxury-leaning full-service hotels who can execute a property-specific narrative. Weaker fit: prototype-driven operators who default to standardized chain expression without design sensitivity.",
    114
  ),
  row(
    "operations.operator_compat.tags",
    "",
    "Marriott soft-brand\nDesign-led\nUpper-upscale to luxury-leaning\nFull-service",
    115
  ),

  // --- Operations & Standards: flexibility indicators ---
  row(
    "operations.flexibility.design",
    "",
    "High",
    200
  ),
  row(
    "operations.flexibility.conversion",
    "",
    "High",
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
    "Medium",
    203
  ),
  row(
    "operations.flexibility.pip",
    "",
    "Medium",
    204
  ),
  row(
    "operations.flexibility.prototype",
    "",
    "Low",
    205
  ),

  // --- Operations & Standards: compliance ---
  row(
    "operations.compliance.qa_cadence",
    "",
    "Design and guest-experience quality reviews typically intensify around conversion, repositioning, and remediation. Owners should confirm cadence, scoring focus, and who owns corrective action plans before treating affiliation as durable value.",
    210
  ),
  row(
    "operations.compliance.training_rigor",
    "",
    "Marriott and Autograph onboarding for opening teams should include design storytelling and Bonvoy service expectations. Confirm property-specific training scope and timing during pre-opening planning rather than treating training as optional after soft opening.",
    211
  ),
  row(
    "operations.compliance.reporting",
    "",
    "Marriott reporting, revenue-management, and loyalty participation expectations typically apply. Owners should confirm ownership reporting cadence, operator versus owner data responsibilities, and system participation for the specific deal rather than assuming independent reporting remains unchanged after affiliation.",
    212
  ),
  row(
    "operations.compliance.brand_interaction",
    "",
    "Development and design-review touchpoints usually cover narrative, public-space design, and opening milestones. Interaction frequency varies by project stage—confirm how often brand and owner teams meet during conversion, opening, and stabilized operations for this opportunity.",
    213
  ),

  // --- Economics & Obligations: opening path ---
  row(
    "economics.opening.step.1",
    "Application & Feasibility",
    "Submit the asset for Marriott development review with market context, ownership structure, and a candid read on the property's design or narrative story for Autograph Collection. Confirm feasibility of franchise or management participation and whether the building supports upper-upscale-to-luxury-leaning operations before detailed design spend.",
    400
  ),
  row(
    "economics.opening.step.2",
    "Design & Standards",
    "Complete Autograph design and brand standards review with Marriott—narrative, public spaces, guest rooms, and F&B concept should cohere into a distinctive story. Treat this as a design-led conversion phase, not a light cosmetic reflag.",
    401
  ),
  row(
    "economics.opening.step.3",
    "Pre-Opening Planning",
    "Build pre-opening budgets for hiring, training, Marriott systems, FF&E, and opening marketing aligned with approved design. Confirm operator responsibilities, opening timeline, and milestone approvals with brand development and your advisors.",
    402
  ),
  row(
    "economics.opening.step.4",
    "Opening Support",
    "Coordinate soft opening, design QA, and Marriott commercial launch support with the operator. Ensure guest-facing teams can deliver the intended narrative while systems and Bonvoy participation go live on schedule.",
    403
  ),
  row(
    "economics.opening.step.5",
    "Stabilization",
    "Stabilize operations with Marriott revenue-management rhythm, guest-feedback loops, and refinement of design programming. Use early performance to validate underwriting on labor, F&B, and capital—not as a substitute for agreement-level economics review.",
    404
  ),

  // --- Footprint & Growth ---
  row(
    "footprint.momentum",
    "Distinctive Independent Conversion Signals",
    "Marriott owner and development materials continue to position Autograph Collection as a growth path for independent-character upper-upscale and luxury-leaning conversions across urban gateway and resort markets. Treat this as directional collection momentum rather than a property-level pipeline disclosure—confirm current activity directly with Marriott development.",
    450
  ),
  row(
    "footprint.momentum",
    "Adaptive Reuse And Heritage Conversion Emphasis",
    "Marriott development materials repeatedly cite adaptive reuse and heritage-building conversions as a natural fit for Autograph Collection's design-led positioning. Owners with distinctive existing buildings should read this as directional interest in conversion pathways—not confirmation of specific incentives or timelines for any given market.",
    451
  ),
  row(
    "footprint.momentum",
    "Resort And Gateway-City Expansion Interest",
    "Marriott owner-facing materials describe continued interest in both gateway-city and resort-destination independent hotels for the collection. Owners evaluating either format should confirm current authorized geography and development priorities directly rather than assuming uniform interest across every market.",
    452
  ),
  row(
    "footprint.portfolio_mix",
    "Portfolio mix",
    "Upper-upscale independents\nLuxury-leaning resorts\nAdaptive reuse / heritage conversions\nDesign-forward new-build",
    460
  ),
  row(
    "footprint.geo_intro",
    "Geographic footprint",
    "Autograph Collection has meaningful presence across U.S. gateway and resort markets, with growing CALA and international representation as owners bring distinctive independent-character properties into Marriott's system. Owners should underwrite mainstream Bonvoy commercial participation and design-review expectations—not assume uniform density across every region.",
    470
  ),
  row(
    "footprint.region.am",
    "Americas",
    "The Americas remain the collection's deepest base—gateway-city and resort independents across the U.S. and Canada provide the clearest comp set for design-review expectations and Bonvoy commercial participation. Confirm local comps and development interest for the specific market.",
    471
  ),
  row(
    "footprint.region.cala",
    "CALA",
    "CALA representation continues to grow as owners of distinctive resort and urban independents seek Marriott Bonvoy distribution. Owners should confirm authorized geography and design-review expectations locally rather than assuming U.S. gateway comps translate directly.",
    472
  ),
  row(
    "footprint.region.eu",
    "Europe",
    "Europe contributes design and heritage-conversion reference points for the collection given the density of adaptive-reuse and historic-building opportunities. Americas or CALA owners can use these as design-narrative references without importing European ramp assumptions.",
    473
  ),
  row(
    "footprint.region.mea",
    "MEA",
    "MEA exposure is market-specific and generally smaller-scale for Autograph relative to Marriott's broader luxury and full-service brands. Confirm authorization and development interest directly rather than assuming interchangeable footprint with other regions.",
    474
  ),
  row(
    "footprint.region.apac",
    "APAC",
    "APAC contributes distinctive independent hotels for international travelers who recognize the collection through global Bonvoy participation. For Americas or CALA deals, treat APAC as brand-recognition context only, not a feasibility or ramp-curve reference.",
    475
  ),
  row(
    "footprint.growth_themes",
    "",
    "Adaptive reuse and heritage conversion\nGateway-city independent repositioning\nResort and destination luxury-leaning conversion\nCALA independent-character expansion",
    480
  ),
  row(
    "footprint.growth_editorial",
    "",
    "Autograph Collection compounds when owners bring a genuine design or heritage story and fund the public-space and F&B capital that story requires, while operators execute a property-specific guest experience inside Marriott's commercial systems. Named collection growth themes are directional context—still underwrite local comps, PIP, and agreement terms independently.",
    481
  ),
  row(
    "footprint.growth_fit",
    "",
    "Best growth fit: owners of architecturally or historically distinctive hotels who want Bonvoy distribution without erasing property identity. Weaker fit: generic assets with no design narrative, or owners expecting a standardized hard-brand prototype experience.",
    482
  ),

  // --- Owner Considerations ---
  row(
    "standards.intro",
    "",
    "Autograph Collection standards center on design coherence, guest-experience quality, and story authenticity rather than a fixed room-and-corridor prototype. Confirm current standard detail and acceptance criteria directly with Marriott development for the specific asset.",
    600
  ),
  row(
    "standards.requirement",
    "Design & narrative review",
    "Marriott design review evaluates whether the property's story, architecture, and public spaces cohere into a distinctive guest experience worthy of the collection—confirm scope and timeline for your asset.",
    601
  ),
  row(
    "standards.requirement",
    "Bonvoy systems participation",
    "PMS/CRS cutover, loyalty integration, and commercial systems participation are typically required. Confirm technical scope and timeline with Marriott development and your systems integrator.",
    602
  ),
  row(
    "standards.requirement",
    "F&B and public-space capital",
    "Outlet mix, kitchen scope, and public-space activation should match the property's narrative and segment level. Confirm expected capital intensity before assuming a light refresh will satisfy collection review.",
    603
  ),
  row(
    "standards.requirement",
    "Guest-room and suite standards",
    "Guest rooms and suites should reflect the property's specific design story within Marriott's baseline guest-experience expectations. Confirm design-review flexibility and any required baseline amenities for the asset.",
    604
  ),
  row(
    "standards.requirement",
    "Training and service culture",
    "Opening and ongoing training should reinforce both Marriott service baselines and the property's own guest-experience narrative. Confirm training scope, timing, and cost during pre-opening planning.",
    605
  ),
  row(
    "standards.requirement",
    "Ongoing design and QA review",
    "Design coherence and guest-experience quality are reviewed periodically after opening, not only at conversion. Confirm current review cadence and remediation expectations before underwriting affiliation value as permanent.",
    606
  ),
  row(
    "standards.conversion",
    "",
    "Conversion suitability depends on whether the building and story can sustain upper-upscale-to-luxury-leaning guest expectations under Marriott systems—not whether the asset merely wants a recognizable flag. Confirm design-review scope and PIP intensity before committing capital.",
    607
  ),
  row(
    "standards.questions",
    "Questions owners should ask",
    [
      "What specific design and narrative elements does Marriott expect us to preserve or develop for collection acceptance?",
      "What is the current design-review timeline and remediation process if elements fall short?",
      "What Marriott systems, loyalty, and commercial participation requirements apply to this specific asset?",
      "How does F&B and public-space capital intensity compare between Autograph, Tribute, and Design Hotels for a property like ours?",
      "What ongoing QA cadence and standards review should we expect after opening, and who owns corrective action?",
    ].join("\n"),
    608
  ),

  // --- Dealality Insight: similar brands ---
  row(
    "insight.similar.1",
    "Tribute Portfolio",
    "Marriott soft-collection peer with a more lifestyle/leisure-conversion focus—compare segment range, design-review intensity, and F&B expectations for independent-character assets within the same parent.",
    700
  ),
  row(
    "insight.similar.2",
    "Design Hotels",
    "Marriott-affiliated curation platform with lighter-touch standards than Autograph's soft-brand review—compare affiliation structure, owner control, and design-curation model for distinctive independents.",
    701
  ),
  row(
    "insight.similar.3",
    "MGallery Collection",
    "Accor soft-collection peer for story-led hotels—compare affiliation model, design-review intensity, and parent-system rigidity outside Marriott.",
    702
  ),
];

export const BRAND_FULL_BUILD_CONTENT = Object.freeze({
  brandSlug: BRAND_SLUG,
  sourcePack: Object.freeze({
    canonicalSite: "marriott.com/en-us/brands/autograph-collection",
    developmentPage: "development.marriott.com — Our Brands (Autograph Collection)",
    propertyPages:
      "Individual property pages live under marriott.com/en-us/hotels/ — use for design/narrative context per asset; do not embed raw property URLs in owner-facing Body copy.",
    parentContextPages: [
      "marriott.com — Marriott International brand family context",
      "news.marriott.com — Marriott press materials (collection growth themes only, no invented counts)",
    ],
    imageSources:
      "marriott.com brand and property galleries; use for design/character reference only — confirm licensing before any external use.",
    domains: ["marriott.com", "development.marriott.com", "news.marriott.com"],
  }),
  brandLens: Object.freeze({
    brandModel:
      "Marriott soft-brand collection — independently distinctive upper-upscale-to-luxury-leaning hotels, each keeping its own design and narrative identity while participating in Marriott Bonvoy and commercial systems.",
    ownerFit:
      "Owners of architecturally or historically distinctive hotels who want Bonvoy distribution and Marriott commercial infrastructure without converting to a standardized hard-brand prototype.",
    propertyFit:
      "Upper-upscale independents through luxury-leaning resorts with a genuine design, heritage, or narrative story; conversion, adaptive reuse, or distinctive new-build.",
    conversionLogic:
      "Design review gates acceptance, not a fixed room-and-corridor spec—capital should protect and amplify the property's story, with PIP scope confirmed directly rather than assumed minimal.",
    operatingImplications:
      "Full-service staffing and F&B/public-space intensity scaled to the property's segment level and narrative; Marriott systems, loyalty, and reporting participation apply regardless of design flexibility.",
    standardsRequirements:
      "Design coherence, guest-experience quality, and story authenticity reviewed at conversion and periodically thereafter; confirm current acceptance criteria and remediation expectations directly with Marriott development.",
    sourceLimitations:
      "Public brand materials describe collection positioning and growth themes only—no property-level counts, fees, or performance data. Confirm agreement-specific terms directly with Marriott development. Internal note: brand-explorer-lifestyle-affiliation-brand-config.js already carries an accurate Marriott/autograph-collection entry consistent with this pack.",
    distinguishFrom:
      "Tribute Portfolio (Marriott soft brand, more lifestyle/leisure-conversion leaning, comparatively narrower segment range) and Design Hotels (Marriott-affiliated curation/affiliation platform with lighter-touch standards than Autograph's design-review-gated soft brand).",
  }),
  presentation: PRESENTATION,
});

export default BRAND_FULL_BUILD_CONTENT;
