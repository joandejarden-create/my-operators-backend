/**
 * Brand Explorer Tab Factory — full build content pack: Radisson Collection.
 *
 * True-incomplete brand (see brand-explorer-built-blocked-content.js →
 * BUILT_BLOCKED_TRUE_INCOMPLETE). Choice Hotels / Radisson Hotel Group's curated
 * collection of iconic, bespoke-design upper-upscale hotels — parent is Choice Hotels
 * International (Radisson Hotel Group brand family), NOT a standalone Radisson entity.
 *
 * Source grounding: fixtures/choice-dev-site-text/our-brands__upscale__radisson-collection.txt
 * ("Radisson Collection is a unique grouping of iconic properties. Each hotel features
 * authentic local character... bespoke design and an assembly of exceptional
 * experiences across dining, fitness, wellness and sustainability... a great
 * opportunity to expand into an upper upscale market.")
 *
 * Copy rules:
 * - Directional, owner-facing. No invented fees, ADR, FDD, Item 19, pipeline counts,
 *   or performance guarantees.
 * - Brand-specific — avoids Choice/Radisson-umbrella boilerplate as the brand story.
 * - No Company Validated claims.
 * - No raw https:// URLs in any Body field (PVQL fails on raw URLs in owner-facing copy).
 * - Distinguishes Radisson Collection from core Radisson (mainstream upscale
 *   full-service), Radisson Blu (upper-upscale design-forward but standards-driven),
 *   Radisson RED (upscale lifestyle/select-service), and Radisson Individuals
 *   (soft-collection with lighter design-intensity requirements).
 */

const BRAND_SLUG = "radisson-collection";
const BRAND_NAME = "Radisson Collection";
const PARENT_COMPANY = "Choice Hotels International, Inc. (Radisson Hotel Group brand family)";
const RECORD_ID = "rec2DDyPu38C6zDBC";

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
    "A curated grouping of iconic, bespoke-design hotels with authentic local character—Radisson Collection sits above core Radisson and Radisson Blu on design intensity, distinct from Radisson RED's lifestyle select-service path and Radisson Individuals' lighter-touch soft collection.",
    10
  ),
  row(
    "Guest Psychographics Description",
    "",
    "Upper-upscale travelers seeking landmark, design-distinctive properties with strong wellness, dining, and sustainability programming—guests who choose a specific iconic hotel for its own character rather than a predictable Radisson-family stay.",
    11
  ),

  // --- Overview ---
  row(
    "overview.typical_use_case",
    "",
    "Iconic or landmark hotels—historic buildings, architecturally distinctive new-build, or destination properties—with bespoke design potential and strong dining, wellness, and sustainability programming, seeking Radisson Hotel Group distribution without a standardized full-service prototype.",
    20
  ),
  row(
    "overview.development_model",
    "",
    "Conversion or repositioning of a landmark or architecturally distinctive asset anchored on curated design and experiential programming rather than a fixed room-and-corridor template. Sponsors should model design review timelines and F&B/wellness capital intensity before assuming a light PIP will satisfy collection standards.",
    21
  ),
  row(
    "overview.relative_positioning",
    "Relative Positioning",
    "Radisson Collection sits at the curated, upper-upscale-to-luxury-leaning top of the Radisson family—above core Radisson's mainstream full-service and Radisson Blu's design-forward-but-standardized full-service, and distinct from Radisson RED's lifestyle select-service and Radisson Individuals' lighter-touch soft-collection flexibility. Owners should compare design-review intensity and F&B/wellness expectations across siblings before selecting a flag.",
    22
  ),
  row(
    "overview.scenario.1",
    "Iconic Landmark Conversion",
    "A historic or architecturally distinctive building with genuine local character seeking curated upper-upscale positioning. Radisson Collection fits when the asset can sustain bespoke design and full-service dining, wellness, and sustainability programming—confirm design-review scope and capital intensity before treating the project as a standard full-service reflag.",
    30
  ),
  row(
    "overview.scenario.2",
    "Wellness- Or Dining-Led Repositioning",
    "A destination or gateway-city hotel where dining, fitness, wellness, and sustainability programming can anchor a curated guest experience. Owners should diligence F&B and wellness capital, operator capability, and design-review timelines against Radisson Collection's experience-led positioning rather than assuming a generic upscale conversion or core Radisson economics will apply.",
    31
  ),
  row(
    "overview.scenario.3",
    "Curated Upper-Upscale Gateway Or Resort",
    "A gateway-city or resort property with enough architectural or narrative distinction to justify curated collection status rather than core Radisson or Radisson Blu standardization. Confirm PIP scope, design sign-off, dining/wellness programming expectations, and Radisson Hotel Group systems integration before committing capital or assuming affiliation alone will elevate the asset.",
    32
  ),
  row(
    "overview.why_value",
    "Why Value Is Strongest",
    "Value concentrates where the property is genuinely iconic or landmark-caliber, can sustain bespoke design and experience-led F&B/wellness programming, and ownership wants curated Radisson Hotel Group distribution without core Radisson or Blu standardization. Weakest fit is a generic upscale asset with no landmark story.",
    33
  ),
  row(
    "overview.proof.1",
    "Curated, Not Standardized",
    "Radisson Collection is described as a unique grouping of iconic properties united by bespoke design rather than a shared prototype—owners should treat this as a design and character bar to clear, not a construction spec, and budget design review accordingly.",
    40
  ),
  row(
    "overview.proof.2",
    "Dining, Wellness, And Sustainability Emphasis",
    "Brand materials emphasize an assembly of exceptional experiences across dining, fitness, wellness, and sustainability as the collection's guest promise. Owners should align F&B and wellness capital and operator capability with that experience-led positioning rather than assuming light-touch programming will suffice.",
    41
  ),
  row(
    "overview.proof.3",
    "Upper-Upscale Market Opportunity",
    "Choice Hotels development materials describe Radisson Collection as an opportunity to expand into the upper-upscale market with iconic properties. Owners should benchmark comp sets against upper-upscale landmark hotels rather than core Radisson's mainstream full-service comp set.",
    42
  ),
  row(
    "overview.proof.4",
    "Design Review As A Gate, Not A Checklist",
    "Collection acceptance and ongoing standards center on design coherence, local authenticity, and experience quality rather than a fixed prototype checklist. Owners should expect qualitative design review at conversion and periodically thereafter—confirm current review cadence and remediation expectations before underwriting affiliation value as permanent.",
    43
  ),
  row(
    "overview.featured_application",
    "Iconic landmark conversion or repositioning",
    "A landmark or architecturally distinctive hotel with genuine local character can use Radisson Collection to gain Choice Hotels distribution and curated positioning while sustaining bespoke design and dining/wellness/sustainability programming. Owners should underwrite design-review scope and F&B/wellness capital—confirming acceptance criteria directly rather than assuming standard full-service Radisson or Blu economics apply.",
    44,
    {
      caseSummaryOverview:
        "Featured path for iconic or landmark upper-upscale assets seeking curated Choice/Radisson distribution under Radisson Collection.",
      caseSummaryBrandRelevance:
        "Matches Radisson Collection's curated design-and-experience lane—above core Radisson and Blu on design intensity, distinct from RED and Individuals.",
      caseSummaryOwnerObjective:
        "Fund design review and dining/wellness/sustainability capital without assuming core Radisson or Blu prototype economics apply.",
      caseSummaryInterpretation:
        "Use as a conversion-fit lens—confirm acceptance criteria and agreement terms directly with Choice Hotels development; not a performance forecast.",
      caseSummaryTags: "soft-brand, curated-collection, upper-upscale, Choice, Radisson",
    }
  ),
  row(
    "overview.differentiators.identity",
    "Experience & Identity",
    [
      "Curated grouping of iconic properties—no shared design template",
      "Bespoke design united with dining, wellness, and sustainability programming",
      "Authentic local character as the acceptance bar, not a construction spec",
      "Upper-upscale positioning above core Radisson and Radisson Blu",
    ].join("\n"),
    45
  ),
  row(
    "overview.differentiators.commercial",
    "Commercial & Distribution",
    [
      "Choice Privileges loyalty earn/redeem participation",
      "Radisson Hotel Group and Choice Hotels commercial systems access",
      "Distribution reach without a standardized full-service prototype",
      "Confirm specific commercial participation terms directly with Choice Hotels development",
    ].join("\n"),
    46
  ),
  row(
    "overview.bestAt.1",
    "Curated Iconic Design",
    "Protecting a genuine landmark or architecturally distinctive story at upper-upscale intensity while adding Choice/Radisson distribution—Radisson Collection's core value versus core Radisson or Blu standardization.",
    47
  ),
  row(
    "overview.bestAt.2",
    "Dining, Wellness, And Sustainability Programming",
    "Delivering an experience-led guest promise across dining, fitness, wellness, and sustainability—owners should benchmark F&B and wellness capital to this positioning rather than a lighter core Radisson or RED comp set.",
    48
  ),
  row(
    "overview.bestAt.3",
    "Upper-Upscale Market Entry Without A Fixed Prototype",
    "Providing curated upper-upscale positioning for landmark properties without forcing a standardized full-service template—distinct from Blu's design-forward-but-standardized approach and Individuals' lighter-touch soft collection.",
    49
  ),
  row(
    "overview.portfolio_context",
    "Portfolio Context",
    "Within the Radisson family under Choice Hotels, Radisson Collection is the curated, design-and-experience-led upper-upscale tier—above core Radisson's mainstream full-service and Radisson Blu's design-forward-but-standardized approach, and distinct from Radisson RED's lifestyle select-service and Radisson Individuals' lighter-touch soft collection. Owners should compare design-review intensity and capital expectations across those siblings before selecting a flag.",
    50
  ),
  row(
    "footprint.portfolio_context",
    "Portfolio Context",
    "Radisson Collection anchors the curated, iconic-property top of the Radisson Hotel Group family under Choice Hotels—positioned above core Radisson, Radisson Blu, and Radisson RED on design/experience intensity, and more design-review-gated than Radisson Individuals' soft-collection flexibility. Owners should weigh design-review rigor against desired operating flexibility before choosing a Radisson-family path.",
    51
  ),
  row(
    "valueOwners.watchouts",
    "",
    [
      "Design review is qualitative and centers on landmark/iconic character—budget time and capital for genuine bespoke design work",
      "Dining, wellness, and sustainability programming can carry meaningful capital and staffing intensity—do not underwrite as a light core Radisson conversion",
      "Confirm current acceptance criteria and remediation expectations directly—do not assume affiliation value is permanent",
      "Do not confuse Radisson Collection with Radisson Blu, RED, or Individuals when benchmarking comps or capital plans",
    ].join("\n"),
    52
  ),

  // --- Value to Owners: lifecycle ---
  row(
    "valueOwners.lifecycle.1",
    "Evaluation",
    "Confirm the property is genuinely iconic or landmark-caliber with authentic local character worth Radisson Collection's curated positioning, not whether it merely wants a Radisson-family flag. Assess design potential, dining/wellness programming feasibility, and Choice Hotels development interest before committing design capital or comparing to Blu/Individuals alternatives.",
    300
  ),
  row(
    "valueOwners.lifecycle.2",
    "Conversion Design",
    "Shape conversion or repositioning design around the property's own bespoke character—public spaces, dining, wellness, and sustainability programming should read as curated, not templated. Sequence design-review milestones with financing and operator selection; treat acceptance-critical work as priority spend.",
    301
  ),
  row(
    "valueOwners.lifecycle.3",
    "Pre-Opening",
    "Coordinate Choice Hotels systems cutover, Choice Privileges integration, staffing, and training with design sign-off and opening readiness. Confirm owner versus operator responsibilities for commercial launch, and budget time for collection-specific design, dining, and wellness-programming orientation ahead of soft opening.",
    302
  ),
  row(
    "valueOwners.lifecycle.4",
    "Opening",
    "Launch with consistent curated design presentation and dining/wellness programming across every guest touchpoint. Opening support typically centers on guest-experience coherence and systems stabilization rather than a standardized full-service prototype punch list—confirm support scope directly with Choice Hotels development.",
    303
  ),
  row(
    "valueOwners.lifecycle.5",
    "Ramp-Up",
    "During ramp-up, calibrate rate positioning and dining/wellness programming against guest-review themes tied to the property's landmark narrative, not only occupancy headlines. Watch labor intensity and F&B/wellness complexity—curated properties can carry more operating complexity than core Radisson.",
    304
  ),
  row(
    "valueOwners.lifecycle.6",
    "Ongoing",
    "On an ongoing basis, refresh design and experience programming within collection guardrails and reassess affiliation value as the property, operator, and market evolve. Confirm renewal, review, and remediation expectations with Choice Hotels development before major repositioning or operator transitions.",
    305
  ),

  // --- Operations & Standards: model ---
  row(
    "operations.model.primary_model",
    "",
    "Curated soft-brand affiliation within Choice Hotels' Radisson Hotel Group family, delivered through franchise or management arrangements that owners must confirm for the specific market and asset.",
    100
  ),
  row(
    "operations.model.management_option",
    "",
    "Third-party management is common for full-service upper-upscale assets with meaningful dining and wellness programming; owner-operated paths require credible design storytelling and experience-led service capability.",
    101
  ),
  row(
    "operations.model.typical_ownership",
    "",
    "Owners of landmark, architecturally distinctive, or iconic hotels who want Choice/Radisson Hotel Group distribution without converting into core Radisson's standardized full-service prototype.",
    102
  ),
  row(
    "operations.model.brand_involvement",
    "",
    "Choice Hotels development and design review typically touch narrative, bespoke design, dining/wellness programming, and opening readiness. Confirm the current review process and touchpoint frequency directly.",
    103
  ),
  row(
    "operations.model.systems_integration",
    "",
    "Radisson Collection participates in Choice Hotels' Choice Privileges loyalty and commercial systems ecosystem. Owners should validate PMS/CRS cutover, training, and commercial systems requirements for the specific deal.",
    104
  ),
  row(
    "operations.model.pre_opening",
    "",
    "Expect design and brand sign-off, dining/wellness programming planning, training, and opening readiness work before soft opening. Sequence PIP and operating setup with financing and operator capacity.",
    105
  ),
  row(
    "operations.model.staffing_intensity",
    "",
    "Full-service upper-upscale staffing across front office, housekeeping, F&B, and wellness/fitness programming. Underwrite labor to the curated experience-led narrative, not a lighter core Radisson skeleton.",
    106
  ),
  row(
    "operations.model.fb_complexity",
    "",
    "Dining is central to the collection's guest promise—outlet mix, kitchen scope, and wellness/fitness programming are material diligence items, generally more intensive than core Radisson.",
    107
  ),
  row(
    "operations.model.training",
    "",
    "Choice Hotels and Radisson Collection opening/service training should be confirmed as part of pre-opening planning. Budget time and cost against the agreement path and the property's dining/wellness programming.",
    108
  ),
  row(
    "operations.model.reporting_discipline",
    "",
    "Choice Hotels reporting and revenue-management cadence typically apply. Confirm owner reporting expectations and system participation in diligence.",
    109
  ),
  row(
    "operations.model.qa_rhythm",
    "",
    "Design and guest-experience QA apply at opening and periodically thereafter, with particular attention to dining and wellness delivery. Confirm review cadence and remediation expectations before treating affiliation as durable value.",
    110
  ),
  row(
    "operations.model.technology",
    "",
    "Choice Hotels technology participation is a diligence item beyond the brand flag alone. Confirm systems, digital, and loyalty integration requirements for the asset.",
    111
  ),
  row(
    "operations.standards_philosophy",
    "",
    "Radisson Collection standards protect each property's iconic character while requiring a coherent, curated guest experience across design, dining, wellness, and sustainability. Owners should underwrite to design narrative and experience-led programming—not marketing language alone.\nDesign and conversion detail: Landmark and heritage conversions fit best when the building and story support bespoke design.\nCapital: Dining and wellness programming typically require material investment; confirm scope directly.\nDifferentiation: Do not assume core Radisson, Blu, RED, or Individuals economics apply.",
    112
  ),
  row(
    "operations.operator_compat.summary",
    "",
    "Operators need to deliver a curated, design-forward upper-upscale stay with strong dining, wellness, and sustainability programming while operating inside Choice Hotels systems and Choice Privileges participation requirements.",
    113
  ),
  row(
    "operations.operator_compat.fit",
    "",
    "Best fit: operators experienced with independent luxury or upper-upscale full-service hotels with strong F&B/wellness capability. Weaker fit: operators accustomed only to core Radisson's standardized full-service model.",
    114
  ),
  row(
    "operations.operator_compat.tags",
    "",
    "Choice / Radisson curated collection\nUpper-upscale\nDining & wellness led\nIconic property",
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
    "High",
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
    "Design and guest-experience quality reviews typically intensify around conversion, repositioning, and remediation, with particular attention to dining and wellness delivery. Owners should confirm cadence, scoring focus, and who owns corrective action plans.",
    210
  ),
  row(
    "operations.compliance.training_rigor",
    "",
    "Choice Hotels and Radisson Collection onboarding for opening teams should include design storytelling and experience-led service expectations across dining and wellness. Confirm property-specific training scope and timing during pre-opening planning.",
    211
  ),
  row(
    "operations.compliance.reporting",
    "",
    "Choice Hotels reporting, revenue-management, and loyalty participation expectations typically apply. Owners should confirm ownership reporting cadence, operator versus owner data responsibilities, and system participation for the specific deal.",
    212
  ),
  row(
    "operations.compliance.brand_interaction",
    "",
    "Development and design-review touchpoints usually cover narrative, bespoke design, dining/wellness programming, and opening milestones. Interaction frequency varies by project stage—confirm directly for this opportunity.",
    213
  ),

  // --- Economics & Obligations: opening path ---
  row(
    "economics.opening.step.1",
    "Application & Feasibility",
    "Submit the asset for Choice Hotels development review with market context, ownership structure, and a candid read on the property's landmark or iconic potential for Radisson Collection. Confirm feasibility of franchise or management participation before detailed design spend.",
    400
  ),
  row(
    "economics.opening.step.2",
    "Design & Standards",
    "Complete Radisson Collection design and brand standards review with Choice Hotels—narrative, bespoke design, dining, wellness, and sustainability programming should cohere into a curated experience. Treat this as a design-led conversion phase, not a light cosmetic reflag.",
    401
  ),
  row(
    "economics.opening.step.3",
    "Pre-Opening Planning",
    "Build pre-opening budgets for hiring, training, Choice Hotels systems, FF&E, dining/wellness programming, and opening marketing aligned with approved design. Confirm operator responsibilities, opening timeline, and milestone approvals with brand development and your advisors.",
    402
  ),
  row(
    "economics.opening.step.4",
    "Opening Support",
    "Coordinate soft opening, design QA, and Choice Hotels commercial launch support with the operator. Ensure guest-facing teams can deliver the intended curated experience while systems and Choice Privileges participation go live on schedule.",
    403
  ),
  row(
    "economics.opening.step.5",
    "Stabilization",
    "Stabilize operations with Choice Hotels revenue-management rhythm, guest-feedback loops, and refinement of dining/wellness programming. Use early performance to validate underwriting on labor, F&B, and capital—not as a substitute for agreement-level economics review.",
    404
  ),

  // --- Footprint & Growth ---
  row(
    "footprint.momentum",
    "Upper-Upscale Market Expansion Signals",
    "Choice Hotels development materials describe Radisson Collection as an opportunity to expand into the upper-upscale market with iconic properties. Treat this as directional collection momentum rather than a property-level pipeline disclosure—confirm current activity directly with Choice Hotels development.",
    450
  ),
  row(
    "footprint.momentum",
    "Landmark And Heritage Conversion Emphasis",
    "Choice Hotels development materials repeatedly cite bespoke design and authentic local character as the collection's growth story, pointing toward landmark and heritage-building conversions. Owners with distinctive existing buildings should read this as directional interest—not confirmation of specific incentives or timelines.",
    451
  ),
  row(
    "footprint.momentum",
    "Dining, Wellness, And Sustainability Programming Focus",
    "Choice Hotels materials continue to emphasize dining, fitness, wellness, and sustainability programming as core to Radisson Collection's growth narrative. Owners evaluating conversion should confirm current programming expectations directly rather than assuming light-touch F&B will suffice.",
    452
  ),
  row(
    "footprint.portfolio_mix",
    "Portfolio mix",
    "Iconic / landmark properties\nUpper-upscale curated design\nDining & wellness-led programming\nHeritage / adaptive-reuse conversion",
    460
  ),
  row(
    "footprint.geo_intro",
    "Geographic footprint",
    "Radisson Collection has presence across gateway and destination markets where landmark or architecturally distinctive properties support curated upper-upscale positioning, with growing CALA and international representation. Owners should underwrite Choice Privileges commercial participation and design-review expectations—not assume uniform density across every region.",
    470
  ),
  row(
    "footprint.region.am",
    "Americas",
    "The Americas provide the clearest comp set for Radisson Collection's curated upper-upscale positioning under Choice Hotels—gateway-city and destination landmark properties anchor design-review expectations. Confirm local comps and development interest for the specific market.",
    471
  ),
  row(
    "footprint.region.cala",
    "CALA",
    "CALA representation continues to grow as owners of landmark or architecturally distinctive properties seek curated Choice/Radisson Hotel Group distribution. Owners should confirm authorized geography and design-review expectations locally rather than assuming core Radisson CALA comps translate directly.",
    472
  ),
  row(
    "footprint.region.eu",
    "Europe",
    "Europe carries deep Radisson Collection heritage and design reference points given the concentration of landmark and historic-building conversions. Americas or CALA owners can use these as design-narrative references without importing European ramp assumptions.",
    473
  ),
  row(
    "footprint.region.mea",
    "MEA",
    "MEA exposure is market-specific for Radisson Collection. Confirm authorization and development interest directly rather than assuming interchangeable footprint with other regions.",
    474
  ),
  row(
    "footprint.region.apac",
    "APAC",
    "APAC contributes landmark and destination properties for international travelers who recognize the collection through Choice/Radisson distribution. For Americas or CALA deals, treat APAC as brand-recognition context only, not a feasibility reference.",
    475
  ),
  row(
    "footprint.growth_themes",
    "",
    "Landmark and heritage conversion\nBespoke design and curated experience\nDining, wellness, and sustainability programming\nUpper-upscale market expansion",
    480
  ),
  row(
    "footprint.growth_editorial",
    "",
    "Radisson Collection compounds when owners bring a genuinely iconic or landmark asset and fund the design, dining, and wellness capital that curated positioning requires, while operators execute an experience-led guest journey inside Choice Hotels' commercial systems. Named collection growth themes are directional context—still underwrite local comps, PIP, and agreement terms independently.",
    481
  ),
  row(
    "footprint.growth_fit",
    "",
    "Best growth fit: owners of landmark, historically significant, or architecturally distinctive hotels who want curated upper-upscale positioning and Choice/Radisson distribution. Weaker fit: generic upscale assets better suited to core Radisson, or owners expecting Individuals-level flexibility without design review.",
    482
  ),

  // --- Owner Considerations ---
  row(
    "standards.intro",
    "",
    "Radisson Collection standards center on bespoke design, authentic local character, and experience-led dining/wellness/sustainability programming rather than a fixed full-service prototype. Confirm current standard detail and acceptance criteria directly with Choice Hotels development for the specific asset.",
    600
  ),
  row(
    "standards.requirement",
    "Design & narrative review",
    "Choice Hotels design review evaluates whether the property's architecture, local character, and public spaces cohere into a curated upper-upscale experience—confirm scope and timeline for your asset.",
    601
  ),
  row(
    "standards.requirement",
    "Choice Privileges systems participation",
    "PMS/CRS cutover, Choice Privileges loyalty integration, and commercial systems participation are typically required. Confirm technical scope and timeline with Choice Hotels development and your systems integrator.",
    602
  ),
  row(
    "standards.requirement",
    "Dining, wellness, and sustainability programming",
    "Outlet mix, kitchen scope, fitness/wellness facilities, and sustainability programming should match the collection's experience-led promise. Confirm expected capital intensity before assuming a light refresh will satisfy collection review.",
    603
  ),
  row(
    "standards.requirement",
    "Guest-room and suite standards",
    "Guest rooms and suites should reflect the property's specific bespoke design story within Choice Hotels' baseline guest-experience expectations. Confirm design-review flexibility and any required baseline amenities for the asset.",
    604
  ),
  row(
    "standards.requirement",
    "Training and service culture",
    "Opening and ongoing training should reinforce both Choice Hotels service baselines and the property's curated, experience-led guest promise. Confirm training scope, timing, and cost during pre-opening planning.",
    605
  ),
  row(
    "standards.requirement",
    "Ongoing design and QA review",
    "Design coherence and experience-led guest quality are reviewed periodically after opening, not only at conversion. Confirm current review cadence and remediation expectations before underwriting affiliation value as permanent.",
    606
  ),
  row(
    "standards.conversion",
    "",
    "Conversion suitability depends on whether the building and story are genuinely iconic or landmark-caliber and can sustain curated upper-upscale guest expectations under Choice Hotels systems—not whether the asset merely wants a Radisson-family flag. Confirm design-review scope and PIP intensity before committing capital.",
    607
  ),
  row(
    "standards.questions",
    "Questions owners should ask",
    [
      "What specific design and local-character elements does Choice Hotels expect us to preserve or develop for Radisson Collection acceptance?",
      "What is the current design-review timeline and remediation process if elements fall short?",
      "What dining, wellness, and sustainability programming capital should we plan for versus core Radisson or Blu?",
      "What Choice Privileges systems and commercial participation requirements apply to this specific asset?",
      "How does Radisson Collection's positioning and capital expectation compare to Blu, RED, and Individuals for a property like ours?",
    ].join("\n"),
    608
  ),

  // --- Dealality Insight: similar brands ---
  row(
    "insight.similar.1",
    "Radisson Blu by Choice",
    "Design-forward upper-upscale Radisson sibling with more standardized full-service expectations than Collection's curated, iconic-property positioning—compare design-review intensity and capital plans.",
    700
  ),
  row(
    "insight.similar.2",
    "Radisson Individuals by Choice",
    "Soft-collection Radisson sibling with lighter-touch design-review requirements than Radisson Collection—compare acceptance criteria and owner-control tradeoffs.",
    701
  ),
  row(
    "insight.similar.3",
    "Autograph Collection",
    "Marriott soft-collection peer for independently distinctive upper-upscale hotels—compare design-review rigor and loyalty participation outside Choice Hotels.",
    702
  ),
];

export const BRAND_FULL_BUILD_CONTENT = Object.freeze({
  brandSlug: BRAND_SLUG,
  sourcePack: Object.freeze({
    canonicalSite: "choicehotels.com/radisson-collection (confirm current consumer path with Choice Hotels; not yet listed in active-brand-website-corrections.js)",
    developmentPage: "choicehotelsdevelopment.com — Our Brands / Upscale / Radisson Collection",
    propertyPages:
      "Individual property pages live under choicehotels.com and radissonhotels.com — use for design/experience context per asset; do not embed raw property URLs in owner-facing Body copy.",
    parentContextPages: [
      "choicehotelsdevelopment.com — Choice Hotels development brand family context",
      "radissonhotels.com — Radisson Hotel Group brand-family context",
    ],
    imageSources:
      "choicehotelsdevelopment.com and radissonhotels.com brand/property galleries; use for design/character reference only — confirm licensing before any external use.",
    domains: ["choicehotelsdevelopment.com", "choicehotels.com", "radissonhotels.com"],
  }),
  brandLens: Object.freeze({
    brandModel:
      "Choice Hotels' curated Radisson Hotel Group collection — iconic, bespoke-design upper-upscale hotels with dining, wellness, and sustainability programming, each keeping its own local character rather than a full-service prototype.",
    ownerFit:
      "Owners of landmark, architecturally distinctive, or iconic hotels who want curated upper-upscale positioning and Choice/Radisson distribution without core Radisson or Blu standardization.",
    propertyFit:
      "Iconic or landmark properties—historic buildings, distinctive new-build, or destination hotels—able to sustain bespoke design and experience-led dining/wellness/sustainability programming.",
    conversionLogic:
      "Design and narrative review gates acceptance, not a fixed room-and-corridor spec; dining, wellness, and sustainability capital should be confirmed directly rather than assumed minimal.",
    operatingImplications:
      "Full-service upper-upscale staffing with meaningful F&B and wellness/fitness programming intensity; Choice Hotels systems, Choice Privileges loyalty, and reporting participation apply.",
    standardsRequirements:
      "Bespoke design coherence, authentic local character, and dining/wellness/sustainability programming quality reviewed at conversion and periodically thereafter; confirm current acceptance criteria directly with Choice Hotels development.",
    sourceLimitations:
      "Public development materials (fixtures/choice-dev-site-text/our-brands__upscale__radisson-collection.txt) describe collection positioning only—no property-level counts, fees, or performance data. Confirm agreement-specific terms directly with Choice Hotels development.",
    distinguishFrom:
      "Core Radisson (mainstream upscale full-service), Radisson Blu (design-forward upper-upscale but more standardized), Radisson RED (upscale lifestyle/select-service), and Radisson Individuals (soft-collection with lighter design-review requirements than Radisson Collection).",
  }),
  presentation: PRESENTATION,
});

export default BRAND_FULL_BUILD_CONTENT;
