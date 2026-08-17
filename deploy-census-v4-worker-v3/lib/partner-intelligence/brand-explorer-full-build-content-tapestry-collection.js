/**
 * Brand Explorer Tab Factory — full build content pack: Tapestry Collection by Hilton.
 *
 * True-incomplete brand (see brand-explorer-built-blocked-content.js →
 * BUILT_BLOCKED_TRUE_INCOMPLETE, slug "tapestry-collection-by-hilton"). Hilton's
 * upscale-tier soft-brand collection of vibrant, independent-minded hotels — a lighter,
 * more conversion-friendly lane than Curio Collection's upper-upscale, culinary-forward
 * positioning.
 *
 * File name uses "tapestry-collection" per the requested deliverable path; the
 * canonical system slug (matching brand-explorer-built-blocked-content.js and
 * reports/brand-explorer-complete-build-tapestry-collection-by-hilton.*) is
 * "tapestry-collection-by-hilton" and is used as brandSlug below and in the index.
 *
 * Copy rules:
 * - Directional, owner-facing. No invented fees, ADR, FDD, Item 19, pipeline counts,
 *   or performance guarantees.
 * - Brand-specific — avoids Hilton-umbrella boilerplate as the brand story.
 * - No Company Validated claims.
 * - No raw https:// URLs in any Body field (PVQL fails on raw URLs in owner-facing copy).
 * - Distinguishes Tapestry from Curio Collection (Hilton's upper-upscale, more
 *   culinary-forward soft collection, launched 2014, generally larger-scale full-service).
 */

import { applyHiltonLoyaltyPresentationSlots } from "./build-hilton-loyalty-presentation-slots.js";

const BRAND_SLUG = "tapestry-collection-by-hilton";
const BRAND_NAME = "Tapestry Collection by Hilton";
const PARENT_COMPANY = "Hilton Worldwide Holdings Inc.";
const RECORD_ID = "reccXxMHEh7NNRhIE";

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

/** Owner Considerations table shape (Typical / Owner Planning / Status / Notes). */
function reqBody({ typical, owner, status = "Confirm with brand", notes }) {
  return [
    `Typical consideration: ${typical}`,
    `Owner planning consideration: ${owner}`,
    `Typical status: ${status}`,
    `Notes to confirm: ${notes}`,
  ].join("\n");
}

const PRESENTATION_BASE = [
  // --- Positioning (Basics-backed slots) ---
  row(
    "Brand Positioning",
    "",
    "A vibrant, independent-minded upscale soft-brand collection—Tapestry Collection sits below Curio Collection's upper-upscale, culinary-forward intensity, offering owners a more conversion-friendly, lighter-touch path into Hilton Honors distribution.",
    10
  ),
  row(
    "Guest Psychographics Description",
    "",
    "Value-conscious upscale travelers who want a memorable, independent-feeling stay with personality and local character, but without the full-service dining intensity or price point of Curio or luxury soft brands—guests who still want dependable Hilton Honors participation.",
    11
  ),

  // --- Overview ---
  row(
    "overview.typical_use_case",
    "",
    "Independent or boutique upscale hotels with a memorable, vibrant character—conversions of existing full- or select-service assets where owners want Hilton Honors distribution and commercial systems without the full-service capital intensity of Curio Collection.",
    20
  ),
  row(
    "overview.development_model",
    "",
    "Conversion-led affiliation is the dominant path—existing independent or lightly branded hotels with a distinctive story repositioned under Tapestry's lighter design-review bar. Sponsors should model PIP scope realistically rather than assuming Curio-level capital intensity is required.",
    21
  ),
  row(
    "overview.relative_positioning",
    "Relative Positioning",
    "Tapestry Collection sits at Hilton's upscale, more conversion-friendly soft-brand tier—below Curio Collection's upper-upscale, culinary-forward full-service positioning and design-review intensity. Owners should compare capital requirements and F&B complexity between the two before selecting a Hilton soft-brand path.",
    22
  ),
  row(
    "overview.scenario.1",
    "Independent Upscale Conversion",
    "An existing independent or lightly branded upscale hotel with a memorable local story seeking Hilton Honors distribution without Curio-level design and F&B capital. Tapestry fits when the story is genuine but the asset is not positioned for upper-upscale culinary-forward intensity—confirm acceptance criteria and PIP scope directly with Hilton development.",
    30
  ),
  row(
    "overview.scenario.2",
    "Boutique Repositioning At Moderate Capital",
    "A boutique or historic hotel needing repositioning capital that is meaningful but lighter than a full luxury or upper-upscale soft-brand conversion. Owners should diligence Tapestry's design-review scope, moderate F&B expectations, and Hilton systems integration timeline against the asset's current condition and comp set before assuming Curio-level capital is required.",
    31
  ),
  row(
    "overview.scenario.3",
    "Value-Conscious Independent-Character Hotel",
    "An independent hotel in a secondary or gateway market where a distinctive story differentiates it from nearby chain-scale competitors, but the market cannot support Curio-level rate and capital intensity. Tapestry fits when Hilton Honors reach and distribution matter more than culinary-forward positioning—confirm design-review scope and moderate F&B expectations directly with Hilton development.",
    32
  ),
  row(
    "overview.why_value",
    "Why Value Is Strongest",
    "Value concentrates where the property has a genuine, memorable independent story, the market supports upscale (not upper-upscale) rate positioning, and ownership wants Hilton Honors distribution without Curio-level F&B and design capital. Weakest fit is an asset that actually needs upper-upscale culinary-forward positioning to compete.",
    33
  ),
  row(
    "overview.proof.1",
    "Vibrant, Independent-Minded Positioning",
    "Tapestry Collection markets itself around vibrant, memorable independent character at the upscale tier—owners should treat this as a lighter design-and-story bar than Curio Collection, not an equivalent upper-upscale standard, and budget PIP and design review accordingly rather than assuming identical requirements across both collections.",
    40
  ),
  row(
    "overview.proof.2",
    "Hilton Honors Distribution",
    "Brand materials position Hilton Honors loyalty participation and Hilton's commercial systems as the core affiliation value at the upscale tier—loyalty earn/redeem and distribution reach layered onto an independently branded stay. Confirm systems integration scope directly with Hilton development for the specific asset.",
    41
  ),
  row(
    "overview.proof.3",
    "Conversion-Friendly Design Review",
    "Tapestry's design-review bar is generally lighter than Curio's upper-upscale, culinary-forward standard, making it a more accessible conversion path for independent owners. Owners should confirm current acceptance criteria rather than assuming Tapestry and Curio require the same capital.",
    42
  ),
  row(
    "overview.proof.4",
    "Moderate F&B And Public-Space Expectations",
    "F&B and public-space intensity at Tapestry is generally lighter than Curio's culinary-forward promise. Owners should still budget for a credible independent guest experience, but should not assume the same outlet count or kitchen scope as an upper-upscale conversion.",
    43
  ),
  row(
    "overview.featured_application",
    "Independent upscale conversion",
    "An existing independent or lightly branded upscale hotel with a memorable local story can use Tapestry Collection to gain Hilton Honors distribution and commercial systems at a lighter capital bar than Curio Collection. Owners should underwrite design-review scope and moderate F&B/public-space capital—confirming acceptance criteria directly rather than assuming Curio-level intensity applies.",
    44,
    {
      caseSummaryOverview:
        "Featured path for independent-character upscale assets seeking Hilton distribution under Tapestry Collection.",
      caseSummaryBrandRelevance:
        "Matches Tapestry's conversion-friendly, upscale lane—lighter design-review and F&B bar than Curio's upper-upscale positioning.",
      caseSummaryOwnerObjective:
        "Fund moderate design/F&B capital and Hilton systems integration without assuming Curio-level intensity is required.",
      caseSummaryInterpretation:
        "Use as a conversion-fit lens—confirm acceptance criteria and agreement terms directly with Hilton development; not a performance forecast.",
      caseSummaryTags: "soft-brand, independent-character, upscale, Hilton, conversion",
    }
  ),
  row(
    "overview.differentiators.identity",
    "Experience & Identity",
    [
      "Vibrant, independent-minded upscale character—no shared design template",
      "Lighter design-review bar than Curio Collection's upper-upscale standard",
      "Story and character authenticity as the acceptance bar, not a construction spec",
      "Conversion-friendly path for existing independent or lightly branded hotels",
    ].join("\n"),
    45
  ),
  row(
    "overview.differentiators.commercial",
    "Commercial & Distribution",
    [
      "Hilton Honors loyalty earn/redeem participation",
      "Hilton global sales and commercial systems access",
      "Distribution reach without Curio-level design or F&B capital requirements",
      "Confirm specific commercial participation terms directly with Hilton development",
    ].join("\n"),
    46
  ),
  row(
    "overview.bestAt.1",
    "Conversion-Friendly Independent Character",
    "Protecting a genuine, memorable independent story at upscale intensity while adding Hilton Honors reach—Tapestry's core value versus a standardized hard-brand conversion.",
    47
  ),
  row(
    "overview.bestAt.2",
    "Moderate Capital Entry Into Hilton Soft Brands",
    "Providing a lighter design-review and F&B capital bar than Curio Collection—owners should benchmark to comparable upscale independents, not upper-upscale culinary-forward properties.",
    48
  ),
  row(
    "overview.bestAt.3",
    "Hilton Systems Without A Fixed Prototype",
    "Delivering Hilton commercial infrastructure and Honors loyalty participation while preserving property-specific character—distinct from Curio's more standards-intensive, culinary-forward positioning.",
    49
  ),
  row(
    "overview.portfolio_context",
    "Portfolio Context",
    "Within Hilton's soft-brand family, Tapestry Collection is the more conversion-friendly, upscale-tier lane—lighter on design review and F&B capital than Curio Collection's upper-upscale, culinary-forward positioning. Owners should compare design-review intensity, F&B expectations, and Hilton Honors participation across those two collections before selecting a path.",
    50
  ),
  row(
    "footprint.portfolio_context",
    "Portfolio Context",
    "Tapestry Collection anchors the accessible, upscale end of Hilton's independent-character soft-brand family—more conversion-friendly than Curio Collection and structurally distinct from Hilton's hard-brand upscale flags. Owners should weigh capital and design-review intensity against desired operating flexibility before choosing between Tapestry and Curio.",
    51
  ),
  row(
    "valueOwners.watchouts",
    "",
    [
      "Design review is lighter than Curio's, but still qualitative—confirm current acceptance criteria before assuming minimal PIP",
      "F&B and public-space expectations are moderate, not culinary-forward—do not underwrite Curio-level outlet complexity",
      "Confirm current remediation and ongoing review expectations directly—do not assume affiliation value is permanent",
      "Do not confuse Tapestry's upscale positioning with Curio's upper-upscale tier when benchmarking comps",
    ].join("\n"),
    52
  ),

  // --- Value to Owners: lifecycle ---
  row(
    "valueOwners.lifecycle.1",
    "Evaluation",
    "Confirm the property has a genuine, memorable independent story worth Tapestry's upscale positioning, not whether it merely wants a Hilton flag. Assess whether the asset fits Tapestry's lighter capital bar or actually needs Curio's upper-upscale positioning before committing design capital.",
    300
  ),
  row(
    "valueOwners.lifecycle.2",
    "Conversion Design",
    "Shape conversion design around the property's own story at a moderate capital level—public spaces, guest rooms, and F&B should read as distinctive without assuming Curio-level intensity. Sequence design-review milestones with financing and operator selection, and treat acceptance-critical work as priority spend over cosmetic changes.",
    301
  ),
  row(
    "valueOwners.lifecycle.3",
    "Pre-Opening",
    "Coordinate Hilton systems cutover, Honors loyalty integration, staffing, and training with design sign-off and opening readiness. Confirm owner versus operator responsibilities for commercial launch, and budget time for collection-specific story orientation rather than a heavier Curio-style pre-opening program.",
    302
  ),
  row(
    "valueOwners.lifecycle.4",
    "Opening",
    "Launch with consistent, vibrant independent-character presentation across every guest touchpoint. Opening support typically centers on guest-experience coherence and Hilton systems stabilization rather than a standardized prototype punch list—confirm support scope and staffing coverage directly with Hilton development before launch week.",
    303
  ),
  row(
    "valueOwners.lifecycle.5",
    "Ramp-Up",
    "During ramp-up, calibrate rate positioning against guest-review themes tied to the property's independent story, not only occupancy headlines. Watch labor and F&B intensity relative to Tapestry's moderate capital bar rather than a Curio-level comp set.",
    304
  ),
  row(
    "valueOwners.lifecycle.6",
    "Ongoing",
    "On an ongoing basis, refresh design and programming within collection guardrails and reassess affiliation value as the property, operator, and market evolve. Confirm renewal, review, and remediation expectations with Hilton development before major repositioning or operator transitions.",
    305
  ),

  // --- Operations & Standards: model ---
  row(
    "operations.model.primary_model",
    "",
    "Soft-brand affiliation within Hilton's upscale independent-character collection, delivered through franchise arrangements that owners must confirm for the specific market and asset.",
    100
  ),
  row(
    "operations.model.management_option",
    "",
    "Independent ownership with third-party or owner-operated management is common at the upscale tier; operators must still preserve the property's distinctive story and service culture.",
    101
  ),
  row(
    "operations.model.typical_ownership",
    "",
    "Owners of independent or lightly branded upscale hotels with a memorable local story who want Hilton Honors reach without Curio-level design and F&B capital.",
    102
  ),
  row(
    "operations.model.brand_involvement",
    "",
    "Hilton development and design review typically touch narrative, public-space design, and opening readiness at a lighter bar than Curio. Confirm the current review process and touchpoint frequency directly.",
    103
  ),
  row(
    "operations.model.systems_integration",
    "",
    "Tapestry participates in Hilton's Honors loyalty and commercial systems ecosystem. Owners should validate PMS/CRS cutover, training, and commercial systems requirements for the specific deal.",
    104
  ),
  row(
    "operations.model.pre_opening",
    "",
    "Expect design and brand sign-off, training, and opening readiness work before soft opening, typically lighter in scope than a Curio conversion. Sequence PIP and operating setup with financing and operator capacity.",
    105
  ),
  row(
    "operations.model.staffing_intensity",
    "",
    "Upscale staffing across front office, housekeeping, and moderate F&B. Underwrite labor to the property's independent narrative at an upscale (not upper-upscale) intensity.",
    106
  ),
  row(
    "operations.model.fb_complexity",
    "",
    "F&B is moderate, not culinary-forward—outlet mix and kitchen scope are generally lighter than Curio Collection, though still material diligence items relative to select-service prototypes.",
    107
  ),
  row(
    "operations.model.training",
    "",
    "Hilton and Tapestry opening/service training should be confirmed as part of pre-opening planning. Budget time and cost against the agreement path and the property's specific story.",
    108
  ),
  row(
    "operations.model.reporting_discipline",
    "",
    "Hilton reporting and revenue-management cadence typically apply. Confirm owner reporting expectations and system participation in diligence.",
    109
  ),
  row(
    "operations.model.qa_rhythm",
    "",
    "Design and guest-experience QA apply at opening and periodically thereafter, at a lighter bar than Curio's culinary-forward review. Confirm review cadence and remediation expectations before treating affiliation as durable value.",
    110
  ),
  row(
    "operations.model.technology",
    "",
    "Hilton technology and Honors participation are diligence items beyond the brand flag alone. Confirm systems, digital, and loyalty integration requirements for the asset.",
    111
  ),
  row(
    "operations.standards_philosophy",
    "",
    "Tapestry Collection standards protect individual property character while requiring a coherent, vibrant independent guest experience at the upscale tier. Owners should underwrite to design narrative and service delivery—not marketing language alone.\nDesign and conversion detail: Conversion is generally more accessible than Curio; confirm current PIP scope directly.\nF&B: Moderate expectations—do not assume culinary-forward intensity is required.\nDifferentiation: Do not assume Curio-level capital or design-review rigor applies.",
    112
  ),
  row(
    "operations.operator_compat.summary",
    "",
    "Operators need to sustain a vibrant, memorable independent hotel identity inside Hilton's Tapestry Collection at an upscale capital and service level—lighter than Curio's culinary-forward, upper-upscale requirements but still requiring genuine story delivery.",
    113
  ),
  row(
    "operations.operator_compat.fit",
    "",
    "Best fit: operators experienced with independent or boutique upscale hotels who can protect character without upper-upscale F&B complexity. Weaker fit: operators expecting Curio-level culinary programming or prototype-driven operators with no design sensitivity.",
    114
  ),
  row(
    "operations.operator_compat.tags",
    "",
    "Hilton soft collection\nUpscale\nConversion-friendly\nIndependent character",
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
    "Very high",
    201
  ),
  row(
    "operations.flexibility.localization",
    "",
    "High",
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
    "Low",
    205
  ),

  // --- Operations & Standards: compliance ---
  row(
    "operations.compliance.qa_cadence",
    "",
    "Design and guest-experience quality reviews apply around conversion and periodically thereafter, generally lighter in scope than Curio's culinary-forward review. Owners should confirm cadence and who owns corrective action plans.",
    210
  ),
  row(
    "operations.compliance.training_rigor",
    "",
    "Hilton and Tapestry onboarding for opening teams should include story-telling and service expectations at the upscale tier. Confirm property-specific training scope and timing during pre-opening planning.",
    211
  ),
  row(
    "operations.compliance.reporting",
    "",
    "Hilton reporting, revenue-management, and loyalty participation expectations typically apply. Owners should confirm ownership reporting cadence, operator versus owner data responsibilities, and system participation for the specific deal.",
    212
  ),
  row(
    "operations.compliance.brand_interaction",
    "",
    "Development and design-review touchpoints usually cover narrative and opening milestones, generally lighter cadence than Curio. Confirm how often brand and owner teams meet during conversion, opening, and stabilized operations.",
    213
  ),

  // --- Economics & Obligations: opening path ---
  row(
    "economics.opening.step.1",
    "Application & Feasibility",
    "Submit the asset for Hilton development review with market context, ownership structure, and a candid read on the property's independent story for Tapestry Collection. Confirm feasibility of franchise participation and whether the asset fits Tapestry's upscale tier versus Curio's upper-upscale bar.",
    400
  ),
  row(
    "economics.opening.step.2",
    "Design & Standards",
    "Complete Tapestry design and brand standards review with Hilton—narrative, public spaces, and guest rooms should cohere into a distinctive, vibrant story. Treat this as a conversion-friendly design phase, lighter than a Curio-level rebuild.",
    401
  ),
  row(
    "economics.opening.step.3",
    "Pre-Opening Planning",
    "Build pre-opening budgets for hiring, training, Hilton systems, FF&E, and opening marketing aligned with approved design. Confirm operator responsibilities, opening timeline, and milestone approvals with brand development and your advisors.",
    402
  ),
  row(
    "economics.opening.step.4",
    "Opening Support",
    "Coordinate soft opening, design QA, and Hilton commercial launch support with the operator. Ensure guest-facing teams can deliver the intended independent character while Honors and systems participation go live on schedule.",
    403
  ),
  row(
    "economics.opening.step.5",
    "Stabilization",
    "Stabilize operations with Hilton revenue-management rhythm and guest-feedback loops. Use early performance to validate underwriting on labor, F&B, and capital at Tapestry's upscale intensity—not as a substitute for agreement-level economics review.",
    404
  ),

  // --- Footprint & Growth ---
  row(
    "footprint.momentum",
    "Independent Upscale Conversion Signals",
    "Hilton owner and development materials continue to position Tapestry Collection as an accessible conversion path for independent upscale hotels seeking Honors distribution. Treat this as directional collection momentum rather than a property-level pipeline disclosure—confirm current activity directly with Hilton development.",
    450
  ),
  row(
    "footprint.momentum",
    "Complement To Curio Within Hilton's Soft Portfolio",
    "Hilton materials describe Tapestry as the more conversion-friendly complement to Curio Collection within the broader soft-brand portfolio—serving upscale independents alongside Curio's upper-upscale positioning. Confirm which collection fits your asset's segment directly with Hilton development.",
    451
  ),
  row(
    "footprint.momentum",
    "Boutique And Secondary-Market Interest",
    "Hilton owner-facing materials describe continued interest in boutique and secondary-market independent hotels for Tapestry given its lighter capital bar. Owners in non-gateway markets should read this as directional interest—not confirmation of specific incentives for any given market.",
    452
  ),
  row(
    "footprint.portfolio_mix",
    "Portfolio mix",
    "Independent upscale conversions\nBoutique / secondary-market hotels\nModerate F&B and public-space intensity\nConversion-friendly heritage and existing-building repositioning",
    460
  ),
  row(
    "footprint.geo_intro",
    "Geographic footprint",
    "Tapestry Collection has presence across U.S. gateway and secondary markets, with growing CALA and international representation as independent owners bring upscale properties into Hilton's Honors ecosystem. Owners should underwrite mainstream Honors commercial participation and design-review expectations—not assume uniform density across every region.",
    470
  ),
  row(
    "footprint.region.am",
    "Americas",
    "The Americas remain Tapestry's deepest base—independent upscale conversions across the U.S. and Canada provide the clearest comp set for design-review expectations and Honors commercial participation. Confirm local comps and development interest for the specific market.",
    471
  ),
  row(
    "footprint.region.cala",
    "CALA",
    "CALA representation continues to grow as independent owners seek Hilton Honors distribution at the upscale tier. Owners should confirm authorized geography and design-review expectations locally rather than assuming U.S. gateway comps translate directly.",
    472
  ),
  row(
    "footprint.region.eu",
    "Europe",
    "Europe contributes independent-character conversion reference points for Hilton's soft-brand family more broadly. Americas or CALA owners can use these as design-narrative references without importing European ramp assumptions.",
    473
  ),
  row(
    "footprint.region.mea",
    "MEA",
    "MEA exposure is market-specific and generally smaller-scale for Tapestry relative to Hilton's broader upscale and upper-upscale brands. Confirm authorization and development interest directly.",
    474
  ),
  row(
    "footprint.region.apac",
    "APAC",
    "APAC contributes independent hotels for international travelers who recognize the collection through Hilton Honors participation. For Americas or CALA deals, treat APAC as brand-recognition context only, not a feasibility reference.",
    475
  ),
  row(
    "footprint.growth_themes",
    "",
    "Independent upscale conversion\nBoutique and secondary-market repositioning\nConversion-friendly heritage building reuse\nComplement to Curio within Hilton's soft portfolio",
    480
  ),
  row(
    "footprint.growth_editorial",
    "",
    "Tapestry Collection compounds when independent owners bring a genuine, vibrant story and Hilton Honors distribution amplifies it at a moderate capital bar—lighter than Curio Collection but still requiring real story delivery. Named collection growth themes are directional context—still underwrite local comps, PIP, and agreement terms independently.",
    481
  ),
  row(
    "footprint.growth_fit",
    "",
    "Best growth fit: owners of independent or lightly branded upscale hotels with a genuine local story and a market that doesn't support upper-upscale culinary-forward positioning. Weaker fit: assets that actually need Curio-level F&B intensity to compete, or generic properties with no independent character.",
    482
  ),

  // --- Owner Considerations ---
  row(
    "standards.intro",
    "",
    "Tapestry Collection standards center on vibrant, independent story authenticity and guest-experience quality at the upscale tier—lighter than Curio Collection's upper-upscale, culinary-forward bar. Confirm current standard detail and acceptance criteria directly with Hilton development for the specific asset.",
    600
  ),
  row(
    "standards.requirement",
    "Design & narrative review",
    reqBody({
      typical:
        "Hilton design review evaluates whether the property's story, architecture, and public spaces cohere into a distinctive upscale experience—lighter bar than Curio.",
      owner:
        "Plan design-review scope, timeline, and any narrative remediation into conversion capital before underwriting Tapestry acceptance.",
      status: "Typically Expected",
      notes: "Confirm scope and timeline for your asset with Hilton development.",
    }),
    601
  ),
  row(
    "standards.requirement",
    "Hilton Honors systems participation",
    reqBody({
      typical:
        "PMS/CRS cutover, Honors loyalty integration, and commercial systems participation are typically required.",
      owner:
        "Budget systems cutover, training, and ongoing Honors/commercial participation separately from the soft-brand design story.",
      status: "Typically Expected",
      notes: "Confirm technical scope and timeline with Hilton development and your systems integrator.",
    }),
    602
  ),
  row(
    "standards.requirement",
    "F&B and public-space capital",
    reqBody({
      typical:
        "Outlet mix and public-space activation should match the property's narrative at a moderate, upscale intensity.",
      owner:
        "Establish required versus elective F&B and public-space capital before assuming zero or Curio-level investment.",
      status: "May Apply",
      notes: "Confirm expected capital intensity for the specific asset with Hilton development.",
    }),
    603
  ),
  row(
    "standards.requirement",
    "Guest-room standards",
    reqBody({
      typical:
        "Guest rooms should reflect the property's specific design story within Hilton's baseline guest-experience expectations at the upscale tier.",
      owner:
        "Confirm design-review flexibility and any required baseline amenities before locking room-product budgets.",
      status: "Typically Expected",
      notes: "Confirm current guest-room expectations and flexibility for your asset.",
    }),
    604
  ),
  row(
    "standards.requirement",
    "Training and service culture",
    reqBody({
      typical:
        "Opening and ongoing training should reinforce both Hilton service baselines and the property's own independent narrative.",
      owner:
        "Define training scope, timing, and cost ownership during pre-opening planning.",
      status: "Typically Expected",
      notes: "Confirm training scope, timing, and cost during pre-opening planning.",
    }),
    605
  ),
  row(
    "standards.requirement",
    "Ongoing design and QA review",
    reqBody({
      typical:
        "Design coherence and guest-experience quality are reviewed periodically after opening, generally at a lighter cadence than Curio.",
      owner:
        "Underwrite remediation risk and owner versus operator responsibilities for ongoing QA before treating affiliation value as permanent.",
      status: "Typically Expected",
      notes: "Confirm current review cadence and remediation expectations with Hilton development.",
    }),
    606
  ),
  row(
    "standards.conversion",
    "",
    "Conversion suitability depends on whether the building and story can sustain a vibrant, independent upscale guest experience under Hilton systems—not whether the asset needs Curio-level design or F&B intensity. Confirm design-review scope and PIP intensity before committing capital.",
    607
  ),
  row(
    "standards.questions",
    "Questions owners should ask",
    [
      "What specific design and story elements does Hilton expect us to preserve or develop for Tapestry acceptance versus Curio?",
      "What is the current design-review timeline and remediation process if elements fall short?",
      "What Hilton Honors systems and commercial participation requirements apply to this specific asset?",
      "How does F&B and public-space capital intensity compare between Tapestry and Curio for a property like ours?",
      "What ongoing QA cadence and standards review should we expect after opening, and who owns corrective action?",
    ].join("\n"),
    608
  ),

  // --- Dealality Insight: similar brands ---
  row(
    "insight.similar.1",
    "Curio Collection by Hilton",
    "Hilton soft-collection sibling with upper-upscale, more culinary-forward positioning than Tapestry's accessible, conversion-friendly lane—compare capital intensity and design-review rigor.",
    700
  ),
  row(
    "insight.similar.2",
    "Autograph Collection",
    "Marriott soft-collection peer for independently distinctive upper-upscale hotels—compare design-review intensity and loyalty participation outside Hilton.",
    701
  ),
  row(
    "insight.similar.3",
    "MGallery Collection",
    "Accor soft-collection peer for story-led hotels—compare affiliation model and parent-system rigidity outside Hilton.",
    702
  ),
];

/** Hilton Honors 2026 loyalty slots (incl. Diamond Reserve) appended for Tab Factory rebuilds. */
const PRESENTATION = applyHiltonLoyaltyPresentationSlots(PRESENTATION_BASE, [], {
  brandName: BRAND_NAME,
});

export const BRAND_FULL_BUILD_CONTENT = Object.freeze({
  brandSlug: BRAND_SLUG,
  sourcePack: Object.freeze({
    canonicalSite: "hilton.com/en/brands/tapestry-collection",
    developmentPage:
      "hiltonfranchise.com — Our Brands (Tapestry Collection); confirm current development-specific URL directly with Hilton before use.",
    propertyPages:
      "Individual property pages live under hilton.com/en/hotels/ — use for design/story context per asset; do not embed raw property URLs in owner-facing Body copy.",
    parentContextPages: [
      "hilton.com — Hilton Worldwide brand family context",
      "hiltonfranchise.com — Hilton development brand materials (collection growth themes only, no invented counts)",
    ],
    imageSources:
      "hilton.com brand and property galleries; use for design/character reference only — confirm licensing before any external use.",
    domains: ["hilton.com", "hiltonfranchise.com"],
  }),
  brandLens: Object.freeze({
    brandModel:
      "Hilton soft-brand collection — vibrant, independent-minded upscale hotels with a lighter, more conversion-friendly design-review bar than Curio Collection, each keeping its own character while participating in Hilton Honors and commercial systems.",
    ownerFit:
      "Owners of independent or lightly branded upscale hotels with a memorable local story who want Hilton Honors distribution without Curio-level design and F&B capital.",
    propertyFit:
      "Independent or boutique upscale hotels—generally lighter-capital conversions than Curio-tier assets—with a credible story and moderate F&B/public-space potential.",
    conversionLogic:
      "Design and story review gates acceptance at a lighter bar than Curio Collection—capital should protect the property's character without assuming culinary-forward, upper-upscale intensity is required.",
    operatingImplications:
      "Upscale staffing and moderate F&B/public-space intensity; Hilton systems, Honors loyalty, and reporting participation apply regardless of design flexibility.",
    standardsRequirements:
      "Story authenticity and guest-experience quality reviewed at conversion and periodically thereafter, generally lighter cadence and scope than Curio; confirm current acceptance criteria directly with Hilton development.",
    sourceLimitations:
      "Public brand materials describe collection positioning and growth themes only—no property-level counts, fees, or performance data. Confirm agreement-specific terms directly with Hilton development.",
    distinguishFrom:
      "Curio Collection by Hilton (upper-upscale, more culinary-forward soft collection launched 2014, generally larger-scale full-service and more design-review-intensive than Tapestry's conversion-friendly, upscale positioning).",
  }),
  presentation: PRESENTATION,
});

export default BRAND_FULL_BUILD_CONTENT;
