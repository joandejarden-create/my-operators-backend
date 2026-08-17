/**
 * Brand Explorer Tab Factory — full build content pack: Handwritten Collection.
 *
 * True-incomplete brand (see brand-explorer-built-blocked-content.js →
 * BUILT_BLOCKED_TRUE_INCOMPLETE). Accor soft/lifestyle collection of independent
 * boutique hotels with a personal, story-rich "handwritten" character — parent is
 * Accor, NOT IHG.
 *
 * Parentage: Accor (not IHG). Lifestyle affiliation config was corrected to Accor.
 *
 * Copy rules:
 * - Directional, owner-facing. No invented fees, ADR, FDD, Item 19, pipeline counts,
 *   or performance guarantees.
 * - Brand-specific — avoids Accor-umbrella boilerplate as the brand story.
 * - No Company Validated claims.
 * - No raw https:// URLs in any Body field (PVQL fails on raw URLs in owner-facing copy).
 * - Distinguishes Handwritten from MGallery Collection (Accor's more established,
 *   broader-international soft/lifestyle collection).
 */

const BRAND_SLUG = "handwritten-collection";
const BRAND_NAME = "Handwritten Collection";
const PARENT_COMPANY = "Accor";
const RECORD_ID = "rec7hTXwMRC81EPqz";

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
    "A personal, story-rich Accor soft-brand collection for independent boutique hotels—each property's own local character and ownership story stay visible, with Accor Live Limitless (ALL) loyalty and commercial distribution layered on top rather than a standardized prototype imposed.",
    10
  ),
  row(
    "Guest Psychographics Description",
    "",
    "Guests seeking a personal, character-led boutique stay with a genuine local or ownership story—travelers who want individuality and warmth over big-box consistency, while still valuing Accor ALL loyalty participation and dependable systems underneath.",
    11
  ),

  // --- Overview ---
  row(
    "overview.typical_use_case",
    "",
    "Independent boutique hotels with a genuine ownership or local story—often smaller-scale or entrepreneurially owned properties—seeking Accor ALL distribution and commercial systems without adopting a standardized prototype or losing their personal character.",
    20
  ),
  row(
    "overview.development_model",
    "",
    "Conversion-led affiliation for existing independent boutique properties rather than a standardized new-build prototype. Sponsors should model design/story review timelines and Accor systems integration before assuming affiliation is a light administrative step.",
    21
  ),
  row(
    "overview.relative_positioning",
    "Relative Positioning",
    "Handwritten Collection sits alongside MGallery within Accor's soft/lifestyle family as a more intimate, story-personal boutique lane—generally smaller-scale and more narrowly independent-owner-focused than MGallery's broader, more internationally established distinctive-hotel positioning. Owners should compare acceptance criteria and scale expectations directly with Accor rather than assuming the two collections are interchangeable.",
    22
  ),
  row(
    "overview.scenario.1",
    "Independent Boutique Affiliation",
    "A smaller-scale, owner-operated boutique hotel with a genuine personal or local story seeking Accor ALL distribution and commercial systems. Handwritten fits when the ownership story is the product—not when the asset merely needs a recognizable flag. Confirm acceptance criteria and design-review scope directly with Accor before committing capital.",
    30
  ),
  row(
    "overview.scenario.2",
    "Story-Led Conversion",
    "An existing independent hotel with character worth protecting—heritage building, distinctive design, or strong local ownership narrative—evaluating Accor affiliation without a standardized prototype conversion. Owners should diligence collection acceptance criteria, F&B and service expectations, and boutique-scale staffing plans against the property's current guest experience before committing renovation capital.",
    31
  ),
  row(
    "overview.scenario.3",
    "Boutique Repositioning In Established Markets",
    "An independent boutique hotel in an established urban or leisure market where a personal story differentiates the asset from nearby competitors. Handwritten fits when the story and physical product can sustain the collection's guest-experience bar under Accor systems—confirm PIP scope and design sign-off before committing capital.",
    32
  ),
  row(
    "overview.why_value",
    "Why Value Is Strongest",
    "Value concentrates where the property already has a genuine, personal ownership or local story, the physical product is boutique-scale and story-forward, and ownership wants Accor ALL distribution without losing individuality to a standardized prototype. Weakest fit is a generic asset with no personal narrative seeking only a recognizable flag.",
    33
  ),
  row(
    "overview.proof.1",
    "Personal, Story-Led Positioning",
    "Handwritten Collection markets itself around personal, individual hotel stories rather than a uniform design template—owners should treat this as a narrative and character bar to clear, not a construction spec, and budget accordingly rather than assuming a light PIP will suffice.",
    40
  ),
  row(
    "overview.proof.2",
    "Accor ALL Distribution",
    "Brand materials position Accor Live Limitless (ALL) loyalty participation and Accor's commercial systems as the core affiliation value—loyalty earn/redeem and distribution reach layered onto an independently branded stay. Confirm systems integration scope and commercial participation requirements directly with Accor for the specific asset.",
    41
  ),
  row(
    "overview.proof.3",
    "Boutique, Independent-Owner Scale",
    "Handwritten Collection skews toward smaller-scale, independently owned boutique hotels rather than larger full-service assets. Owners should benchmark comp sets, staffing intensity, and F&B complexity against comparable boutique independents, not larger MGallery-scale properties, when underwriting affiliation value or comparing capital plans.",
    42
  ),
  row(
    "overview.proof.4",
    "Design And Story Review As A Gate",
    "Collection acceptance and ongoing standards center on story authenticity and guest-experience quality rather than a fixed prototype checklist. Owners should expect qualitative review at conversion and periodically thereafter—confirm current review cadence and remediation expectations before underwriting affiliation value as permanent.",
    43
  ),
  row(
    "overview.featured_application",
    "Independent boutique conversion or affiliation",
    "An independently owned boutique hotel with a genuine personal or local story can use Handwritten Collection to gain Accor ALL distribution and commercial systems while keeping its own character intact. Owners should underwrite design/story review scope and Accor systems integration—confirming acceptance criteria directly rather than assuming a light reflag covers collection standards.",
    44,
    {
      caseSummaryOverview:
        "Featured path for independently owned boutique assets seeking Accor distribution under Handwritten Collection.",
      caseSummaryBrandRelevance:
        "Matches Handwritten's personal, story-led boutique lane—more intimate scale than MGallery within Accor's soft-collection family.",
      caseSummaryOwnerObjective:
        "Fund design/story review and Accor ALL systems integration without erasing the property's own personal narrative.",
      caseSummaryInterpretation:
        "Use as a conversion-fit lens—confirm acceptance criteria and agreement terms directly with Accor; not a performance forecast.",
      caseSummaryTags: "soft-brand, boutique, independent-owner, Accor, conversion",
    }
  ),
  row(
    "overview.differentiators.identity",
    "Experience & Identity",
    [
      "Personal, story-led boutique character—no shared design template",
      "Independently owned, smaller-scale properties within Accor's soft-collection family",
      "Story and character authenticity as the acceptance bar, not a construction spec",
      "Local ownership narrative preserved after Accor affiliation",
    ].join("\n"),
    45
  ),
  row(
    "overview.differentiators.commercial",
    "Commercial & Distribution",
    [
      "Accor Live Limitless (ALL) loyalty earn/redeem participation",
      "Accor global sales and commercial systems access",
      "Distribution reach without a standardized franchise prototype",
      "Confirm specific commercial participation terms directly with Accor",
    ].join("\n"),
    46
  ),
  row(
    "overview.bestAt.1",
    "Personal, Story-Led Boutique Character",
    "Protecting a genuine ownership or local story at boutique scale while adding Accor ALL reach—Handwritten's core value versus a standardized hard-brand conversion.",
    47
  ),
  row(
    "overview.bestAt.2",
    "Independent-Owner Fit",
    "Suiting smaller-scale, entrepreneurially owned boutique hotels where the story is the product—owners should benchmark to comparable independent-scale properties, not larger MGallery-tier assets.",
    48
  ),
  row(
    "overview.bestAt.3",
    "Accor Systems Without A Fixed Prototype",
    "Delivering Accor commercial infrastructure and ALL loyalty participation while preserving property-specific character—distinct from MGallery's broader, more established distinctive-hotel positioning.",
    49
  ),
  row(
    "overview.portfolio_context",
    "Portfolio Context",
    "Within Accor's soft/lifestyle family, Handwritten Collection is the more intimate, personal-story boutique lane—generally smaller in scale and narrower in owner profile than MGallery Collection's broader, more internationally established distinctive-hotel positioning. Owners should compare acceptance criteria, scale expectations, and F&B intensity across those two collections before selecting a path.",
    50
  ),
  row(
    "footprint.portfolio_context",
    "Portfolio Context",
    "Handwritten Collection anchors the personal, story-led boutique end of Accor's soft-collection family—more intimate in scale than MGallery Collection and structurally distinct from Accor's economy and midscale brands. Owners should weigh personal-narrative intensity against desired scale and F&B complexity before choosing between Handwritten and MGallery.",
    51
  ),
  row(
    "valueOwners.watchouts",
    "",
    [
      "Story and design review is qualitative, not a fixed checklist—budget time and capital for genuine narrative work",
      "Collection skews boutique-scale; benchmark to comparably sized independents, not larger MGallery-tier assets",
      "Confirm current acceptance criteria and remediation expectations directly—do not assume affiliation value is permanent",
      "Accor is the parent company—confirm this directly with Accor development rather than relying on any legacy internal reference to IHG",
    ].join("\n"),
    52
  ),

  // --- Value to Owners: lifecycle ---
  row(
    "valueOwners.lifecycle.1",
    "Evaluation",
    "Confirm the property already has—or can credibly build—a genuine personal or local story worth Handwritten Collection's boutique-character positioning, not whether it merely wants an Accor flag. Assess scale fit against MGallery alternatives, physical product condition, and Accor development interest before committing design capital.",
    300
  ),
  row(
    "valueOwners.lifecycle.2",
    "Conversion Design",
    "Shape conversion design around the property's own story—public spaces, guest rooms, and F&B should read as personal and distinctive, not templated. Sequence design-review milestones with financing and operator selection; treat acceptance-critical work as priority spend over decorative changes that do not improve guest-facing substance.",
    301
  ),
  row(
    "valueOwners.lifecycle.3",
    "Pre-Opening",
    "Coordinate Accor systems cutover, ALL loyalty integration, staffing, and training with design sign-off and opening readiness. Confirm owner versus operator responsibilities for commercial launch, and budget time for collection-specific story orientation rather than a generic hard-brand opening checklist.",
    302
  ),
  row(
    "valueOwners.lifecycle.4",
    "Opening",
    "Launch with consistent story-led service and personal presentation across every guest touchpoint. Opening support typically centers on guest-experience coherence and Accor systems stabilization rather than a standardized prototype punch list—confirm support scope and staffing coverage directly with Accor before launch week.",
    303
  ),
  row(
    "valueOwners.lifecycle.5",
    "Ramp-Up",
    "During ramp-up, calibrate rate positioning and programming against guest-review themes tied to the property's personal narrative, not only occupancy headlines. Watch labor intensity, F&B complexity, and Accor ALL channel contribution relative to the property's boutique scale before assuming steady-state performance matches opening-week momentum.",
    304
  ),
  row(
    "valueOwners.lifecycle.6",
    "Ongoing",
    "On an ongoing basis, refresh design and programming within collection guardrails and reassess affiliation value as the property, operator, and market evolve. Confirm renewal, review, and remediation expectations with Accor before major repositioning or operator transitions so collection participation remains intentional.",
    305
  ),

  // --- Operations & Standards: model ---
  row(
    "operations.model.primary_model",
    "",
    "Soft-brand affiliation within Accor's boutique lifestyle collection, delivered through management or franchise-style arrangements that owners must confirm for the specific market and asset.",
    100
  ),
  row(
    "operations.model.management_option",
    "",
    "Independent, owner-operated management is common at Handwritten's boutique scale; third-party operators must still preserve the property's personal storytelling and service culture.",
    101
  ),
  row(
    "operations.model.typical_ownership",
    "",
    "Entrepreneurial and independent owners of boutique hotels with a genuine personal or local story who want Accor ALL reach without converting into a standardized hard-brand prototype.",
    102
  ),
  row(
    "operations.model.brand_involvement",
    "",
    "Accor development and design/story review typically touch narrative, public-space design, and opening readiness. Confirm the current review process and touchpoint frequency directly.",
    103
  ),
  row(
    "operations.model.systems_integration",
    "",
    "Handwritten participates in Accor's ALL loyalty and commercial systems ecosystem. Owners should validate PMS/CRS cutover, training, and commercial systems requirements for the specific deal.",
    104
  ),
  row(
    "operations.model.pre_opening",
    "",
    "Expect design/story sign-off, training, and opening readiness work before soft opening. Sequence any PIP and operating setup with financing and operator capacity.",
    105
  ),
  row(
    "operations.model.staffing_intensity",
    "",
    "Boutique-scale staffing across front office, housekeeping, and F&B. Underwrite labor to the property's personal guest-experience narrative rather than a larger full-service prototype.",
    106
  ),
  row(
    "operations.model.fb_complexity",
    "",
    "F&B and public spaces often carry part of the property's personal story at boutique scale—complexity is generally lighter than MGallery's broader distinctive-hotel positioning but should still match the narrative.",
    107
  ),
  row(
    "operations.model.training",
    "",
    "Accor and Handwritten opening/service training should be confirmed as part of pre-opening planning. Budget time and cost against the agreement path and the property's specific story.",
    108
  ),
  row(
    "operations.model.reporting_discipline",
    "",
    "Accor reporting and commercial participation cadence typically apply. Confirm owner reporting expectations and system participation in diligence.",
    109
  ),
  row(
    "operations.model.qa_rhythm",
    "",
    "Story and guest-experience QA apply at opening and periodically thereafter. Confirm review cadence, scoring focus, and remediation expectations before treating affiliation as durable value.",
    110
  ),
  row(
    "operations.model.technology",
    "",
    "Accor technology and ALL participation are diligence items beyond the brand flag alone. Confirm systems, digital, and loyalty integration requirements for the asset.",
    111
  ),
  row(
    "operations.standards_philosophy",
    "",
    "Handwritten Collection standards protect individual property character while requiring a coherent, personal guest experience at boutique scale. Owners should underwrite to story, design, and service delivery together—not marketing language alone.\nDesign and conversion detail: Boutique conversion is strongest when the asset already has a personal story worth curating.\nLocalization: Local and ownership narrative are part of the premise, not optional decoration.\nPIP / capital: Confirm what must change for acceptance versus what can remain as distinctive character.",
    112
  ),
  row(
    "operations.operator_compat.summary",
    "",
    "Operators need to sustain a distinctive, personal boutique hotel identity inside Accor's Handwritten Collection—strong on story-telling, service warmth, and day-to-day coherence with the collection's positioning. Operators who default to prototype chain expression usually struggle.",
    113
  ),
  row(
    "operations.operator_compat.fit",
    "",
    "Best fit: operators experienced with boutique or independent hotels who can protect individuality at smaller scale. Weaker fit: prototype-driven operators seeking a soft flag without a property-specific personal story.",
    114
  ),
  row(
    "operations.operator_compat.tags",
    "",
    "Accor soft collection\nBoutique\nStory-led\nIndependent-owner scale",
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
    "Low",
    203
  ),
  row(
    "operations.flexibility.pip",
    "",
    "Moderate",
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
    "Story and guest-experience quality reviews typically intensify around onboarding, repositioning, and remediation. Owners should confirm cadence, scoring focus, and who owns corrective action plans before treating affiliation as durable value.",
    210
  ),
  row(
    "operations.compliance.training_rigor",
    "",
    "Accor and Handwritten orientation for service culture and boutique presentation—confirm depth for opening versus steady-state teams.",
    211
  ),
  row(
    "operations.compliance.reporting",
    "",
    "Accor commercial and affiliation reporting typically applies once a hotel participates in Handwritten Collection. Owners should validate required metrics, system participation, and owner versus operator reporting responsibilities for the specific property agreement.",
    212
  ),
  row(
    "operations.compliance.brand_interaction",
    "",
    "Curation and design touchpoints with collection brand teams—frequency varies by acceptance stage and renovation scope.",
    213
  ),

  // --- Economics & Obligations: opening path ---
  row(
    "economics.opening.step.1",
    "Application & Feasibility",
    "Begin membership dialogue with Accor using an honest asset profile—personal story, design identity, and operating track record. Confirm selective eligibility and whether Handwritten or MGallery fits the property's scale before detailed commercial modeling.",
    400
  ),
  row(
    "economics.opening.step.2",
    "Design & Standards",
    "Complete Handwritten design and story review with Accor—narrative, public spaces, guest rooms, and F&B concept should cohere into a personal, boutique-scale experience. Treat this as a story-led conversion phase, not a light cosmetic reflag.",
    401
  ),
  row(
    "economics.opening.step.3",
    "Pre-Opening Planning",
    "Build pre-opening budgets for hiring, training, Accor systems, FF&E, and opening marketing aligned with approved design. Confirm operator responsibilities, opening timeline, and milestone approvals with brand development and your advisors.",
    402
  ),
  row(
    "economics.opening.step.4",
    "Opening Support",
    "Coordinate soft opening, story/design QA, and Accor commercial launch support with the operator. Ensure guest-facing teams can deliver the intended personal experience while ALL loyalty and systems go live on schedule.",
    403
  ),
  row(
    "economics.opening.step.5",
    "Stabilization",
    "Stabilize operations with Accor commercial rhythm, ALL channel contribution, and guest-feedback loops. Use early performance to validate underwriting on labor, F&B, and capital intensity—not as a substitute for agreement-level economics review or confirmed affiliation terms.",
    404
  ),

  // --- Footprint & Growth ---
  row(
    "footprint.momentum",
    "Americas Boutique Conversion Emphasis",
    "Accor owner-facing materials describe Handwritten Collection as a growth path for independent boutique hotels, with early emphasis on Americas markets. Treat this as directional collection momentum rather than a property-level pipeline disclosure—confirm current activity directly with Accor development.",
    450
  ),
  row(
    "footprint.momentum",
    "Story-Led Independent Owner Interest",
    "Accor materials continue to position Handwritten Collection around personal, owner-led hotel stories as the core growth narrative. Owners with a genuine local or personal story should read this as directional interest in conversion pathways—not confirmation of specific incentives for any given market.",
    451
  ),
  row(
    "footprint.momentum",
    "Complement To MGallery Within Accor's Soft Portfolio",
    "Accor development materials describe Handwritten as a complement to MGallery within the broader soft/lifestyle portfolio—serving smaller-scale independent boutiques alongside MGallery's broader distinctive-hotel positioning. Confirm which collection fits your asset's scale directly with Accor development.",
    452
  ),
  row(
    "footprint.portfolio_mix",
    "Portfolio mix",
    "Independent boutique\nStory-led / personal narrative\nSmaller-scale full or limited-service\nConversion-led affiliation",
    460
  ),
  row(
    "footprint.geo_intro",
    "Geographic footprint",
    "Handwritten Collection has early emphasis in Americas markets as Accor brings independent boutique owners into the ALL ecosystem, with room to grow in other regions as the collection matures. Owners should underwrite mainstream Accor ALL commercial participation and story-review expectations—not assume uniform density across every region.",
    470
  ),
  row(
    "footprint.region.am",
    "Americas",
    "The Americas are the collection's early growth focus—independent boutique owners bringing personal stories into Accor's ALL ecosystem provide the clearest comp set for design-review expectations. Confirm local comps and development interest for the specific market.",
    471
  ),
  row(
    "footprint.region.cala",
    "CALA",
    "CALA representation is part of the collection's Americas-anchored growth story, with independent boutique and heritage properties as natural candidates. Owners should confirm authorized geography and story-review expectations locally rather than assuming U.S. comps translate directly.",
    472
  ),
  row(
    "footprint.region.eu",
    "Europe",
    "Europe contributes boutique and heritage-property reference points more through MGallery's established footprint than through Handwritten specifically. Americas owners can use these as design-narrative references without importing them as feasibility data for Handwritten.",
    473
  ),
  row(
    "footprint.region.mea",
    "MEA",
    "MEA exposure is market-specific and not a current focus area for Handwritten relative to Accor's broader brand portfolio. Confirm authorization and development interest directly rather than assuming interchangeable footprint with the Americas.",
    474
  ),
  row(
    "footprint.region.apac",
    "APAC",
    "APAC contributes to Accor's broader soft-collection recognition more through MGallery than Handwritten at this stage. For Americas deals, treat APAC as brand-recognition context only, not a feasibility or ramp-curve reference.",
    475
  ),
  row(
    "footprint.growth_themes",
    "",
    "Independent boutique conversion\nStory-led owner affiliation\nAmericas-anchored expansion\nComplement to MGallery within Accor's soft portfolio",
    480
  ),
  row(
    "footprint.growth_editorial",
    "",
    "Handwritten Collection compounds when independent owners bring a genuine personal story and Accor's ALL distribution amplifies it without erasing character. Named collection growth themes are directional context—still underwrite local comps, PIP, and agreement terms independently.",
    481
  ),
  row(
    "footprint.growth_fit",
    "",
    "Best growth fit: independent owners of boutique-scale hotels with a genuine personal or local story who want Accor ALL distribution. Weaker fit: larger full-service assets better suited to MGallery, or generic properties with no personal narrative.",
    482
  ),

  // --- Owner Considerations ---
  row(
    "standards.intro",
    "",
    "Handwritten Collection standards center on personal story authenticity and guest-experience quality at boutique scale rather than a fixed prototype. Confirm current standard detail and acceptance criteria directly with Accor development for the specific asset.",
    600
  ),
  row(
    "standards.requirement",
    "Story & design review",
    "Accor design/story review evaluates whether the property's personal narrative, design, and public spaces cohere into a distinctive boutique guest experience—confirm scope and timeline for your asset.",
    601
  ),
  row(
    "standards.requirement",
    "ALL systems participation",
    "PMS/CRS cutover, ALL loyalty integration, and commercial systems participation are typically required. Confirm technical scope and timeline with Accor development and your systems integrator.",
    602
  ),
  row(
    "standards.requirement",
    "F&B and public-space capital",
    "Outlet mix and public-space activation should match the property's boutique narrative and scale. Confirm expected capital intensity before assuming a light refresh will satisfy collection review.",
    603
  ),
  row(
    "standards.requirement",
    "Guest-room standards",
    "Guest rooms should reflect the property's specific personal story within Accor's baseline guest-experience expectations. Confirm design-review flexibility and any required baseline amenities for the asset.",
    604
  ),
  row(
    "standards.requirement",
    "Training and service culture",
    "Opening and ongoing training should reinforce both Accor service baselines and the property's own personal narrative. Confirm training scope, timing, and cost during pre-opening planning.",
    605
  ),
  row(
    "standards.requirement",
    "Ongoing story and QA review",
    "Story authenticity and guest-experience quality are reviewed periodically after opening, not only at conversion. Confirm current review cadence and remediation expectations before underwriting affiliation value as permanent.",
    606
  ),
  row(
    "standards.conversion",
    "",
    "Conversion suitability depends on whether the property's personal or local story and boutique-scale physical product can sustain Handwritten's guest-experience bar under Accor ALL systems—not whether the asset merely wants a recognizable flag. Confirm design-review scope and PIP intensity before committing capital.",
    607
  ),
  row(
    "standards.questions",
    "Questions owners should ask",
    [
      "What specific personal-story and design elements does Accor expect us to preserve or develop for Handwritten acceptance versus MGallery?",
      "What is the current design/story review timeline and remediation process if elements fall short?",
      "What Accor ALL systems, loyalty, and commercial participation requirements apply to this specific asset?",
      "How does F&B and public-space capital intensity compare between Handwritten and MGallery for a property like ours?",
      "What ongoing QA cadence and standards review should we expect after opening, and who owns corrective action?",
    ].join("\n"),
    608
  ),

  // --- Dealality Insight: similar brands ---
  row(
    "insight.similar.1",
    "MGallery Collection",
    "Accor soft-collection sibling with broader, more established distinctive-hotel positioning—compare scale expectations, F&B intensity, and acceptance criteria within the same parent.",
    700
  ),
  row(
    "insight.similar.2",
    "Autograph Collection",
    "Marriott soft-collection peer for independent-character hotels—compare affiliation model, design-review intensity, and loyalty participation outside Accor.",
    701
  ),
  row(
    "insight.similar.3",
    "Tribute Portfolio",
    "Marriott lifestyle-conversion soft-brand peer—compare story-led positioning, scale expectations, and commercial systems across parent companies.",
    702
  ),
];

export const BRAND_FULL_BUILD_CONTENT = Object.freeze({
  brandSlug: BRAND_SLUG,
  sourcePack: Object.freeze({
    canonicalSite: "handwrittencollection.accor.com",
    developmentPage:
      "group.accor.com — Brands & Experiences (Handwritten Collection); confirm current development-specific URL directly with Accor before use.",
    propertyPages:
      "Individual property pages live under handwrittencollection.accor.com and all.accor.com — use for story/design context per asset; do not embed raw property URLs in owner-facing Body copy.",
    parentContextPages: [
      "accor.com — Accor Group brand family context",
      "group.accor.com — Accor development and brand materials (collection growth themes only, no invented counts)",
    ],
    imageSources:
      "handwrittencollection.accor.com and all.accor.com brand/property galleries; use for story/character reference only — confirm licensing before any external use.",
    domains: ["accor.com", "handwrittencollection.accor.com", "group.accor.com", "all.accor.com"],
  }),
  brandLens: Object.freeze({
    brandModel:
      "Accor soft/lifestyle collection — personal, story-led boutique hotels at smaller scale than MGallery, each keeping its own local or ownership narrative while participating in Accor ALL and commercial systems.",
    ownerFit:
      "Independent, entrepreneurial owners of boutique-scale hotels with a genuine personal or local story who want Accor ALL distribution without converting to a standardized prototype.",
    propertyFit:
      "Independently owned boutique hotels, generally smaller in scale than MGallery-tier properties, with a credible personal or local narrative; conversion-led affiliation more than new-build.",
    conversionLogic:
      "Story and design review gates acceptance, not a fixed room-and-corridor spec—capital should protect and amplify the property's personal narrative, with PIP scope confirmed directly rather than assumed minimal.",
    operatingImplications:
      "Boutique-scale staffing and F&B/public-space intensity scaled to the property's narrative; Accor systems, ALL loyalty, and reporting participation apply regardless of design flexibility.",
    standardsRequirements:
      "Story authenticity, design coherence, and guest-experience quality reviewed at conversion and periodically thereafter; confirm current acceptance criteria and remediation expectations directly with Accor development.",
    sourceLimitations:
      "Public brand materials describe collection positioning and growth themes only—no property-level counts, fees, or performance data. Parent company is Accor; confirm agreement-specific terms directly with Accor development.",
    distinguishFrom:
      "MGallery Collection (Accor's broader, more established, and larger-scale soft/lifestyle collection with more international distinctive-hotel positioning than Handwritten's boutique, personal-story focus).",
  }),
  presentation: PRESENTATION,
});

export default BRAND_FULL_BUILD_CONTENT;
