import {
  buildRecentMomentumCard,
  RECENT_MOMENTUM_DEFAULT_LABEL,
  withRecentMomentumSortOrder,
} from "./brand-explorer-recent-momentum-contract.js";

/**
 * Brand Explorer Tab Factory — full build content pack: Trademark Collection by Wyndham.
 *
 * Wyndham Hotels & Resorts' soft-brand collection for independent and distinctive
 * hotels — an accessible conversion path that preserves each property's own character
 * while adding Wyndham Rewards distribution and commercial systems. Distinguished
 * from Dazzler by Wyndham's defined lifestyle design template and from La Quinta
 * and other Wyndham core prototype flags.
 *
 * Factory status: queued for Tab Factory build, "Under Review" while in the factory
 * (see brand-explorer-wyndham-factory-build-queue.js). Not Active/Live; not public-full.
 *
 * Copy rules:
 * - Directional, owner-facing. No invented fees, ADR, FDD, Item 19, pipeline counts,
 *   or performance guarantees.
 * - Brand-specific — distinguishes Trademark Collection from Dazzler by Wyndham,
 *   La Quinta, and core Wyndham midscale flags.
 * - No Company Validated claims.
 * - No raw https:// URLs in any Body field (PVQL fails on raw URLs in owner-facing copy).
 * - Loyalty slots reference Wyndham Rewards directionally only; no invented point or
 *   night thresholds — elite ladder labels are conservative and flagged to confirm.
 */

const BRAND_SLUG = "trademark-collection-by-wyndham";
const BRAND_NAME = "Trademark Collection by Wyndham";
const PARENT_COMPANY = "Wyndham Hotels & Resorts";
const LOYALTY_PROGRAM = "Wyndham Rewards";
const RECORD_ID = "recob7tgHRryRSbeO";

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

const PRESENTATION = [
  // --- Positioning (Basics-backed slots) ---
  row(
    "Brand Positioning",
    "",
    "Trademark Collection by Wyndham is Wyndham Hotels & Resorts' soft-brand collection for independent and distinctive hotels — an accessible conversion path that preserves each property's own identity while adding Wyndham Rewards distribution and commercial systems. It sits apart from Dazzler's defined lifestyle design template and from La Quinta and other Wyndham core prototype flags.",
    10
  ),
  row(
    "Guest Psychographics Description",
    "",
    "Travelers seeking a distinctive independent hotel experience with local character and property-specific personality — guests who value individuality over a standardized prototype stay, while still wanting Wyndham Rewards participation and recognizable distribution support.",
    11
  ),

  // --- Overview ---
  row(
    "overview.typical_use_case",
    "",
    "Independent hotels, boutique conversions, and distinctive existing assets that want Wyndham platform benefits without adopting a fixed brand prototype. The strongest candidates already have a credible independent identity, service culture, and local market position.",
    20
  ),
  row(
    "overview.development_model",
    "",
    "Conversion-led affiliation is the dominant path — existing independent or lightly branded hotels with a genuine local story repositioned under Trademark Collection's more accessible design-review bar. Underwrite the required product, systems, and service work from the specific agreement and review process rather than assuming a light reflag.",
    21
  ),
  row(
    "overview.relative_positioning",
    "Relative Positioning",
    "Trademark Collection is Wyndham's independent-hotel soft-brand path, offering more property-specific design flexibility than Dazzler's defined lifestyle template and more than core Wyndham midscale flags such as La Quinta. It remains subject to applicable brand standards and is not a no-standards affiliation.",
    22
  ),
  row(
    "overview.scenario.1",
    "Independent Hotel Conversion",
    "An established independent hotel with a genuine local story, strong location, and service culture that wants broader distribution and Wyndham Rewards participation. Trademark Collection is worth evaluating when ownership wants to preserve property identity rather than convert to a core Wyndham flag, and when the asset can support the collection's applicable guest-experience expectations.",
    30
  ),
  row(
    "overview.scenario.2",
    "Boutique Or Historic Property Repositioning",
    "A boutique or historic hotel needing repositioning capital and a distribution platform, where a fixed brand prototype would erase the asset's differentiating character. Confirm the scope of required improvements, review milestones, and systems participation with Wyndham development before committing capital to a Trademark Collection conversion path.",
    31
  ),
  row(
    "overview.scenario.3",
    "Secondary-Market Independent Repositioning",
    "An independent hotel in a secondary or gateway market where local character is commercially important and owner control over positioning matters. Trademark Collection may fit when the property needs distribution support without losing its independent narrative — test local comp positioning, operator capability, and systems readiness before proceeding.",
    32
  ),
  row(
    "overview.why_value",
    "Why Value Is Strongest",
    [
      "Strongest where the hotel already has a credible independent story and ownership values property-specific positioning over a standardized Wyndham prototype.",
      "Wyndham Rewards distribution and commercial systems can expand reach when the asset's identity, product condition, and service culture can support collection expectations.",
      "Design-review scope is generally more accessible than a fixed prototype flag — still underwrite improvement capital and standards diligence before treating conversion as cosmetic.",
      "Compare Trademark Collection against Dazzler and core Wyndham midscale flags on how much independent narrative the asset can defend after systems cutover.",
      "Weaker for generic assets seeking only a familiar sign, or for properties unable to support applicable Trademark Collection standards, systems, and guest-experience requirements.",
    ].join("\n"),
    33
  ),
  row(
    "overview.proof.1",
    "Individual Character, Not A Prototype",
    "Trademark Collection is designed for properties that retain their own identity while participating in the Wyndham platform. Owners should view this as a balance of independence and standards: the property's story remains central, but the experience must still support the collection's guest-experience expectations and applicable brand requirements.",
    40
  ),
  row(
    "overview.proof.2",
    "Wyndham Rewards Distribution",
    "Brand materials position Wyndham Rewards participation and Wyndham's commercial systems as core affiliation value — loyalty earn and redeem plus distribution reach layered onto an independently branded stay. Confirm systems integration scope, training, and commercial participation directly with Wyndham development for the specific asset.",
    41
  ),
  row(
    "overview.proof.3",
    "Accessible Design Review For Conversions",
    "Trademark Collection's design-review bar is generally more accessible than a fixed prototype flag, making it a practical conversion path for independent owners. Confirm current acceptance criteria, improvement expectations, and residual standards directly rather than assuming a minimal-review conversion applies.",
    42
  ),
  row(
    "overview.proof.4",
    "Design Review As A Gate, Not A Checklist",
    "The collection model can preserve a property's individual expression, but acceptance and ongoing participation still depend on meeting applicable Wyndham requirements. Confirm the current review criteria, improvement expectations, and operating obligations for the asset instead of assuming independent positioning eliminates brand discipline.",
    43
  ),
  row(
    "overview.featured_application",
    "Independent hotel conversion preserving property identity",
    "A differentiated independent hotel can use Trademark Collection by Wyndham to retain its own identity while adding Wyndham Rewards loyalty and commercial infrastructure. The owner case depends on whether the property's story, product condition, and operating capability can support the collection's expectations — not solely on brand recognition.",
    44,
    {
      caseSummaryOverview:
        "Featured path for independent hotels seeking Wyndham platform participation without a fixed core-brand prototype.",
      caseSummaryBrandRelevance:
        "Trademark Collection provides a more property-specific, independent-character lane than Dazzler or core Wyndham midscale flags.",
      caseSummaryOwnerObjective:
        "Protect property identity while funding the systems and service work needed for conversion.",
      caseSummaryInterpretation:
        "Use as a conversion-fit lens; confirm requirements, territory, and commercial terms directly with Wyndham.",
      caseSummaryTags: "soft-brand, independent, conversion, Wyndham",
    }
  ),
  row(
    "overview.differentiators.identity",
    "Experience & Identity",
    [
      "Independent hotel identity remains prominent",
      "Conversion-friendly, more accessible design-review bar",
      "Local character can shape the guest experience",
      "More property-specific flexibility than Dazzler or core Wyndham midscale flags",
    ].join("\n"),
    45
  ),
  row(
    "overview.differentiators.commercial",
    "Commercial & Distribution",
    [
      "Wyndham Rewards participation and Wyndham commercial ecosystem",
      "Reservation and distribution infrastructure for affiliated hotels",
      "Platform benefits without a fixed core-brand prototype",
      "Confirm agreement-specific commercial and systems obligations directly",
    ].join("\n"),
    46
  ),
  row(
    "overview.bestAt.1",
    "Independent Hotel Character At Scale",
    "Supporting independent hotels that need Wyndham platform distribution without adopting a standardized prototype, while preserving local design and service expression.",
    47
  ),
  row(
    "overview.bestAt.2",
    "Broad Segment Range Under One Collection",
    "Giving owners a conversion path that can retain property identity and operating choice, subject to the collection's applicable standards, systems, and quality-review obligations.",
    48
  ),
  row(
    "overview.bestAt.3",
    "Wyndham Systems Without A Fixed Prototype",
    "Layering Wyndham Rewards, distribution, and commercial infrastructure beneath an independently positioned hotel rather than recasting the asset as a standardized prototype.",
    49
  ),
  row(
    "overview.portfolio_context",
    "Portfolio Context",
    "Within Wyndham's independent-hotel offering, Trademark Collection is the collection built for property-specific flexibility, distinct from Dazzler's defined lifestyle design template and from core Wyndham midscale flags such as La Quinta. Owners should compare product readiness, desired guest experience, and degree of independent expression before selecting a path.",
    50
  ),
  row(
    "footprint.portfolio_context",
    "Portfolio Context",
    "Trademark Collection represents Wyndham's independent-hotel soft-brand lane. It should be assessed separately from Dazzler's design-led lifestyle positioning and from Wyndham's core prototype flags, which offer less property-specific expression within the same broader platform.",
    51
  ),
  row(
    "valueOwners.overview",
    "What Owners Are Buying",
    "Trademark Collection by Wyndham gives owners of differentiated independent hotels a Wyndham soft-brand path that pairs property-specific identity with Wyndham Rewards, distribution, and commercial infrastructure. The owner proposition is independent-hotel flexibility — subject to product, systems, and quality obligations confirmed for the specific asset.",
    51
  ),
  row(
    "valueOwners.watchouts",
    "",
    [
      "Trademark Collection positioning requires more than an independent nameplate; test product and service readiness honestly",
      "Compare Trademark Collection, Dazzler, and core Wyndham flags against the asset's actual segment, capital plan, and market comp set",
      "Confirm current acceptance, improvement, systems, and quality-review requirements for the specific deal",
      "Do not assume owner control removes the need for operating discipline inside the Wyndham platform",
    ].join("\n"),
    52
  ),

  // --- Value to Owners: lifecycle ---
  row(
    "valueOwners.lifecycle.1",
    "Evaluation",
    "Start with the asset's existing identity, physical condition, target guest, and local comp set. Determine whether the hotel can credibly support Trademark Collection's expectations and whether it offers a better owner-control and distribution balance than Dazzler or a core Wyndham brand.",
    300
  ),
  row(
    "valueOwners.lifecycle.2",
    "Conversion Design",
    "Translate the property's independent story into visible guest experience: arrival, rooms, public spaces, F&B, and service standards should align. Prioritize work that closes genuine product and operating gaps, and sequence Wyndham review milestones with financing, design, and operator decisions.",
    301
  ),
  row(
    "valueOwners.lifecycle.3",
    "Pre-Opening",
    "Coordinate systems integration, Wyndham Rewards readiness, staffing, sales setup, and training with conversion completion. Establish clear owner, operator, and Wyndham responsibilities for the launch, and avoid treating the collection affiliation as a substitute for disciplined pre-opening planning.",
    302
  ),
  row(
    "valueOwners.lifecycle.4",
    "Opening",
    "Launch the hotel with its own identity consistently expressed across service, digital channels, and public spaces while Wyndham systems stabilize. Confirm opening support, quality review, and escalation paths before launch so the operator can resolve issues without diluting the property's positioning.",
    303
  ),
  row(
    "valueOwners.lifecycle.5",
    "Ramp-Up",
    "Use early guest feedback, channel mix, and operational performance to refine the independent experience and local positioning. Watch whether staffing, amenity delivery, and public-space programming actually support the intended guest promise, rather than relying only on affiliation visibility.",
    304
  ),
  row(
    "valueOwners.lifecycle.6",
    "Ongoing",
    "Maintain the asset's differentiated character while meeting applicable Wyndham quality, systems, and commercial obligations. Revisit capital needs, operator alignment, and market positioning as the hotel matures, and clarify review or remediation expectations before major changes or renewals.",
    305
  ),

  // --- Operations & Standards: model ---
  row(
    "operations.model.primary_model",
    "",
    "Independent-hotel soft-brand collection affiliation within Wyndham Hotels & Resorts, structured through the agreement path available for the relevant market and property. Owners should confirm whether franchise, management, or another arrangement is applicable to the specific opportunity.",
    100
  ),
  row(
    "operations.model.management_option",
    "",
    "Third-party management can suit independent assets when the operator can protect property character while working within Wyndham systems. Owner-operated models require credible leadership, service discipline, and capacity to meet collection expectations.",
    101
  ),
  row(
    "operations.model.typical_ownership",
    "",
    "Owners of differentiated independent hotels who want a Wyndham collection position, platform support, and continued control over the property's local identity.",
    102
  ),
  row(
    "operations.model.brand_involvement",
    "",
    "Wyndham development and brand teams may engage on conversion readiness, product presentation, systems, and quality expectations. Confirm the current review stages, documentation, and interaction cadence for the individual asset.",
    103
  ),
  row(
    "operations.model.systems_integration",
    "",
    "Trademark Collection hotels participate in the relevant Wyndham reservation, Wyndham Rewards, distribution, and technology ecosystem. Validate PMS, CRS, training, digital, and commercial integration requirements before committing to a conversion timeline.",
    104
  ),
  row(
    "operations.model.pre_opening",
    "",
    "Expect product readiness, systems setup, team training, and commercial-launch work before opening or relaunch. Sequence these requirements with financing and construction so operational readiness does not become the critical path.",
    105
  ),
  row(
    "operations.model.staffing_intensity",
    "",
    "Staffing should match the property's intended independent guest experience, not a minimal core-brand operating model. Front office, housekeeping, service recovery, and any food-and-beverage offer should be underwritten to the hotel's intended positioning.",
    106
  ),
  row(
    "operations.model.fb_complexity",
    "",
    "F&B complexity varies by property, but public spaces and dining can materially shape the hotel's independent identity. Review outlet concept, service hours, kitchen scope, and operator capability against local demand rather than applying a uniform collection assumption.",
    107
  ),
  row(
    "operations.model.training",
    "",
    "Training should connect Wyndham platform expectations with the hotel's own service and design identity. Confirm required modules, timing, delivery responsibilities, and ongoing refresh expectations as part of the pre-opening plan.",
    108
  ),
  row(
    "operations.model.reporting_discipline",
    "",
    "Wyndham systems and commercial participation create reporting and operating rhythms that owners should understand during diligence. Confirm available owner reporting, data access, and operator responsibilities for the particular agreement.",
    109
  ),
  row(
    "operations.model.qa_rhythm",
    "",
    "Quality and brand review support the collection's guest-experience promise at conversion and during operations. Confirm the current cadence, review focus, corrective-action process, and responsibility for remediation before relying on affiliation value in underwriting.",
    110
  ),
  row(
    "operations.model.technology",
    "",
    "Technology participation should be reviewed as a conversion workstream, not an afterthought. Validate required Wyndham systems, connectivity, digital distribution, Wyndham Rewards integration, implementation support, and any asset-specific constraints.",
    111
  ),
  row(
    "operations.standards_philosophy",
    "",
    "Trademark Collection seeks an independent-hotel experience supported by Wyndham platform standards rather than a single prototype.\nDesign and conversion detail: each property should retain its own story while meeting applicable quality expectations.\nPIP / lifecycle capital: establish scope from the actual asset review; do not presume a cosmetic refresh is sufficient.\nSegment fit: compare the hotel with relevant independent competitors and Dazzler, not with all Wyndham brands.",
    112
  ),
  row(
    "operations.operator_compat.summary",
    "",
    "Operators need to deliver a genuine independent guest experience while maintaining Wyndham systems, Wyndham Rewards, commercial, and quality obligations. The operator must protect local character without treating the collection as operationally unstructured.",
    113
  ),
  row(
    "operations.operator_compat.fit",
    "",
    "Best fit: operators experienced in independent or boutique hotels with strong service, revenue, and systems discipline. Weaker fit: operators optimized only for standardized prototype models or unable to sustain a differentiated property identity.",
    114
  ),
  row(
    "operations.operator_compat.tags",
    "",
    "Wyndham soft-brand\nIndependent character\nAccessible conversion path\nProperty-specific design",
    115
  ),

  // --- Operations & Standards: flexibility indicators ---
  row("operations.flexibility.design", "", "High", 200),
  row("operations.flexibility.conversion", "", "Very high", 201),
  row("operations.flexibility.localization", "", "High", 202),
  row("operations.flexibility.operational_rigidity", "", "Low", 203),
  row("operations.flexibility.pip", "", "Moderate", 204),
  row("operations.flexibility.prototype", "", "Low", 205),

  // --- Operations & Standards: compliance ---
  row(
    "operations.compliance.qa_cadence",
    "",
    "Quality review may be most important at conversion, opening, and when a property needs remediation. Confirm current review timing, standards emphasis, scoring, and escalation procedures directly for the proposed affiliation.",
    210
  ),
  row(
    "operations.compliance.training_rigor",
    "",
    "Training should prepare teams for both Wyndham platform participation and a differentiated independent guest experience. Define who trains whom, how readiness is assessed, and what refresh work is expected after opening.",
    211
  ),
  row(
    "operations.compliance.reporting",
    "",
    "Owners should clarify Wyndham reporting, Wyndham Rewards, distribution, and revenue-management obligations alongside the operator's reporting role. Agreement-level expectations may vary and should not be inferred from the collection label alone.",
    212
  ),
  row(
    "operations.compliance.brand_interaction",
    "",
    "Brand interaction typically centers on development, conversion, systems, quality, and commercial readiness. Establish a practical calendar of decision points and contacts so owner, operator, and Wyndham teams can resolve issues before they affect launch or guest experience.",
    213
  ),

  // --- Economics & Obligations: opening path ---
  row(
    "economics.opening.step.1",
    "Application & Feasibility",
    "Present the asset's market context, ownership objectives, product condition, and independent identity for Wyndham development review. Test whether Trademark Collection's positioning is credible for the property, and compare the path with Dazzler or a core Wyndham flag before allocating detailed conversion capital.",
    400
  ),
  row(
    "economics.opening.step.2",
    "Design & Standards",
    "Align design, guest rooms, public spaces, amenities, service model, and technology with the property's intended positioning and applicable Wyndham requirements. Treat this as an asset-specific conversion plan rather than a generic flag-change checklist.",
    401
  ),
  row(
    "economics.opening.step.3",
    "Pre-Opening Planning",
    "Build the implementation plan around systems, Wyndham Rewards readiness, training, staffing, sales, marketing, and operating procedures. Clarify the responsibilities of owner, operator, vendors, and Wyndham, and tie critical milestones to construction completion and commercial launch.",
    402
  ),
  row(
    "economics.opening.step.4",
    "Opening Support",
    "Coordinate relaunch communications, systems go-live, quality readiness, and guest-service recovery plans with the operator and Wyndham contacts. Keep the property's independent story prominent while ensuring the platform components work reliably from the first guest stay.",
    403
  ),
  row(
    "economics.opening.step.5",
    "Stabilization",
    "Use the stabilized operating period to refine service, channel strategy, and local positioning against actual guest feedback and market response. Reassess capital, staffing, and operating assumptions through the asset's performance, not as a substitute for agreement-level diligence.",
    404
  ),

  // --- Footprint & Growth ---
  row("footprint.momentum_label", "", RECENT_MOMENTUM_DEFAULT_LABEL, 449),
  ...withRecentMomentumSortOrder([
    buildRecentMomentumCard({
      title: "MB Hotel Miami Beach Trademark Collection affiliation signal",
      dateLine: "2024",
      summary:
        "MB Hotel in Miami Beach shows Trademark Collection's independent-character soft-brand path for owners comparing Wyndham Rewards distribution while retaining a property-specific identity rather than converting to a core Wyndham prototype flag.",
      url: "https://www.wyndhamhotels.com/trademark/miami-beach-florida/mb-hotel-trademark-collection-by-wyndham/overview",
      sort: 1,
    }),
    buildRecentMomentumCard({
      title: "Chula Vista Resort Wisconsin Dells Trademark Collection reference",
      dateLine: "2023",
      summary:
        "Chula Vista Resort illustrates Trademark Collection's destination-leisure conversion fit for owners underwriting property-specific guest experiences alongside Wyndham commercial systems and Rewards participation.",
      url: "https://www.wyndhamhotels.com/trademark/wisconsin-dells-wisconsin/chula-vista-resort-trademark/overview",
      sort: 2,
    }),
    buildRecentMomentumCard({
      title: "The Walden Pigeon Forge Trademark Collection leisure signal",
      dateLine: "2022",
      summary:
        "The Walden in Pigeon Forge is a leisure-market Trademark Collection reference for owners evaluating independent identity retention, design-review scope, and Wyndham platform access on a destination asset.",
      url: "https://www.wyndhamhotels.com/trademark/pigeon-forge-tennessee/the-walden-trademark-collection/overview",
      sort: 3,
    }),
  ]).map((c) => row("footprint.momentum", c.title, c.body, 449 + c.sort)),
  row(
    "footprint.portfolio_mix",
    "Portfolio mix",
    "Independent hotel conversions\nBoutique / secondary-market hotels\nHistoric building repositioning\nProperty-specific design retention",
    460
  ),
  row(
    "footprint.geo_intro",
    "Geographic footprint",
    "Trademark Collection by Wyndham serves as a soft-brand option across Wyndham's broader operating footprint, with actual availability and depth varying by market. Owners should confirm local development focus, comparable properties, and distribution relevance rather than inferring fit from global Wyndham brand presence.",
    470
  ),
  row(
    "footprint.region.am",
    "Americas",
    "Americas markets provide a useful reference point for Wyndham's independent-hotel platform, particularly where owners seek recognizable distribution without a full prototype conversion. Confirm local Trademark Collection presence, segment gaps, and development interest for the target market.",
    471
  ),
  row(
    "footprint.region.cala",
    "CALA",
    "CALA opportunities should be evaluated with market-specific Wyndham development input, not broad global assumptions. A differentiated independent hotel may be relevant where platform distribution and owner-controlled identity align, subject to local authorization and conversion requirements.",
    472
  ),
  row(
    "footprint.region.eu",
    "Europe",
    "European independent-hotel markets can illustrate the collection logic of local character supported by broader distribution. For a specific deal, test country-level systems, development, and operating considerations rather than applying another market's conversion experience.",
    473
  ),
  row(
    "footprint.region.mea",
    "MEA",
    "MEA relevance is market-specific and should be confirmed directly with Wyndham for the proposed country and asset. Treat wider Wyndham presence as context, not evidence that the Trademark Collection path is available or commercially equivalent in every market.",
    474
  ),
  row(
    "footprint.region.apac",
    "APAC",
    "APAC can provide international brand-recognition context for travelers, but property-level feasibility remains dependent on local Wyndham development strategy and systems support. Do not use global collection presence as a proxy for an individual market's conversion case.",
    475
  ),
  row(
    "footprint.growth_themes",
    "",
    "Independent hotel conversions\nBoutique and secondary-market repositioning\nHistoric building reuse\nComplement to Dazzler within Wyndham's upscale and independent offering",
    480
  ),
  row(
    "footprint.growth_editorial",
    "",
    "Trademark Collection compounds when independent owners bring a genuine local story and Wyndham Rewards distribution amplifies it without erasing the property's identity. Named collection growth themes are directional context — still underwrite local comps, PIP, and agreement terms independently.",
    481
  ),
  row(
    "footprint.growth_fit",
    "",
    "Best growth fit: owners of independent or lightly branded hotels with a genuine local story who want Wyndham platform distribution without adopting a fixed prototype. Weaker fit: owners seeking a defined design template like Dazzler, or generic properties with no independent character.",
    482
  ),

  // --- Owner Considerations ---
  row(
    "standards.intro",
    "",
    "Trademark Collection standards should support an independent, differentiated guest experience alongside Wyndham platform participation. Current acceptance, product, technology, training, and quality details must be confirmed directly with Wyndham for the specific asset and market.",
    600
  ),
  row(
    "standards.requirement",
    "Design & narrative review",
    reqBody({
      typical:
        "The property should present a credible independent experience through rooms, public spaces, arrival, and overall design. Confirm how Wyndham evaluates existing conditions, proposed improvements, and collection-level differentiation for the asset.",
      owner:
        "Plan design-review scope, timeline, and any narrative remediation into conversion capital before underwriting acceptance.",
      status: "Typically Expected",
      notes: "Confirm scope and timeline for your asset with Wyndham development.",
    }),
    601
  ),
  row(
    "standards.requirement",
    "Wyndham Rewards systems participation",
    reqBody({
      typical:
        "Reservation, Wyndham Rewards loyalty, distribution, and related platform systems may form part of the affiliation.",
      owner:
        "Confirm required technology, implementation sequencing, training, and responsibilities with Wyndham and the selected operator.",
      status: "Typically Expected",
      notes: "Confirm technical scope and timeline with Wyndham development and your systems integrator.",
    }),
    602
  ),
  row(
    "standards.requirement",
    "F&B and public-space capital",
    reqBody({
      typical:
        "Public spaces, amenities, and any F&B offer should support the hotel's intended independent identity and local guest demand.",
      owner: "Establish required versus elective improvements before finalizing a conversion budget.",
      status: "May Apply",
      notes: "Confirm expected capital intensity for the specific asset with Wyndham development.",
    }),
    603
  ),
  row(
    "standards.requirement",
    "Guest-room and suite standards",
    reqBody({
      typical:
        "Guest rooms and suites should align with the property's differentiated concept while meeting applicable collection expectations.",
      owner:
        "Validate room-product gaps, accessibility work, amenity requirements, and design flexibility as part of diligence.",
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
        "Team training should connect Wyndham platform participation with the property's own service promise.",
      owner: "Define the service standards, onboarding sequence, and ownership of ongoing coaching before opening or relaunch.",
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
        "Ongoing quality expectations preserve the collection's guest-experience positioning after conversion.",
      owner:
        "Underwrite remediation risk and owner versus operator responsibilities for ongoing QA before treating affiliation value as permanent.",
      status: "Typically Expected",
      notes: "Confirm review timing, performance thresholds, and remediation process with Wyndham development.",
    }),
    606
  ),
  row(
    "standards.conversion",
    "",
    "Conversion suitability depends on the hotel's ability to deliver an independent guest experience within Wyndham systems, not merely its desire for a recognizable platform. Compare Trademark Collection and Dazzler carefully, and establish the actual product, service, and technology scope before committing capital.",
    607
  ),
  row(
    "standards.questions",
    "Questions owners should ask",
    [
      "What product, design, and service characteristics distinguish Trademark Collection from Dazzler or a core Wyndham flag for a hotel like ours?",
      "Which improvements are required before conversion, and how are they prioritized and reviewed?",
      "What Wyndham Rewards, reservation, technology, and commercial systems must the property implement?",
      "How much independent design and operating control remains after affiliation?",
      "What quality-review cadence, remediation process, and owner or operator responsibilities apply after opening?",
    ].join("\n"),
    608
  ),

  // --- Dealality Insight: similar brands ---
  row(
    "insight.similar.1",
    "Ascend Hotel Collection",
    "Choice Hotels' soft-brand peer for independent hotels — compare design-review flexibility, loyalty platform scale, and conversion capital intensity outside Wyndham.",
    700
  ),
  row(
    "insight.similar.2",
    "BW Premier Collection",
    "BWH Hotels' soft-brand peer for independent hotels — compare acceptance bar, design-intensity expectations, and conversion capital scope against Trademark Collection.",
    701
  ),
  row(
    "insight.similar.3",
    "Dazzler by Wyndham",
    "Sibling Wyndham brand with a defined lifestyle design template rather than Trademark Collection's independent-hotel flexibility — compare desired level of property-specific expression before choosing a path.",
    702
  ),

  // --- Wyndham Rewards loyalty context (directional; confirm current thresholds) ---
  row(
    "loyalty.hero_title",
    "",
    `${BRAND_NAME} · ${LOYALTY_PROGRAM} — loyalty context at a glance`,
    750
  ),
  row(
    "loyalty.ecosystem",
    "",
    "Wyndham Rewards is Wyndham Hotels & Resorts' loyalty program, spanning the company's economy-through-upscale portfolio, including Trademark Collection by Wyndham. It provides points earning, redemption, and member recognition across participating Wyndham-affiliated hotels. Confirm current program structure and participating-brand detail directly with Wyndham.",
    751
  ),
  row(
    "loyalty.owner_lens",
    "",
    "Model Wyndham Rewards on a net economics basis after member discounts, elite benefits, and program participation costs — not headline distribution reach alone. Confirm Trademark Collection-specific loyalty booking mix, program fees, and net revenue impact in your development materials with the brand rather than assuming a uniform figure across the Wyndham portfolio.",
    752
  ),
  row(
    "loyalty.earn",
    "",
    [
      "Points earned on eligible paid stays at participating Wyndham-affiliated hotels, including Trademark Collection by Wyndham",
      "Potential bonus-earning promotions and partner-earning options — confirm current terms directly with Wyndham",
      "Earning mechanics and rates are set at the program level and can change — do not assume figures from other Wyndham brands apply identically",
    ].join("\n"),
    753
  ),
  row(
    "loyalty.redeem",
    "",
    [
      "Points redeemable for free-night stays and other program rewards at participating Wyndham-affiliated hotels",
      "Redemption value and award-night availability vary by property, date, and program terms",
      "Confirm current redemption structure, blackout considerations, and any collection-specific limitations directly with Wyndham",
    ].join("\n"),
    754
  ),
  row(
    "loyalty.elite",
    "Member",
    "Base enrollment tier for Wyndham Rewards members. Confirm current qualification criteria, benefits, and any updates to this tier directly with Wyndham — this label is directional and not a confirmed program specification.",
    755
  ),
  row(
    "loyalty.elite",
    "Blue",
    "A mid-tier level within Wyndham Rewards' elite structure as publicly described. Confirm current qualification thresholds (stays/nights/points) and associated benefits directly with Wyndham before using in owner underwriting.",
    756
  ),
  row(
    "loyalty.elite",
    "Gold",
    "A higher elite level within Wyndham Rewards as publicly described, generally associated with enhanced recognition benefits. Confirm current qualification thresholds and benefit detail directly with Wyndham.",
    757
  ),
  row(
    "loyalty.elite",
    "Platinum",
    "The upper elite level within Wyndham Rewards as publicly described, generally associated with the program's top recognition benefits. Confirm current qualification thresholds, benefit detail, and any additional tiers directly with Wyndham — do not assume this is the program's final or only top-tier structure without confirmation.",
    758
  ),
];

export const BRAND_FULL_BUILD_CONTENT = Object.freeze({
  brandSlug: BRAND_SLUG,
  sourcePack: Object.freeze({
    canonicalSite: "wyndhamhotels.com — Trademark Collection by Wyndham brand context",
    developmentPage: "wyndhamhotels.com/develop — Wyndham Hotels & Resorts development and brand context",
    propertyPages:
      "Individual Trademark Collection by Wyndham property pages on wyndhamhotels.com; use for property-specific design and location context, not as universal requirements.",
    parentContextPages: [
      "wyndhamhotels.com — Wyndham Hotels & Resorts brand family context",
      "wyndhamhotels.com/develop — Wyndham development and brand materials (collection growth themes only, no invented counts)",
    ],
    imageSources:
      "wyndhamhotels.com brand and property galleries; confirm licensing before external use.",
    domains: ["wyndhamhotels.com"],
  }),
  brandLens: Object.freeze({
    brandModel:
      "Wyndham Hotels & Resorts independent-hotel soft-brand collection — property-specific identity paired with Wyndham Rewards distribution and commercial systems, without a fixed core-brand prototype.",
    ownerFit:
      "Owners of differentiated independent hotels who value local identity, distribution support, Wyndham Rewards participation, and a conversion path with more flexibility than a conventional hard brand.",
    propertyFit:
      "Independent hotels, boutique conversions, and historic repositionings with a credible local story, product condition, and market positioning.",
    conversionLogic:
      "Trademark Collection offers more property-specific design flexibility than Dazzler's defined lifestyle template or core Wyndham prototype flags; determine capital, systems, and quality requirements from the asset-specific review.",
    operatingImplications:
      "Independent identity remains visible, but Wyndham systems, Wyndham Rewards, commercial participation, and quality expectations require disciplined execution.",
    standardsRequirements:
      "Product, design, systems, training, and ongoing quality expectations should be confirmed directly with Wyndham for the relevant market and agreement.",
    sourceLimitations:
      "Public Wyndham materials provide brand positioning and platform context, not agreement-specific economics, acceptance decisions, pipeline counts, or performance outcomes. Confirm those matters directly with Wyndham.",
    distinguishFrom:
      "Dazzler by Wyndham (defined lifestyle design template with less property-specific flexibility), La Quinta and other core Wyndham midscale flags (standardized prototype), and independent-hotel soft-brand peers such as Ascend Hotel Collection and BW Premier Collection outside Wyndham.",
  }),
  presentation: PRESENTATION,
});

export default BRAND_FULL_BUILD_CONTENT;
