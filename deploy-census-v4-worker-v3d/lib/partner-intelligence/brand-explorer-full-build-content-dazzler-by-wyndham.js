import {
  buildRecentMomentumCard,
  RECENT_MOMENTUM_DEFAULT_LABEL,
  withRecentMomentumSortOrder,
} from "./brand-explorer-recent-momentum-contract.js";

/**
 * Brand Explorer Tab Factory — full build content pack: Dazzler by Wyndham.
 *
 * Wyndham Hotels & Resorts' upscale lifestyle/boutique brand — a defined design
 * personality with a distinct guest experience, concentrated historically in Latin
 * America and lifestyle-forward urban markets. Dazzler is a designed lifestyle brand
 * with its own prototype and identity guardrails, not an independent-hotel soft-brand
 * collection like Trademark Collection by Wyndham.
 *
 * Factory status: queued for Tab Factory build, "Under Review" while in the factory
 * (see brand-explorer-wyndham-factory-build-queue.js). Not Active/Live; not public-full.
 *
 * Copy rules:
 * - Directional, owner-facing. No invented fees, ADR, FDD, Item 19, pipeline counts,
 *   or performance guarantees.
 * - Brand-specific — distinguishes Dazzler from Trademark Collection by Wyndham and
 *   core Wyndham midscale flags.
 * - No Company Validated claims.
 * - No raw https:// URLs in any Body field (PVQL fails on raw URLs in owner-facing copy).
 * - Loyalty slots reference Wyndham Rewards directionally only; no invented point or
 *   night thresholds — elite ladder labels are conservative and flagged to confirm.
 */

const BRAND_SLUG = "dazzler-by-wyndham";
const BRAND_NAME = "Dazzler by Wyndham";
const PARENT_COMPANY = "Wyndham Hotels & Resorts";
const LOYALTY_PROGRAM = "Wyndham Rewards";
const RECORD_ID = "rec5CNMM4ZUD7ZHlM";

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
    "Dazzler by Wyndham is Wyndham Hotels & Resorts' upscale lifestyle brand, built around a distinct design personality and a vibrant, design-forward guest experience — historically concentrated in Latin America and lifestyle-oriented urban markets. Dazzler carries a defined prototype and design point of view, distinguishing it from Trademark Collection's independent-hotel flexibility and from core Wyndham midscale flags.",
    10
  ),
  row(
    "Guest Psychographics Description",
    "",
    "Design-conscious upscale travelers who want a distinctive, energetic lifestyle stay with a clear aesthetic identity — guests seeking more personality and design intensity than a standardized midscale stay, while still valuing Wyndham Rewards participation and recognizable commercial systems.",
    11
  ),

  // --- Overview ---
  row(
    "overview.typical_use_case",
    "",
    "New-build and conversion opportunities in urban and lifestyle-forward markets — particularly Latin America gateway and secondary cities — where ownership wants a defined upscale design identity rather than an open independent-hotel canvas. Best suited to sites that can support Dazzler's design personality and guest-experience intensity.",
    20
  ),
  row(
    "overview.development_model",
    "",
    "Both new construction and conversion are relevant, with conversion requiring a credible read on how much of the existing asset can absorb Dazzler's design identity without a ground-up rebuild. Underwrite the specific design, systems, and service scope from the current brand standards and development review rather than assuming a light reflag.",
    21
  ),
  row(
    "overview.relative_positioning",
    "Relative Positioning",
    "Dazzler sits as a defined-identity upscale lifestyle brand within Wyndham's portfolio — more design-intensive and prototype-driven than Trademark Collection's independent-hotel soft-brand path, and positioned above Wyndham's core midscale flags on design ambition and guest-experience intensity. It is not an open independent-conversion collection; confirm current design standards before comparing it to a soft-brand path.",
    22
  ),
  row(
    "overview.scenario.1",
    "Lifestyle-Forward Urban New Build",
    "A ground-up development in a gateway or secondary lifestyle market where ownership wants a defined design identity and Wyndham Rewards distribution rather than a generic midscale prototype. Dazzler fits when the market can support an upscale lifestyle rate position, design capital is planned from the outset, and the site can carry Dazzler's guest-experience intensity without diluting the brand's aesthetic point of view.",
    30
  ),
  row(
    "overview.scenario.2",
    "Latin America Urban Or Resort-Adjacent Conversion",
    "An existing hotel in a Latin America urban or resort-adjacent market with a workable layout for Dazzler's design template. Confirm the current conversion scope, design-review process, systems requirements, and capital intensity with Wyndham development before assuming a limited reflag or cosmetic refresh can meet Dazzler's lifestyle guest promise.",
    31
  ),
  row(
    "overview.scenario.3",
    "Design-Forward Repositioning Outside Core Latin America Markets",
    "A hotel outside Dazzler's traditional Latin America base that wants the brand's design personality and Wyndham Rewards reach. This is a less typical placement — confirm brand development interest, design fit, market authorization, and competitive rate support for the specific location before underwriting the opportunity as a standard conversion path.",
    32
  ),
  row(
    "overview.why_value",
    "Why Value Is Strongest",
    [
      "Strongest where the market rewards a distinctive upscale lifestyle experience and ownership is prepared to fund Dazzler's design identity rather than a light reflag.",
      "Requires an asset or site that can credibly support Dazzler's guest promise, service intensity, and aesthetic standards without erasing the brand's defined point of view.",
      "Wyndham Rewards distribution and commercial systems are the platform payoff — underwrite systems integration and design-review timelines before treating affiliation as automatic lift.",
      "Compare Dazzler against Trademark Collection and core Wyndham midscale flags on design intensity, prototype pressure, and how much aesthetic capital the asset can defend.",
      "Weaker for generic assets seeking only a familiar sign, or for markets that cannot sustain an upscale lifestyle rate position with Wyndham Rewards distribution.",
    ].join("\n"),
    33
  ),
  row(
    "overview.proof.1",
    "Defined Design Personality, Not An Open Canvas",
    "Dazzler brand materials describe a specific design point of view and lifestyle guest experience rather than an independent-hotel template. Owners should treat this as a brand-standards-driven identity: the design program is set by Wyndham, not authored freely by each property, which is the key contrast with Trademark Collection.",
    40
  ),
  row(
    "overview.proof.2",
    "Wyndham Rewards Distribution",
    "Brand materials position Wyndham Rewards participation and Wyndham's commercial systems as core affiliation value — loyalty earn and redeem plus distribution reach layered onto Dazzler's lifestyle guest experience. Confirm systems integration scope, training, and commercial participation directly with Wyndham development for the specific asset.",
    41
  ),
  row(
    "overview.proof.3",
    "Latin America Concentration With Broader Ambition",
    "Dazzler's public brand presence has historically concentrated in Latin America urban and lifestyle markets. Owners outside that base should confirm current development interest, market authorization, and competitive fit directly rather than assuming uniform availability across all Wyndham regions.",
    42
  ),
  row(
    "overview.proof.4",
    "Design Review As A Gate, Not A Checklist",
    "Acceptance and ongoing participation depend on meeting Dazzler's design and brand-standards expectations. Confirm the current review criteria, design-manual scope, and improvement expectations for the asset instead of assuming a lifestyle label alone secures approval or a light conversion package.",
    43
  ),
  row(
    "overview.featured_application",
    "Design-forward new build or conversion in a lifestyle-oriented market",
    "A site or existing asset in an urban or lifestyle-forward market — particularly in Latin America — can use Dazzler by Wyndham to deliver a defined upscale design identity with Wyndham Rewards distribution. The owner case depends on whether the market supports the intended rate position and whether design, systems, and service capital can be funded to the brand's standards — not solely on brand recognition.",
    44,
    {
      caseSummaryOverview:
        "Featured path for design-forward upscale sites or conversions seeking Wyndham platform participation under a defined lifestyle identity.",
      caseSummaryBrandRelevance:
        "Matches Dazzler's design-led, lifestyle-forward positioning — more prototype-driven than Trademark Collection's independent-hotel flexibility.",
      caseSummaryOwnerObjective:
        "Fund the design, systems, and service program required to deliver Dazzler's upscale lifestyle guest promise.",
      caseSummaryInterpretation:
        "Use as a brand-fit lens — confirm current design standards, market authorization, and agreement terms directly with Wyndham development; not a performance forecast.",
      caseSummaryTags: "lifestyle, upscale, design-forward, Wyndham, Latin America",
    }
  ),
  row(
    "overview.differentiators.identity",
    "Experience & Identity",
    [
      "Defined design personality and lifestyle guest experience",
      "Brand-standards-driven identity, not an open independent canvas",
      "Historically concentrated in Latin America and urban lifestyle markets",
      "More design-intensive positioning than Trademark Collection or core Wyndham midscale flags",
    ].join("\n"),
    45
  ),
  row(
    "overview.differentiators.commercial",
    "Commercial & Distribution",
    [
      "Wyndham Rewards loyalty earn/redeem participation",
      "Wyndham global sales, reservation, and commercial systems access",
      "Distribution reach paired with a defined upscale lifestyle identity",
      "Confirm specific commercial participation terms directly with Wyndham development",
    ].join("\n"),
    46
  ),
  row(
    "overview.bestAt.1",
    "Design-Led Upscale Lifestyle Identity",
    "Delivering a distinctive, design-forward guest experience under a defined brand template — Dazzler's core value versus an open independent-hotel affiliation.",
    47
  ),
  row(
    "overview.bestAt.2",
    "Latin America And Lifestyle-Urban Market Reach",
    "Providing a recognizable upscale lifestyle option in markets where Dazzler's design identity and Wyndham Rewards distribution have historical presence and brand-development familiarity.",
    48
  ),
  row(
    "overview.bestAt.3",
    "Wyndham Systems Under A Defined Prototype",
    "Layering Wyndham Rewards, distribution, and commercial infrastructure beneath a brand-standards-driven design identity rather than an owner-authored independent story.",
    49
  ),
  row(
    "overview.portfolio_context",
    "Portfolio Context",
    "Within Wyndham's upscale and lifestyle offering, Dazzler is the more design-intensive, prototype-driven choice relative to Trademark Collection's independent-hotel soft-brand path. Owners should compare design ambition, market fit, and desired level of property-specific expression before selecting a path.",
    50
  ),
  row(
    "footprint.portfolio_context",
    "Portfolio Context",
    "Dazzler represents Wyndham's design-led upscale lifestyle lane, distinct from Trademark Collection's independent-hotel flexibility and from Wyndham's core midscale flags. It should be assessed on design fit and market alignment rather than treated as interchangeable with either.",
    51
  ),
  row(
    "valueOwners.overview",
    "What Owners Are Buying",
    "Dazzler by Wyndham gives owners of upscale lifestyle-forward sites a defined design identity paired with Wyndham Rewards, distribution, and commercial infrastructure. The owner proposition is a brand-standards-driven lifestyle experience — not an open independent-conversion canvas — subject to design, systems, and quality obligations confirmed for the specific asset.",
    51
  ),
  row(
    "valueOwners.watchouts",
    "",
    [
      "Dazzler's design identity is brand-defined; confirm the current design manual before assuming property-specific latitude",
      "Historical concentration in Latin America means development interest and comps outside that base need direct confirmation",
      "Compare Dazzler and Trademark Collection against the asset's actual segment, market, and desired level of independent expression",
      "Confirm current acceptance, improvement, systems, and quality-review requirements for the specific deal",
    ].join("\n"),
    52
  ),

  // --- Value to Owners: lifecycle ---
  row(
    "valueOwners.lifecycle.1",
    "Evaluation",
    "Assess whether the market and site can support Dazzler's upscale lifestyle rate position and design identity, and whether Wyndham development considers the location a fit for the brand's current geographic and design focus. Compare against Trademark Collection or a core Wyndham flag before committing.",
    300
  ),
  row(
    "valueOwners.lifecycle.2",
    "Design & Conversion Planning",
    "Translate Dazzler's brand-standards design program into a property-specific plan — arrival, guest rooms, public spaces, F&B, and service model should align with the brand's lifestyle identity. Sequence design-review milestones with financing, construction, and operator selection.",
    301
  ),
  row(
    "valueOwners.lifecycle.3",
    "Pre-Opening",
    "Coordinate Wyndham systems integration, Wyndham Rewards readiness, staffing, sales setup, and training with design and construction completion for the Dazzler launch. Establish clear owner, operator, and Wyndham responsibilities for opening support, quality readiness, and escalation before the first guest stay.",
    302
  ),
  row(
    "valueOwners.lifecycle.4",
    "Opening",
    "Launch with Dazzler's design identity and lifestyle service model consistently expressed across guest touchpoints while Wyndham systems stabilize. Confirm opening support, quality review, guest-recovery paths, and brand contacts before launch so design intensity does not outrun operating readiness.",
    303
  ),
  row(
    "valueOwners.lifecycle.5",
    "Ramp-Up",
    "Use early guest feedback, channel mix, and operational performance to refine the lifestyle experience and local rate positioning. Watch whether staffing, design maintenance, and public-space programming actually support Dazzler's intended upscale promise rather than a midscale operating default.",
    304
  ),
  row(
    "valueOwners.lifecycle.6",
    "Ongoing",
    "Maintain design and service standards consistent with Dazzler's brand identity while meeting applicable Wyndham quality, systems, and commercial obligations. Revisit capital needs and market positioning as the hotel matures, and clarify renewal or remediation expectations before major changes.",
    305
  ),

  // --- Operations & Standards: model ---
  row(
    "operations.model.primary_model",
    "",
    "Upscale lifestyle brand affiliation within Wyndham Hotels & Resorts, structured through the agreement path available for the relevant market and property. Owners should confirm whether franchise, management, or another arrangement applies to the specific opportunity.",
    100
  ),
  row(
    "operations.model.management_option",
    "",
    "Third-party management can suit lifestyle-forward upscale assets when the operator can execute Dazzler's design and service program within Wyndham systems. Owner-operated models require credible leadership and capacity to meet brand-standards expectations.",
    101
  ),
  row(
    "operations.model.typical_ownership",
    "",
    "Owners of design-forward upscale sites or conversions — historically concentrated in Latin America and lifestyle urban markets — who want a defined Wyndham lifestyle identity with Wyndham Rewards distribution.",
    102
  ),
  row(
    "operations.model.brand_involvement",
    "",
    "Wyndham development and design teams engage on design-manual compliance, product presentation, and quality expectations. Confirm current review stages, documentation, and interaction cadence for the individual asset.",
    103
  ),
  row(
    "operations.model.systems_integration",
    "",
    "Dazzler hotels participate in the relevant Wyndham reservation, Wyndham Rewards, distribution, and technology ecosystem. Validate PMS, CRS, training, digital, and commercial integration requirements before committing to a development timeline.",
    104
  ),
  row(
    "operations.model.pre_opening",
    "",
    "Expect design-manual compliance, systems setup, team training, and commercial-launch work before opening. Sequence these requirements with financing and construction so readiness does not become the critical path.",
    105
  ),
  row(
    "operations.model.staffing_intensity",
    "",
    "Staffing should match an upscale lifestyle guest experience, not a minimal midscale operating model. Front office, housekeeping, service recovery, and any food-and-beverage offer should be underwritten to Dazzler's intended positioning.",
    106
  ),
  row(
    "operations.model.fb_complexity",
    "",
    "F&B and public-space programming are material to Dazzler's lifestyle identity. Review outlet concept, service hours, kitchen scope, and operator capability against local demand and the brand's design expectations rather than assuming a light F&B footprint.",
    107
  ),
  row(
    "operations.model.training",
    "",
    "Training should connect Wyndham platform participation with Dazzler's specific service and design identity. Confirm required modules, timing, delivery responsibilities, and ongoing refresh expectations as part of pre-opening planning.",
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
    "Quality and design review support Dazzler's lifestyle guest promise at conversion, opening, and during operations. Confirm current review timing, standards emphasis, scoring, and remediation process directly with Wyndham development.",
    110
  ),
  row(
    "operations.model.technology",
    "",
    "Technology participation should be reviewed as a development workstream, not an afterthought. Validate required Wyndham systems, connectivity, digital distribution, Wyndham Rewards integration, implementation support, and any asset-specific constraints.",
    111
  ),
  row(
    "operations.standards_philosophy",
    "",
    "Dazzler standards protect a defined upscale lifestyle design identity across the portfolio rather than allowing an open independent narrative.\nDesign and conversion detail: the design manual sets the guest-experience template; confirm current scope directly with Wyndham development.\nPIP / lifecycle capital: establish scope from the actual asset review; do not presume a cosmetic refresh is sufficient.\nSegment fit: compare the hotel with relevant upscale lifestyle competitors in its specific market, not with all Wyndham brands.",
    112
  ),
  row(
    "operations.operator_compat.summary",
    "",
    "Operators need to deliver a distinctive upscale lifestyle guest experience consistent with Dazzler's design template while maintaining Wyndham systems, Wyndham Rewards, commercial, and quality obligations.",
    113
  ),
  row(
    "operations.operator_compat.fit",
    "",
    "Best fit: operators experienced in upscale lifestyle or boutique hotels with strong design execution and service discipline, particularly in Latin America or comparable lifestyle-urban markets. Weaker fit: operators optimized only for standardized midscale prototypes or unable to sustain Dazzler's design-intensive service model.",
    114
  ),
  row(
    "operations.operator_compat.tags",
    "",
    "Wyndham lifestyle brand\nUpscale design-led\nLatin America concentration\nDefined prototype",
    115
  ),

  // --- Operations & Standards: flexibility indicators ---
  row("operations.flexibility.design", "", "Medium", 200),
  row("operations.flexibility.conversion", "", "Medium", 201),
  row("operations.flexibility.localization", "", "Medium", 202),
  row("operations.flexibility.operational_rigidity", "", "Medium", 203),
  row("operations.flexibility.pip", "", "Medium", 204),
  row("operations.flexibility.prototype", "", "High", 205),

  // --- Operations & Standards: compliance ---
  row(
    "operations.compliance.qa_cadence",
    "",
    "Design and guest-experience quality reviews apply at conversion, opening, and periodically thereafter. Confirm current review timing, standards emphasis, scoring, and escalation procedures directly for the proposed affiliation.",
    210
  ),
  row(
    "operations.compliance.training_rigor",
    "",
    "Training should prepare teams for both Wyndham systems participation and Dazzler's specific design and service identity. Define who trains whom, how readiness is assessed, and what refresh work is expected after opening.",
    211
  ),
  row(
    "operations.compliance.reporting",
    "",
    "Owners should clarify Wyndham reporting, Wyndham Rewards, distribution, and revenue-management obligations alongside the operator's reporting role. Agreement-level expectations may vary and should not be inferred from the brand name alone.",
    212
  ),
  row(
    "operations.compliance.brand_interaction",
    "",
    "Brand interaction typically centers on design-manual compliance, development, and opening readiness. Establish a practical calendar of decision points and contacts so owner, operator, and Wyndham teams can resolve issues before they affect launch or guest experience.",
    213
  ),

  // --- Economics & Obligations: opening path ---
  row(
    "economics.opening.step.1",
    "Application & Feasibility",
    "Present the site or asset's market context, ownership objectives, and design potential for Wyndham development review. Test whether Dazzler's upscale lifestyle positioning is credible for the location, and compare the path with Trademark Collection or a core Wyndham flag before allocating detailed capital.",
    400
  ),
  row(
    "economics.opening.step.2",
    "Design & Standards",
    "Align design, guest rooms, public spaces, amenities, service model, and technology with Dazzler's brand-standards design program. Treat this as a defined-template development plan rather than an open independent-conversion process, and sequence design-review milestones with capital and construction.",
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
    "Coordinate launch communications, systems go-live, quality readiness, and guest-service recovery plans with the operator and Wyndham contacts. Keep Dazzler's design identity prominent while ensuring platform components work reliably from the first guest stay.",
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
      title: "Dazzler Buenos Aires Palermo urban lifestyle affiliation signal",
      dateLine: "2024",
      summary:
        "Dazzler by Wyndham Buenos Aires Palermo shows the brand's design-led upscale lifestyle path in a Latin America gateway market for owners comparing defined aesthetic identity with Wyndham Rewards distribution.",
      url: "https://www.wyndhamhotels.com/dazzler/buenos-aires-argentina/dazzler-palermo-buenos-aires/overview",
      sort: 1,
    }),
    buildRecentMomentumCard({
      title: "Dazzler Buenos Aires Recoleta design-forward urban reference",
      dateLine: "2023",
      summary:
        "Dazzler by Wyndham Buenos Aires Recoleta illustrates Dazzler's urban lifestyle guest promise for owners underwriting design intensity, public-space programming, and Wyndham commercial systems on an existing city asset.",
      url: "https://www.wyndhamhotels.com/dazzler/buenos-aires-argentina/dazzler-recoleta/overview",
      sort: 2,
    }),
    buildRecentMomentumCard({
      title: "Dazzler Buenos Aires San Martin gateway lifestyle signal",
      dateLine: "2022",
      summary:
        "Dazzler by Wyndham Buenos Aires San Martin is a gateway urban reference for owners evaluating how Dazzler's defined design template and Wyndham Rewards participation land on a commercial Latin America site.",
      url: "https://www.wyndhamhotels.com/dazzler/buenos-aires-argentina/dazzler-san-martin/overview",
      sort: 3,
    }),
  ]).map((c) => row("footprint.momentum", c.title, c.body, 449 + c.sort)),
  row(
    "footprint.portfolio_mix",
    "Portfolio mix",
    "Upscale lifestyle new-build hotels\nLatin America urban and lifestyle-forward conversions\nDesign-led repositionings\nSites supporting an upscale rate position",
    460
  ),
  row(
    "footprint.geo_intro",
    "Geographic footprint",
    "Dazzler by Wyndham has historically concentrated in Latin America urban and lifestyle-forward markets, with broader interest described directionally in Wyndham development materials. Owners should confirm local development focus, comparable properties, and market authorization rather than inferring fit from global Wyndham brand presence.",
    470
  ),
  row(
    "footprint.region.am",
    "Americas",
    "The Americas — and Latin America specifically — represent Dazzler's deepest historical base, providing the clearest comp set for design-review expectations and Wyndham Rewards commercial participation. Confirm local comps and development interest for the specific market.",
    471
  ),
  row(
    "footprint.region.cala",
    "CALA",
    "CALA is central to Dazzler's historical footprint and brand story. Owners should confirm authorized geography, current design-review expectations, and local market comps directly with Wyndham development rather than assuming uniform terms across every CALA market.",
    472
  ),
  row(
    "footprint.region.eu",
    "Europe",
    "European presence is limited relative to Dazzler's Latin America concentration. Owners considering a European site should confirm current brand development interest and design-manual applicability directly rather than assuming CALA market comps translate.",
    473
  ),
  row(
    "footprint.region.mea",
    "MEA",
    "MEA relevance is market-specific and should be confirmed directly with Wyndham for the proposed country and asset. Treat wider Wyndham presence as context, not evidence that Dazzler is available or commercially equivalent in every market.",
    474
  ),
  row(
    "footprint.region.apac",
    "APAC",
    "APAC exposure is limited relative to Dazzler's Latin America base. For a specific market, confirm local Wyndham development strategy and design-manual applicability rather than using global brand presence as a proxy for feasibility.",
    475
  ),
  row(
    "footprint.growth_themes",
    "",
    "Latin America lifestyle-urban development\nDesign-forward new-build and conversion\nUpscale rate-position repositioning\nComplement to Trademark Collection within Wyndham's upscale offering",
    480
  ),
  row(
    "footprint.growth_editorial",
    "",
    "Dazzler compounds when a site or asset can genuinely support an upscale lifestyle rate position and ownership is prepared to fund the brand's design identity end to end. Named growth themes are directional context — still underwrite local comps, design capital, and agreement terms independently.",
    481
  ),
  row(
    "footprint.growth_fit",
    "",
    "Best growth fit: owners of design-forward upscale sites, particularly in Latin America or comparable lifestyle-urban markets, prepared to fund Dazzler's design program. Weaker fit: owners seeking maximum property-specific design latitude, or assets better aligned to Trademark Collection's independent-hotel flexibility.",
    482
  ),

  // --- Owner Considerations ---
  row(
    "standards.intro",
    "",
    "Dazzler by Wyndham standards center on delivering the brand's defined upscale lifestyle design identity and guest-experience quality. Confirm current standard detail and acceptance criteria directly with Wyndham development for the specific asset and market.",
    600
  ),
  row(
    "standards.requirement",
    "Design & narrative review",
    reqBody({
      typical:
        "Wyndham design review evaluates whether the property's architecture, rooms, and public spaces can deliver Dazzler's defined lifestyle design identity.",
      owner:
        "Plan design-review scope, timeline, and any remediation into development or conversion capital before underwriting Dazzler acceptance.",
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
        "PMS/CRS integration, Wyndham Rewards loyalty participation, and commercial systems participation are typically required.",
      owner:
        "Budget systems integration, training, and ongoing Wyndham Rewards/commercial participation separately from the design program.",
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
        "Outlet mix and public-space activation should match Dazzler's lifestyle design identity at an upscale intensity.",
      owner:
        "Establish required versus elective F&B and public-space capital before finalizing a development or conversion budget.",
      status: "Typically Expected",
      notes: "Confirm expected capital intensity for the specific asset with Wyndham development.",
    }),
    603
  ),
  row(
    "standards.requirement",
    "Guest-room and suite standards",
    reqBody({
      typical:
        "Guest rooms and suites should align with Dazzler's design template while meeting applicable upscale guest-experience baselines.",
      owner:
        "Validate room-product gaps, accessibility work, amenity requirements, and design-manual flexibility as part of diligence.",
      status: "Typically Expected",
      notes: "Confirm current guest-room expectations and design-manual detail for your asset.",
    }),
    604
  ),
  row(
    "standards.requirement",
    "Training and service culture",
    reqBody({
      typical:
        "Opening and ongoing training should reinforce both Wyndham systems participation and Dazzler's specific service and design identity.",
      owner: "Define training scope, timing, and cost ownership during pre-opening planning.",
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
        "Design coherence and guest-experience quality are reviewed periodically after opening to protect Dazzler's lifestyle positioning.",
      owner:
        "Underwrite remediation risk and owner versus operator responsibilities for ongoing QA before treating affiliation value as permanent.",
      status: "Typically Expected",
      notes: "Confirm current review cadence and remediation expectations with Wyndham development.",
    }),
    606
  ),
  row(
    "standards.conversion",
    "",
    "Conversion suitability depends on whether the building can credibly absorb Dazzler's design template and sustain an upscale lifestyle guest experience under Wyndham systems — not merely on ownership's desire for a recognizable platform. Confirm design-review scope and PIP intensity before committing capital.",
    607
  ),
  row(
    "standards.questions",
    "Questions owners should ask",
    [
      "What specific design elements does Wyndham expect us to deliver for Dazzler acceptance, and how much latitude exists versus the standard design manual?",
      "What is the current design-review timeline and remediation process if elements fall short?",
      "What Wyndham Rewards systems and commercial participation requirements apply to this specific asset?",
      "How does Dazzler's design and F&B capital intensity compare with Trademark Collection or a core Wyndham flag for a property like ours?",
      "What ongoing QA cadence and standards review should we expect after opening, and who owns corrective action?",
    ].join("\n"),
    608
  ),

  // --- Dealality Insight: similar brands ---
  row(
    "insight.similar.1",
    "Trademark Collection by Wyndham",
    "Sibling Wyndham soft-brand collection built for independent-hotel flexibility rather than Dazzler's defined design template — compare desired level of property-specific expression before choosing a path.",
    700
  ),
  row(
    "insight.similar.2",
    "Aloft Hotels",
    "Marriott lifestyle brand peer with a defined design template and upscale lifestyle positioning — compare design-manual rigidity, loyalty platform scale, and market concentration outside Wyndham.",
    701
  ),
  row(
    "insight.similar.3",
    "Cambria Hotels",
    "Choice Hotels' design-forward upscale brand peer — compare prototype flexibility, market positioning, and loyalty ecosystem scale outside Wyndham's soft- and lifestyle-brand offering.",
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
    "Wyndham Rewards is Wyndham Hotels & Resorts' loyalty program, spanning the company's economy-through-upscale portfolio, including Dazzler by Wyndham. It provides points earning, redemption, and member recognition across participating Wyndham-affiliated hotels. Confirm current program structure and participating-brand detail directly with Wyndham.",
    751
  ),
  row(
    "loyalty.owner_lens",
    "",
    "Model Wyndham Rewards on a net economics basis after member discounts, elite benefits, and program participation costs — not headline distribution reach alone. Confirm Dazzler-specific loyalty booking mix, program fees, and net revenue impact in your development materials with the brand rather than assuming a uniform figure across the Wyndham portfolio.",
    752
  ),
  row(
    "loyalty.earn",
    "",
    [
      "Points earned on eligible paid stays at participating Wyndham-affiliated hotels, including Dazzler by Wyndham",
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
      "Confirm current redemption structure, blackout considerations, and any brand-specific limitations directly with Wyndham",
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
    canonicalSite: "wyndhamhotels.com — Dazzler by Wyndham brand context",
    developmentPage: "wyndhamhotels.com/develop — Wyndham Hotels & Resorts development and brand context",
    propertyPages:
      "Individual Dazzler by Wyndham property pages on wyndhamhotels.com; use for property-specific design and location context, not as universal requirements.",
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
      "Wyndham Hotels & Resorts upscale lifestyle brand with a defined design personality and guest-experience template, historically concentrated in Latin America and lifestyle-forward urban markets — distinct from Wyndham's independent-hotel soft-brand collection.",
    ownerFit:
      "Owners of design-forward upscale sites or conversions, particularly in Latin America or comparable lifestyle-urban markets, who want a defined Wyndham lifestyle identity and Wyndham Rewards distribution.",
    propertyFit:
      "New-build and conversion opportunities that can credibly absorb Dazzler's design template and sustain an upscale lifestyle rate position and guest experience.",
    conversionLogic:
      "Design review gates acceptance against Dazzler's brand-standards template — capital should fund the defined lifestyle identity rather than an open independent narrative; confirm scope from the asset-specific review.",
    operatingImplications:
      "Upscale staffing and material F&B/public-space intensity; Wyndham systems, Wyndham Rewards, and reporting participation apply regardless of design flexibility.",
    standardsRequirements:
      "Design-manual compliance and guest-experience quality reviewed at conversion, opening, and periodically thereafter; confirm current acceptance criteria directly with Wyndham development.",
    sourceLimitations:
      "Public brand materials describe design positioning, geographic concentration, and growth themes only — no property-level counts, fees, or performance data. Confirm agreement-specific terms directly with Wyndham development.",
    distinguishFrom:
      "Trademark Collection by Wyndham (independent-hotel soft-brand collection with more property-specific design flexibility), and core Wyndham midscale flags (standardized prototype without Dazzler's lifestyle design intensity).",
  }),
  presentation: PRESENTATION,
});

export default BRAND_FULL_BUILD_CONTENT;
