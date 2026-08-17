/**
 * Field-gate Presentation content for Quality Inn (built-blocked remediation).
 * Patches failing slots from reports/built-blocked-defect-inventory.json (quality-inn).
 * Choice economy/midscale — owner utility, conversion path, standards, distribution, value lodging.
 * Directional owner copy — no invented fees, pipeline counts, or performance claims.
 */

function row(slotKey, body, extra = {}) {
  return {
    slotKey,
    body,
    title: extra.title || "",
    caseSummaryOverview: extra.caseSummaryOverview || null,
    caseSummaryBrandRelevance: extra.caseSummaryBrandRelevance || null,
    caseSummaryOwnerObjective: extra.caseSummaryOwnerObjective || null,
    caseSummaryInterpretation: extra.caseSummaryInterpretation || null,
    caseSummaryTags: extra.caseSummaryTags || null,
  };
}

export const QUALITY_INN_BUILT_BLOCKED_CONTENT = Object.freeze([
  row(
    "overview.typical_use_case",
    "Highway and suburban conversions, portfolio standardization, and owners who want recognizable midscale Choice distribution with Value Q guest promises—Q Bed, Q Breakfast, Q Shower, Q Service, and Q Essentials—without funding upscale public-space or full-service dining programs."
  ),
  row(
    "overview.development_model",
    "Conversion-heavy development and selective new construction across a broad geographic footprint where midscale economics, conversion-ready within brand standards planning, and franchisee-friendly operating intensity matter. Owners sequence property improvement scope, prototype alignment, and Choice systems cutover before major spend."
  ),
  row(
    "overview.relative_positioning",
    "Quality Inn anchors Choice Hotels' core midscale value band—the founding Choice retail many owners still recognize—above economy Rodeway and below upper-midscale Comfort and Country select-service. It competes on reliable essentials and conversion-ready economics, not upscale design, lifestyle lobby theater, or extended-stay kitchenette models.",
    { title: "Relative Positioning" }
  ),
  row(
    "overview.scenario.1",
    "Conversion-heavy midscale reflag fits tired independents or aging economy assets where a disciplined property improvement plan and Value Q execution can lift guest perception without upscale capital. Quality's broad open-hotel footprint de-risks the flag decision for owners standardizing on midscale Choice distribution—confirm local comps, improvement scope, and operator breakfast and housekeeping capacity before treating conversion as a low-spend logo swap.",
    { title: "Conversion-Heavy Midscale Reflag" }
  ),
  row(
    "overview.scenario.2",
    "Highway and suburban Value Q corridors suit assets competing on Q Bed, Q Breakfast, Q Shower, Q Service, and Q Essentials—not lobby bars or resort amenities. Owners should underwrite lean public space, daily breakfast where required, and housekeeping rhythm that delivers consistent midscale retail at value price points. Weaker when the market demands upper-midscale breakfast theater or full-service group demand.",
    { title: "Value-Q Highway Portfolio" }
  ),
  row(
    "overview.scenario.3",
    "Multi-hotel sponsors standardizing midscale portfolios on high-awareness Choice retail gain pricing credibility in midscale comp sets when Value Q standards are operationalized property-wide. Best for owners who want distribution, loyalty participation, and repeatable conversion playbooks—not aspirational upscale repositioning. Validate participation costs and systems cutover per asset; portfolio scale is context, not a property performance guarantee.",
    { title: "Midscale Portfolio Standardization" }
  ),
  row(
    "overview.proof.1",
    "Quality Inn is the founding Choice Hotels midscale flag—high consumer awareness supports competitive positioning in midscale corridors when guestrooms, breakfast, and service essentials match Value Q promises. Owners should treat awareness as a retail head start that still requires funded property improvement and daily operating discipline to convert into sustained performance.",
    { title: "Founding Choice Brand" }
  ),
  row(
    "overview.proof.2",
    "Quality Inn operates one of the largest midscale footprints in the Choice system globally, with dense United States presence and active conversion-led growth. System scale supports distribution and loyalty context for owners—local demand, capital plan, and operator fit still determine whether any single address succeeds under the flag.",
    { title: "Global Midscale Footprint" }
  ),
  row(
    "overview.proof.3",
    "Growth is fueled by conversions that align tired product with Value Q standards—Q Bed, Q Breakfast, Q Shower, Q Service, and Q Essentials—rather than bespoke design spend. Owners should model property improvement timing, breakfast and bath refresh, and housekeeping standards as the conversion return story, not headline occupancy alone.",
    { title: "Conversion-Led Value Q" }
  ),
  row(
    "overview.proof.4",
    "Among Tier One midscale Choice flags, Quality's royalty tier is positioned for conversion economics versus Sleep and Comfort siblings—useful for owner feasibility framing when comparing participation costs across midscale alternatives. Confirm agreement-specific fees, marketing assessments, and capital requirements directly during brand engagement; do not treat tier comparisons as net operating income forecasts.",
    { title: "Midscale Participation Economics" }
  ),
  row(
    "overview.featured_application",
    "Highway, suburban, and secondary-market hotels where owners want midscale Choice distribution, Value Q consistency, and conversion-ready within brand standards execution without upscale amenity spend. Featured application is reflag or new-build when property improvement scope, breakfast and guestroom essentials, and operator discipline can meet Quality standards on opening day.",
    {
      title: "Midscale conversion / standardization",
      caseSummaryOverview:
        "Conversion or portfolio-standardization path for midscale assets seeking recognizable Quality retail and Choice Privileges participation.",
      caseSummaryBrandRelevance:
        "Matches Quality Inn as core midscale Value Q brand—essentials-led, conversion-ready—not upper-midscale or lifestyle tiers.",
      caseSummaryOwnerObjective:
        "Execute property improvement and Value Q operating standards while evaluating participation costs, systems cutover, and midscale comp-set positioning.",
      caseSummaryInterpretation:
        "Use as a midscale affiliation lens—confirm improvement scope and commercial terms directly; not a performance forecast.",
      caseSummaryTags: "midscale, conversion, Value Q, Choice, highway",
    }
  ),
  row(
    "valueOwners.lifecycle.1",
    "Screen market tier, competitive midscale set, capital envelope, and whether Quality Inn matches the physical asset—guestroom layout, breakfast feasibility, bath condition, and highway or suburban demand. Confirm Choice development interest, Value Q gap analysis, and operator capability for conversion-ready execution before committing property improvement capital.",
    { title: "Evaluation" }
  ),
  row(
    "valueOwners.lifecycle.2",
    "Shape conversion design around Value Q scope—Q Bed, bath, breakfast, service, and essentials—while locking property improvement sequencing and systems integration. Align furniture, fixtures, and equipment and operator staffing with midscale labor economics; avoid underfunding guestroom and bath refresh that fails quality assurance on opening.",
    { title: "Conversion Design" }
  ),
  row(
    "valueOwners.lifecycle.3",
    "Sequence hiring, Choice and brand training, property management and central reservations cutover, and soft-opening plans with design sign-off. Budget time for breakfast setup where applicable, supply partners, and commercial launch. Third-party operators typically lead daily pre-opening execution while owners fund capital and confirm milestone approvals.",
    { title: "Pre-Opening" }
  ),
  row(
    "valueOwners.lifecycle.4",
    "Execute opening with rate integrity, channel mix discipline, and early quality focus across the first ninety to one hundred twenty days—Value Q consistency is an immediate guest-facing test. Confirm owner versus operator ownership of commercial launch and brand quality touchpoints in the agreement path.",
    { title: "Opening" }
  ),
  row(
    "valueOwners.lifecycle.5",
    "During ramp-up, calibrate loyalty-driven demand, seasonal travel patterns, and midscale retail tuning against guest satisfaction and cost control. Watch housekeeping and breakfast labor tied to Value Q promises—not only occupancy headlines—and revisit improvement-plan residuals if early scores expose unfunded gaps.",
    { title: "Ramp-Up" }
  ),
  row(
    "valueOwners.lifecycle.6",
    "On an ongoing basis, maintain capital expenditure planning, brand initiatives, and portfolio benchmarks inside Choice reporting rhythms. Reassess operator fit, improvement-plan timing, and midscale competitive set when markets shift—value lodging depends on sustained essentials execution plus system participation.",
    { title: "Ongoing" }
  ),
  row(
    "footprint.growth_fit",
    "Midscale conversion and reflag priority\nHighway and suburban value corridors\nPortfolio standardization on Value Q\nBreakfast-led limited-service where required\nChoice Privileges midscale distribution"
  ),
  row(
    "footprint.openings",
    "Urban CALA hub example: Quality Inn in the San José corridor serves corporate transient, airport access, and leisure mix on a midscale Value Q operating model—owners evaluating Americas conversion timing can study how essentials-led product and Choice distribution behave in a capital-city comp set without assuming identical economics at every site.",
    {
      title: "Quality Inn — San José, Costa Rica",
      caseSummaryOverview:
        "San José corridor midscale example showing Quality Value Q retail in a Central American capital-city demand mix.",
      caseSummaryBrandRelevance:
        "Illustrates core midscale Choice flag positioning—essentials and conversion-ready standards, not upscale public-space spend.",
      caseSummaryOwnerObjective:
        "Benchmark midscale operating intensity and improvement scope for CALA urban conversions; confirm local authorization and terms for your site.",
      caseSummaryInterpretation:
        "Portfolio geography example only—not a performance forecast or template capital plan for other markets.",
      caseSummaryTags: "CALA, urban, midscale, Value Q, conversion",
    }
  ),
  row(
    "footprint.openings",
    "Capital metro Caribbean example: midscale Quality positioning in Santo Domingo supports business and leisure travel on Value Q essentials—guestroom consistency, breakfast where brand standards require, and lean public areas—useful for owners comparing midscale Choice retail in dense urban Latin American markets versus highway-only assets.",
    {
      title: "Quality Inn — Santo Domingo, Dominican Republic",
      caseSummaryOverview:
        "Caribbean capital metro midscale example for Quality Inn Value Q execution and Choice distribution context.",
      caseSummaryBrandRelevance:
        "Shows founding midscale brand in urban CALA—not upper-midscale Comfort or full-service Radisson tiers.",
      caseSummaryOwnerObjective:
        "Evaluate urban midscale comp sets and property improvement scope when standardizing portfolio flags in CALA.",
      caseSummaryInterpretation:
        "Directional market example; underwriting remains asset-specific with local comps and operator diligence.",
      caseSummaryTags: "CALA, urban, midscale, portfolio, Value Q",
    }
  ),
  row(
    "footprint.openings",
    "Regional Mexico highway and city-edge example: Quality Inn midscale presence supports conversion-ready owners who need recognizable Value Q retail and Choice systems participation without upscale lobby investment—study how essentials-led product competes in mixed business and leisure submarkets before committing improvement capital.",
    {
      title: "Quality Inn — Regional Mexico Corridor",
      caseSummaryOverview:
        "Regional Americas midscale example highlighting Quality conversion and Value Q standards outside dense U.S. highway-only clichés.",
      caseSummaryBrandRelevance:
        "Reinforces midscale essentials positioning and Choice platform participation for conversion-heavy owners.",
      caseSummaryOwnerObjective:
        "Compare property improvement sequencing and operator fit for midscale reflags in regional Latin American corridors.",
      caseSummaryInterpretation:
        "Geography illustration for affiliation planning—not a guarantee of identical fees, scope, or performance elsewhere.",
      caseSummaryTags: "Americas, midscale, conversion, Value Q, regional",
    }
  ),
  row(
    "operations.model.primary_model",
    "Franchise midscale within Choice Hotels. Confirm participation structure, Value Q standards, and owner obligations for each asset during diligence."
  ),
  row(
    "operations.model.management_option",
    "Third-party management is common; brand typically reviews operator fit for midscale housekeeping, breakfast where required, and Value Q quality rhythms."
  ),
  row(
    "operations.model.typical_ownership",
    "Institutional and entrepreneurial owners and select franchisees operating conversion-heavy midscale portfolios along highway and suburban corridors."
  ),
  row(
    "operations.model.systems_integration",
    "Choice property management system, central reservations, and loyalty participation are typically mandatory under franchise terms. Validate cutover, training, and commercial requirements during diligence."
  ),
  row(
    "operations.model.pre_opening",
    "Brand-led milestones with operator execution on staffing, training, and opening readiness. Sequence systems, furniture and equipment, and quality touchpoints with financing and hiring capacity."
  ),
  row(
    "operations.model.staffing_intensity",
    "Moderate midscale intensity—front desk and housekeeping core, with breakfast labor where brand standards require daily complimentary or enhanced breakfast execution."
  ),
  row(
    "operations.model.fb_complexity",
    "Low to moderate—Value Q breakfast and limited public-area food service rather than full-service restaurant or banquet complexity."
  ),
  row(
    "operations.model.training",
    "Choice University and brand opening programs typically apply. Confirm property-specific Value Q training scope and timing in pre-opening planning."
  ),
  row(
    "operations.model.reporting_discipline",
    "Financial and quality reporting through mandated Choice systems. Confirm owner versus operator reporting responsibilities in the agreement path."
  ),
  row(
    "operations.model.qa_rhythm",
    "Recurring property assessments aligned with midscale brand tier and Value Q expectations. Confirm cadence and remediation paths before underwriting affiliation support."
  ),
  row(
    "operations.operator_compat.summary",
    "Reliable midscale stays with recognizable Quality Value Q retail—operators who execute conversion property improvement plans, essentials-led service, and lean public-space operations without over-building upscale amenities."
  ),
  row(
    "operations.operator_compat.tags",
    "Core midscale Value Q\nConversion-ready within brand standards\nChoice Privileges context\nHighway and suburban portfolios"
  ),
  row(
    "operations.compliance.qa_cadence",
    "Recurring midscale quality assurance and brand inspections cover Value Q essentials and opening readiness—not ad hoc reviews only. Confirm scoring expectations and remediation paths for the specific Quality asset before treating affiliation as durable support."
  ),
  row(
    "operations.compliance.training_rigor",
    "High rigor for guest experience and brand programs relative to midscale tier—disciplined onboarding on Value Q standards, housekeeping, and breakfast execution where applicable."
  ),
  row(
    "operations.compliance.reporting",
    "Financial, quality, and agreement reporting through mandated Choice systems. Owners should confirm cadence, data ownership, and commercial participation expectations rather than assuming legacy reporting rhythms continue unchanged."
  ),
  row(
    "operations.compliance.brand_interaction",
    "Structured pre-opening support with day-to-day operations led by the owner or operator once stabilized. Confirm development touchpoints for conversion, opening, and ongoing midscale operations."
  ),
  row(
    "economics.opening.step.1",
    "Align on asset fit, market tier, Value Q gap analysis, and conversion scope with Choice brand development before major property improvement spend. Confirm midscale prototype alignment and mandatory Choice system participation for the address.",
    { title: "Application & Feasibility" }
  ),
  row(
    "economics.opening.step.2",
    "Complete prototype and standards review—lock property improvement scope, Value Q residuals, and brand presentation approvals before major capital commits. Midscale retail still requires credible guestrooms, baths, and essentials the operator can deliver on opening day.",
    { title: "Design & standards" }
  ),
  row(
    "economics.opening.step.3",
    "Plan furniture and equipment, Choice systems implementation, hiring, and franchise readiness checklists with the operator. Sequence training and commercial setup so soft opening is not delayed by late connectivity or staffing gaps.",
    { title: "Pre-Opening Planning" }
  ),
  row(
    "economics.opening.step.4",
    "Coordinate training, quality readiness, and brand-led opening touchpoints with the commercial launch plan. Guest-facing teams should deliver Value Q cues while Choice loyalty and distribution tools go live on schedule.",
    { title: "Opening Support" }
  ),
  row(
    "economics.opening.step.5",
    "Stabilize with heightened reporting and quality assurance in early months, then return to operator-led rhythm inside Choice systems. Use early performance to validate midscale labor and capital assumptions—not as a substitute for agreement-level economics review.",
    { title: "Stabilization" }
  ),
]);

export const QUALITY_INN_BUILT_BLOCKED_SLOT_KEYS = [
  ...new Set(QUALITY_INN_BUILT_BLOCKED_CONTENT.map((r) => r.slotKey)),
];
