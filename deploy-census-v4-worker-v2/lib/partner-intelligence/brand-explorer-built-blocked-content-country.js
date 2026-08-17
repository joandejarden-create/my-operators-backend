/**
 * Field-gate Presentation content for Country Inn & Suites by Choice (built-blocked remediation).
 * Patches failing slots from reports/built-blocked-defect-inventory.json (country-inn-suites).
 * Upper-midscale select-service — comfort, value, travel consistency; not lifestyle or collection voice.
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

export const COUNTRY_BUILT_BLOCKED_CONTENT = Object.freeze([
  row(
    "overview.typical_use_case",
    "Suburban and highway upper-midscale corridors where families, extended-stay weekend travelers, and business guests expect a consistent, comfortable select-service stay—suite layouts, complimentary breakfast, and predictable amenities under recognizable Country Inn & Suites retail within the Choice portfolio."
  ),
  row(
    "overview.development_model",
    "Conversion and new construction in suburban nodes, airport edges, and leisure gateways across the Americas. Owners typically sequence property improvement planning, prototype alignment, and Choice systems cutover so the asset opens as a credible breakfast-led upper-midscale product—not a cosmetic reflag without funded guestroom and public-space refresh."
  ),
  row(
    "overview.relative_positioning",
    "Country Inn & Suites by Choice sits in the upper-midscale select-service band—suite-oriented comfort and travel consistency above midscale Quality and Sleep formulas and below upscale Cambria and full-service Radisson tiers. It is not a lifestyle collection, soft brand, or economy highway-only play; owners should compare breakfast labor, suite mix, and capital intensity across Choice siblings before selecting a flag.",
    { title: "Relative Positioning" }
  ),
  row(
    "Guest Psychographics Description",
    "Practical travelers—families, small groups, and road-trip and suburban business guests—who prioritize reliable comfort, spacious guestrooms or suites, complimentary breakfast, and consistent service over lifestyle programming or full-service dining theater."
  ),
  row(
    "overview.scenario.1",
    "Warm upper-midscale suburban corridors fit when the asset competes with familiar select-service peers on comfort, breakfast, and guestroom consistency for families and weekend leisure—not urban lifestyle or upscale food and beverage stories. Country Inn & Suites works when suite or oversized room layouts, pool and fitness expectations, and a welcoming lobby can be sustained within brand standards after conversion-ready planning. Confirm operator breakfast execution and property improvement scope before underwriting as a light sign change.",
    { title: "Warm Upper-Midscale Suburban" }
  ),
  row(
    "overview.scenario.2",
    "Breakfast-led upper-midscale repositioning suits owners who want Choice Privileges participation, enterprise distribution, and a recognizable family-travel retail story in suburban and highway comp sets. The operating model centers on daily complimentary breakfast, consistent guestroom presentation, and moderate select-service labor—not restaurant-heavy full-service intensity. Validate participation costs, systems cutover, and quality rhythms with brand development; treat portfolio distribution narratives as context, not property-level forecasts.",
    { title: "Breakfast-Led Upper-Midscale" }
  ),
  row(
    "overview.scenario.3",
    "Conversion-ready within brand standards works for upper-midscale assets that need Choice scale and suite-oriented comfort without funding a full-service ballroom or signature restaurant program. Best when the building supports Country guestroom and public-space cues, pool and fitness where expected, and an operator who can deliver breakfast and housekeeping consistency on day one. Weaker when the market demands upscale meetings theater or the physical product cannot reach prototype expectations without heroic capital.",
    { title: "Conversion-Ready Select-Service" }
  ),
  row(
    "overview.proof.1",
    "Country Inn & Suites by Choice markets a welcoming, comfortable upper-midscale stay—suite-friendly layouts, complimentary breakfast, and travel-ready amenities for families and business guests. Owners should treat that promise as an operating contract: guestrooms, breakfast presentation, and public-space upkeep must feel consistent visit to visit, not tagline-only marketing without funded labor and supplies.",
    { title: "Comfort-Forward Upper-Midscale" }
  ),
  row(
    "overview.proof.2",
    "Complimentary breakfast and select-service simplicity define competitive posture versus many upper-midscale peers that compete on the same family-and-road-trip occasions. Owners must budget kitchen staffing, supply rhythm, and quality assurance for breakfast daily while maintaining suite or oversized room presentation—breakfast is core identity, not an optional bolt-on.",
    { title: "Breakfast-Led Operating Model" }
  ),
  row(
    "overview.proof.3",
    "Suburban, airport-edge, and leisure-gateway demand patterns fit Country's travel-consistency story—multi-night family stays, tournament weekends, and suburban business travel—not urban upscale dining or economy-only transient boxes. Underwrite local comp sets and seasonality; brand scale supports distribution context but does not replace asset-level feasibility.",
    { title: "Suburban & Family-Oriented Markets" }
  ),
  row(
    "overview.proof.4",
    "Choice enterprise channels and Choice Privileges participation give owners familiar commercial tools when the physical product matches upper-midscale expectations. Validate channel mix, loyalty fulfillment, and agreement-specific participation costs during diligence—execute on central reservations connectivity and member-benefit delivery rather than assuming automatic pricing power from portfolio headlines alone.",
    { title: "Choice Distribution Participation" }
  ),
  row(
    "overview.featured_application",
    "Suburban and highway upper-midscale hotels where families and business travelers expect suite-friendly comfort, complimentary breakfast, and reliable Country Inn & Suites retail under Choice distribution. Featured application is new-build or conversion when property improvement scope, breakfast execution, and guestroom consistency can meet brand standards on opening day—not a logo swap without funded select-service readiness.",
    {
      title: "Suite-led upper-midscale affiliation",
      caseSummaryOverview:
        "Affiliation path for suburban or highway assets seeking Country comfort positioning with Choice Privileges and enterprise distribution.",
      caseSummaryBrandRelevance:
        "Matches Country Inn & Suites as upper-midscale select-service—breakfast-led, travel-consistent, suite-oriented—not lifestyle or collection tiers.",
      caseSummaryOwnerObjective:
        "Fund conversion-ready guestrooms and breakfast operations while evaluating participation costs, systems cutover, and operator fit for daily select-service delivery.",
      caseSummaryInterpretation:
        "Use as an upper-midscale affiliation lens—confirm property improvement scope and commercial terms directly; not a performance forecast.",
      caseSummaryTags: "upper-midscale, suites, breakfast, Choice, suburban",
    }
  ),
  row(
    "overview.bestAt.1",
    "Delivering welcoming upper-midscale comfort with suite-friendly layouts and complimentary breakfast—consistent select-service execution versus midscale boxes that lack suite mix or travel-ready amenity depth.",
    { title: "Comfort & Suite Positioning" }
  ),
  row(
    "overview.bestAt.3",
    "Suburban and highway corridors where Choice enterprise distribution and loyalty participation support family, leisure, and business travel when breakfast and guestroom standards are sustained daily.",
    { title: "Travel-Corridor Distribution" }
  ),
  row(
    "valueOwners.lifecycle.1",
    "Screen market tier, competitive set, capital envelope, and whether Country Inn & Suites by Choice matches the physical asset—suite mix, breakfast footprint, pool and fitness feasibility, and suburban or highway demand. Confirm Choice development interest, prototype alignment, and operator capability for breakfast-led select-service before committing conversion or new-build capital.",
    { title: "Evaluation" }
  ),
  row(
    "valueOwners.lifecycle.2",
    "Shape conversion or new-build design around Country prototype cues—guestrooms, public spaces, and breakfast layout—while locking property improvement scope and systems sequencing. Align furniture, fixtures, and equipment, kitchen layout, and staffing models with the complimentary breakfast promise; avoid underfunding the amenity and suite story guests expect from the brand.",
    { title: "Conversion Design" }
  ),
  row(
    "valueOwners.lifecycle.3",
    "Sequence hiring, brand and Choice training, property management system and central reservations cutover, and soft-opening plans with design sign-off. Budget time for breakfast supply setup, quality touchpoints, and commercial launch. Third-party operators typically lead daily pre-opening execution while owners fund capital and confirm milestone approvals with brand development.",
    { title: "Pre-Opening" }
  ),
  row(
    "valueOwners.lifecycle.4",
    "Execute opening with rate integrity, channel mix discipline, and early quality focus across the first ninety to one hundred twenty days—breakfast consistency and guestroom presentation are immediate guest-facing tests. Confirm who owns commercial launch versus brand quality assurance in the specific agreement path.",
    { title: "Opening" }
  ),
  row(
    "valueOwners.lifecycle.5",
    "During ramp-up, calibrate loyalty-driven demand, seasonal family and tournament travel, and breakfast cost control against guest satisfaction signals. Watch select-service labor and food cost tied to complimentary breakfast—not only occupancy headlines—and revisit capital residuals if early quality reviews expose prototype gaps.",
    { title: "Ramp-Up" }
  ),
  row(
    "valueOwners.lifecycle.6",
    "On an ongoing basis, maintain capital expenditure planning, brand initiatives, and portfolio benchmarks inside Choice reporting and quality rhythms. Reassess operator fit, improvement-plan timing, and competitive set when markets shift—upper-midscale value depends on reliable breakfast execution plus consistent suite and guestroom presentation.",
    { title: "Ongoing" }
  ),
  row(
    "footprint.growth_editorial",
    "Country Inn & Suites by Choice continues to develop across suburban and travel corridors in the Americas where upper-midscale select-service demand supports suite-friendly comfort and breakfast-led operations. Growth prioritizes conversion-ready assets and new construction that can deliver consistent Country retail under Choice Privileges and enterprise distribution—owners should confirm local authorization, prototype fit, and development interest for each site rather than assuming uniform pipeline timing portfolio-wide."
  ),
  row(
    "footprint.momentum",
    "Brand momentum reflects steady upper-midscale development and refresh activity within the Choice Hotels system—suburban family travel, highway convenience, and suite-oriented product upgrades. Treat published system context as directional affiliation framing; property underwriting still requires local comps, capital plan, and operator capacity."
  ),
  row(
    "operations.model.primary_model",
    "Franchise upper-midscale select-service within Choice Hotels. Confirm participation structure, breakfast standards, suite prototype expectations, and owner obligations for each asset during diligence."
  ),
  row(
    "operations.model.management_option",
    "Third-party management is common; brand typically reviews operator fit for breakfast execution, housekeeping consistency, and upper-midscale quality rhythms before approval."
  ),
  row(
    "operations.model.typical_ownership",
    "Institutional and entrepreneurial owners and select franchisees operating single-property or multi-asset upper-midscale portfolios along suburban and travel corridors."
  ),
  row(
    "operations.model.systems_integration",
    "Choice property management system, central reservations, and loyalty participation are typically mandatory under franchise terms. Validate cutover, training, and commercial-system requirements during diligence—not after construction trades are committed."
  ),
  row(
    "operations.model.pre_opening",
    "Brand-led milestones with operator execution on staffing, breakfast setup, training, and opening readiness. Sequence systems, furniture and equipment, and quality assurance with financing and hiring capacity."
  ),
  row(
    "operations.model.staffing_intensity",
    "Moderate select-service intensity—front desk, housekeeping, and daily breakfast service must sustain complimentary breakfast delivery and consistent guestroom presentation for families and business guests."
  ),
  row(
    "operations.model.fb_complexity",
    "Low to moderate but daily—complimentary breakfast drives kitchen labor, supply chain, and quality assurance rather than full-service restaurant complexity or upscale banquet intensity."
  ),
  row(
    "operations.model.training",
    "Choice University and brand opening programs typically apply. Confirm property-specific training scope, breakfast standard work, and timing in pre-opening planning with the operator."
  ),
  row(
    "operations.model.reporting_discipline",
    "Financial and quality reporting through mandated Choice systems. Confirm owner versus operator reporting responsibilities and commercial participation expectations in the agreement path."
  ),
  row(
    "operations.model.qa_rhythm",
    "Recurring property assessments aligned with upper-midscale brand tier expectations. Confirm cadence, scoring focus on breakfast and guestrooms, and remediation paths before underwriting affiliation support."
  ),
  row(
    "operations.operator_compat.summary",
    "Comfort-forward upper-midscale stays with suite-friendly layouts and daily breakfast—operators who execute select-service consistency, housekeeping rhythm, and guest-facing warmth without over-building upscale public-space theater."
  ),
  row(
    "operations.operator_compat.tags",
    "Upper-midscale select-service\nComplimentary breakfast execution\nChoice Privileges context\nConversion-ready within brand standards"
  ),
  row(
    "operations.compliance.qa_cadence",
    "Recurring upper-midscale quality assurance and brand inspections cover opening readiness, breakfast presentation, and ongoing guestroom standards—not ad hoc reviews only. Confirm scoring expectations and remediation paths for the specific Country asset before treating affiliation as durable support."
  ),
  row(
    "operations.compliance.training_rigor",
    "High rigor—guest experience, breakfast standards, and brand programs require disciplined onboarding and refresh training. Confirm how Choice and Country training reinforce daily select-service delivery and travel-consistency expectations."
  ),
  row(
    "operations.compliance.reporting",
    "Financial, quality, and agreement reporting through mandated Choice systems. Owners should confirm cadence, data ownership, and commercial participation expectations rather than assuming legacy reporting rhythms continue unchanged after affiliation."
  ),
  row(
    "operations.compliance.brand_interaction",
    "Structured pre-opening support with day-to-day operations led by the owner or operator once stabilized. Interaction intensity varies by project stage—confirm development and brand touchpoints for conversion, new-build, opening, and ongoing operations."
  ),
  row(
    "economics.opening.step.1",
    "Align on asset fit, market tier, suite mix feasibility, and scope with Choice brand development before major design or construction spend. Confirm prototype alignment, breakfast footprint, and mandatory Choice system participation for the address alongside realistic capital and labor underwriting.",
    { title: "Application & Feasibility" }
  ),
  row(
    "economics.opening.step.2",
    "Complete prototype and standards review—lock property improvement or new-build scope, design residuals, and brand presentation approvals before major capital commits. Upper-midscale retail still requires coherent guestrooms, a credible breakfast experience, and public-space quality the operator can deliver on opening day.",
    { title: "Design & standards" }
  ),
  row(
    "economics.opening.step.3",
    "Plan furniture and equipment, Choice systems implementation, hiring, breakfast supply partners, and franchise readiness checklists with the operator. Sequence training and commercial setup so soft opening is not delayed by late connectivity, kitchen readiness, or staffing gaps.",
    { title: "Pre-Opening Planning" }
  ),
  row(
    "economics.opening.step.4",
    "Coordinate training, quality readiness, and brand-led opening touchpoints with the commercial launch plan. Guest-facing teams should deliver Country comfort cues—breakfast, suite presentation, welcoming lobby—while Choice loyalty and distribution tools go live on schedule.",
    { title: "Opening Support" }
  ),
  row(
    "economics.opening.step.5",
    "Stabilize with heightened reporting and quality assurance in early months, then return to operator-led rhythm inside Choice systems. Use early performance to validate breakfast cost, select-service labor, and capital underwriting—not as a substitute for agreement-level economics review with counsel and operators.",
    { title: "Stabilization" }
  ),
]);

export const COUNTRY_BUILT_BLOCKED_SLOT_KEYS = [
  ...new Set(COUNTRY_BUILT_BLOCKED_CONTENT.map((r) => r.slotKey)),
];
