/**
 * Field-gate Presentation content for Comfort Inn & Suites — public profile stabilization.
 * Patches slots that fail rendered completeness / golden quality in legacy public brands.
 * Directional owner copy — no invented fees, pipeline counts, or performance tables.
 * Avoids residual-forbidden tokens (raw URLs, disclosure-document phrasing, ADR/RevPAR shorthand).
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

export const COMFORT_STABILIZATION_CONTENT = Object.freeze([
  row(
    "overview.relative_positioning",
    "Comfort Inn & Suites anchors Choice Hotels' upper-midscale select-service band—recognizable family and business retail with complimentary hearty breakfast, smoke-free rooms, and predictable amenities. It sits above midscale Quality and Sleep formulas and below upscale Cambria and full-service Radisson paths—not economy, extended-stay economy, or upper-upscale collection tiers.",
    { title: "Relative Positioning" }
  ),
  row(
    "overview.scenario.1",
    "Independent or tired midscale assets along interstates and suburban corridors where guests expect complimentary hearty breakfast, smoke-free rooms, and upper-midscale retail—not economy-only amenities. Comfort fits when the building can support breakfast-led labor, refreshed guestroom presentation, and Choice systems cutover after property improvement planning. Confirm prototype alignment, operator breakfast execution, and commercial proposal terms before underwriting as a light reflag.",
    { title: "Interstate & Suburban Reflag" }
  ),
  row(
    "overview.scenario.2",
    "Greenfield pads in growth corridors where new construction and the refreshed prototype—modern guestroom cues, pillow choice, and waffle-bar breakfast—are the competitive story. Owners should sequence design approval, OS&E, and pre-opening hiring so opening day delivers the post-renaissance guest promise. Treat published pipeline mix as directional market context only; local demand and cost to build remain the feasibility base.",
    { title: "New-Build Breakfast-Led NC" }
  ),
  row(
    "overview.scenario.3",
    "Multi-hotel sponsors aligning several assets to one upper-midscale flag after the brand renaissance—consistent guest experience, loyalty participation, and operating discipline across a portfolio. Best when operators can standardize breakfast execution, quality assurance rhythms, and Choice reporting without eroding the reliable stay families and road warriors expect. Weaker when assets cannot fund prototype-level guestroom and public-space refresh.",
    { title: "Portfolio Upper-Midscale Standardization" }
  ),
  row(
    "overview.proof.1",
    "Comfort Inn & Suites markets a fully smoke-free guest experience—core identity versus many midscale and economy competitors still managing mixed inventory. For owners, smoke-free positioning is both a guest promise and an operating constraint: renovation, housekeeping, and policy enforcement must align before opening under the flag.",
    { title: "Largest Smoke-Free Footprint" }
  ),
  row(
    "overview.proof.2",
    "Among the largest upper-midscale systems in the Choice portfolio with broad North American presence and active development pipeline weighted toward new construction. Owners should treat system scale as distribution and loyalty context—not as a guarantee of property-level occupancy or average daily rate for any single address.",
    { title: "System scale context" }
  ),
  row(
    "overview.proof.3",
    "The Welcome-to-Goodbye transformation refreshed product, service cues, and guest-facing retail—brand materials cite strong guest and owner satisfaction momentum after the renaissance. Use that narrative as directional brand health context; property underwriting still requires local comps, capital plan, and operator fit.",
    { title: "Post-Renaissance Momentum" }
  ),
  row(
    "overview.proof.4",
    "Complimentary hearty breakfast—with hot proteins, waffle bar, and refreshed in-room amenities—defines the operating model, labor plan, and guest expectation. Owners must budget kitchen staffing, supply rhythm, and quality assurance for breakfast daily; it is not an optional amenity bolt-on in this flag.",
    { title: "Hearty Breakfast Standard" }
  ),
  row(
    "overview.featured_application",
    "Suburban, interstate, and growth-corridor hotels where families and business travelers expect a reliable, smoke-free, breakfast-led upper-midscale stay under refreshed Choice retail. Featured application is new-build or conversion when the prototype story—modern rooms, hearty breakfast, and Choice Privileges—can be executed consistently by the operator on day one.",
    {
      title: "Breakfast-led upper-midscale affiliation",
      caseSummaryOverview:
        "New-build or conversion path for upper-midscale select-service assets seeking recognizable Comfort retail.",
      caseSummaryBrandRelevance:
        "Matches Comfort's breakfast-led, smoke-free, family-and-business promise within Choice distribution.",
      caseSummaryOwnerObjective:
        "Deliver prototype-quality guestrooms and breakfast execution while evaluating participation costs and systems cutover.",
      caseSummaryInterpretation:
        "Use as an affiliation lens—not a performance forecast. Confirm PIP scope and commercial terms directly.",
      caseSummaryTags: "upper-midscale, breakfast, conversion, Choice",
    }
  ),
  row(
    "overview.differentiators.identity",
    [
      "Refreshed logo and modern guest-facing retail signaling brand renaissance—not legacy tired midscale presentation.",
      "Fully smoke-free positioning across the guest journey.",
      "Refreshed bath amenities and firm/soft pillow choice reinforcing consistent upper-midscale room quality.",
      "Complimentary hearty breakfast with waffle bar, hot proteins, and balanced options—core identity touchpoint owners must operationalize daily.",
      "Choice Privileges participation tying road-trip and business travel demand to loyalty-aware revenue management.",
    ].join("\n")
  ),
  row(
    "overview.portfolio_context",
    "Comfort Inn & Suites is Choice's upper-midscale breakfast-led flagship—right of Quality and Sleep midscale boxes, left of upscale Cambria and full-service Radisson tiers, and distinct from economy Rodeway and extended-stay formulas. Owners should compare capital intensity, breakfast labor, and prototype expectations across those siblings before selecting a flag."
  ),
  row(
    "valueOwners.lifecycle.1",
    "Screen market tier, capital plan, and whether the physical asset can sustain a breakfast-led upper-midscale stay under Comfort—not merely whether the sign needs refreshing. Confirm Choice development interest, prototype fit, smoke-free compliance path, and operator breakfast capability before committing conversion or new-build capital.",
    { title: "Evaluation" }
  ),
  row(
    "valueOwners.lifecycle.2",
    "Shape conversion or new-build design around the refreshed prototype—guestrooms, public spaces, and breakfast footprint—while locking property improvement scope and systems sequencing. Align FF&E, kitchen layout, and operator staffing models with the hearty-breakfast promise; avoid underfunding the amenity story guests expect from the brand.",
    { title: "Conversion Design" }
  ),
  row(
    "valueOwners.lifecycle.3",
    "Sequence hiring, brand training, Choice PMS/CRS/loyalty cutover, and soft-opening plans with design sign-off. Budget time for breakfast supply setup, quality touchpoints, and commercial launch. Third-party operators typically lead daily pre-opening execution while owners fund capital and confirm milestone approvals.",
    { title: "Pre-Opening" }
  ),
  row(
    "valueOwners.lifecycle.4",
    "Execute opening with rate integrity, channel mix discipline, and early quality assurance across the first ninety to one hundred twenty days—breakfast consistency and smoke-free policy are immediate guest-facing tests. Confirm who owns commercial launch versus brand quality assurance in the specific agreement.",
    { title: "Opening" }
  ),
  row(
    "valueOwners.lifecycle.5",
    "During ramp-up, calibrate loyalty-driven demand, seasonal travel patterns, and breakfast-cost control against guest satisfaction scores. Watch labor and food cost tied to the complimentary breakfast model—not only occupancy headlines—and revisit capital residuals if prototype gaps appear in early quality reviews.",
    { title: "Ramp-Up" }
  ),
  row(
    "valueOwners.lifecycle.6",
    "On an ongoing basis, maintain capex planning, brand initiatives, and portfolio benchmarks inside Choice reporting and quality rhythms. Reassess operator fit, improvement-plan timing, and competitive set when markets shift—upper-midscale value depends on reliable breakfast execution plus system participation.",
    { title: "Ongoing" }
  ),
  row(
    "footprint.growth_fit",
    "Suburban and interstate upper-midscale corridors\nNew construction and conversion development\nSmoke-free, breakfast-led limited-service\nFamily and business travel demand nodes"
  ),
  row(
    "operations.model.primary_model",
    "Franchise upper-midscale select-service within Choice Hotels. Exact participation structure, breakfast standards, and owner obligations must be confirmed for each asset."
  ),
  row(
    "operations.model.management_option",
    "Third-party management is common; brand typically reviews operator fit for breakfast execution and upper-midscale quality rhythms."
  ),
  row(
    "operations.model.typical_ownership",
    "Institutional and entrepreneurial owners and select franchisees operating multi-asset or single-property upper-midscale portfolios."
  ),
  row(
    "operations.model.systems_integration",
    "Choice PMS, CRS, and loyalty participation is typically mandatory under franchise terms. Validate cutover, training, and commercial-system requirements during diligence."
  ),
  row(
    "operations.model.pre_opening",
    "Brand-led milestones with operator execution on staffing, breakfast setup, training, and opening readiness. Sequence systems, FF&E, and quality assurance with financing and hiring capacity."
  ),
  row(
    "operations.model.staffing_intensity",
    "Moderate select-service intensity—front desk, housekeeping, and breakfast service must sustain daily hearty-breakfast delivery and smoke-free guest experience."
  ),
  row(
    "operations.model.fb_complexity",
    "Low to moderate but daily—complimentary breakfast drives kitchen labor, supply chain, and quality assurance rather than full-service restaurant complexity."
  ),
  row(
    "operations.model.training",
    "Choice University and brand opening programs typically apply. Confirm property-specific training scope, breakfast standard work, and timing in pre-opening planning."
  ),
  row(
    "operations.model.reporting_discipline",
    "Financial and quality reporting through mandated Choice systems. Confirm owner versus operator reporting responsibilities in the agreement path."
  ),
  row(
    "operations.model.qa_rhythm",
    "Recurring property assessments aligned with upper-midscale brand tier expectations. Confirm cadence and remediation paths before underwriting affiliation support."
  ),
  row(
    "operations.operator_compat.tags",
    "Upper-midscale select-service\nHearty breakfast execution\nChoice Privileges context\nNew build and conversion"
  ),
  row(
    "operations.compliance.qa_cadence",
    "Recurring upper-midscale quality assurance and brand inspections cover opening readiness, breakfast presentation, and ongoing guestroom standards. Confirm scoring expectations and remediation paths for the specific Comfort asset before treating affiliation as durable support."
  ),
  row(
    "operations.compliance.training_rigor",
    "High—guest experience, breakfast standards, and brand programs require disciplined onboarding. Confirm how Choice and Comfort training reinforce smoke-free policy and daily breakfast delivery."
  ),
  row(
    "operations.compliance.reporting",
    "Financial, quality, and agreement reporting through mandated Choice systems. Owners should confirm cadence, data ownership, and commercial participation expectations rather than assuming legacy reporting rhythms continue unchanged."
  ),
  row(
    "operations.compliance.brand_interaction",
    "Structured pre-opening support with day-to-day operations led by the owner or operator once stabilized. Interaction intensity varies by project stage—confirm development and brand touchpoints for new-build, conversion, opening, and ongoing operations."
  ),
  row(
    "economics.opening.step.1",
    "Align on asset fit, market, and scope with Choice development before major design or construction spend. Confirm prototype alignment, smoke-free compliance, breakfast footprint feasibility, and mandatory Choice system participation for the address.",
    { title: "Application & Feasibility" }
  ),
  row(
    "economics.opening.step.2",
    "Complete prototype and standards review—lock property improvement or new-build scope, design residuals, and brand presentation approvals before major capital is committed. Upper-midscale retail still requires coherent guestrooms and a breakfast experience the operator can deliver on opening day.",
    { title: "Design & Standards" }
  ),
  row(
    "economics.opening.step.3",
    "Plan OS&E, Choice systems implementation, hiring, breakfast supply partners, and franchise readiness checklists with the operator. Sequence training and commercial setup so soft opening is not delayed by late connectivity, kitchen readiness, or staffing gaps.",
    { title: "Pre-Opening Planning" }
  ),
  row(
    "economics.opening.step.4",
    "Coordinate training, quality readiness, and brand-led opening touchpoints with the commercial launch plan. Guest-facing teams should deliver Comfort renaissance cues—breakfast, smoke-free policy, refreshed rooms—while Choice loyalty and distribution tools go live on schedule.",
    { title: "Opening Support" }
  ),
  row(
    "economics.opening.step.5",
    "Stabilize with heightened reporting and quality assurance in early months, then return to operator-led rhythm inside Choice systems. Use early performance to validate breakfast cost, labor, and capital underwriting—not as a substitute for agreement-level economics review.",
    { title: "Stabilization" }
  ),
]);

