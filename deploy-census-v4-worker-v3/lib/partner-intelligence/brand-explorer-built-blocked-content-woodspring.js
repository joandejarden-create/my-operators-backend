/**
 * Field-gate Presentation content for WoodSpring Suites (built-blocked remediation).
 * Patches failing slots from reports/built-blocked-defect-inventory.json (woodspring-suites).
 * Economy extended-stay — operating simplicity, weekly stays, lean staffing, conversion/new-build.
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

export const WOODSPRING_BUILT_BLOCKED_CONTENT = Object.freeze([
  row(
    "overview.typical_use_case",
    "Owners seeking extended-stay Choice distribution, Choice Privileges participation, and recognizable WoodSpring retail in employment, medical, and logistics corridors—weekly and longer-stay guests with in-room kitchens and lean operating simplicity, including selective Americas growth contexts."
  ),
  row(
    "overview.development_model",
    "Conversion and new construction where local weekly extended-stay demand supports purpose-built suite product with in-room kitchens and lean public areas. Sponsors should confirm prototype fit, PIP scope, and operating model assumptions directly with brand development before major capital commits."
  ),
  row(
    "overview.relative_positioning",
    "WoodSpring Suites sits in Choice's extended-stay family as an economy-to-midscale longer-stay flag built around in-room kitchens and operational simplicity—distinct from nightly midscale boxes, MainStay residential extended, Suburban economy studio tier, and upscale full-service conversion plays.",
    { title: "Relative Positioning" }
  ),
  row(
    "overview.scenario.1",
    "Extended-stay new build or conversion on employment, medical, or logistics corridors where weekly mix, in-room kitchen economics, and lean housekeeping beat nightly midscale operating intensity. Confirm prototype kitchen scope, utility underwriting, and operator extended-stay discipline before treating the asset as a generic limited-service reflag or upscale conversion play.",
    { title: "Extended-Stay Corridor Conversion" }
  ),
  row(
    "overview.scenario.2",
    "Suburban or secondary markets with growing longer-stay guest segments—WoodSpring suits sponsors evaluating simple suite product, extended-stay positioning, and Choice platform distribution when local supply supports weekly demand rather than convention or resort transient peaks alone. Validate operator hiring for lean extended-stay rhythms before underwriting opening labor.",
    { title: "Weekly-Demand Growth Market" }
  ),
  row(
    "overview.scenario.3",
    "Portfolio standardization on lean extended-stay when owners want a recognizable Choice extended flag with practical in-room kitchens and limited public-area complexity— weaker when the market demands residential upscale extended finish or nightly breakfast-led midscale retail. Pair conversion scope with operator capacity for appliance wear and weekly billing integrity.",
    { title: "Lean Extended-Stay Standardization" }
  ),
  row(
    "overview.proof.1",
    "WoodSpring Suites participates in the Choice Hotels extended-stay portfolio with a primarily U.S. operating footprint and selective Americas context. Owners should review local extended-stay supply, corridor demand, weekly mix potential, and competitive positioning within their target market—not assume national scale substitutes for site diligence.",
    { title: "U.S. Extended-Stay Footprint" }
  ),
  row(
    "overview.proof.2",
    "WoodSpring is positioned for extended-stay development where weekly and longer-stay demand supports a kitchen-equipped suite model. Owners should evaluate development timing, prototype fit, construction context, and operator hiring profile during site selection rather than importing nightly hotel opening playbooks.",
    { title: "Extended-Stay Development Context" }
  ),
  row(
    "overview.proof.3",
    "WoodSpring expects purpose-built extended-stay prototype thinking—in-room kitchens and lean public-area design even when converting legacy product. Owners should compare room mix, kitchen FF&E scope, and service model assumptions against local extended-stay competitors before underwriting affiliation capital.",
    { title: "New-Build / Prototype Fit" }
  ),
  row(
    "overview.proof.4",
    "WoodSpring sits within Choice Hotels' extended-stay brand family alongside other longer-stay flags. Owners should understand how Choice platform distribution and brand-family positioning support guest discovery for weekly stays while confirming participation costs and systems scope for the specific asset during diligence.",
    { title: "Choice Platform Context" }
  ),
  row(
    "overview.featured_application",
    "Conversion or new-build extended-stay where sponsors want WoodSpring's simple longer-stay lodging—in-room kitchens, lean staffing, weekly guest mix, and Choice Privileges participation—when employment or project corridors support extended economics and operators can manage wear-and-tear discipline.",
    {
      title: "Extended-Stay Owner Fit",
      caseSummaryOverview:
        "Featured path for corridor extended-stay assets seeking WoodSpring's kitchen-equipped weekly-stay model under Choice Hotels.",
      caseSummaryBrandRelevance:
        "Matches WoodSpring operating simplicity and in-room kitchen extended positioning within Choice extended-stay family.",
      caseSummaryOwnerObjective:
        "Fund prototype-aligned kitchens, weekly billing ops, and lean staffing with realistic utility and wear reserves.",
      caseSummaryInterpretation:
        "Use as an extended-stay diligence lens—confirm PIP, operator fit, and agreement terms directly.",
      caseSummaryTags: "WoodSpring, extended-stay, weekly, Choice, kitchen",
    }
  ),
  row(
    "overview.bestAt.1",
    "Markets where weekly extended-stay demand and kitchen-equipped suites align with WoodSpring prototype amenity expectations—not nightly transient-only corridors lacking longer-stay depth.",
    { title: "Weekly corridor markets" }
  ),
  row(
    "overview.bestAt.2",
    "Owners who model operating outcomes after brand participation costs, loyalty mix, and channel blend for weekly extended product—not nightly select-service assumptions alone.",
    { title: "Weekly economics diligence" }
  ),
  row(
    "overview.bestAt.3",
    "Operators with extended-stay QA discipline, kitchen safety routines, and opening readiness matched to economy extended tier expectations—not full-service hospitality overhead.",
    { title: "Extended-stay operator discipline" }
  ),
  row(
    "valueOwners.lifecycle.1",
    "Screen market tier, weekly demand depth, kitchen layout, capital envelope, and whether WoodSpring Suites' extended-stay promise matches the physical asset and operator—not whether it merely needs a Choice logo. Confirm corridor fit, utility economics, and prototype alignment before committing conversion capital.",
    { title: "Evaluation" }
  ),
  row(
    "valueOwners.lifecycle.2",
    "Shape conversion design around in-room kitchen prototype, weekly billing infrastructure, and Choice systems requirements. Align PIP, appliance scope, and lean public areas with extended-stay wear assumptions; avoid underfunding kitchens, utilities, or housekeeping rhythm expected for weekly guests.",
    { title: "Conversion Design" }
  ),
  row(
    "valueOwners.lifecycle.3",
    "Sequence lean staffing hires, extended-stay training, PMS/CRS/loyalty cutover, and weekly-rate readiness with design sign-off. Budget commercial setup, kitchen FF&E delivery, and QA touchpoints. Operators typically lead daily pre-opening execution while owners fund kitchen capital and confirm milestones with brand development.",
    { title: "Pre-Opening" }
  ),
  row(
    "valueOwners.lifecycle.4",
    "Execute opening with weekly rate integrity, channel mix discipline, and early QA across the first 90–120 days. Teams should deliver consistent extended-stay basics while Choice systems stabilize. Confirm owner versus operator ownership of commercial launch and brand QA.",
    { title: "Opening" }
  ),
  row(
    "valueOwners.lifecycle.5",
    "During ramp-up, calibrate loyalty contribution, project-season demand, and weekly retail tuning against appliance wear and utility trends—not only occupancy headlines—and carefully revisit kitchen replacement reserves and housekeeping cadence before surprise capital calls or guest-quality drift.",
    { title: "Ramp-Up" }
  ),
  row(
    "valueOwners.lifecycle.6",
    "On an ongoing basis, maintain capex planning for kitchens and suites, brand initiatives, and portfolio benchmarks inside Choice reporting rhythms. Reassess operator fit when corridors shift—WoodSpring value depends on sustained weekly operations plus reliable system participation.",
    { title: "Ongoing" }
  ),
  row(
    "footprint.growth_fit",
    "Extended-stay growth in employment and logistics corridors\nAmericas conversion and new construction where prototype fits\nChoice Privileges weekly-stay distribution\nLean kitchen-equipped suite operating model\nSelective CALA context—confirm geography with brand development"
  ),
  row(
    "footprint.portfolio_mix",
    "Extended-stay kitchen-equipped suites\nWeekly and longer-stay demand corridors\nConversion-ready within brand standards\nLean public-area operating model\nChoice extended-stay family positioning"
  ),
  row(
    "operations.model.primary_model",
    "Franchise within Choice Hotels for WoodSpring extended-stay suite product. Confirm participation structure and owner obligations for each asset during diligence."
  ),
  row(
    "operations.model.management_option",
    "Third-party management is common; brand typically reviews operator fit for weekly extended-stay operations, kitchen wear, and lean staffing discipline."
  ),
  row(
    "operations.model.typical_ownership",
    "Institutional and entrepreneurial owners pursuing extended-stay with conversion or new-build capacity and realistic utility and wear underwriting."
  ),
  row(
    "operations.model.pre_opening",
    "Brand-led milestones with operator execution on lean staffing, kitchen readiness, and opening QA. Sequence systems and FF&E with financing and hiring capacity."
  ),
  row(
    "operations.model.staffing_intensity",
    "Moderate lean extended-stay intensity—front desk and housekeeping rhythms tuned to weekly stays rather than full-service or breakfast-led midscale staffing models."
  ),
  row(
    "operations.model.fb_complexity",
    "Low complexity—guest in-room kitchen preparation with minimal public-area food service beyond brand standards; no restaurant-led operating burden."
  ),
  row(
    "operations.model.training",
    "Choice University and WoodSpring opening programs typically apply. Confirm extended-stay training scope, kitchen safety, and weekly-stay service expectations in pre-opening planning."
  ),
  row(
    "operations.model.reporting_discipline",
    "Financial and quality reporting through mandated Choice systems. Confirm owner versus operator reporting responsibilities in the agreement path."
  ),
  row(
    "operations.model.qa_rhythm",
    "Recurring property assessments aligned with extended-stay expectations—including kitchen and suite wear indicators. Confirm cadence before underwriting affiliation support."
  ),
  row(
    "operations.operator_compat.summary",
    "Extended-stay guest experience with in-room kitchens and lean operations—operators who meet WoodSpring prototype expectations, loyalty fulfillment, weekly billing discipline, and QA rhythms rather than nightly select-service habits alone."
  ),
  row(
    "operations.operator_compat.tags",
    "Extended-stay kitchen-equipped suites\nChoice Privileges context\nConversion-ready within brand standards\nWeekly corridor demand"
  ),
  row(
    "operations.compliance.qa_cadence",
    "Recurring extended-stay QA and brand inspections cover kitchen condition, suite wear, and weekly-stay basics—not ad hoc reviews only. Confirm scoring expectations and remediation paths before affiliation decisions."
  ),
  row(
    "operations.compliance.training_rigor",
    "High rigor on extended-stay guest experience through Choice and brand programs—confirm training reinforces weekly-stay service, kitchen safety, and lean public-area standards without over-building complexity."
  ),
  row(
    "operations.compliance.reporting",
    "Financial, quality, and agreement reporting through mandated Choice tools. Owners should confirm cadence and commercial participation expectations for extended-stay weekly mix reporting."
  ),
  row(
    "operations.compliance.brand_interaction",
    "Structured pre-opening support with day-to-day operations led by the owner/operator once stabilized. Confirm development touchpoints for conversion, kitchen PIP, and extended-stay opening discipline."
  ),
  row(
    "economics.opening.step.1",
    "Align on asset fit, corridor weekly demand, kitchen layout, and conversion or new-build scope with Choice brand development before detailed PIP spend. Confirm the property can sustain WoodSpring extended-stay operations and Choice system participation.",
    { title: "Application & Feasibility" }
  ),
  row(
    "economics.opening.step.2",
    "Complete extended-stay prototype and standards review—lock PIP scope, in-room kitchen specifications, and weekly-rate infrastructure before major capital commits. Lean extended-stay still requires credible kitchen quality operators can maintain through wear cycles.",
    { title: "Design & standards" }
  ),
  row(
    "economics.opening.step.3",
    "Plan OS&E, kitchen FF&E, Choice systems implementation, lean hiring, and franchise readiness checklists with the operator. Sequence training and weekly billing setup so soft opening is not delayed by incomplete kitchens or connectivity gaps.",
    { title: "Pre-Opening Planning" }
  ),
  row(
    "economics.opening.step.4",
    "Coordinate extended-stay training, QA readiness, and brand-led opening touchpoints with weekly-rate commercial launch. Teams should deliver consistent suite basics while Choice loyalty and distribution tools go live on schedule with clear owner versus operator accountability.",
    { title: "Opening Support" }
  ),
  row(
    "economics.opening.step.5",
    "Stabilize with heightened reporting and QA in early months on kitchens and wear indicators, then shift to operator-led rhythm inside Choice systems. Use early weekly mix to validate utility and housekeeping assumptions—not as a substitute for agreement-level economics review.",
    { title: "Stabilization" }
  ),
  row(
    "footprint.openings",
    "Directional U.S. extended-stay corridor example: kitchen-equipped suites serving weekly project and employment demand with lean public areas and WoodSpring operating simplicity. Compare kitchen FF&E scope and housekeeping cadence to your asset during diligence—not as a performance benchmark for your market.",
    {
      title: "Corridor extended-stay example",
      caseSummaryOverview:
        "Illustrative WoodSpring extended-stay context for weekly demand near employment or logistics corridors.",
      caseSummaryBrandRelevance:
        "Shows in-room kitchen extended positioning and lean operating model within Choice extended-stay family.",
      caseSummaryOwnerObjective:
        "Compare prototype kitchen scope, weekly billing ops, and wear assumptions to your opportunity.",
      caseSummaryInterpretation:
        "Orientation only—confirm local comps, PIP, and agreement terms directly with brand development.",
      caseSummaryTags: "WoodSpring, extended-stay, weekly, kitchen, Choice",
    }
  ),
  row(
    "footprint.openings",
    "Secondary-market extended-stay conversion context where legacy suite product can align to WoodSpring kitchen standards with funded PIP and operator weekly-stay discipline. Use to stress-test conversion capital and utility economics before affiliation—not as guaranteed outcomes for dissimilar assets.",
    {
      title: "Conversion suite example",
      caseSummaryOverview:
        "Conversion-oriented extended-stay example emphasizing kitchen PIP and weekly operations alignment.",
      caseSummaryBrandRelevance:
        "Matches WoodSpring conversion-ready within brand standards when layout supports in-room kitchens.",
      caseSummaryOwnerObjective:
        "Validate PIP sequencing, appliance reserves, and operator extended-stay capability against this pattern.",
      caseSummaryInterpretation:
        "Pattern reference only—each asset requires site-specific feasibility and brand approval.",
      caseSummaryTags: "WoodSpring, conversion, extended-stay, weekly",
    }
  ),
  row(
    "footprint.openings",
    "New-build extended-stay prototype context: purpose-built suites with in-room kitchens, limited lobby complexity, and staffing matched to weekly guest mix under WoodSpring brand standards. Owners should compare construction scope and opening timeline assumptions to local extended competitors during site selection.",
    {
      title: "Purpose-built suite example",
      caseSummaryOverview:
        "New-build extended-stay illustration for kitchen-equipped suites and lean public-area design.",
      caseSummaryBrandRelevance:
        "Reflects WoodSpring purpose-built extended prototype expectations within Choice platform.",
      caseSummaryOwnerObjective:
        "Align development budget, room mix, and operator hiring to prototype before groundbreaking.",
      caseSummaryInterpretation:
        "Development orientation—not a forecast; confirm entitlements, costs, and agreement terms locally.",
      caseSummaryTags: "WoodSpring, new-build, extended-stay, prototype",
    }
  ),
]);
