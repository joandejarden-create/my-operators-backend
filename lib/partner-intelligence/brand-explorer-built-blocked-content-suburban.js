/**
 * Field-gate Presentation content for Suburban Studios (built-blocked remediation).
 * Patches failing slots from reports/built-blocked-defect-inventory.json (suburban-studios).
 * Economy extended-stay / conversion-oriented studio product — not generic select-service.
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

export const SUBURBAN_BUILT_BLOCKED_CONTENT = Object.freeze([
  row(
    "overview.typical_use_case",
    "Economy extended-stay studio conversions near employment centers, medical corridors, and project-driven demand where guests stay by the week with in-suite kitchenettes—owners seeking Choice platform distribution at leaner extended-stay economics than MainStay residential upscale extended."
  ),
  row(
    "overview.development_model",
    "Conversion and new construction focused on studio kitchenette product, weekly rate mix, and lean extended-stay operations. Sponsors should model appliance wear, utility intensity, and reduced housekeeping cadence versus nightly midscale flags before underwriting affiliation."
  ),
  row(
    "overview.relative_positioning",
    "Suburban Studios sits at the economy extended-stay base of Choice's longer-stay ladder—studio kitchenettes and weekly billing below MainStay residential extended and Everhome's newer extended prototype—not nightly midscale select-service or upscale full-service conversion targets.",
    { title: "Relative Positioning" }
  ),
  row(
    "overview.scenario.1",
    "Economy extended-stay studio repositioning where the comp set is budget extended and weekly guests expect in-suite kitchenettes—not MainStay's residential upscale extended finish or nightly highway midscale transient mix. Confirm PIP scope for kitchen FF&E, weekly rate infrastructure, and operator discipline on wear-and-tear before committing capital.",
    { title: "Economy Extended-Stay Studio" }
  ),
  row(
    "overview.scenario.2",
    "Employment-center corridors—industrial parks, medical campuses, and project-driven markets—where guest demand is measured in weeks rather than single nights and operators can run lean staffing with practical housekeeping rhythms. Suburban Studios fits sponsors who prioritize utility, kitchenette reliability, and appliance economics over lobby theater or breakfast-led limited-service positioning in those longer-stay corridors.",
    { title: "Weekly-Stay Employment Corridor" }
  ),
  row(
    "overview.scenario.3",
    "Kitchenette conversions of older studio product needing Choice systems participation when the physical layout supports economy extended-stay standards and weekly billing operations. Pair conversion scope with realistic loyalty and enterprise mix expectations for economy extended tier—confirm participation costs and prototype requirements directly during brand engagement.",
    { title: "Kitchenette Conversion" }
  ),
  row(
    "overview.proof.1",
    "Suburban Studios delivers economy extended-stay with in-suite kitchenettes for weekly and monthly guests—leaner amenity and finish than MainStay residential extended and distinct from nightly midscale boxes. Owners should treat kitchen FF&E quality and housekeeping cadence as core operating levers, not optional conversion shortcuts.",
    { title: "Studio Kitchenette Product" }
  ),
  row(
    "overview.proof.2",
    "Brand materials emphasize employment-center and project-corridor fit—industrial, medical, and logistics adjacency—not convention, resort, or interstate transient-only markets where extended-stay economics do not align. Validate local weekly demand depth and competitive extended supply before affiliation decisions.",
    { title: "Employment-Center Studios" }
  ),
  row(
    "overview.proof.3",
    "Economy extended tier pricing and guest spend sit below MainStay residential extended positioning—sponsors comparing Choice extended flags should evaluate Everhome prototype direction, kitchen scope, and weekly mix assumptions side by side during diligence, not assume interchangeable extended-stay underwriting.",
    { title: "Economy Extended Tier" }
  ),
  row(
    "overview.proof.4",
    "Returns for studio extended-stay depend on housekeeping rhythm, appliance replacement reserves, and utility economics as much as headline occupancy—owners should model wear, reduced daily service, and weekly rate integrity with operators experienced in economy extended product rather than nightly select-service habits alone.",
    { title: "Kitchen Wear & Utilities" }
  ),
  row(
    "overview.featured_application",
    "Conversion or new-build economy extended-stay studio where weekly guests need in-suite kitchenettes, lean operations, and Choice Privileges distribution—when the sponsor can fund kitchen FF&E and operator discipline on extended-stay wear without MainStay-level residential finish.",
    {
      title: "Economy studio conversion / new build",
      caseSummaryOverview:
        "Featured path for employment-corridor studio assets seeking Suburban's economy extended-stay positioning under Choice Hotels.",
      caseSummaryBrandRelevance:
        "Matches Suburban's kitchenette weekly-stay lane below MainStay residential extended—not nightly midscale.",
      caseSummaryOwnerObjective:
        "Fund kitchenette PIP, weekly billing setup, and operator model for utilities, wear, and lean housekeeping.",
      caseSummaryInterpretation:
        "Use as an extended-stay conversion lens—confirm scope and agreement terms directly; not a performance forecast.",
      caseSummaryTags: "extended-stay, economy, studio, Choice, weekly",
    }
  ),
  row(
    "overview.differentiators.identity",
    [
      "Economy extended-stay studio positioning with in-suite kitchenettes for weekly guests",
      "Leaner amenity and finish than MainStay residential extended within Choice portfolio",
      "Employment-center and project-corridor demand—not convention or resort transient playbooks",
      "Conversion-ready within brand standards when kitchen layout and weekly ops can be sustained",
    ].join("\n")
  ),
  row(
    "overview.bestAt.1",
    "Studio kitchenette extended-stay where weekly guests cook in-room and operators manage appliance wear—leaner positioning than MainStay residential extended within Choice longer-stay family.",
    { title: "Studio Kitchenette Product" }
  ),
  row(
    "overview.bestAt.2",
    "Weekly rate economics with explicit utility, housekeeping cadence, and kitchen FF&E replacement reserves modeled against economy extended comp sets—not nightly midscale operating assumptions.",
    { title: "Weekly Rate Economics" }
  ),
  row(
    "overview.bestAt.3",
    "Economy extended tier with more retail and weekly-rate sensitivity than MainStay's higher enterprise mix—owners should calibrate channel and loyalty participation expectations during diligence.",
    { title: "Economy Extended Tier" }
  ),
  row(
    "overview.portfolio_context",
    "Within Choice Hotels, Suburban Studios anchors economy extended-stay at the portfolio base—kitchen-led longer stays below midscale MainStay and newer Everhome extended formats. Owners should compare kitchen scope, weekly mix, and operating simplicity across extended siblings rather than treating Suburban as a nightly midscale or upscale conversion target."
  ),
  row(
    "valueOwners.lifecycle.1",
    "Screen market tier, weekly demand depth, kitchen layout, capital envelope, and whether Suburban Studios' economy extended-stay model matches the physical asset and operator—not whether it merely needs a Choice flag. Confirm employment-corridor fit and utility economics before committing conversion capital.",
    { title: "Evaluation" }
  ),
  row(
    "valueOwners.lifecycle.2",
    "Shape conversion design around studio kitchenette prototype, weekly billing infrastructure, and Choice systems requirements. Align PIP, appliance scope, and operator capacity with extended-stay wear assumptions; avoid treating Suburban as a light reflag that underfunds kitchens or housekeeping rhythm.",
    { title: "Conversion Design" }
  ),
  row(
    "valueOwners.lifecycle.3",
    "Sequence lean staffing hires, extended-stay training, PMS/CRS/loyalty cutover, and weekly-rate readiness with design sign-off. Budget time for commercial setup and QA touchpoints. Operators typically lead daily pre-opening execution while owners fund kitchen FF&E and confirm milestones with brand development.",
    { title: "Pre-Opening" }
  ),
  row(
    "valueOwners.lifecycle.4",
    "Execute opening with weekly rate integrity, channel mix discipline, and early QA across the first 90–120 days. Guest-facing teams should deliver consistent extended-stay basics while Choice systems stabilize. Confirm owner versus operator ownership of commercial launch and brand QA.",
    { title: "Opening" }
  ),
  row(
    "valueOwners.lifecycle.5",
    "During ramp-up, calibrate loyalty contribution, seasonal project demand, and weekly retail tuning against appliance wear and utility trends—not only occupancy headlines. Revisit kitchen replacement reserves, housekeeping labor, and guest-quality indicators carefully before year-one capital surprises force reactive spend.",
    { title: "Ramp-Up" }
  ),
  row(
    "valueOwners.lifecycle.6",
    "On an ongoing basis, maintain capex planning for kitchens and FF&E, brand initiatives, and portfolio benchmarks inside Choice reporting rhythms. Reassess operator fit when employment corridors shift—Suburban value depends on sustained weekly operations plus reliable system participation.",
    { title: "Ongoing" }
  ),
  row(
    "footprint.growth_editorial",
    "Suburban Studios bridges economy and extended-stay for owners who want Choice platform tools at studio kitchenette economics—employment-center corridors, weekly mix, and conversion-ready within brand standards when kitchen layout supports the prototype. Confirm studio counts, PIP scope, and authorized geography directly with brand development rather than assuming parity with upscale extended flags."
  ),
  row(
    "footprint.growth_fit",
    "Economy extended-stay studio kitchenettes\nWeekly rate mix and lean housekeeping\nConversion-ready within brand standards\nEmployment-center and project corridors\nChoice Privileges extended-stay distribution"
  ),
  row(
    "footprint.openings",
    "Directional property example for economy extended-stay studio affiliation: employment-adjacent corridor with in-suite kitchenettes, weekly guest mix, and lean operating rhythm aligned to Suburban prototype expectations. Use as underwriting context for kitchen FF&E and utility intensity—not as a performance benchmark for your site.",
    {
      title: "Employment-corridor studio example",
      caseSummaryOverview:
        "Illustrative economy extended-stay studio context for weekly demand near employment centers.",
      caseSummaryBrandRelevance:
        "Shows Suburban kitchenette positioning below MainStay residential extended within Choice portfolio.",
      caseSummaryOwnerObjective:
        "Compare kitchen scope, weekly billing ops, and wear assumptions to your asset during diligence.",
      caseSummaryInterpretation:
        "Orientation only—confirm local comps, PIP, and agreement terms for your opportunity.",
      caseSummaryTags: "Suburban, extended-stay, studio, weekly, Choice",
    }
  ),
  row(
    "operations.model.primary_model",
    "Franchise within Choice Hotels for economy extended-stay studio product. Confirm participation structure and owner obligations for each asset during diligence."
  ),
  row(
    "operations.model.management_option",
    "Third-party management is common; brand typically reviews operator fit for weekly extended-stay operations, kitchen wear, and lean staffing discipline."
  ),
  row(
    "operations.model.typical_ownership",
    "Institutional and entrepreneurial owners pursuing economy extended-stay with conversion or new-build capacity and realistic utility underwriting."
  ),
  row(
    "operations.model.systems_integration",
    "Mandatory Choice PMS, CRS, and loyalty participation under standard franchise agreement requirements. Validate cutover and weekly-rate commercial setup during diligence."
  ),
  row(
    "operations.model.pre_opening",
    "Brand-led milestones with operator execution on lean staffing, kitchen readiness, and opening QA. Sequence systems and FF&E with financing and hiring capacity."
  ),
  row(
    "operations.model.staffing_intensity",
    "Moderate lean extended-stay intensity—front desk and housekeeping rhythms tuned to weekly stays rather than full-service or breakfast-led midscale staffing."
  ),
  row(
    "operations.model.fb_complexity",
    "Low complexity—no restaurant-led F&B; in-suite kitchenette guest preparation with minimal public-area food service beyond brand standards."
  ),
  row(
    "operations.model.training",
    "Choice University and Suburban opening programs typically apply. Confirm extended-stay training scope and kitchen safety expectations in pre-opening planning."
  ),
  row(
    "operations.model.reporting_discipline",
    "Financial and quality reporting through mandated Choice systems. Confirm owner versus operator reporting responsibilities in the agreement path."
  ),
  row(
    "operations.model.qa_rhythm",
    "Recurring property assessments aligned with economy extended-stay expectations—including kitchen and guestroom wear indicators. Confirm cadence before underwriting affiliation support."
  ),
  row(
    "operations.operator_compat.summary",
    "Affordable extended stays with in-suite kitchenettes—operators who understand weekly billing, appliance wear, utility economics, and lean extended-stay staffing rather than nightly select-service habits alone."
  ),
  row(
    "operations.operator_compat.tags",
    "Economy extended-stay studio\nChoice Privileges context\nConversion-ready within brand standards\nWeekly employment-corridor demand"
  ),
  row(
    "operations.compliance.qa_cadence",
    "Recurring economy extended-stay QA and brand inspections cover kitchen condition, guestroom wear, and weekly-stay basics—not ad hoc reviews only. Confirm scoring expectations and remediation paths for studio assets before affiliation.",
  ),
  row(
    "operations.compliance.training_rigor",
    "High rigor on guest experience and extended-stay basics through Choice and brand programs—confirm training reinforces weekly-stay service and kitchen safety without over-building public-area complexity.",
  ),
  row(
    "operations.compliance.reporting",
    "Financial, quality, and agreement reporting through mandated Choice tools. Owners should confirm cadence and commercial participation expectations for extended-stay weekly mix reporting.",
  ),
  row(
    "operations.compliance.brand_interaction",
    "Structured pre-opening support with day-to-day operations led by the owner/operator once stabilized. Confirm development touchpoints for conversion, kitchen PIP, and extended-stay opening discipline.",
  ),
  row(
    "economics.opening.step.1",
    "Align on asset fit, employment-corridor demand, kitchen layout, and conversion scope with Choice brand development before detailed PIP spend. Confirm the property can sustain Suburban economy extended-stay operations and Choice system participation.",
    { title: "Application & Feasibility" }
  ),
  row(
    "economics.opening.step.2",
    "Complete studio kitchenette prototype and standards review—lock PIP scope, appliance specifications, and weekly-rate infrastructure before major capital commits. Economy extended-stay still requires credible in-room kitchen quality operators can maintain through wear cycles.",
    { title: "Design & standards" }
  ),
  row(
    "economics.opening.step.3",
    "Plan OS&E, kitchen FF&E, Choice systems implementation, lean hiring, and franchise readiness checklists with the operator. Sequence training and weekly billing setup so soft opening is not delayed by incomplete kitchens or connectivity gaps.",
    { title: "Pre-Opening Planning" }
  ),
  row(
    "economics.opening.step.4",
    "Coordinate extended-stay training, QA readiness, and brand-led opening touchpoints with the weekly-rate commercial launch plan. On-property teams should deliver consistent studio basics and kitchen readiness while Choice loyalty and distribution tools go live on the agreed schedule.",
    { title: "Opening Support" }
  ),
  row(
    "economics.opening.step.5",
    "Stabilize with heightened reporting and QA in early months on kitchens and wear indicators, then shift to operator-led rhythm inside Choice systems. Use early weekly mix to validate utility and housekeeping assumptions—not as a substitute for agreement-level economics review.",
    { title: "Stabilization" }
  ),
]);
