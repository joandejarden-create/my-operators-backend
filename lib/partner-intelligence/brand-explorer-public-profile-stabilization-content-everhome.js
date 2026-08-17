/**
 * Public Profile Stabilization — Everhome Suites Presentation field-gate copy.
 * Directional owner copy; no invented pipeline counts or performance tables.
 * Avoids forbidden tokens (raw URLs, FDD/LOI/Item 19 phrasing, ADR/RevPAR, fee stack, net contribution).
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

export const EVERHOME_STABILIZATION_CONTENT = Object.freeze([
  row(
    "overview.development_model",
    "New construction and conversion in U.S. extended-stay corridors where weekly demand supports residential suites with in-suite kitchens. Everhome follows a purpose-built prototype path with brand-led design review, PIP sequencing for conversions, and professional third-party management expectations per Choice development materials—confirm scope, timing, and participation costs directly for the asset before committing major capital.",
    { title: "Development Model" }
  ),
  row(
    "overview.relative_positioning",
    "Everhome Suites sits in Choice Hotels' midscale extended-stay band—residential kitchen suites and weekly-stay positioning above economy extended flags such as Suburban and alongside mature MainStay, without upscale full-service or upper-upscale Radisson paths. Owners should compare prototype vintage, standards intensity, and Choice system participation across those siblings rather than treating every extended-stay flag as interchangeable on the same pad economics.",
    { title: "Relative Positioning" }
  ),
  row(
    "overview.scenario.1",
    "Greenfield or conversion extended-stay in markets with project, relocation, training, and medical-adjacent demand where guests often stay multiple weeks. Full kitchens, residential suites, and weekly rate structures can reduce churn versus nightly midscale flags when operators discipline length-of-stay mix and housekeeping cadence. Confirm market study, conversion scope, and operator extended-stay experience before underwriting—published brand materials are directional context, not a property-level forecast.",
    { title: "Weekly-Stay Revenue Stability" }
  ),
  row(
    "overview.scenario.2",
    "Developers choosing Choice's newest midscale extended-stay platform—co-developed with extended-stay operators for efficient new builds and selective conversions. Energy-oriented systems and modular FF&E packages aim to control capex and utility intensity versus ad hoc extended conversions that retrofit kitchens room-by-room. Use prototype and standards reviews to lock scope before hard costs; agreement-specific fees and incentives vary by deal and must be confirmed directly.",
    { title: "Efficient Purpose-Built Prototype" }
  ),
  row(
    "overview.scenario.3",
    "Owners who want midscale extended-stay scale with Choice distribution, loyalty enrollment, and dedicated extended-stay opening support—modern residential product without forcing a legacy MainStay prototype on a greenfield pad or an economy extended formula where weekly mix is weak. Best when the sponsor can fund residential amenity expectations and weekly billing operations; weaker when the market behaves like transient midscale or the operator lacks extended-stay depth.",
    { title: "Midscale Platform With Choice Scale" }
  ),
  row(
    "overview.why_value",
    [
      "Why owners choose Everhome: Choice's midscale extended-stay platform built for residential weekly stays with in-suite kitchens and Homebase-style market touchpoints—not nightly transient boxes.",
      "Guest promise: suite comfort for longer stays, outdoor and social spaces where the prototype provides them, and predictable residential amenities that support multi-week guests.",
      "Commercial frame: Choice distribution and loyalty participation alongside extended-stay development support; confirm participation costs, systems, and standards timing directly for the asset.",
      "Fit filter: developers and operators with professional third-party management, weekly-stay revenue discipline, and tolerance for a newer-brand standards curve—not opportunistic midscale conversions without kitchen economics.",
    ].join("\n")
  ),
  row(
    "overview.proof.1",
    "Choice introduced Everhome Suites as a midscale extended-stay brand in the 2020 launch window, with early new-construction milestones in California and multi-market development agreements highlighted in public brand announcements. Treat published scale narratives as directional system context for a developing extended-stay platform—not as a guarantee of performance or pipeline timing for any single opportunity.",
    { title: "Midscale extended-stay launch" }
  ),
  row(
    "overview.proof.2",
    "Purpose-built prototype and standards materials emphasize developer and operator input on efficient extended-stay footprints, modular FF&E, and ready-to-build sequencing for new construction and qualified conversions. Owners should underwrite to the current design manual and milestone plan for their asset rather than assuming a generic extended-stay conversion path will meet Everhome presentation expectations on opening day.",
    { title: "Purpose-built prototype" }
  ),
  row(
    "overview.proof.3",
    "Residential weekly suites with fully equipped in-suite kitchens, dishware, and appliances align the product with project, relocation, and training stays rather than upscale full-service F&B complexity. Guest-length expectations in brand materials center on multi-week stays—operators must align housekeeping, market operations, and billing rhythm to that mix instead of discounting into nightly transient behavior.",
    { title: "Residential weekly suites" }
  ),
  row(
    "overview.proof.4",
    "Published agreement materials for Everhome cite a midscale extended-stay royalty structure on room revenue—owners should compare prototype scope, participation costs, and incentive packages against MainStay and Suburban paths on the same site economics during diligence, rather than assuming identical fee curves across Choice extended-stay flags.",
    { title: "Extended-stay participation economics" }
  ),
  row(
    "overview.featured_application",
    "New-construction midscale extended-stay pad or qualified conversion where weekly demand, kitchen-equipped suites, and professional third-party management can meet Everhome prototype and QA expectations. Featured application is the greenfield or conversion path for sponsors comparing Choice's newest extended-stay platform against mature MainStay or economy Suburban on the same corridor feasibility.",
    {
      title: "New-build / conversion extended-stay",
      caseSummaryOverview:
        "Development or conversion lens for midscale extended-stay assets seeking Choice affiliation on the Everhome platform.",
      caseSummaryBrandRelevance:
        "Matches Everhome's residential weekly-stay prototype, kitchen suite product, and Choice extended-stay development support model.",
      caseSummaryOwnerObjective:
        "Validate market weekly mix, prototype fit, operator capability, and agreement-specific participation costs before capital commitment.",
      caseSummaryInterpretation:
        "Use as a feasibility and affiliation framing—not a performance forecast or fee schedule. Confirm terms and scope directly with Choice development.",
      caseSummaryTags: "extended-stay, new construction, conversion, Choice, weekly stay",
    }
  ),
  row(
    "overview.portfolio_context",
    "Within Choice Hotels, Everhome occupies the midscale extended-stay residential band—kitchen suites and weekly-stay positioning above economy WoodSpring and Suburban extended products and distinct from mature MainStay, without entering upscale Cambria or upper-upscale Radisson full-service paths. Owners should compare prototype dependence, standards vintage, and opening support intensity across those siblings when selecting a flag for the same site."
  ),
  row(
    "valueOwners.lifecycle.1",
    "Screen market tier, capital plan, and whether the asset can sustain midscale extended-stay weekly economics under Everhome—not whether it merely needs any Choice flag. Confirm development interest, operator extended-stay depth, kitchen and market operations capacity, and how the prototype fits the pad or conversion before committing spend or relying on loyalty mix assumptions from other brands.",
    { title: "Evaluation" }
  ),
  row(
    "valueOwners.lifecycle.2",
    "Shape conversion or new-build design around the Everhome prototype, PIP scope, FF&E packages, and Choice systems requirements. Align financing, modular delivery, and operator capacity with the intended weekly guest journey—avoid treating affiliation as cosmetic reflagging that leaves kitchen, market, or outdoor amenity gaps unfunded.",
    { title: "Conversion Design" }
  ),
  row(
    "valueOwners.lifecycle.3",
    "Sequence hiring, brand and Choice training, PMS and CRS cutover, loyalty enrollment, and soft-opening plans with design sign-off. Budget time for extended-stay opening checklists and QA touchpoints. Third-party operators typically lead daily pre-opening execution while owners fund capital and confirm milestone approvals with brand development.",
    { title: "Pre-Opening" }
  ),
  row(
    "valueOwners.lifecycle.4",
    "Execute opening with weekly rate integrity, channel mix discipline, and early QA focus across the first ninety to one hundred twenty days. Guest-facing teams should deliver residential extended-stay service cues while Choice systems and loyalty participation stabilize. Confirm who owns commercial launch versus brand QA in the specific agreement.",
    { title: "Opening" }
  ),
  row(
    "valueOwners.lifecycle.5",
    "During ramp-up, calibrate loyalty mix, seasonal retail, and length-of-stay targets against housekeeping intensity and guest feedback. Watch utility and market operations tied to weekly stays—not only occupancy headlines—and revisit capital and fee assumptions before year-one repositioning spend.",
    { title: "Ramp-Up" }
  ),
  row(
    "valueOwners.lifecycle.6",
    "On an ongoing basis, maintain capex planning, brand initiatives, and portfolio benchmarks inside Choice reporting and QA rhythms. Reassess operator fit, PIP timing, and competitive extended-stay set when markets shift—Everhome value depends on sustained weekly-stay discipline plus reliable system participation.",
    { title: "Ongoing" }
  ),
  row(
    "footprint.portfolio_mix",
    [
      "Midscale extended-stay new construction",
      "Qualified extended-stay conversions",
      "Weekly and multi-week guest mix",
      "U.S. development focus within Choice Americas",
      "Residential kitchen suites versus economy extended flags",
    ].join("\n")
  ),
  row(
    "operations.model.primary_model",
    "Franchise affiliation within Choice Hotels for midscale extended-stay assets on the Everhome platform. Exact participation structure, owner obligations, and management requirements must be confirmed for each development or conversion opportunity."
  ),
  row(
    "operations.model.management_option",
    "Professional third-party management is common and typically subject to brand review of operator fit for extended-stay operations. Owner-operated paths require credible weekly-stay billing, kitchen, and market execution—not transient midscale playbooks alone."
  ),
  row(
    "operations.model.typical_ownership",
    "Institutional and entrepreneurial sponsors developing extended-stay pads, plus select franchisees with conversion capacity and tolerance for newer-brand standards milestones during ramp."
  ),
  row(
    "operations.model.pre_opening",
    "Brand-led development milestones with operator execution on staffing, training, systems cutover, and opening readiness. Sequence FF&E, prototype sign-offs, and QA with financing and hiring capacity so soft opening is not delayed by late connectivity."
  ),
  row(
    "operations.model.staffing_intensity",
    "Moderate extended-stay intensity—leaner than full-service urban hotels but requiring disciplined front desk, housekeeping, and market or Homebase operations for multi-week guests."
  ),
  row(
    "operations.model.fb_complexity",
    "Low traditional F&B complexity relative to full-service flags; complexity concentrates in in-suite kitchens, optional market or grab-and-go touchpoints, and outdoor amenity programming per prototype—not banquet or outlet-driven operations."
  ),
  row(
    "operations.model.training",
    "Choice University and Everhome opening programs typically apply. Confirm property-specific training scope, timing, and pass-through costs in pre-opening planning with the operator."
  ),
  row(
    "operations.model.reporting_discipline",
    "Financial and quality reporting through mandated Choice systems and brand QA tools. Confirm owner versus operator reporting responsibilities and data ownership in the agreement path before underwriting support burden."
  ),
  row(
    "operations.model.qa_rhythm",
    "Recurring property assessments aligned with midscale extended-stay standards—not ad hoc inspections only. Confirm cadence, scoring expectations, and remediation paths before treating affiliation as durable operational support."
  ),
  row(
    "operations.operator_compat.summary",
    "Strong fit when the operator runs weekly-stay extended-stay properties with kitchen operations, market discipline, and professional third-party management that meets Everhome prototype and QA expectations. Weak fit when the team defaults to nightly transient midscale habits or cannot execute residential amenity and housekeeping cadence for multi-week guests."
  ),
  row(
    "operations.operator_compat.tags",
    "Midscale extended-stay\nWeekly-stay operations\nChoice Privileges participation\nNew construction and conversion\nKitchen suite product discipline"
  ),
  row(
    "operations.compliance.qa_cadence",
    "Recurring midscale extended-stay QA and brand inspections cover opening readiness and ongoing presentation of residential suites, market touchpoints, and guest-facing standards. Confirm current scoring expectations and remediation paths for the specific asset before underwriting affiliation support."
  ),
  row(
    "operations.compliance.training_rigor",
    "High—extended-stay guest experience, market operations, and brand programs require disciplined onboarding for weekly-stay service. Confirm how Choice and Everhome training reinforce residential product delivery without skipping housekeeping or billing protocols."
  ),
  row(
    "operations.compliance.reporting",
    "Financial, quality, and agreement reporting through mandated Choice systems and brand tools. Owners should confirm cadence, audit expectations, and commercial participation reporting rather than assuming independent extended-stay reporting remains unchanged after affiliation."
  ),
  row(
    "operations.compliance.brand_interaction",
    "Structured pre-opening and opening support with day-to-day operations led by the owner or operator once stabilized. Interaction intensity varies by project stage—confirm development and brand touchpoints for design, opening, and early stabilization versus steady-state rhythm."
  ),
  row(
    "economics.opening.step.1",
    "Align on asset fit, market weekly demand, and new-build or conversion scope with Choice development before detailed design spend. Confirm whether the property can sustain Everhome residential presentation, kitchen suite operations, and Choice system participation alongside operator capability.",
    { title: "Application & Feasibility" }
  ),
  row(
    "economics.opening.step.2",
    "Complete prototype and standards review—lock PIP scope, design residuals, and brand presentation approvals before major capital is committed. Extended-stay efficiency still requires coherent guest journeys, credible kitchen and market quality, and opening-day readiness the operator can deliver.",
    { title: "Design & Standards" }
  ),
  row(
    "economics.opening.step.3",
    "Plan OS&E, Choice systems implementation, hiring, and opening readiness checklists with the operator. Sequence training and commercial setup so soft opening is not delayed by late connectivity, market stocking, or staffing gaps on weekly-stay workflows.",
    { title: "Pre-Opening Planning" }
  ),
  row(
    "economics.opening.step.4",
    "Coordinate training, QA readiness, and brand-led opening touchpoints with the commercial launch plan. Guest-facing teams should deliver residential extended-stay cues while Choice loyalty and distribution tools go live on schedule, with clear owner versus operator ownership of each workstream.",
    { title: "Opening Support" }
  ),
  row(
    "economics.opening.step.5",
    "Stabilize with heightened reporting and QA in early months, then return to operator-led rhythm inside Choice systems. Use early weekly mix and expense patterns to validate labor, utilities, and capital underwriting—not as a substitute for agreement-level economics review.",
    { title: "Stabilization" }
  ),
]);
