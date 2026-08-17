/**
 * Field-gate Presentation content for Kimpton Public Profile Stabilization.
 * Patches only slots that fail rendered completeness / golden quality in inventory.
 * Directional owner copy — no invented fees, pipeline counts, or performance claims.
 * Avoids residual-forbidden tokens (raw URLs, FDD/LOI/Item 19 phrasing, ADR/RevPAR).
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

export const KIMPTON_STABILIZATION_CONTENT = Object.freeze([
  row(
    "overview.scenario.1",
    "Independent or soft-brand urban hotels in lifestyle districts where guests pay for personality, restaurant-forward F&B, and neighborhood character—not commodity limited-service boxes. Kimpton fits when the asset already has credible public spaces and a local story that design, wine hour, and pet-friendly policy can amplify under IHG distribution. Confirm conversion PIP scope, operator F&B depth, and commercial terms directly before treating affiliation as a light cosmetic reflag.",
    { title: "Urban Lifestyle Conversion" }
  ),
  row(
    "overview.scenario.2",
    "Gateway new-build or adaptive reuse where a design-led lobby, signature restaurant, and social gathering spaces justify upper-upscale pricing and IHG commercial reach. Owners should underwrite full-service lifestyle intensity—guestroom narrative, F&B labor, and FF&E—not select-service economics. Best when IHG development confirms market tier, design narrative, and systems cutover timing alongside a realistic conversion or ground-up capital plan.",
    { title: "Gateway New-Build or Adaptive Reuse" }
  ),
  row(
    "overview.scenario.3",
    "Multi-asset sponsors aligning several lifestyle conversions to one IHG flag with a consistent design narrative, restaurant-forward operating model, and IHG One Rewards participation across the portfolio. Kimpton can standardize guest promise and QA rhythm while preserving property-level character in each gateway or resort asset. Weaker when assets differ widely in F&B capability or when markets cannot support the design and service investment Kimpton presentation expects.",
    { title: "Portfolio Lifestyle Standardization" }
  ),
  row(
    "overview.proof.1",
    "Pet-friendly positioning is a Kimpton guest-acquisition lever in urban and resort markets—not a generic amenity checkbox. For owners, it implies operating policy, housekeeping rhythm, and liability planning woven into the lifestyle promise rather than an afterthought. Treat published pet-friendly standards as an operating requirement during diligence alongside F&B and design presentation expectations.",
    { title: "Pet-Friendly Lifestyle" }
  ),
  row(
    "overview.proof.2",
    "IHG development materials describe a growing Kimpton Americas footprint with conversion-weighted growth in gateway urban and CALA markets—use that scale as directional context for lifestyle momentum, not as a property-level pipeline guarantee. Owners should still underwrite from local comps, capital plan, and operator capacity rather than assuming brand scale substitutes for asset feasibility.",
    { title: "60+ Americas Hotels" }
  ),
  row(
    "overview.proof.3",
    "Kimpton joined IHG in 2015, pairing lifestyle brand equity with IHG revenue management, loyalty, and systems infrastructure. For owners, the integration frame is upper-upscale lifestyle boutique within IHG—not a standalone soft brand without mandatory stack participation. Confirm agreement-specific systems, training, and participation costs for the asset rather than inferring economics from portfolio narrative alone.",
    { title: "IHG Integration Since 2015" }
  ),
  row(
    "overview.proof.4",
    "Wine hour and restaurant-forward experiences define Kimpton's operating model and labor plan—not complimentary breakfast-led limited service. Owners should budget F&B leadership, service rituals, and public-space programming so guest-facing teams can deliver the social F&B story on opening day. Weak F&B execution undermines the lifestyle rate positioning Kimpton retail expects in gateway markets.",
    { title: "Wine Hour & Social F&B" }
  ),
  row(
    "overview.featured_application",
    "Featured application: conversion or new-build affiliation of a design-led urban or gateway resort asset where restaurant-forward F&B, wine hour, and neighborhood character can meet Kimpton presentation standards under IHG systems. Best when ownership and operator can fund lifestyle PIP scope and sustain full-service operating intensity after opening—not when the property needs a select-service or extended-stay formula.",
    {
      title: "Lifestyle conversion / gateway new-build",
      caseSummaryOverview:
        "Affiliation path for design-led full-service assets seeking IHG distribution with Kimpton lifestyle retail and F&B depth.",
      caseSummaryBrandRelevance:
        "Matches Kimpton's urban boutique premise: personality, social F&B, and pet-friendly policy within IHG One Rewards participation.",
      caseSummaryOwnerObjective:
        "Preserve property character while validating PIP scope, operator F&B capability, IHG systems cutover, and agreement-specific participation costs.",
      caseSummaryInterpretation:
        "Use as a conversion or new-build diligence lens—not a performance forecast or fee schedule. Confirm scope and terms directly with IHG development.",
      caseSummaryTags: "lifestyle, conversion, IHG, urban gateway, F&B",
    }
  ),
  row(
    "valueOwners.lifecycle.1",
    "Screen market tier, capital plan, and whether Kimpton matches the physical asset, F&B capability, and design narrative—not merely whether the owner wants an IHG flag. Confirm IHG development interest, operator depth for restaurant-forward service, and how wine hour, pet policy, and guestroom design will show in arrival and public spaces before committing conversion capital.",
    { title: "Evaluation" }
  ),
  row(
    "valueOwners.lifecycle.2",
    "Shape conversion design around Kimpton's lifestyle narrative while meeting IHG standards: PIP scope, adaptive-reuse constraints, restaurant and bar program, and systems sequencing. Align FF&E, prototype exceptions, and operator capacity with the intended guest journey—avoid treating Kimpton as a cosmetic reflag that leaves F&B and design gaps unfunded.",
    { title: "Conversion Design" }
  ),
  row(
    "valueOwners.lifecycle.3",
    "Sequence hiring, IHG and Kimpton brand training, Opera PMS and Concerto CRS cutover, and soft-opening plans with design and F&B sign-off. Budget department-head certification and pre-opening marketing inside IHG rhythms. Third-party operators typically lead daily pre-opening execution while owners fund capital and confirm milestone approvals with brand development.",
    { title: "Pre-Opening" }
  ),
  row(
    "valueOwners.lifecycle.4",
    "Execute opening with rate integrity, channel mix discipline, and QA focus across the first ninety to one hundred twenty days. Guest-facing teams should deliver wine hour and restaurant-forward service cues while IHG One Rewards and distribution tools go live on schedule. Confirm who owns commercial launch versus brand QA in the specific agreement path.",
    { title: "Opening" }
  ),
  row(
    "valueOwners.lifecycle.5",
    "During ramp-up, calibrate loyalty contribution, seasonal F&B programming, and local neighborhood storytelling against service consistency and guest feedback. Watch labor and kitchen intensity tied to Kimpton's social F&B promise—not only occupancy headlines—and revisit capital residuals before funding year-one repositioning spend.",
    { title: "Ramp-Up" }
  ),
  row(
    "valueOwners.lifecycle.6",
    "On an ongoing basis, maintain capex planning, brand initiatives, and portfolio benchmarks inside IHG reporting and QA rhythms. Reassess operator fit, PIP timing, and competitive set when markets shift—Kimpton value depends on sustained design-led presentation plus reliable IHG systems participation.",
    { title: "Ongoing" }
  ),
  row(
    "footprint.growth_fit",
    "Gateway urban lifestyle conversions where restaurant-forward F&B and design narrative justify upper-upscale investment.\nSelect new-build in high-rate gateway markets with credible operator depth.\nRestaurant-forward repositioning of independents or soft brands seeking IHG scale.\nPet-friendly and design-led differentiation—not select-service or extended-stay formulas."
  ),
  row(
    "footprint.portfolio_mix",
    "Upper-upscale lifestyle boutique\nUrban and gateway conversions\nRestaurant-forward full service\nAmericas-focused growth"
  ),
  row(
    "operations.operator_compat.tags",
    "Lifestyle full-service F&B\nUrban conversion depth\nIHG systems integration\nDesign-led guest experience"
  ),
  row(
    "operations.compliance.training_rigor",
    "High—IHG opening training and ongoing Kimpton service standards apply to wine hour, F&B rituals, and guest experience consistency. Confirm training scope, timing, and participation costs in commercial agreement materials during pre-opening planning—not after staffing commitments are fixed."
  ),
  row(
    "operations.compliance.reporting",
    "Mandatory IHG financial and operational reporting through mandated systems. Owners should confirm reporting cadence, data ownership, audit rights, and quality score expectations in the agreement path rather than assuming independent reporting rhythms continue unchanged after affiliation."
  ),
  row(
    "operations.compliance.brand_interaction",
    "Structured design review, F&B program approval, and QA interaction with IHG brand teams—budget owner and management time for milestone reviews, remediation, and pre-opening touchpoints. Interaction intensity varies by conversion versus new-build stage; confirm development and brand contact expectations before underwriting affiliation support."
  ),
  row(
    "economics.opening.step.1",
    "Qualify urban gateway, resort, or conversion assets with IHG development—market tier, lifestyle F&B capability, design narrative fit, and conversion PIP scope before detailed design spend or term-sheet reliance. Confirm the property can sustain Kimpton presentation and full-service operating intensity alongside mandatory IHG stack participation.",
    { title: "Application & Feasibility" }
  ),
  row(
    "economics.opening.step.2",
    "Complete Kimpton design narrative and F&B program review—adaptive reuse, neighborhood character, wine hour and restaurant-forward spaces, and IHG design approval before major FF&E commitment. Lifestyle flexibility still requires a coherent guest journey and credible public-space quality the operator can deliver on opening day.",
    { title: "Design & Standards" }
  ),
  row(
    "economics.opening.step.3",
    "Plan PIP sequencing, OS&E, Opera PMS and IHG Concerto CRS cutover, department-head certification budget, and F&B onboarding aligned to lifestyle service standards. Sequence training and commercial setup so soft opening is not delayed by late connectivity, kitchen readiness, or staffing gaps.",
    { title: "Pre-Opening Planning" }
  ),
  row(
    "economics.opening.step.4",
    "Coordinate IHG opening training, Kimpton service and wine hour execution, design and F&B QA, soft opening, and IHG One Rewards launch with the commercial plan. Guest-facing teams should deliver lifestyle experience cues while distribution and loyalty tools go live on schedule, with clear owner versus operator ownership of each workstream.",
    { title: "Opening Support" }
  ),
  row(
    "economics.opening.step.5",
    "Stabilize with heightened lifestyle QA on design, F&B, and guest experience during early ramp; third-party operators run day-to-day while IHG development tracks milestone remediation. Use early operating feedback to validate labor, F&B, and capital underwriting—not as a substitute for agreement-level economics review.",
    { title: "Stabilization" }
  ),
  row(
    "insight.similar",
    "Hotel Indigo — upper-upscale lifestyle with lighter F&B intensity; compare conversion scope, operating labor, and design bar versus Kimpton restaurant-forward model.\nvoco — conversion-friendly IHG upscale; less social F&B programming; different guest promise and capital profile.\nEVEN Hotels — wellness-led select-service; not comparable on F&B depth or full-service lifestyle positioning.\nInterContinental — luxury tier within IHG; higher service and asset thresholds; different economics and prototype expectations."
  ),
]);
