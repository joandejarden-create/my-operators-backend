/**
 * Field-gate Presentation content for Ascend Hotel Collection — public profile stabilization.
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

export const ASCEND_STABILIZATION_CONTENT = Object.freeze([
  row(
    "overview.typical_use_case",
    "Independent upscale and boutique hotels that want Choice Privileges reach, central reservation, and loyalty participation without forcing a single-chain prototype. Best for historic, design-forward, and neighborhood-driven assets where local identity is part of the rate story—not highway limited-service boxes."
  ),
  row(
    "overview.development_model",
    "Affiliation and conversion of hand-selected unique independents and boutique properties within collection standards that preserve character. Owners should expect design review, property improvement planning, and Choice systems cutover—not a uniform new-build prototype path."
  ),
  row(
    "overview.relative_positioning",
    "Ascend Hotel Collection sits in Choice Hotels' upscale soft-collection band for distinctive midscale-upscale independents—more flexible than hard-brand Radisson flags, below upper-upscale Individuals and Collection paths, and above upper-midscale breakfast-led Comfort and Quality formulas. Owners underwrite to individuality plus Choice participation, not homogenized chain retail.",
    { title: "Relative Positioning" }
  ),
  row(
    "overview.scenario.1",
    "Boutique independent conversions that need Choice distribution, loyalty, and central reservation without erasing a credible local story. The asset should already deliver arrival, public-space, and guest-experience quality that can meet collection presentation—then Ascend amplifies discoverability while standards govern what must change. Confirm property improvement scope, operator fit, and commercial proposal terms before treating affiliation as light cosmetic repositioning.",
    { title: "Boutique Independent Conversion" }
  ),
  row(
    "overview.scenario.2",
    "Historic urban repositioning where architecture, neighborhood narrative, and design-forward interiors are the competitive edge. Ascend fits when owners can fund the collection presentation expected in brand materials while preserving the elements guests pay for—confirm contractually how much identity survives design review and quality assurance. Use published brand context as directional only; local comps and capital plan remain the underwriting base.",
    { title: "Historic Urban Repositioning" }
  ),
  row(
    "overview.scenario.3",
    "Markets where guests pay for authentic food-and-beverage and local operators—not a stripped limited-service model. Owners accept collection compliance, participation costs, and marketing/reservation charges defined in commercial agreement materials rather than assuming a midscale amenity formula. Weaker fit when the asset cannot sustain differentiated F&B or when operators need a rigid prototype box instead of flexible collection standards.",
    { title: "Local F&B Preserved" }
  ),
  row(
    "overview.proof.1",
    "Choice Hotels positions Ascend as a growing upscale soft collection spanning many affiliated properties—scale that supports loyalty and distribution without implying every asset shares one prototype. Owners should treat published collection breadth as market context for soft-brand momentum, not as a property-level performance forecast or guarantee for any single conversion opportunity.",
    { title: "Collection scale context" }
  ),
  row(
    "overview.proof.2",
    "Soft-collection positioning emphasizes direct and proprietary booking mix alongside third-party channels—important for independents that rely on story-driven retail rather than economy-only price shopping. Confirm channel strategy, rate integrity, and loyalty participation for the asset; public materials do not replace property-level commercial diligence or agreement-specific economics.",
    { title: "Direct booking emphasis" }
  ),
  row(
    "overview.proof.3",
    "Ascend is built for unique independents—boutique, historic, and design conversions—not standardized highway limited-service boxes. That filter is an owner diligence gate: if the property cannot sustain distinctive presentation and service, a harder Choice flag or different collection path may fit better than forcing collection affiliation.",
    { title: "Local character preserved" }
  ),
  row(
    "overview.proof.4",
    "Commercial agreement materials describe membership, marketing, and reservation participation costs tied to gross room revenues—owners must read the full commercial proposal for the asset rather than relying on headline percentages alone. Build feasibility from local comps, capital plan, operator capacity, and agreement-specific charges; public profile copy is not a substitute for underwriting.",
    { title: "Participation cost diligence" }
  ),
  row(
    "overview.featured_application",
    "Distinctive independent or boutique hotels seeking Choice-family distribution and Choice Privileges while keeping a hand-selected soft-collection identity. Featured application is conversion or affiliation of an upscale asset where local story, design character, and guest experience can meet Ascend presentation expectations without a hard-brand prototype rebuild.",
    {
      title: "Soft-collection conversion / affiliation",
      caseSummaryOverview:
        "Affiliation path for upscale independents seeking Choice reach without erasing property character.",
      caseSummaryBrandRelevance:
        "Matches Ascend's soft-collection premise: individuality plus Choice commercial tools and loyalty participation.",
      caseSummaryOwnerObjective:
        "Preserve local identity while evaluating collection standards, systems cutover, and participation costs for the asset.",
      caseSummaryInterpretation:
        "Use as a conversion lens—not a fee schedule or performance forecast. Confirm scope and commercial terms directly.",
      caseSummaryTags: "soft collection, conversion, Choice, boutique",
    }
  ),
  row(
    "overview.bestAt.1",
    "Local character with Choice scale: independent-spirited hotels united by distribution, loyalty, and collection compliance—not one-box prototype homogenization.",
    { title: "Local Character + Choice Scale" }
  ),
  row(
    "overview.portfolio_context",
    "Within Choice Hotels, Ascend occupies the upscale soft-collection tier for independent character—below upper-upscale Radisson Blu, Individuals, and Collection paths and above upper-midscale Comfort, Quality, and Sleep formulas. Owners should compare conversion flexibility, presentation expectations, and system participation across those siblings rather than treating all Choice flags as interchangeable."
  ),
  row(
    "valueOwners.lifecycle.1",
    "Screen market tier, capital plan, and whether the physical asset can sustain an upscale soft-collection stay under Ascend—not merely whether it needs any flag. Confirm Choice development interest, operator capability for local differentiation, and how character will show in arrival, public spaces, and service before committing conversion capital or relying on loyalty assumptions alone.",
    { title: "Evaluation" }
  ),
  row(
    "valueOwners.lifecycle.2",
    "Shape conversion design around preserving property identity while meeting Ascend presentation and Choice systems requirements. Align property improvement scope, prototype exceptions, FF&E, and operator capacity with the intended guest journey—avoid treating the soft collection as a cosmetic reflag that leaves product and service gaps unfunded.",
    { title: "Conversion Design" }
  ),
  row(
    "valueOwners.lifecycle.3",
    "Sequence hiring, brand training, Choice PMS/CRS/loyalty cutover, and soft-opening plans with design sign-off. Budget time for commercial setup and quality touchpoints. Third-party operators typically lead daily pre-opening execution while owners fund capital and confirm milestone approvals with brand development.",
    { title: "Pre-Opening" }
  ),
  row(
    "valueOwners.lifecycle.4",
    "Execute opening with rate integrity, channel mix discipline, and early quality assurance focus across the first ninety to one hundred twenty days. Guest-facing teams should deliver collection-oriented service while Choice systems and loyalty participation stabilize. Confirm who owns commercial launch versus brand quality assurance in the specific agreement.",
    { title: "Opening" }
  ),
  row(
    "valueOwners.lifecycle.5",
    "During ramp-up, calibrate loyalty-driven demand, seasonal retail, and local programming against service consistency and guest feedback. Watch labor and F&B intensity tied to the soft-collection promise—not only occupancy headlines—and revisit capital residuals before year-one repositioning spend.",
    { title: "Ramp-Up" }
  ),
  row(
    "valueOwners.lifecycle.6",
    "On an ongoing basis, maintain capex planning, brand initiatives, and portfolio benchmarks inside Choice reporting and quality rhythms. Reassess operator fit, improvement-plan timing, and competitive set when markets shift—soft-collection value depends on sustained individuality plus reliable system participation.",
    { title: "Ongoing" }
  ),
  row(
    "footprint.growth_fit",
    "Boutique and independent conversion corridors\nHistoric and design-forward urban and resort assets\nUpscale soft collection within Choice commercial systems\nHand-selected properties with local differentiation capacity"
  ),
  row(
    "footprint.portfolio_mix",
    "Upscale soft collection\nIndependent & boutique conversions\nUrban / resort unique properties\nChoice-family distribution"
  ),
  row(
    "operations.model.primary_model",
    "Franchise / soft-collection affiliation within Choice Hotels for hand-selected upscale independents. Exact participation structure and owner obligations must be confirmed for each asset."
  ),
  row(
    "operations.model.management_option",
    "Third-party management is common; brand typically reviews operator fit for collection presentation and local storytelling capacity."
  ),
  row(
    "operations.model.typical_ownership",
    "Institutional and entrepreneurial owners seeking Choice-family reach while keeping property identity—select franchisees and regional groups with conversion capacity."
  ),
  row(
    "operations.model.systems_integration",
    "Choice PMS, CRS, and loyalty participation is typically mandatory under franchise terms. Validate cutover, training, and commercial-system requirements during diligence."
  ),
  row(
    "operations.model.pre_opening",
    "Brand-led milestones with operator execution on staffing, training, and opening readiness. Sequence systems, FF&E, and quality assurance with financing and hiring capacity."
  ),
  row(
    "operations.model.staffing_intensity",
    "Moderate to elevated for an upscale soft collection—front office, housekeeping, and guest experience must support distinctive presentation and local character."
  ),
  row(
    "operations.model.fb_complexity",
    "Moderate to high where public spaces and outlets carry local character. Underwrite kitchen scope and service rhythm to the intended guest journey and collection standards."
  ),
  row(
    "operations.model.training",
    "Choice University and brand opening programs typically apply. Confirm property-specific training scope, timing, and cost in pre-opening planning."
  ),
  row(
    "operations.model.reporting_discipline",
    "Financial and quality reporting through mandated Choice systems. Confirm owner versus operator reporting responsibilities in the agreement path."
  ),
  row(
    "operations.model.qa_rhythm",
    "Recurring property assessments aligned with upscale soft-collection expectations. Confirm cadence and remediation paths before underwriting affiliation support."
  ),
  row(
    "operations.operator_compat.summary",
    "Operators who celebrate unique hotels with Choice backing—maintaining local differentiation, collection compliance, and reliable loyalty fulfillment without defaulting to generic midscale operating habits."
  ),
  row(
    "operations.operator_compat.tags",
    "Upscale soft collection\nChoice Privileges context\nBoutique conversion path\nLocal storytelling discipline"
  ),
  row(
    "operations.compliance.qa_cadence",
    "Recurring upscale soft-collection quality assurance and brand inspections cover opening readiness and ongoing presentation. Confirm current scoring expectations and remediation paths for the specific Ascend asset before treating affiliation as durable support."
  ),
  row(
    "operations.compliance.training_rigor",
    "High—guest experience and brand programs require disciplined onboarding. Confirm how Choice and Ascend training reinforce collection service expectations without erasing property identity."
  ),
  row(
    "operations.compliance.reporting",
    "Financial, quality, and agreement reporting through mandated Choice systems. Owners should confirm cadence, data ownership, and commercial participation expectations rather than assuming independent reporting remains unchanged after affiliation."
  ),
  row(
    "operations.compliance.brand_interaction",
    "Structured pre-opening support with day-to-day operations led by the owner or operator. Interaction intensity varies by project stage—confirm development and brand touchpoints for conversion, opening, and stabilized operations."
  ),
  row(
    "economics.opening.step.1",
    "Align on asset fit, market, and conversion scope with Choice development before detailed design spend. Confirm whether the property can sustain Ascend presentation and soft-collection service expectations alongside mandatory Choice system participation.",
    { title: "Application & Feasibility" }
  ),
  row(
    "economics.opening.step.2",
    "Complete prototype and standards review—lock property improvement scope, design residuals, and brand presentation approvals before major capital is committed. Collection flexibility still requires a coherent guest journey and credible public-space quality the operator can deliver on opening day.",
    { title: "Design & Standards" }
  ),
  row(
    "economics.opening.step.3",
    "Plan OS&E, Choice systems implementation, hiring, and franchise readiness checklists with the operator. Sequence training and commercial setup so soft opening is not delayed by late connectivity or staffing gaps.",
    { title: "Pre-Opening Planning" }
  ),
  row(
    "economics.opening.step.4",
    "Coordinate training, quality readiness, and brand-led opening touchpoints with the commercial launch plan. Guest-facing teams should deliver Ascend collection cues while Choice loyalty and distribution tools go live on schedule, with clear owner versus operator ownership of each workstream.",
    { title: "Opening Support" }
  ),
  row(
    "economics.opening.step.5",
    "Stabilize with heightened reporting and quality assurance in early months, then return to operator-led rhythm inside Choice systems. Use early performance to validate labor, F&B, and capital underwriting—not as a substitute for agreement-level economics review.",
    { title: "Stabilization" }
  ),
]);

