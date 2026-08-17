/**
 * Field-gate Presentation content for Design Hotels public profile stabilization.
 * Patches slots failing rendered completeness (blank / too_thin) per stabilization inventory.
 * Affiliation / curated soft-collection voice — no invented economics or performance claims.
 * Avoids forbidden tokens (raw URLs, FDD/LOI/Item phrasing, ADR/RevPAR, fee-stack language).
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

function buildRequirementBody({ typical, owner, status, notes }) {
  return [
    `Typical consideration: ${typical}`,
    `Owner planning consideration: ${owner}`,
    `Typical status: ${status}`,
    `Notes to confirm: ${notes}`,
  ].join("\n");
}

export const DESIGN_HOTELS_STABILIZATION_CONTENT = Object.freeze([
  row(
    "overview.relative_positioning",
    "Design Hotels sits in Marriott International's lifestyle and soft-collection band for hand-selected, design-forward independents—alongside Autograph Collection and Tribute Portfolio, but with a longer design-curation heritage and a membership model built around architecture, culture, and local identity rather than a uniform chain prototype. Owners should compare curation intensity, operating autonomy, and selective Bonvoy participation against those siblings before treating any Marriott soft collection as interchangeable.",
    { title: "Relative Positioning" }
  ),
  row(
    "overview.scenario.1",
    "Architecturally distinctive independents and urban boutiques where design narrative, arrival experience, and cultural programming already define the product. Design Hotels fits when owners want curated global recognition and selective Marriott Bonvoy context without surrendering independent ownership, localized F&B decisions, or place-specific storytelling. Confirm membership criteria, design review scope, and participation mechanics directly before underwriting affiliation as a light cosmetic reflag.",
    { title: "Design-Led Independent Hotel" }
  ),
  row(
    "overview.scenario.2",
    "Repositioned lifestyle or boutique assets where conversion capital must preserve design integrity—not impose a standardized prototype. Owners evaluate curation standards, owner control, storytelling requirements, and affiliation value while sequencing any capital work around collection design review. Weaker fit when the asset needs a hard-brand Marriott look-and-feel or cannot fund the presentation quality the collection signals in public materials.",
    { title: "Conversion With Design Integrity" }
  ),
  row(
    "overview.scenario.3",
    "Urban hotels anchored in neighborhood culture, art, and local programming where guests choose the property for place and perspective. Design Hotels suits owners who prioritize independent character, design credibility, and editorial collection positioning over flag standardization. Best when operators can sustain experiential public spaces and guest rituals; confirm market tier and operator capacity before assuming collection affiliation replaces local comp diligence.",
    { title: "Urban Cultural Destination" }
  ),
  row(
    "overview.proof.1",
    "Design Hotels curates independent, design-forward member hotels worldwide—each retained as its own architectural and cultural statement. Collection membership signals curation quality and design leadership to guests, operators, and capital partners rather than chain standardization. Owners should treat that filter as a diligence gate: if the asset cannot sustain distinctive presentation and guest experience, a harder Marriott flag or a different affiliation path may fit better.",
    { title: "Independent Design-Led Collection" }
  ),
  row(
    "overview.proof.2",
    "Member hotels remain independently owned and operated within a curated collection context. Public materials describe membership pathways, design expectations, and curation standards—owners should confirm participation criteria, review cadence, and owner obligations directly with Design Hotels and counsel rather than inferring uniform operating mandates from collection marketing alone.",
    { title: "Member Hotel Identity & Curation" }
  ),
  row(
    "overview.proof.3",
    "The global member directory illustrates collection breadth across regions and property types—urban, resort, and experiential contexts appear together under one design-led umbrella. Use directory context for positioning and market storytelling only, not as a property-level performance forecast, pipeline guarantee, or substitute for local market underwriting and operator feasibility work.",
    { title: "Global Collection Context" }
  ),
  row(
    "overview.proof.4",
    "Directions and Further editorial hubs reinforce design, culture, and destination storytelling—the collection's differentiation is narrative and experiential, not prototype-driven rollouts. For owners, that editorial layer is part of the affiliation value proposition when the property can live up to the design promise in arrival, public spaces, and guest programming on opening day and through stabilized operations.",
    { title: "Culture, Design & Local Identity" }
  ),
  row(
    "overview.featured_application",
    "Affiliation for a design-led independent where architecture, local culture, and guest experience already differentiate the asset—owners seek curated collection credibility and selective Marriott Bonvoy participation while keeping independent operating identity and localized F&B decisions.",
    {
      title: "Curated membership / affiliation",
      caseSummaryOverview:
        "Membership path for a distinctive independent seeking global design-collection recognition without a standardized chain prototype rebuild.",
      caseSummaryBrandRelevance:
        "Matches Design Hotels' curated soft-collection premise: design integrity, local identity, and selective Marriott ecosystem participation where agreements support it.",
      caseSummaryOwnerObjective:
        "Preserve property character while evaluating curation standards, design review, distribution participation, and agreement-specific obligations.",
      caseSummaryInterpretation:
        "Use as an affiliation and curation lens—not a performance forecast or confidential economics schedule. Confirm scope and terms directly.",
      caseSummaryTags: "soft collection, curation, independent, Bonvoy",
    }
  ),
  row(
    "overview.portfolio_context",
    "Within Marriott International, Design Hotels occupies the design-led independent collection lane—cousin to Autograph Collection and Tribute Portfolio, but oriented around global design curation, member-hotel individuality, and editorial cultural programming rather than a single conversion prototype. Owners should compare curation intensity, Bonvoy participation variability, and operating autonomy across those soft-collection paths when screening affiliation fit.",
    { title: "Design Hotels — Marriott Soft-Collection Affiliation" }
  ),
  row(
    "valueOwners.lifecycle.1",
    "Screen market tier, capital plan, and whether the asset can sustain a design-forward independent stay under Design Hotels—not merely whether it needs Marriott ecosystem context. Confirm membership interest, operator capability for local storytelling and public-space quality, and how design identity will show in arrival, guest programming, and F&B before committing affiliation capital or relying on loyalty contribution assumptions.",
    { title: "Evaluate — Affiliation Fit" }
  ),
  row(
    "valueOwners.lifecycle.2",
    "Review public membership materials and engage brand representatives on participation criteria, design review expectations, distribution mechanics, agreement structure, and owner control retained post-affiliation. Map Bonvoy participation scope, reporting obligations, and curation touchpoints to the operator model—affiliation diligence here is closer to membership fit than to a prototype conversion checklist.",
    { title: "Diligence — Membership Criteria" }
  ),
  row(
    "valueOwners.lifecycle.3",
    "Plan design review, property storytelling, photography, and collection integration milestones while preserving independent operating identity. Sequence any capital work, FF&E, and public-space upgrades with curation feedback so onboarding is not delayed by late design sign-off or unresolved guest-journey gaps the collection expects to be tangible on day one.",
    { title: "Onboarding — Design & Brand Standards" }
  ),
  row(
    "valueOwners.lifecycle.4",
    "Operate with local programming, design integrity, and guest experience differentiation within collection affiliation guardrails. Guest-facing teams should deliver the property's design narrative consistently while distribution and Bonvoy participation—where applicable—stabilize. Confirm who owns daily experience versus curation review in the specific agreement path.",
    { title: "Operate — Independent Character" }
  ),
  row(
    "valueOwners.lifecycle.5",
    "Confirm Bonvoy and distribution participation scope per property agreement—benefits, channels, and recognition mechanics may vary by participating member hotel. Calibrate channel mix, group and leisure retail, and local programming against service consistency and guest feedback rather than treating collection affiliation as uniform commercial lift across every asset.",
    { title: "Distribute — Platform Participation" }
  ),
  row(
    "valueOwners.lifecycle.6",
    "Periodically reassess collection fit, design evolution, operator performance, and affiliation value as markets and owner strategy change. Soft-collection value depends on sustained individuality plus reliable platform participation—revisit curation alignment, capital planning, and competitive set when repositioning or renewal decisions approach.",
    { title: "Review — Ongoing Alignment" }
  ),
  row(
    "footprint.geo_intro",
    "Design Hotels maintains a global member directory spanning multiple regions and property contexts—urban gateways, resort and leisure destinations, and culturally anchored neighborhoods appear together under one design-led collection. Use directory breadth for positioning and owner storytelling only; do not treat directory scale as open-hotel counts, pipeline disclosures, or property-level performance representations unless confirmed at corporate level for the specific asset.",
    { title: "" }
  ),
  row(
    "footprint.growth_editorial",
    "Design Hotels growth follows curation quality and member-hotel fit—not standardized prototype rollout or uniform conversion economics. Public materials emphasize design leadership, cultural programming, and global collection credibility; owners should confirm current collection priorities, market interest, and membership pathways directly with the brand platform before underwriting expansion or affiliation timing assumptions from Explorer context alone.",
    { title: "" }
  ),
  row(
    "footprint.portfolio_mix",
    [
      "Design-led independent members",
      "Urban cultural destinations",
      "Resort & leisure experiential",
      "Global curated collection",
      "Selective Bonvoy alignment",
    ].join("\n")
  ),
  row(
    "operations.model.typical_ownership",
    "Entrepreneurial and institutional owners of distinctive boutique and lifestyle assets—often family groups, design-forward investors, or regional operators—seeking curated global recognition while retaining property identity and localized operating decisions."
  ),
  row(
    "operations.model.pre_opening",
    "Membership onboarding emphasizes design narrative, curation review, storytelling assets, and platform integration rather than franchise-style prototype compliance. Sequence design sign-off, photography, guest-journey readiness, and Bonvoy cutover—where applicable—with operator hiring and opening plans."
  ),
  row(
    "operations.model.staffing_intensity",
    "Moderate to high for design-forward full-service or boutique lifestyle stays—front office, housekeeping, and guest experience teams must support distinctive presentation and localized programming, not limited-service efficiency alone."
  ),
  row(
    "operations.model.fb_complexity",
    "Variable by property—often moderate to high where public spaces, bars, and restaurants carry local character and design narrative; underwrite kitchen scope and service rhythm to the intended guest journey."
  ),
  row(
    "operations.model.training",
    "Moderate collection orientation plus property-specific onboarding for operating teams—emphasis on design story, guest experience rituals, and affiliation guardrails rather than uniform chain service scripts."
  ),
  row(
    "operations.model.reporting_discipline",
    "Moderate property-level reporting aligned with affiliation agreement and Marriott ecosystem requirements where Bonvoy participation applies—confirm cadence, data ownership, and owner versus operator responsibilities during diligence."
  ),
  row(
    "operations.standards_philosophy",
    "Standards emphasize design quality, cultural authenticity, and guest experience coherence within a curated member collection—not homogenized chain prototypes. Curation review protects collection credibility while member hotels retain independent operating identity; owners should confirm specific requirements, review cadence, and remediation paths in membership materials and agreements before underwriting affiliation support."
  ),
  row(
    "operations.operator_compat.fit",
    "Strong fit when distinctive architecture, cultural programming, and guest experience already differentiate the asset before affiliation—lifestyle and design-led operators typically outperform limited-service operators without public-space or experiential capacity. Weaker fit when the owner needs rigid chain operating playbooks or cannot sustain design presentation through stabilized operations.",
    { title: "" }
  ),
  row(
    "operations.compliance.qa_cadence",
    "Periodic design and experience review cycles aligned with collection curation—not uniform hard-brand inspection calendars. Confirm current review expectations, scoring themes, and remediation paths for the specific member asset before treating affiliation as durable quality support.",
    { title: "" }
  ),
  row(
    "operations.compliance.training_rigor",
    "Collection orientation plus property-specific onboarding for operating teams—training reinforces design narrative and guest experience consistency without erasing local identity. Confirm onboarding scope, timing, and owner versus operator responsibilities in pre-opening planning.",
    { title: "" }
  ),
  row(
    "operations.compliance.reporting",
    "Property-level reporting aligned with affiliation agreement and Marriott ecosystem requirements where Bonvoy participation applies. Owners should confirm cadence, data ownership, and commercial participation expectations rather than assuming independent reporting rhythms remain unchanged after membership.",
    { title: "" }
  ),
  row(
    "operations.compliance.brand_interaction",
    "Curation and design review touchpoints across membership evaluation, onboarding, and repositioning—frequency varies by project stage and capital scope. Day-to-day operations remain owner or operator led; confirm development and curation touchpoints for affiliation, refresh, and renewal decisions.",
    { title: "" }
  ),
  row(
    "economics.opening.step.1",
    "Begin with membership interest and asset overview—confirm whether the property aligns with curation criteria, design narrative, and operating model before detailed commercial or capital workstreams. Early alignment prevents spend on affiliation paths that cannot meet collection presentation expectations or owner control objectives.",
    { title: "Initial Affiliation Conversation" }
  ),
  row(
    "economics.opening.step.2",
    "Expect design narrative, architecture, interior experience, and guest journey review as part of collection evaluation—not a standardized chain prototype compliance checklist. Lock curation feedback, storytelling requirements, and any capital scope tied to membership readiness before major FF&E or public-space commitments.",
    { title: "Design & Curation Review" }
  ),
  row(
    "economics.opening.step.3",
    "Diligence distribution participation, Bonvoy scope, and affiliation economics directly with brand representatives and counsel—public Explorer content does not disclose confidential participation schedules or property-level performance representations. Map owner obligations, renewal triggers, and participation variability before underwriting affiliation lift.",
    { title: "Commercial & Participation Terms" }
  ),
  row(
    "economics.opening.step.4",
    "Coordinate collection onboarding, photography and storytelling assets, operator training, and commercial launch plans so guest-facing teams deliver the design promise while Bonvoy and distribution tools—where applicable—go live on schedule. Clarify owner versus operator ownership of curation milestones versus daily opening execution.",
    { title: "Membership Onboarding & Launch" }
  ),
  row(
    "economics.opening.step.5",
    "Stabilize with heightened attention to guest feedback, design presentation, and channel mix discipline in early months, then return to operator-led rhythm inside affiliation guardrails. Use early operating results to validate labor, F&B, and capital underwriting—not as a substitute for agreement-level economics review with counsel.",
    { title: "Stabilization" }
  ),
  row(
    "standards.requirement",
    buildRequirementBody({
      typical:
        "Collection curation expects design-forward guest experience, architecture, interior narrative, and place-making integrity that justify global design-collection positioning.",
      owner:
        "Obtain specific membership criteria from brand materials and confirm fit with property character, capital plan, and operator capability before underwriting affiliation.",
      status: "Confirm with brand",
      notes:
        "Architecture, interiors, and guest experience should meet curation expectations—owners obtain specific criteria from membership materials and design review discussions.",
    }),
    { title: "Design & Experience Integrity" }
  ),
  row(
    "standards.requirement",
    buildRequirementBody({
      typical:
        "Member hotels remain independently owned and operated within a curated collection context rather than as standardized chain clones.",
      owner:
        "Confirm how collection affiliation affects day-to-day operating decisions, staffing, F&B concept control, and guest experience ownership post-membership.",
      status: "Confirm with brand",
      notes:
        "Operating autonomy is a core affiliation premise—map decision rights, brand touchpoints, and curation review against the operator agreement.",
    }),
    { title: "Independent Operating Identity" }
  ),
  row(
    "standards.requirement",
    buildRequirementBody({
      typical:
        "Selective Marriott Bonvoy participation may apply where property agreements support it—scope can vary by member hotel and market context.",
      owner:
        "Diligence distribution participation, recognition mechanics, channel obligations, and owner reporting tied to Bonvoy where applicable.",
      status: "Confirm with brand",
      notes:
        "Bonvoy participation and distribution scope may vary by property—confirm participation mechanics, benefits, and owner obligations during diligence.",
    }),
    { title: "Distribution & Bonvoy Participation" }
  ),
  row(
    "standards.requirement",
    buildRequirementBody({
      typical:
        "Cultural programming, local storytelling, and neighborhood context often reinforce collection differentiation in public materials and guest journeys.",
      owner:
        "Budget operating capacity for programming, partnerships, and guest experience rituals that make the design narrative tangible—not marketing language alone.",
      status: "Confirm with brand",
      notes:
        "Editorial and cultural content expectations should be mapped to operator staffing and F&B or events capacity before affiliation.",
    }),
    { title: "Cultural Programming & Local Story" }
  ),
  row(
    "standards.requirement",
    buildRequirementBody({
      typical:
        "Arrival, lobby, and signature public spaces typically carry disproportionate weight in design-curation evaluations and guest first impressions.",
      owner:
        "Sequence capital and FF&E to prioritize guest-facing design moments that collection review and guest reviews will scrutinize early.",
      status: "Confirm with brand",
      notes:
        "Confirm photography, signage, and public-space standards during onboarding—not only guestroom finish levels.",
    }),
    { title: "Public Spaces & Arrival Quality" }
  ),
  row(
    "standards.requirement",
    buildRequirementBody({
      typical:
        "Membership agreements govern affiliation term, standards maintenance, repositioning review, and exit or transfer provisions.",
      owner:
        "Review agreement structure, renewal triggers, and curation remediation paths with counsel—public Explorer content is not a contract substitute.",
      status: "Confirm with brand",
      notes:
        "Confirm termination, transfer, and design refresh obligations before treating affiliation as easily reversible.",
    }),
    { title: "Membership Agreement & Ongoing Curation" }
  ),
]);

/** Slot keys patched by this pack (excludes uncovered). */
export const DESIGN_HOTELS_STABILIZATION_SLOT_KEYS = Object.freeze([
  ...new Set(DESIGN_HOTELS_STABILIZATION_CONTENT.map((r) => r.slotKey)),
]);

/**
 * Slots not covered by this body pack—require separate Presentation rows or non-text remediation.
 */
export const DESIGN_HOTELS_STABILIZATION_UNCOVERED = Object.freeze([
  {
    slotKey: "insight.similar",
    reason:
      "Multi-row Similar Brands table needs two or more distinct brand-card rows (title/body per peer)—not fillable from a single body pack entry.",
  },
]);
