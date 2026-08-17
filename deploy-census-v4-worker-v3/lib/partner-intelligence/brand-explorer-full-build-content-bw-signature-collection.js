/**
 * Brand Explorer Tab Factory — full build content pack: BW Signature Collection.
 *
 * BWH Hotels / Best Western soft-brand collection for independent upper-midscale
 * and upscale hotels seeking distribution and loyalty participation while retaining
 * a property-specific identity. This is a more flexible, independent-identity
 * leaning path than BW Premier Collection's more elevated soft-brand positioning.
 *
 * Copy rules:
 * - Directional, owner-facing. No invented fees, ADR, FDD, Item 19, pipeline counts,
 *   or performance guarantees.
 * - Brand-specific; avoids generic Best Western umbrella boilerplate.
 * - No Company Validated claims or raw protocol URLs in Body fields.
 * - Distinguishes BW Signature from BW Premier Collection and selected soft-brand peers.
 */

const BRAND_SLUG = "bw-signature-collection";
const BRAND_NAME = "BW Signature Collection";
const PARENT_COMPANY = "BWH Hotels / Best Western";
const RECORD_ID = "recdeh1NsP4gjrv80";

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

const PRESENTATION = [
  // --- Positioning (Basics-backed slots, matches built-blocked convention) ---
  row(
    "Brand Positioning",
    "",
    "A soft-brand collection for independent upper-midscale and upscale hotels that want to retain a local identity while participating in BWH distribution, loyalty, and commercial systems. BW Signature Collection is generally the more flexible, independent-identity leaning collection option below BW Premier Collection's more elevated positioning.",
    10
  ),
  row(
    "Guest Psychographics Description",
    "",
    "Travelers looking for an independent-feeling hotel with recognizable booking, loyalty, and service infrastructure behind it. The appeal is practical character rather than a uniform lifestyle concept: guests can choose a locally expressed stay while owners connect the property to BWH's broader commercial platform.",
    11
  ),

  // --- Overview ---
  row(
    "overview.typical_use_case",
    "",
    "An existing independent hotel, conversion, or selective repositioning where ownership wants broader distribution and loyalty participation without replacing the asset's name, local story, or operating personality with a rigid hard-brand prototype. It is most relevant when upper-midscale or upscale positioning is credible but BW Premier Collection intensity is unnecessary.",
    20
  ),
  row(
    "overview.development_model",
    "",
    "Primarily a conversion-oriented soft-brand path for independently identified hotels, with requirements assessed against the individual asset rather than a single room-and-corridor template. Owners should confirm current admission criteria, improvement scope, systems integration, and market availability before treating the collection as a light-touch reflag.",
    21
  ),
  row(
    "overview.relative_positioning",
    "Relative Positioning",
    "BW Signature Collection is BWH's more flexible independent-hotel collection path for upper-midscale and upscale assets. BW Premier Collection generally signals a more elevated upscale soft-brand expression; Tribute Portfolio and Curio sit in different parent systems with their own design, service, and commercial expectations. Compare the actual asset bar—not just the soft-brand label.",
    22
  ),
  row(
    "overview.scenario.1",
    "Design-Led Independent Conversion",
    "An established independent with local recognition, usable physical product, and an owner who wants distribution and loyalty reach without surrendering the property's identity. BW Signature can be worth evaluating where a conventional hard-brand conversion would require more visible standardization; confirm the improvement plan and systems obligations before framing it as low-disruption.",
    30
  ),
  row(
    "overview.scenario.2",
    "Luxury-Leaning Independent Repositioning",
    "An asset improving guest rooms, public areas, or service consistency that needs a clearer commercial platform but does not require BW Premier Collection's more elevated positioning. The owner decision is whether the property can preserve its independent character while meeting the collection's current quality and guest-experience requirements.",
    31
  ),
  row(
    "overview.scenario.3",
    "Urban Or Resort Gateway Repositioning",
    "A locally differentiated hotel in a market where keeping its established name, story, or guest base matters. BW Signature offers a path to pair that local equity with BWH systems, provided ownership can fund required improvements and operate consistently enough for the intended upper-midscale or upscale positioning.",
    32
  ),
  row(
    "overview.why_value",
    "Why Value Is Strongest",
    "Value is strongest when an independent asset already has usable identity and market relevance, but needs a broader booking, loyalty, and commercial platform. It is weaker for a property seeking a turnkey prototype, a luxury repositioning, or a minimal-capital solution without willingness to meet collection and systems requirements.",
    33
  ),
  row(
    "overview.proof.1",
    "Individual Character, Not A Prototype",
    "BW Signature Collection is structured around independently identified hotels rather than one standardized visual prototype. That creates room for local naming and character, but owners should confirm which identity elements can remain and which guest-facing, operational, or digital standards still apply to the specific conversion.",
    40
  ),
  row(
    "overview.proof.2",
    "BWH Distribution And Loyalty Platform",
    "The affiliation proposition centers on access to BWH distribution, loyalty, sales, and operating infrastructure while the property retains its own identity. Owners should verify the applicable loyalty program participation, channel obligations, technology cutover, and commercial responsibilities in the actual agreement path.",
    41
  ),
  row(
    "overview.proof.3",
    "Upper-Upscale To Luxury-Leaning Range",
    "The collection can suit a broader range of upper-midscale and upscale independent conditions than a higher-intensity soft-brand path. Flexibility does not remove the need for quality review, property improvement work, service execution, or systems participation—those requirements should be confirmed asset by asset.",
    42
  ),
  row(
    "overview.proof.4",
    "Design Review As A Gate, Not A Checklist",
    "BW Premier Collection is the closer BWH comparison, but it is generally positioned for a more elevated upscale soft-brand expression. BW Signature can be the more practical option where an independent hotel needs platform support and identity retention without underwriting the same level of product or positioning ambition.",
    43
  ),
  row(
    "overview.featured_application",
    "Design-led independent conversion or new-build",
    "An upper-midscale or upscale independent hotel can use BW Signature Collection to keep a property-specific identity while connecting to BWH distribution and loyalty infrastructure. The owner case depends on conversion scope, operating capability, and commercial fit—confirming current admission, improvement, and systems requirements before committing capital.",
    44,
    {
      caseSummaryOverview:
        "Featured path for independently identified upper-midscale or upscale hotels seeking BWH platform participation without a hard-brand prototype.",
      caseSummaryBrandRelevance:
        "Matches BW Signature's flexible, independent-identity leaning collection lane, distinct from BW Premier Collection's more elevated positioning.",
      caseSummaryOwnerObjective:
        "Preserve useful local equity while funding the improvements and integration needed for a credible BWH-affiliated guest experience.",
      caseSummaryInterpretation:
        "Use as a conversion-fit lens—confirm current collection criteria and agreement terms directly with BWH; not a performance forecast.",
      caseSummaryTags: "soft-brand, independent identity, BWH, conversion, upper-midscale, upscale",
    }
  ),
  row(
    "overview.differentiators.identity",
    "Experience & Identity",
    [
      "Property-specific name and local identity rather than a fixed prototype",
      "Flexible collection lane for upper-midscale and upscale independents",
      "Independent character supported by a recognizable commercial platform",
      "More practical positioning than a higher-intensity upscale soft-brand path",
    ].join("\n"),
    45
  ),
  row(
    "overview.differentiators.commercial",
    "Commercial & Distribution",
    [
      "BWH distribution and booking-platform participation",
      "Loyalty ecosystem access subject to the applicable agreement",
      "Commercial infrastructure without replacing the property's identity",
      "Confirm channel, technology, sales, and loyalty requirements directly with BWH",
    ].join("\n"),
    46
  ),
  row(
    "overview.bestAt.1",
    "Design-Led Independent Character",
    "Keeping a locally meaningful hotel identity while adding BWH distribution and loyalty infrastructure—particularly relevant when a conventional hard-brand conversion would over-standardize the guest proposition.",
    47
  ),
  row(
    "overview.bestAt.2",
    "Broad Segment Range Under One Collection",
    "Providing a collection path for assets that can deliver credible upper-midscale or upscale quality but may not need BW Premier Collection's more elevated soft-brand positioning.",
    48
  ),
  row(
    "overview.bestAt.3",
    "BWH Systems Without A Fixed Prototype",
    "Helping owners connect an independent asset to BWH systems while preserving useful local naming and story—subject to improvement, operational, and commercial requirements for the individual property.",
    49
  ),
  row(
    "overview.portfolio_context",
    "Portfolio Context",
    "Within BWH's collection portfolio, BW Signature Collection is the more flexible independent-identity leaning path for upper-midscale and upscale hotels. BW Premier Collection is the closer elevated alternative; owners should compare quality bar, property-improvement scope, market fit, and desired guest positioning rather than assume the collections are interchangeable.",
    50
  ),
  row(
    "footprint.portfolio_context",
    "Portfolio Context",
    "BW Signature Collection gives BWH an independent-hotel conversion lane below or more flexible than BW Premier Collection's more elevated soft-brand posture. It can suit owners seeking commercial platform access without building a uniform branded expression, while still requiring diligence on standards, systems, and the asset's actual market position.",
    51
  ),
  row(
    "valueOwners.overview",
    "What Owners Are Buying",
    "BW Signature Collection gives owners of upper-midscale and upscale independents a more flexible BWH soft-brand path to keep a local hotel identity while connecting to distribution, loyalty, and commercial systems. The owner proposition is platform participation without a hard-brand prototype—generally below BW Premier Collection's more elevated design intensity, with obligations confirmed asset by asset.",
    51
  ),
  row(
    "valueOwners.watchouts",
    "",
    [
      "Independent identity does not mean no standards—confirm current quality review and improvement requirements",
      "Do not assume BW Signature and BW Premier Collection have the same product bar or capital implications",
      "Distribution and loyalty value depend on actual systems, channel, and agreement participation",
      "Protect useful local equity, but test whether the property can still meet a consistent upper-midscale or upscale guest expectation",
    ].join("\n"),
    52
  ),

  // --- Value to Owners: lifecycle ---
  row(
    "valueOwners.lifecycle.1",
    "Evaluation",
    "Start with the asset's own identity, physical condition, and market position. Test whether BWH platform participation would add more value than a hard-brand conversion or remaining independent, then compare BW Signature Collection with BW Premier Collection on quality bar, conversion scope, and the owner's desired level of flexibility.",
    300
  ),
  row(
    "valueOwners.lifecycle.2",
    "Conversion Design",
    "Build the conversion plan around the property elements worth preserving and the deficiencies that would weaken guest confidence. Prioritize guest rooms, arrival, public areas, service touchpoints, and digital readiness according to agreed collection requirements—not decorative changes disconnected from the intended positioning.",
    301
  ),
  row(
    "valueOwners.lifecycle.3",
    "Pre-Opening",
    "Sequence property improvements, BWH systems integration, loyalty and distribution setup, staffing, and commercial launch work. Confirm owner versus operator responsibilities and any collection-specific readiness milestones early, especially where the property keeps an existing name and needs clear guest-facing transition messaging.",
    302
  ),
  row(
    "valueOwners.lifecycle.4",
    "Opening",
    "Open with the independent identity and BWH commercial presence working together: guest-facing storytelling should remain clear while booking, loyalty, and service processes operate reliably. Confirm launch support, channel activation, and operational coverage rather than assuming affiliation alone creates an immediate market reset.",
    303
  ),
  row(
    "valueOwners.lifecycle.5",
    "Ramp-Up",
    "Use early guest feedback, channel mix, loyalty participation, and local-demand response to refine the property's positioning. Watch for a gap between the independent promise and operational consistency; a flexible brand model still requires reliable delivery across rooms, service, and core guest touchpoints.",
    304
  ),
  row(
    "valueOwners.lifecycle.6",
    "Ongoing",
    "Maintain the local identity intentionally while reviewing quality, technology, commercial participation, and lifecycle capital with the operator and BWH. Before major repositioning, renovation, or management change, reconfirm how the property remains within collection expectations and whether the affiliation still supports the owner's goals.",
    305
  ),

  // --- Operations & Standards: model ---
  row(
    "operations.model.primary_model",
    "",
    "Soft-brand affiliation for independently identified hotels within the BWH portfolio, delivered through the applicable franchise or other approved arrangement for the asset and market.",
    100
  ),
  row(
    "operations.model.management_option",
    "",
    "Third-party management or owner-led operations may be relevant depending on local market, agreement structure, and operator capability. The key test is whether the operating team can deliver consistent collection-quality guest experience while integrating BWH systems and commercial practices.",
    101
  ),
  row(
    "operations.model.typical_ownership",
    "",
    "Owners of established or repositioning independent upper-midscale and upscale hotels who want broader commercial reach without replacing useful local identity with a fixed hard-brand expression.",
    102
  ),
  row(
    "operations.model.brand_involvement",
    "",
    "BWH development and brand teams typically assess the asset's quality, guest proposition, conversion readiness, and platform integration. Confirm current review steps, approval responsibilities, and property-specific touchpoints directly with BWH.",
    103
  ),
  row(
    "operations.model.systems_integration",
    "",
    "Participation can involve BWH booking, loyalty, distribution, reporting, and operating systems. Owners should validate PMS/CRS requirements, training, data responsibilities, and implementation sequencing for the specific property.",
    104
  ),
  row(
    "operations.model.pre_opening",
    "",
    "Expect agreed property improvements, operating readiness, technology setup, training, and commercial activation before launch. Sequence these workstreams with financing and operator capacity; do not treat a soft-brand conversion as automatically light on coordination.",
    105
  ),
  row(
    "operations.model.staffing_intensity",
    "",
    "Staffing should fit the asset's actual upper-midscale or upscale service proposition and amenity mix. The collection does not eliminate the need for dependable front office, housekeeping, maintenance, and guest-recovery capability.",
    106
  ),
  row(
    "operations.model.fb_complexity",
    "",
    "F&B can range from limited to more developed depending on the individual independent asset. Owners should underwrite outlet scope, labor, and guest expectations from the property's market position—not assume the collection itself determines a single F&B model.",
    107
  ),
  row(
    "operations.model.training",
    "",
    "Training should cover BWH systems, loyalty and distribution practices, and the property's own guest proposition. Confirm the required training scope, sequence, and responsibility split as part of conversion planning.",
    108
  ),
  row(
    "operations.model.reporting_discipline",
    "",
    "Commercial, systems, and performance reporting expectations may apply through BWH participation. Confirm reporting cadence, data ownership, operator obligations, and how independent-property reporting will interface with platform requirements.",
    109
  ),
  row(
    "operations.model.qa_rhythm",
    "",
    "Quality review occurs around admission, conversion, and ongoing brand participation under current BWH processes. Confirm review cadence, guest-experience focus, and remediation process before treating affiliation as a permanent commercial solution.",
    110
  ),
  row(
    "operations.model.technology",
    "",
    "Technology participation is part of the affiliation decision, not an afterthought. Confirm booking, loyalty, guest-data, property-system, and digital requirements for the asset before setting capital and launch timelines.",
    111
  ),
  row(
    "operations.standards_philosophy",
    "",
    "BW Signature Collection balances independent property identity with BWH quality, commercial, and guest-experience expectations.\nDesign and conversion detail: Preserve meaningful local character while correcting the physical or service gaps that would undermine the intended position.\nPIP / lifecycle capital: Confirm required scope directly; flexibility is not evidence that investment will be minimal.\nSegment range: Benchmark against comparable upper-midscale or upscale independent hotels, not elevated BW Premier Collection or luxury soft-brand outliers.",
    112
  ),
  row(
    "operations.operator_compat.summary",
    "",
    "Operators need to preserve an independent hotel's local strengths while delivering consistent core operations and executing BWH systems, distribution, loyalty, and reporting requirements.",
    113
  ),
  row(
    "operations.operator_compat.fit",
    "",
    "Best fit: operators experienced in independent, select-service, or full-service upper-midscale and upscale hotels who can protect local character without losing operating discipline. Weaker fit: teams expecting a fully standardized prototype or lacking systems-integration and guest-recovery capability.",
    114
  ),
  row(
    "operations.operator_compat.tags",
    "",
    "BWH soft-brand\nIndependent identity\nUpper-midscale to upscale\nConversion-oriented",
    115
  ),

  // --- Operations & Standards: flexibility indicators ---
  row(
    "operations.flexibility.design",
    "",
    "High",
    200
  ),
  row(
    "operations.flexibility.conversion",
    "",
    "High",
    201
  ),
  row(
    "operations.flexibility.localization",
    "",
    "High",
    202
  ),
  row(
    "operations.flexibility.operational_rigidity",
    "",
    "Medium",
    203
  ),
  row(
    "operations.flexibility.pip",
    "",
    "Medium",
    204
  ),
  row(
    "operations.flexibility.prototype",
    "",
    "Low",
    205
  ),

  // --- Operations & Standards: compliance ---
  row(
    "operations.compliance.qa_cadence",
    "",
    "Quality review and corrective action can matter at admission, conversion, and ongoing participation. Owners should confirm current cadence, review criteria, and responsibility for remediating deficiencies before relying on the affiliation in long-range plans.",
    210
  ),
  row(
    "operations.compliance.training_rigor",
    "",
    "Opening teams need practical onboarding for BWH systems, loyalty and distribution practices, and the property's service proposition. Confirm required modules and timing during implementation rather than treating training as a post-opening cleanup item.",
    211
  ),
  row(
    "operations.compliance.reporting",
    "",
    "Platform participation may require commercial and operating reporting. Establish the owner, operator, and BWH responsibilities for data quality, cadence, and system access before conversion so independent processes are not disrupted unexpectedly.",
    212
  ),
  row(
    "operations.compliance.brand_interaction",
    "",
    "Brand interaction is likely to be most active through feasibility, conversion, system onboarding, and quality review. Confirm the actual contacts, decisions, and escalation path for the asset rather than extrapolating from another BWH brand or collection.",
    213
  ),

  // --- Economics & Obligations: opening path ---
  row(
    "economics.opening.step.1",
    "Application & Feasibility",
    "Present the independent asset's market context, ownership plan, physical condition, operating model, and local identity for BWH review. Clarify whether BW Signature Collection or BW Premier Collection better matches the intended position before committing to a capital plan.",
    400
  ),
  row(
    "economics.opening.step.2",
    "Design & Standards",
    "Agree the property-improvement, guest-experience, identity-retention, and collection-readiness scope with BWH. Address meaningful product and operational gaps first; do not reduce the workstream to cosmetic rebranding simply because the hotel retains its own name.",
    401
  ),
  row(
    "economics.opening.step.3",
    "Pre-Opening Planning",
    "Build a conversion plan for technology, distribution, loyalty, training, hiring, operating procedures, and guest communications alongside approved improvements. Confirm responsibility allocation, milestone approvals, and launch dependencies with BWH, the operator, and relevant vendors.",
    402
  ),
  row(
    "economics.opening.step.4",
    "Opening Support",
    "Coordinate property readiness with BWH commercial and systems activation. The opening should make the independent identity legible to guests while ensuring booking, loyalty, and guest-service processes operate reliably from day one.",
    403
  ),
  row(
    "economics.opening.step.5",
    "Stabilization",
    "Use guest feedback, channel performance, quality findings, and operating results to improve delivery after launch. Review labor, amenities, maintenance, and capital needs against the asset's own position; affiliation does not substitute for disciplined asset management.",
    404
  ),

  // --- Footprint & Growth ---
  row(
    "footprint.momentum",
    "Distinctive Independent Conversion Signals",
    "BWH presents BW Signature Collection as an option for independently identified hotels seeking broader commercial reach while retaining local character. This is directional positioning, not a property-level approval or pipeline signal—confirm current market appetite and collection availability with BWH.",
    450
  ),
  row(
    "footprint.momentum",
    "Adaptive Reuse And Heritage Conversion Emphasis",
    "The collection's relevance comes from offering a practical independent-hotel affiliation path below or more flexible than BW Premier Collection's more elevated posture. Owners should treat that as a positioning distinction, not a claim that acceptance, capital requirements, or operating obligations will be light.",
    451
  ),
  row(
    "footprint.momentum",
    "Resort And Gateway-City Expansion Interest",
    "BW Signature Collection can be relevant where an independent asset needs a recognizable booking and loyalty platform without discarding local equity. The decision remains market-specific: validate comparable hotels, channel demand, conversion scope, and BWH development interest for the individual opportunity.",
    452
  ),
  row(
    "footprint.portfolio_mix",
    "Portfolio mix",
    "Independent upper-midscale hotels\nIndependent upscale hotels\nConversion and repositioning candidates\nLocally identified market hotels",
    460
  ),
  row(
    "footprint.geo_intro",
    "Geographic footprint",
    "BW Signature Collection is designed for independent-hotel affiliation within BWH's broader network. Geographic relevance and commercial value vary by market; owners should assess local distribution need, competitive set, and current BWH authorization rather than infer a uniform conversion case from the parent platform.",
    470
  ),
  row(
    "footprint.region.am",
    "Americas",
    "The Americas provide a practical context for independent-hotel conversion and BWH commercial-platform evaluation. Owners should compare local upper-midscale and upscale independent supply, BWH customer relevance, and conversion economics for the specific market rather than rely on parent-level footprint alone.",
    471
  ),
  row(
    "footprint.region.cala",
    "CALA",
    "CALA applicability is market-specific. Independent owners should confirm country-level collection availability, systems readiness, loyalty relevance, and local commercial support directly with BWH before using broader regional brand recognition as a conversion assumption.",
    472
  ),
  row(
    "footprint.region.eu",
    "Europe",
    "European independent-hotel markets can illustrate the value of preserving local property identity while adding a distribution platform. For other regions, use this as contextual reference only; local standards, competitive supply, and commercial participation must be assessed separately.",
    473
  ),
  row(
    "footprint.region.mea",
    "MEA",
    "MEA relevance depends on BWH's current country and market strategy, plus the individual asset's positioning and operating readiness. Confirm authorization and support directly rather than assuming the collection follows the same footprint as every BWH brand.",
    474
  ),
  row(
    "footprint.region.apac",
    "APAC",
    "APAC relevance should be evaluated market by market, with attention to BWH's current distribution support, local independent supply, and the property's target guest mix. Treat broader network recognition as context, not evidence of a specific conversion pathway.",
    475
  ),
  row(
    "footprint.growth_themes",
    "",
    "Independent-hotel conversion\nUpper-midscale and upscale repositioning\nLocal-identity retention\nCommercial-platform integration",
    480
  ),
  row(
    "footprint.growth_editorial",
    "",
    "BW Signature Collection can make sense when an independent hotel's local equity is worth preserving and BWH platform participation addresses a genuine distribution or loyalty need. The affiliation case is weakest when an asset needs a fixed prototype, elevated soft-brand repositioning, or capital-light outcome without operational change.",
    481
  ),
  row(
    "footprint.growth_fit",
    "",
    "Best growth fit: established or repositioning upper-midscale and upscale independents with defensible local identity and an owner willing to meet BWH requirements. Weaker fit: generic assets needing a turnkey hard-brand formula, or hotels better suited to BW Premier Collection's more elevated aspiration.",
    482
  ),

  // --- Owner Considerations ---
  row(
    "standards.intro",
    "",
    "BW Signature Collection standards should be understood as the operating and quality framework that makes independent identity commercially credible within BWH—not as a promise of zero standardization. Confirm current details, admission criteria, and improvement requirements directly for the specific asset.",
    600
  ),
  row(
    "standards.requirement",
    "Design & narrative review",
    "Assess the guest journey, physical condition, cleanliness, maintenance, service consistency, and public-facing identity against current BWH expectations. Confirm the review scope and priorities for the individual property.",
    601
  ),
  row(
    "standards.requirement",
    "Bonvoy systems participation",
    "Booking, loyalty, distribution, reporting, and property-technology requirements may apply. Validate technical scope, timing, vendors, and operational responsibilities before setting the conversion critical path.",
    602
  ),
  row(
    "standards.requirement",
    "F&B and public-space capital",
    "Required improvements should close the gaps between the asset's current condition and the intended collection position while preserving worthwhile independent identity. Confirm scope, approvals, and sequencing rather than assuming a limited refresh.",
    603
  ),
  row(
    "standards.requirement",
    "Guest-room and suite standards",
    "Guest rooms, bathrooms, arrival, circulation, and public spaces should support the hotel's stated upper-midscale or upscale value proposition. Confirm required baselines and which distinctive property elements can remain.",
    604
  ),
  row(
    "standards.requirement",
    "Training and service culture",
    "Teams need consistent operating practices alongside BWH systems knowledge and the property's own service character. Confirm opening and ongoing training, reporting, and quality-management expectations with the assigned BWH contacts.",
    605
  ),
  row(
    "standards.requirement",
    "Ongoing design and QA review",
    "Collection participation may require continued quality and guest-experience review after conversion. Confirm cadence, remediation triggers, responsibilities, and how major renovation or management changes affect ongoing eligibility.",
    606
  ),
  row(
    "standards.conversion",
    "",
    "Conversion suitability depends on whether the independent asset can preserve credible local identity while reliably meeting current BWH quality, systems, and commercial requirements. Test the agreed improvement scope and operator readiness before committing capital or a public transition plan.",
    607
  ),
  row(
    "standards.questions",
    "Questions owners should ask",
    [
      "What makes BW Signature Collection a better fit for this asset than BW Premier Collection or a conventional BWH hard brand?",
      "Which local identity elements can remain, and which guest-facing or operational changes are required?",
      "What property-improvement, quality-review, and remediation obligations apply at conversion and after opening?",
      "What booking, loyalty, technology, reporting, and commercial participation requirements apply to this specific agreement?",
      "Who owns implementation, training, guest communication, and corrective action if the property falls short of collection expectations?",
    ].join("\n"),
    608
  ),

  // --- Dealality Insight: similar brands ---
  row(
    "insight.similar.1",
    "Tribute Portfolio",
    "The closest BWH comparison, generally carrying a more elevated upscale soft-brand expression. Compare product ambition, required improvements, guest expectation, and owner appetite for standards intensity; BW Signature can be the more flexible independent-identity leaning choice.",
    700
  ),
  row(
    "insight.similar.2",
    "Design Hotels",
    "A Marriott soft-brand peer for independent hotels with its own lifestyle and commercial framing. Compare property character, systems requirements, loyalty reach, design expectations, and conversion capital rather than treating both as interchangeable flexible affiliations.",
    701
  ),
  row(
    "insight.similar.3",
    "MGallery Collection",
    "A Hilton soft-brand peer often considered for distinctive independents seeking a large global platform. Compare the asset's segment, local identity, operating complexity, commercial reach, and improvement requirements against BW Signature's more practical upper-midscale-to-upscale collection lane.",
    702
  ),
];

export const BRAND_FULL_BUILD_CONTENT = Object.freeze({
  brandSlug: BRAND_SLUG,
  sourcePack: Object.freeze({
    canonicalSite: "bestwestern.com — BW Signature Collection",
    developmentPage: "bwhhotels.com — Development and brand portfolio context",
    propertyPages:
      "Individual BW Signature Collection property pages on bestwestern.com — use for property identity and market context; do not embed raw property URLs in owner-facing Body copy.",
    parentContextPages: [
      "bestwestern.com — BWH Hotels guest and loyalty context",
      "bwhhotels.com — BWH Hotels development and portfolio context",
    ],
    imageSources:
      "BWH Hotels and individual property galleries; use for identity and design reference only — confirm licensing before external use.",
    domains: ["bestwestern.com", "bwhhotels.com"],
  }),
  brandLens: Object.freeze({
    brandModel:
      "BWH soft-brand collection for independently identified upper-midscale and upscale hotels seeking BWH distribution, loyalty, and commercial participation while retaining a property-specific identity.",
    ownerFit:
      "Owners of established or repositioning independent hotels who value local equity and need a broader commercial platform without a fixed hard-brand prototype.",
    propertyFit:
      "Upper-midscale and upscale independent hotels with credible product condition, local identity, and operating discipline; particularly conversion or repositioning candidates.",
    conversionLogic:
      "Retain worthwhile identity while resolving the physical, service, technology, and commercial gaps required for collection participation; confirm improvement scope directly rather than assuming a light reflag.",
    operatingImplications:
      "Independent expression remains meaningful, but BWH quality, systems, loyalty, distribution, training, and reporting requirements create operating structure that owners and operators must plan for.",
    standardsRequirements:
      "Current asset review, property improvement, guest-experience quality, systems participation, and ongoing review requirements should be confirmed directly with BWH for the individual market and agreement.",
    sourceLimitations:
      "Public BWH materials describe brand positioning and parent-platform context, not property-level terms, fees, performance, or approval outcomes. Confirm agreement-specific and market-specific details directly with BWH. This pack is AI-assisted, source-informed owner-useful context rather than company validation.",
    distinguishFrom:
      "BW Premier Collection (more elevated upscale soft-brand posture within BWH), Tribute Portfolio (Marriott soft-brand peer with different systems and lifestyle framing), and Curio Collection by Hilton (Hilton soft-brand peer with different segment and commercial expectations).",
  }),
  presentation: PRESENTATION,
});

export default BRAND_FULL_BUILD_CONTENT;
