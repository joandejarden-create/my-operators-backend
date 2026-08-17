/**
 * Brand Explorer Tab Factory — full build content pack: BW Premier Collection.
 *
 * BWH Hotels' upscale soft-brand collection for independent hotels seeking
 * Best Western platform participation while retaining a more elevated,
 * design-conscious identity than the core Best Western family.
 *
 * Copy rules:
 * - Directional, owner-facing; no fees, FDD data, performance claims, or pipeline counts.
 * - Brand-specific; distinguishes BW Premier Collection from BW Signature Collection.
 * - No Company Validated claims or raw https:// URLs in Body fields.
 */

const BRAND_SLUG = "bw-premier-collection";
const BRAND_NAME = "BW Premier Collection";
const PARENT_COMPANY = "BWH Hotels / Best Western";
const RECORD_ID = "recwXZ5gVZ8ZH8ekA";

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
  row("Brand Positioning", "", "BW Premier Collection is BWH Hotels' more upscale, design-conscious soft-brand path for independent hotels. It pairs a property-specific identity with access to Best Western commercial and loyalty infrastructure, while maintaining a higher design and guest-experience bar than core Best Western flags.", 10),
  row("Guest Psychographics Description", "", "Upscale travelers seeking a distinctive independent hotel with the reassurance of a recognizable loyalty and distribution platform. The guest proposition favors elevated design, local character, and full-service or high-touch experiences over a standardized midscale stay.", 11),

  row("overview.typical_use_case", "", "Independent upscale hotels, boutique urban assets, and destination properties with a credible design point of view that seek BWH platform benefits without adopting a core Best Western prototype. The strongest candidates already support a differentiated guest experience and a clear market position.", 20),
  row("overview.development_model", "", "Most relevant to conversion, repositioning, and selected new-build opportunities where the owner wants to retain the asset's own identity. Underwrite the required product, design, systems, and service work from the specific agreement and review process rather than assuming a light reflag.", 21),
  row("overview.relative_positioning", "Relative Positioning", "BW Premier Collection sits above BW Signature Collection in BWH's independent-hotel spectrum: Premier generally calls for a more upscale physical product, stronger design expression, and elevated guest experience. It remains more flexible than a conventional prototype-led flag, but is not a no-standards affiliation.", 22),
  row("overview.scenario.1", "Design-Led Independent Conversion", "An established independent hotel with strong location, service culture, and differentiated design that wants broader distribution and loyalty participation. Premier is worth evaluating when the property can sustain an upscale guest promise and ownership wants to preserve its own identity rather than convert to a core flag.", 30),
  row("overview.scenario.2", "Luxury-Leaning Independent Repositioning", "A well-located hotel requiring a sharper upscale identity, public-space refresh, or service repositioning. Premier can provide a BWH platform path when the asset's design and experience can credibly move beyond core Best Western expectations; confirm the scope of required capital and review milestones first.", 31),
  row("overview.scenario.3", "Urban Or Resort Gateway Repositioning", "A boutique urban, resort, or destination asset where local character is commercially important and owner control over positioning matters. Premier may fit when the property needs distribution support without losing its independent narrative; test local comp positioning, operator capability, and systems readiness before proceeding.", 32),
  row("overview.why_value", "Why Value Is Strongest", "Value is strongest where the hotel already has a differentiated upscale story, ownership values independent positioning, and BWH distribution can expand reach. It is a weaker fit for generic assets seeking only a familiar sign, or for properties unable to support the product and service expectations of an upscale collection.", 33),
  row("overview.proof.1", "Individual Character, Not A Prototype", "BW Premier Collection is designed for properties that retain their own identity while participating in the BWH platform. Owners should view this as a balance of independence and standards: the property's story remains central, but the experience must support an elevated collection position.", 40),
  row("overview.proof.2", "Best Western Distribution And Loyalty", "The affiliation can connect an independent hotel to Best Western's loyalty, reservation, distribution, and commercial ecosystem. The practical value depends on the specific market, agreement path, and systems requirements, which should be confirmed directly during development and conversion diligence.", 41),
  row("overview.proof.3", "Upper-Upscale To Luxury-Leaning Range", "Premier is generally the more upscale and design-intensive BWH collection option relative to BW Signature Collection. Owners should compare product condition, public-space ambition, service delivery, and intended rate position against both paths rather than treating them as interchangeable collection labels.", 42),
  row("overview.proof.4", "Design Review As A Gate, Not A Checklist", "The collection model can preserve a property's individual expression, but acceptance and ongoing participation still depend on meeting applicable BWH requirements. Confirm the current review criteria, improvement expectations, and operating obligations for the asset instead of assuming independent positioning eliminates brand discipline.", 43),
  row("overview.featured_application", "Design-led independent conversion or new-build", "A differentiated independent hotel can use BW Premier Collection to retain its own identity while adding BWH loyalty and commercial infrastructure. The owner case depends on whether product quality, design, operating capability, and conversion capital can support an upscale collection position—not solely on brand recognition.", 44, {
    caseSummaryOverview: "Featured path for independent upscale hotels seeking BWH platform participation without a core-brand prototype.",
    caseSummaryBrandRelevance: "Premier provides a more elevated, design-conscious collection lane than BW Signature Collection.",
    caseSummaryOwnerObjective: "Protect property identity while funding the systems, product, and service work needed for an upscale conversion.",
    caseSummaryInterpretation: "Use as a conversion-fit lens; confirm requirements, territory, and commercial terms directly with BWH.",
    caseSummaryTags: "soft-brand, upscale, independent, BWH, conversion",
  }),
  row("overview.differentiators.identity", "Experience & Identity", ["Independent hotel identity remains prominent", "Upscale and design-conscious collection positioning", "Local character can shape the guest experience", "More elevated expression than BW Signature Collection"].join("\n"), 45),
  row("overview.differentiators.commercial", "Commercial & Distribution", ["Best Western Rewards participation and BWH commercial ecosystem", "Reservation and distribution infrastructure for affiliated hotels", "Platform benefits without a fixed core-brand prototype", "Confirm agreement-specific commercial and systems obligations directly"].join("\n"), 46),
  row("overview.bestAt.1", "Design-Led Independent Character", "Supporting independent hotels that need a more elevated BWH collection position than a core Best Western flag, while preserving local design and service expression.", 47),
  row("overview.bestAt.2", "Broad Segment Range Under One Collection", "Giving owners a conversion path that can retain property identity and operating choice, subject to the collection's applicable standards, systems, and quality-review obligations.", 48),
  row("overview.bestAt.3", "BWH Systems Without A Fixed Prototype", "Layering Best Western loyalty, distribution, and commercial infrastructure beneath an independently positioned upscale hotel rather than recasting the asset as a standardized prototype.", 49),
  row("overview.portfolio_context", "Portfolio Context", "Within BWH's independent-hotel offering, BW Premier Collection is the more upscale, higher-design-intensity choice relative to BW Signature Collection. Owners should compare the two on product readiness, desired guest experience, capital scope, and degree of independent expression before selecting a path.", 50),
  row("footprint.portfolio_context", "Portfolio Context", "BW Premier Collection represents BWH's upscale collection lane for differentiated independent hotels. It should be assessed separately from BW Signature Collection, which generally offers a more accessible and less design-intensive conversion position within the same broader platform.", 51),
  row("valueOwners.overview", "What Owners Are Buying", "BW Premier Collection gives owners of differentiated upscale independents a BWH soft-brand path that pairs property-specific identity with loyalty, distribution, and commercial infrastructure. The owner proposition is elevated collection positioning with more independent expression than a core Best Western flag—subject to product, design, systems, and quality obligations confirmed for the specific asset.", 51),
  row("valueOwners.watchouts", "", ["Premier positioning requires more than an independent nameplate; test product, design, and service readiness honestly", "Compare Premier and Signature against the asset's actual segment, capital plan, and market comp set", "Confirm current acceptance, improvement, systems, and quality-review requirements for the specific deal", "Do not assume owner control removes the need for operating discipline inside the BWH platform"].join("\n"), 52),

  row("valueOwners.lifecycle.1", "Evaluation", "Start with the asset's existing identity, physical condition, target guest, and local comp set. Determine whether the hotel can credibly support an upscale collection position and whether Premier offers a better owner-control and distribution balance than BW Signature Collection or a core BWH brand.", 300),
  row("valueOwners.lifecycle.2", "Conversion Design", "Translate the property's independent story into visible guest experience: arrival, rooms, public spaces, F&B, and service standards should align. Prioritize work that closes genuine product and operating gaps, and sequence BWH review milestones with financing, design, and operator decisions.", 301),
  row("valueOwners.lifecycle.3", "Pre-Opening", "Coordinate systems integration, loyalty readiness, staffing, sales setup, and training with conversion completion. Establish clear owner, operator, and BWH responsibilities for the launch, and avoid treating the collection affiliation as a substitute for disciplined pre-opening planning.", 302),
  row("valueOwners.lifecycle.4", "Opening", "Launch the hotel with its own upscale identity consistently expressed across service, digital channels, and public spaces while BWH systems stabilize. Confirm opening support, quality review, and escalation paths before launch so the operator can resolve issues without diluting the property's positioning.", 303),
  row("valueOwners.lifecycle.5", "Ramp-Up", "Use early guest feedback, channel mix, and operational performance to refine the independent experience and local positioning. Watch whether staffing, amenity delivery, and public-space programming actually support the intended upscale promise, rather than relying only on affiliation visibility.", 304),
  row("valueOwners.lifecycle.6", "Ongoing", "Maintain the asset's differentiated character while meeting applicable BWH quality, systems, and commercial obligations. Revisit capital needs, operator alignment, and market positioning as the hotel matures, and clarify review or remediation expectations before major changes or renewals.", 305),

  row("operations.model.primary_model", "", "Upscale soft-brand collection affiliation within BWH Hotels, structured through the agreement path available for the relevant market and property. Owners should confirm whether franchise, management, or another arrangement is applicable to the specific opportunity.", 100),
  row("operations.model.management_option", "", "Third-party management can suit independent upscale assets when the operator can protect property character while working within BWH systems. Owner-operated models require credible leadership, service discipline, and capacity to meet collection expectations.", 101),
  row("operations.model.typical_ownership", "", "Owners of differentiated independent hotels who want a more upscale BWH collection position, platform support, and continued control over the property's local identity.", 102),
  row("operations.model.brand_involvement", "", "BWH development and brand teams may engage on conversion readiness, product presentation, systems, and quality expectations. Confirm the current review stages, documentation, and interaction cadence for the individual asset.", 103),
  row("operations.model.systems_integration", "", "Premier hotels participate in the relevant BWH reservation, loyalty, distribution, and technology ecosystem. Validate PMS, CRS, training, digital, and commercial integration requirements before committing to a conversion timeline.", 104),
  row("operations.model.pre_opening", "", "Expect product readiness, systems setup, team training, and commercial-launch work before opening or relaunch. Sequence these requirements with financing and construction so operational readiness does not become the critical path.", 105),
  row("operations.model.staffing_intensity", "", "Staffing should match an upscale independent guest experience, not a minimal core-brand operating model. Front office, housekeeping, service recovery, and any food-and-beverage offer should be underwritten to the hotel's intended positioning.", 106),
  row("operations.model.fb_complexity", "", "F&B complexity varies by property, but public spaces and dining can materially shape Premier's upscale identity. Review outlet concept, service hours, kitchen scope, and operator capability against local demand rather than applying a uniform collection assumption.", 107),
  row("operations.model.training", "", "Training should connect BWH platform expectations with the hotel's own service and design identity. Confirm required modules, timing, delivery responsibilities, and ongoing refresh expectations as part of the pre-opening plan.", 108),
  row("operations.model.reporting_discipline", "", "BWH systems and commercial participation create reporting and operating rhythms that owners should understand during diligence. Confirm available owner reporting, data access, and operator responsibilities for the particular agreement.", 109),
  row("operations.model.qa_rhythm", "", "Quality and brand review support the collection's upscale guest promise at conversion and during operations. Confirm the current cadence, review focus, corrective-action process, and responsibility for remediation before relying on affiliation value in underwriting.", 110),
  row("operations.model.technology", "", "Technology participation should be reviewed as a conversion workstream, not an afterthought. Validate required BWH systems, connectivity, digital distribution, loyalty integration, implementation support, and any asset-specific constraints.", 111),
  row("operations.standards_philosophy", "", "BW Premier Collection seeks an elevated independent experience supported by BWH platform standards rather than a single prototype.\nDesign and conversion detail: each property should retain its own story while meeting applicable quality expectations.\nPIP / lifecycle capital: establish scope from the actual asset review; do not presume a cosmetic refresh is sufficient.\nSegment fit: compare the hotel with relevant upscale Premier and local independent competitors, not with all BWH brands.", 112),
  row("operations.operator_compat.summary", "", "Operators need to deliver an upscale, independent guest experience while maintaining BWH systems, loyalty, commercial, and quality obligations. The operator must protect local character without treating the collection as operationally unstructured.", 113),
  row("operations.operator_compat.fit", "", "Best fit: operators experienced in independent or boutique upscale hotels with strong service, revenue, and systems discipline. Weaker fit: operators optimized only for standardized select-service models or unable to sustain a differentiated property identity.", 114),
  row("operations.operator_compat.tags", "", "BWH soft-brand\nUpscale independent\nDesign-conscious\nConversion-oriented", 115),

  row("operations.flexibility.design", "", "High", 200),
  row("operations.flexibility.conversion", "", "High", 201),
  row("operations.flexibility.localization", "", "High", 202),
  row("operations.flexibility.operational_rigidity", "", "Medium", 203),
  row("operations.flexibility.pip", "", "Medium", 204),
  row("operations.flexibility.prototype", "", "Low", 205),

  row("operations.compliance.qa_cadence", "", "Quality review may be most important at conversion, opening, and when a property needs remediation. Confirm current review timing, standards emphasis, scoring, and escalation procedures directly for the proposed affiliation.", 210),
  row("operations.compliance.training_rigor", "", "Training should prepare teams for both BWH participation and a differentiated upscale guest experience. Define who trains whom, how readiness is assessed, and what refresh work is expected after opening.", 211),
  row("operations.compliance.reporting", "", "Owners should clarify BWH reporting, loyalty, distribution, and revenue-management obligations alongside the operator's reporting role. Agreement-level expectations may vary and should not be inferred from the collection label alone.", 212),
  row("operations.compliance.brand_interaction", "", "Brand interaction typically centers on development, conversion, systems, quality, and commercial readiness. Establish a practical calendar of decision points and contacts so owner, operator, and BWH teams can resolve issues before they affect launch or guest experience.", 213),

  row("economics.opening.step.1", "Application & Feasibility", "Present the asset's market context, ownership objectives, product condition, and independent identity for BWH review. Test whether Premier's upscale collection position is credible for the property, and compare the path with BW Signature Collection before allocating detailed conversion capital.", 400),
  row("economics.opening.step.2", "Design & Standards", "Align design, guest rooms, public spaces, amenities, service model, and technology with the property's intended upscale position and applicable BWH requirements. Treat this as an asset-specific conversion plan rather than a generic flag-change checklist.", 401),
  row("economics.opening.step.3", "Pre-Opening Planning", "Build the implementation plan around systems, loyalty readiness, training, staffing, sales, marketing, and operating procedures. Clarify the responsibilities of owner, operator, vendors, and BWH, and tie critical milestones to construction completion and commercial launch.", 402),
  row("economics.opening.step.4", "Opening Support", "Coordinate relaunch communications, systems go-live, quality readiness, and guest-service recovery plans with the operator and BWH contacts. Keep the property's independent story prominent while ensuring the platform components work reliably from the first guest stay.", 403),
  row("economics.opening.step.5", "Stabilization", "Use the stabilized operating period to refine service, channel strategy, and local positioning against actual guest feedback and market response. Reassess capital, staffing, and operating assumptions through the asset's performance, not as a substitute for agreement-level diligence.", 404),

  row("footprint.momentum", "Distinctive Independent Conversion Signals", "BW Premier Collection remains relevant for differentiated independent hotels seeking an upscale BWH platform path. This is directional collection context, not a statement about current property-level pipeline volume; confirm present development priorities and market availability directly with BWH.", 450),
  row("footprint.momentum", "Adaptive Reuse And Heritage Conversion Emphasis", "The Premier collection position gives owners a distinct option when a hotel requires more upscale design expression and guest-experience ambition than BW Signature Collection. Treat the distinction as a product-fit consideration and validate the current acceptance bar for the individual asset.", 451),
  row("footprint.momentum", "Resort And Gateway-City Expansion Interest", "Premier's continuing relevance is tied to independent hotels that want commercial infrastructure without abandoning their local identity. Owners should assess whether BWH platform participation strengthens their market story or creates obligations that exceed the asset's operational readiness.", 452),
  row("footprint.portfolio_mix", "Portfolio mix", "Upscale independent hotels\nBoutique urban properties\nDestination and resort-oriented assets\nDesign-led conversions and repositionings", 460),
  row("footprint.geo_intro", "Geographic footprint", "BW Premier Collection serves as an international BWH collection option for differentiated independent hotels, with actual availability and depth varying by market. Owners should confirm local development focus, comparable properties, and distribution relevance rather than inferring fit from global brand presence.", 470),
  row("footprint.region.am", "Americas", "Americas markets provide a useful reference point for BWH's independent-hotel platform, particularly where owners seek recognizable distribution without a full prototype conversion. Confirm local Premier presence, segment gaps, and development interest for the target market.", 471),
  row("footprint.region.cala", "CALA", "CALA opportunities should be evaluated with market-specific BWH development input, not broad global assumptions. A differentiated urban or destination hotel may be relevant where platform distribution and owner-controlled identity align, subject to local authorization and conversion requirements.", 472),
  row("footprint.region.eu", "Europe", "European independent-hotel markets can illustrate the collection logic of local character supported by broader distribution. For a specific deal, test country-level systems, development, and operating considerations rather than applying another market's conversion experience.", 473),
  row("footprint.region.mea", "MEA", "MEA relevance is market-specific and should be confirmed directly with BWH for the proposed country and asset. Treat wider BWH presence as context, not evidence that the Premier collection path is available or commercially equivalent in every market.", 474),
  row("footprint.region.apac", "APAC", "APAC can provide international brand-recognition context for travelers, but property-level feasibility remains dependent on local BWH development strategy and systems support. Do not use global collection presence as a proxy for an individual market's conversion case.", 475),
  row("footprint.growth_themes", "", "Upscale independent conversions\nDesign-conscious repositionings\nBoutique urban hotel affiliation\nDestination-property platform access", 480),
  row("footprint.growth_editorial", "", "BW Premier Collection is most compelling when an independent hotel's elevated product and local identity are already credible, and BWH participation can add practical distribution value. Growth themes remain directional; owners should independently diligence local comps, capital scope, operator readiness, and agreement terms.", 481),
  row("footprint.growth_fit", "", "Best growth fit: differentiated independent hotels ready for an upscale BWH collection position. Weaker fit: generic properties seeking a low-change affiliation, assets better aligned to BW Signature Collection, or hotels unable to sustain an elevated guest experience.", 482),

  row("standards.intro", "", "BW Premier Collection standards should support an upscale, differentiated independent experience alongside BWH platform participation. Current acceptance, product, technology, training, and quality details must be confirmed directly with BWH for the specific asset and market.", 600),
  row("standards.requirement", "Design & narrative review", "The property should present a credible upscale experience through rooms, public spaces, arrival, and overall design. Confirm how BWH evaluates existing conditions, proposed improvements, and collection-level differentiation for the asset.", 601),
  row("standards.requirement", "Bonvoy systems participation", "Reservation, loyalty, distribution, and related platform systems may form part of the affiliation. Confirm required technology, implementation sequencing, training, and responsibilities with BWH and the selected operator.", 602),
  row("standards.requirement", "F&B and public-space capital", "Public spaces, amenities, and any F&B offer should support the hotel's intended upscale identity and local guest demand. Establish required versus elective improvements before finalizing a conversion budget.", 603),
  row("standards.requirement", "Guest-room and suite standards", "Guest rooms and suites should align with the property's differentiated upscale concept while meeting applicable collection expectations. Validate room-product gaps, accessibility work, amenity requirements, and design flexibility as part of diligence.", 604),
  row("standards.requirement", "Training and service culture", "Team training should connect BWH platform participation with the property's own service promise. Define the service standards, onboarding sequence, and ownership of ongoing coaching before the hotel opens or relaunches.", 605),
  row("standards.requirement", "Ongoing design and QA review", "Ongoing quality expectations preserve the collection's upscale positioning after conversion. Confirm review timing, performance thresholds, remediation process, and the capital or operating responsibilities associated with any corrective action.", 606),
  row("standards.conversion", "", "Conversion suitability depends on the hotel's ability to deliver an upscale independent experience within BWH systems, not merely its desire for a recognizable platform. Compare Premier and Signature carefully, and establish the actual product, service, and technology scope before committing capital.", 607),
  row("standards.questions", "Questions owners should ask", ["What product, design, and service characteristics distinguish Premier from BW Signature Collection for a hotel like ours?", "Which improvements are required before conversion, and how are they prioritized and reviewed?", "What loyalty, reservation, technology, and commercial systems must the property implement?", "How much independent design and operating control remains after affiliation?", "What quality-review cadence, remediation process, and owner or operator responsibilities apply after opening?"].join("\n"), 608),

  row("insight.similar.1", "Tribute Portfolio", "BW Signature Collection is the closest BWH peer for independent hotels, generally positioned at a more accessible and less design-intensive level. Compare the asset's product quality, desired guest experience, capital scope, and owner-control needs before choosing between Signature and Premier.", 700),
  row("insight.similar.2", "Design Hotels", "Autograph Collection is a Marriott soft-brand peer for distinctive upscale and luxury-leaning independents. Compare parent-system scale, design-review expectations, loyalty platform, and the degree of operational flexibility appropriate for the property's market position.", 701),
  row("insight.similar.3", "MGallery Collection", "Tribute Portfolio is a Marriott collection peer for independent-character hotels with lifestyle positioning. Compare segment ambition, conversion discipline, systems requirements, and owner control against Premier rather than assuming all soft-brand options deliver the same operating model.", 702),
];

export const BRAND_FULL_BUILD_CONTENT = Object.freeze({
  brandSlug: BRAND_SLUG,
  sourcePack: Object.freeze({
    canonicalSite: "bestwestern.com — BW Premier Collection brand context",
    developmentPage: "bwhhotels.com — BWH Hotels development and brand context",
    propertyPages:
      "Individual BW Premier Collection property pages on bestwestern.com; use for property-specific design and location context, not as universal requirements.",
    parentContextPages: [
      "bestwestern.com — Best Western and BW Premier Collection brand context",
      "bwhhotels.com — BWH Hotels company and development context",
    ],
    imageSources:
      "bestwestern.com and bwhhotels.com brand or property galleries; confirm licensing before external use.",
    domains: ["bestwestern.com", "bwhhotels.com"],
  }),
  brandLens: Object.freeze({
    brandModel:
      "BWH Hotels upscale soft-brand collection for independently positioned hotels seeking Best Western platform participation without a core-brand prototype.",
    ownerFit:
      "Owners of differentiated upscale independent hotels who value local identity, distribution support, loyalty participation, and a conversion path with more flexibility than a conventional hard brand.",
    propertyFit:
      "Boutique urban hotels, destination assets, and design-conscious independent conversions with credible upscale product, service, and market positioning.",
    conversionLogic:
      "Premier is generally the more upscale and higher-design-intensity collection option relative to BW Signature Collection; determine capital, systems, and quality requirements from the asset-specific review.",
    operatingImplications:
      "Independent identity remains visible, but BWH systems, loyalty, commercial participation, quality expectations, and an upscale service model require disciplined execution.",
    standardsRequirements:
      "Product, design, systems, training, and ongoing quality expectations should be confirmed directly with BWH for the relevant market and agreement.",
    sourceLimitations:
      "Public BWH materials provide brand positioning and platform context, not agreement-specific economics, acceptance decisions, pipeline counts, or performance outcomes. Confirm those matters directly with BWH.",
    distinguishFrom:
      "BW Signature Collection (generally more accessible and less design-intensive within BWH), Autograph Collection, and Tribute Portfolio (Marriott soft-brand peers with different platform scale and collection requirements).",
  }),
  presentation: PRESENTATION,
});

export default BRAND_FULL_BUILD_CONTENT;
