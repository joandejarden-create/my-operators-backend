/**
 * Brand Explorer Tab Factory — full build content pack: Preferred Hotels & Resorts.
 *
 * Independent-hotel representation and affiliation platform, not a conventional
 * franchise prototype. Copy is owner-facing, source-informed, and deliberately
 * excludes fees, performance claims, pipeline counts, and agreement assumptions.
 */

const BRAND_SLUG = "preferred-hotels-and-resorts";
const BRAND_NAME = "Preferred Hotels & Resorts";
const PARENT_COMPANY = "Preferred Hotels & Resorts";
const RECORD_ID = "recwl5JOYxlChuCAr";

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
  row("Brand Positioning", "", "A global representation and affiliation platform for independent luxury and upscale hotels. Preferred Hotels & Resorts helps member properties retain their identity and owner control while accessing sales, marketing, distribution, and loyalty-oriented commercial support.", 10),
  row("Guest Psychographics Description", "", "Independent-minded luxury and upscale travelers seeking distinctive hotels with local character, service credibility, and a recognizable collection relationship—guests who value individuality over a uniform prototype.", 11),
  row("overview.typical_use_case", "", "Independent luxury, upper-upscale, resort, urban, or lifestyle hotels with a differentiated guest proposition that want broader commercial reach without converting to a traditional franchise prototype. Fit depends on property quality, service delivery, market positioning, and the relevance of Preferred's representation model.", 20),
  row("overview.development_model", "", "Primarily an affiliation or representation pathway for operating independents, repositioned assets, and selected new developments with a clear independent identity. Sponsors should confirm member-category fit, commercial participation, systems requirements, and launch scope directly rather than treating this as a standardized conversion program.", 21),
  row("overview.relative_positioning", "Relative Positioning", "Preferred is a representation-led independent-hotel platform: more commercially organized than a purely informal marketing affiliation, but structurally different from soft brands such as Autograph Collection, Curio, Tribute Portfolio, and Vignette that sit inside major hotel-brand systems. It also differs from Design Hotels' design-curation lens and SLH's luxury-focused collection model.", 22),
  row("overview.scenario.1", "Design-Led Independent Conversion", "An established luxury independent with strong service and a differentiated guest experience that seeks broader sales, marketing, distribution, and loyalty-oriented support while preserving its own name, design language, and operating choices. Confirm the applicable affiliation category and commercial obligations before underwriting value.", 30),
  row("overview.scenario.2", "Luxury-Leaning Independent Repositioning", "A well-located independent hotel completing a product or service repositioning where ownership wants a commercial platform without adopting a hard-brand prototype. The relevant question is whether the asset can credibly meet collection expectations—not whether affiliation alone can solve a physical-product or operating gap.", 31),
  row("overview.scenario.3", "Urban Or Resort Gateway Repositioning", "A destination resort or urban independent with a credible identity, service proposition, and target guest. Preferred can be a consideration where owners want representation and distribution support while retaining control over the hotel's positioning, operator, and experience design.", 32),
  row("overview.why_value", "Why Value Is Strongest", "Value is strongest where the hotel already has a compelling independent proposition and ownership can use commercial representation to widen reach without diluting that proposition. It is a weaker fit for generic assets seeking a turnkey prototype, guaranteed demand outcome, or a substitute for needed capital and operating capability.", 33),
  row("overview.proof.1", "Individual Character, Not A Prototype", "Preferred's platform is built around independently branded hotels rather than a single room, lobby, or F&B prototype. Owners should view identity preservation as a commercial and experiential discipline, not as an absence of membership expectations.", 40),
  row("overview.proof.2", "Marriott Bonvoy Distribution", "Official materials emphasize sales, marketing, distribution, and guest-loyalty-oriented support for member hotels. Confirm which programs, channels, systems, and regional resources apply to the specific property and agreement.", 41),
  row("overview.proof.3", "Upper-Upscale To Luxury-Leaning Range", "The portfolio spans multiple independent hotel expressions, from urban luxury to destination resorts. Owners should benchmark against relevant category and market peers rather than assume every Preferred property represents the same service level or capital profile.", 42),
  row("overview.proof.4", "Design Review As A Gate, Not A Checklist", "Representation works best when the physical product, service culture, and guest proposition are already credible. Owners should confirm acceptance, review, and ongoing participation expectations directly rather than assume a marketing relationship is entirely light touch.", 43),
  row("overview.featured_application", "Design-led independent conversion or new-build", "A differentiated independent luxury or upscale hotel can consider Preferred Hotels & Resorts for commercial representation while retaining its own identity and owner-led operating choices. Underwrite the property proposition, member-category fit, systems integration, and commercial participation with the company and advisors.", 44, { caseSummaryOverview: "Featured path for independent luxury and upscale hotels seeking representation without a traditional franchise prototype.", caseSummaryBrandRelevance: "Matches Preferred's independent-hotel affiliation and commercial-support model.", caseSummaryOwnerObjective: "Broaden commercial reach while retaining the hotel's own identity, operator choices, and guest proposition.", caseSummaryInterpretation: "Use as an affiliation-fit lens; confirm program terms, acceptance, and scope directly with Preferred.", caseSummaryTags: "independent hotels, representation, luxury, commercial support, affiliation" }),
  row("overview.differentiators.identity", "Experience & Identity", ["Independent property names and design identities", "Owner-controlled hotel positioning and guest experience", "Luxury and upscale collection context", "Property-specific story rather than a fixed prototype"].join("\n"), 45),
  row("overview.differentiators.commercial", "Commercial & Distribution", ["Sales, marketing, and distribution representation", "Loyalty-oriented guest engagement programs where applicable", "Collection visibility for differentiated independents", "Confirm program scope and participation requirements directly"].join("\n"), 46),
  row("overview.bestAt.1", "Design-Led Independent Character", "Supporting hotels that want broader commercial representation without replacing their individual name, design, or market proposition with a standardized brand expression.", 47),
  row("overview.bestAt.2", "Broad Segment Range Under One Collection", "Providing an affiliation option for owners who want to preserve meaningful choice around operator, experience design, and property positioning while participating in a broader platform.", 48),
  row("overview.bestAt.3", "Marriott Systems Without A Fixed Prototype", "Connecting differentiated luxury and upscale hotels to collection-oriented commercial support—distinct from a conventional franchise structure or a narrowly design-curated membership model.", 49),
  row("overview.portfolio_context", "Portfolio Context", "Preferred sits in the independent-hotel representation lane: distinguish it from SLH's luxury collection approach, Design Hotels' curation-led affiliation, and Marriott or IHG soft brands that combine independent character with parent-brand standards and systems. Owners should compare owner control, commercial reach, standards, and loyalty implications.", 50),
  row("footprint.portfolio_context", "Portfolio Context", "The portfolio is organized around independently distinctive hotels rather than a common prototype. For an owner, the key comparison is the depth of commercial representation and membership expectations relative to other affiliation, curation, and soft-brand alternatives.", 51),
  row("valueOwners.watchouts", "", ["Affiliation is not a substitute for an asset's product, service, or market-positioning work", "Confirm category fit, acceptance process, commercial scope, and ongoing participation obligations directly", "Do not assume the program carries the same systems or standards profile as a major-chain soft brand", "Benchmark relevant market and property peers, not collection outliers"].join("\n"), 52),

  row("valueOwners.lifecycle.1", "Evaluation", "Assess whether the hotel has a credible independent luxury or upscale proposition and whether Preferred's representation model addresses the owner's commercial objective. Compare with SLH, Design Hotels, and soft-brand alternatives on owner control, distribution, loyalty, standards, and operating implications.", 300),
  row("valueOwners.lifecycle.2", "Conversion Design", "For a repositioning or opening, protect the hotel's own identity while closing guest-experience gaps that could weaken collection fit. Prioritize product and service work that improves the independent proposition rather than adopting generic brand styling.", 301),
  row("valueOwners.lifecycle.3", "Pre-Opening", "Coordinate commercial launch planning, property content, channel setup, training, and operator responsibilities with the applicable Preferred program. Confirm timing, technical requirements, and owner versus operator accountabilities before opening.", 302),
  row("valueOwners.lifecycle.4", "Opening", "Launch with a consistent independent identity and service delivery that match the promised guest proposition. Use commercial representation as an amplifier, not as a replacement for operating readiness or local market activation.", 303),
  row("valueOwners.lifecycle.5", "Ramp-Up", "Review guest feedback, channel mix, and commercial activity against the hotel's own target segment and comp set. Adjust programming, sales focus, and service execution without assuming that collection participation alone determines ramp-up.", 304),
  row("valueOwners.lifecycle.6", "Ongoing", "Refresh the property's positioning and commercial plan as the market, operator, and guest proposition evolve. Reconfirm membership expectations and program fit before major repositioning, operator change, or renewal decisions.", 305),

  row("operations.model.primary_model", "", "Independent-hotel representation and affiliation model. It is not a standardized traditional franchise prototype; agreement structure and available programs must be confirmed for the specific property.", 100),
  row("operations.model.management_option", "", "Hotels may retain or appoint operators subject to their own ownership and affiliation arrangements. The owner should confirm any operator, service, systems, or commercial-delivery expectations connected to the applicable Preferred program.", 101),
  row("operations.model.typical_ownership", "", "Owners of differentiated independent luxury and upscale hotels seeking commercial support while preserving property identity and meaningful operating control.", 102),
  row("operations.model.brand_involvement", "", "Preferred's involvement centers on representation, commercial programs, and membership fit rather than a common physical prototype. Confirm review touchpoints, brand-use requirements, and operating expectations directly.", 103),
  row("operations.model.systems_integration", "", "Distribution, booking, loyalty-oriented, content, and reporting requirements vary by program and property. Validate technical integration, data ownership, and implementation responsibilities during diligence.", 104),
  row("operations.model.pre_opening", "", "Expect property positioning, commercial launch, content, channel, and readiness work before activation. Sequence this with operator setup and opening plans rather than assuming affiliation can be added without preparation.", 105),
  row("operations.model.staffing_intensity", "", "Staffing follows the hotel's intended luxury or upscale service proposition, not a Preferred prototype. Underwrite front office, housekeeping, concierge, and F&B resources to the individual asset.", 106),
  row("operations.model.fb_complexity", "", "F&B is property-specific. For many independents it is central to local identity and guest appeal, so owners should evaluate outlet strategy and operating capability independently of affiliation.", 107),
  row("operations.model.training", "", "Confirm available onboarding, commercial, service, and program training with Preferred and the selected operator. Training needs should reflect both the hotel's own identity and any applicable guest-program expectations.", 108),
  row("operations.model.reporting_discipline", "", "Commercial and program reporting requirements should be confirmed at agreement level. Owners should define the reporting cadence and data responsibilities they need from both the operator and representation partner.", 109),
  row("operations.model.qa_rhythm", "", "Membership, service, content, and guest-experience review expectations may apply. Confirm cadence, criteria, and remediation responsibilities rather than assuming all independent affiliations have identical oversight.", 110),
  row("operations.model.technology", "", "Technology participation is a property-specific diligence item. Confirm reservations, distribution, guest-program, content, and reporting requirements before committing implementation time or budget.", 111),
  row("operations.standards_philosophy", "", "Preferred's model supports independent hotel identity while pairing it with commercial representation and program participation.\nDesign and conversion detail: Property-specific identity remains central; confirm whether a repositioning meets the relevant membership category.\nPIP / lifecycle capital: Determine capital needs from the hotel's own guest proposition and agreed requirements, not from a presumed prototype.\nSegment range: Benchmark local luxury or upscale peers and relevant Preferred members.", 112),
  row("operations.operator_compat.summary", "", "Operators should be able to deliver a differentiated independent luxury or upscale experience while collaborating on commercial, distribution, content, and guest-program requirements.", 113),
  row("operations.operator_compat.fit", "", "Best fit: operators with credible independent, boutique, resort, or luxury experience and a disciplined commercial interface. Weaker fit: operators relying on a parent-brand prototype or unable to sustain the hotel's own service and identity standards.", 114),
  row("operations.operator_compat.tags", "", "Independent hotels\nRepresentation affiliation\nLuxury and upscale\nCommercial collaboration", 115),

  row("operations.flexibility.design", "", "Very high", 200),
  row("operations.flexibility.conversion", "", "High", 201),
  row("operations.flexibility.localization", "", "Very high", 202),
  row("operations.flexibility.operational_rigidity", "", "Low", 203),
  row("operations.flexibility.pip", "", "Moderate", 204),
  row("operations.flexibility.prototype", "", "Minimal", 205),

  row("operations.compliance.qa_cadence", "", "Confirm current membership review, service, guest-experience, and content expectations with Preferred. Treat any quality requirements as agreement-specific rather than universal assumptions.", 210),
  row("operations.compliance.training_rigor", "", "Training should cover the hotel's own luxury or upscale service proposition plus applicable commercial and guest-program practices. Confirm scope and timing before opening or program activation.", 211),
  row("operations.compliance.reporting", "", "Confirm program reporting, distribution, loyalty-oriented, and commercial data responsibilities with Preferred and the operator. Preserve owner visibility into channel and guest-performance information.", 212),
  row("operations.compliance.brand_interaction", "", "Interaction is likely to concentrate around membership, commercial planning, content, launch, and ongoing program participation. Confirm contacts, cadence, and escalation paths for the specific affiliation.", 213),

  row("economics.opening.step.1", "Application & Feasibility", "Assess property quality, independent positioning, target guest, market context, and owner objective. Confirm which Preferred affiliation or representation pathway, if any, is appropriate before treating membership as a conversion solution.", 400),
  row("economics.opening.step.2", "Design & Standards", "Review property presentation, membership fit, commercial programs, distribution, guest engagement, and any applicable quality expectations. Keep agreement-level obligations separate from general platform positioning.", 401),
  row("economics.opening.step.3", "Pre-Opening Planning", "Build the launch plan around operator readiness, content, commercial activation, distribution setup, training, and the hotel's independent guest proposition. Confirm milestones and responsibilities with all parties.", 402),
  row("economics.opening.step.4", "Opening Support", "Coordinate commercial activation with an operating launch that delivers the promised independent experience. Resolve content, channel, guest-program, and service-readiness issues before relying on broader representation.", 403),
  row("economics.opening.step.5", "Stabilization", "Use guest feedback, commercial reporting, and market response to refine the property's independent positioning and operating plan. Review affiliation performance as one input, not a substitute for agreement and asset-level diligence.", 404),

  row("footprint.momentum", "Distinctive Independent Conversion Signals", "Preferred continues to position its platform around independently distinctive hotels seeking broader commercial representation and guest reach. Treat this as directional model context, not a property-level pipeline disclosure.", 450),
  row("footprint.momentum", "Adaptive Reuse And Heritage Conversion Emphasis", "Official materials present a broad independent-hotel collection across luxury and upscale travel occasions. Owners should confirm current category criteria and regional commercial relevance for their specific property.", 451),
  row("footprint.momentum", "Resort And Gateway-City Expansion Interest", "The platform's value proposition includes international sales, marketing, and distribution context for independent hotels. Local market fit, account coverage, and launch support should be tested directly rather than inferred from global presence.", 452),
  row("footprint.portfolio_mix", "Portfolio mix", "Independent luxury hotels\nUpscale lifestyle hotels\nDestination resorts\nUrban and heritage independents", 460),
  row("footprint.geo_intro", "Geographic footprint", "Preferred Hotels & Resorts presents a global collection of independent hotels across major travel markets. Owners should verify regional representation, relevant comp set, and commercial support for the target market rather than assume uniform depth.", 470),
  row("footprint.region.am", "Americas", "The Americas offer a broad range of independent luxury, resort, and urban-hotel contexts. Use local member and competitor examples to test segment fit and commercial differentiation for the specific market.", 471),
  row("footprint.region.cala", "CALA", "CALA is relevant for destination, resort, and major-city independents that can sustain a differentiated luxury or upscale proposition. Confirm country-level representation, commercial support, and guest-program relevance directly.", 472),
  row("footprint.region.eu", "Europe", "Europe provides extensive independent luxury, heritage, and lifestyle context for the platform. It can inform identity and service benchmarks, but should not be used to assume comparable commercial outcomes elsewhere.", 473),
  row("footprint.region.mea", "MEA", "MEA relevance is market- and property-specific. Confirm authorized participation, sales support, and the fit between the asset's guest proposition and regional independent-luxury demand.", 474),
  row("footprint.region.apac", "APAC", "APAC contributes a diverse independent luxury and resort context for international travelers. For other regions, treat it as portfolio context rather than an operating or ramp-up benchmark.", 475),
  row("footprint.growth_themes", "", "Independent luxury representation\nOwner-controlled affiliation\nDestination and resort differentiation\nUrban lifestyle and heritage independents", 480),
  row("footprint.growth_editorial", "", "Preferred is most relevant where a hotel already has an independent proposition worth commercializing and an owner wants support without a standardized brand conversion. The platform should be evaluated alongside the property's operator, product, market, and agreement-level obligations.", 481),
  row("footprint.growth_fit", "", "Best growth fit: differentiated independents seeking representation and commercial support while retaining identity. Weaker fit: assets seeking a uniform prototype, automatic demand lift, or an affiliation that can replace fundamental product or operating work.", 482),

  row("standards.intro", "", "Preferred membership and participation expectations should be confirmed for the applicable program and property. The central owner question is whether the hotel can sustain a credible independent luxury or upscale guest proposition alongside commercial participation.", 600),
  row("standards.requirement", "Design & narrative review", "The hotel should have a clear property-specific identity, target guest, and service proposition. Confirm how these are assessed for the relevant Preferred pathway.", 601),
  row("standards.requirement", "Bonvoy systems participation", "Sales, marketing, distribution, loyalty-oriented, content, and reporting participation may vary. Confirm the exact program scope and owner or operator responsibilities.", 602),
  row("standards.requirement", "F&B and public-space capital", "Guest rooms, public spaces, and amenities should credibly support the hotel's intended segment and story. Determine needed capital from asset condition and confirmed requirements.", 603),
  row("standards.requirement", "Guest-room and suite standards", "Room and suite quality should align with the independent hotel's positioning and target guest. Confirm any relevant baseline expectations directly with Preferred.", 604),
  row("standards.requirement", "Training and service culture", "Service delivery must reinforce the hotel's own guest proposition while supporting applicable program experiences. Confirm onboarding and ongoing training resources.", 605),
  row("standards.requirement", "Ongoing design and QA review", "Confirm the nature, cadence, and remediation process for any ongoing membership, guest-experience, content, or quality review.", 606),
  row("standards.conversion", "", "Conversion suitability depends on whether the hotel has—or can create—a credible independent proposition, not simply whether it wants a recognizable collection name. Confirm member fit, commercial scope, and any readiness work before committing capital.", 607),
  row("standards.questions", "Questions owners should ask", ["Which Preferred program and category best match our hotel's independent proposition?", "What commercial, distribution, guest-program, technology, and reporting participation is expected?", "What review or quality expectations apply at entry and on an ongoing basis?", "How does Preferred compare with SLH, Design Hotels, Autograph, Curio, Tribute, and Vignette for our desired owner control?", "Which launch activities and responsibilities sit with Preferred, ownership, and our operator?"].join("\n"), 608),

  row("insight.similar.1", "Tribute Portfolio", "Small Luxury Hotels of the World is a luxury-focused independent-hotel collection. Compare luxury positioning, member selectivity, commercial reach, loyalty implications, and the degree to which each platform preserves owner-led identity.", 700),
  row("insight.similar.2", "Design Hotels", "A design-curation-led affiliation platform for distinctive independents. Compare the importance of design authorship, curation, commercial representation, and operating flexibility.", 701),
  row("insight.similar.3", "MGallery Collection", "Autograph Collection is a Marriott soft-brand collection combining independent character with Marriott systems and standards. Compare Preferred's representation-led model against a parent-brand soft-brand structure on owner control, loyalty, operating requirements, and prototype expectations.", 702),
];

export const BRAND_FULL_BUILD_CONTENT = Object.freeze({
  brandSlug: BRAND_SLUG,
  sourcePack: Object.freeze({
    canonicalSite: "preferredhotels.com",
    developmentPage: "preferredhotels.com — official membership and hotel collection materials",
    propertyPages: "Individual hotel pages under preferredhotels.com — use for property-specific identity and amenity context; do not embed raw URLs in owner-facing Body copy.",
    parentContextPages: ["preferredhotels.com — Preferred Hotels & Resorts official platform context", "iprefer.com — official guest-loyalty program context where relevant"],
    imageSources: "preferredhotels.com official brand and hotel galleries; confirm licensing before external use.",
    domains: ["preferredhotels.com", "iprefer.com"],
  }),
  brandLens: Object.freeze({
    brandModel: "Independent-hotel representation and affiliation platform for luxury and upscale properties; not a traditional franchise prototype.",
    ownerFit: "Owners of differentiated independent hotels seeking commercial representation, distribution, sales and marketing support, and guest-program access while retaining meaningful control over identity and operations.",
    propertyFit: "Luxury and upscale independent urban hotels, resorts, heritage properties, and lifestyle assets with a credible guest proposition and service delivery.",
    conversionLogic: "Assess membership and commercial-program fit from the individual asset's identity, quality, and market context; confirm requirements directly rather than assume a standardized conversion path.",
    operatingImplications: "The operator must sustain the hotel's own guest proposition while coordinating commercial, distribution, content, guest-program, and reporting obligations that apply to the selected affiliation.",
    standardsRequirements: "Property-specific membership, quality, commercial, and program expectations must be confirmed with Preferred; no universal prototype or assumed PIP applies.",
    sourceLimitations: "Official materials support general platform positioning only. They do not establish property-specific agreement terms, fees, performance, acceptance, regional support, or operating obligations.",
    distinguishFrom: "SLH (luxury independent collection), Design Hotels (design-curation affiliation), and Autograph Collection (Marriott soft brand). Also compare Curio, Tribute, and Vignette as parent-brand soft-brand alternatives rather than treating them as equivalent representation models.",
  }),
  presentation: PRESENTATION,
});

export default BRAND_FULL_BUILD_CONTENT;
