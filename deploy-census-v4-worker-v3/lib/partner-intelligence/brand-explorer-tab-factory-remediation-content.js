/**
 * Tab-factory remediation packs for lifestyle / soft-collection Brand Explorer profiles.
 * Directional owner-facing copy — no invented fees, hotel counts, or Company Validated claims.
 */

const LIFECYCLE_PHASES = Object.freeze([
  { slot: "valueOwners.lifecycle.1", title: "Evaluation" },
  { slot: "valueOwners.lifecycle.2", title: "Conversion Design" },
  { slot: "valueOwners.lifecycle.3", title: "Pre-Opening" },
  { slot: "valueOwners.lifecycle.4", title: "Opening" },
  { slot: "valueOwners.lifecycle.5", title: "Ramp-Up" },
  { slot: "valueOwners.lifecycle.6", title: "Ongoing" },
]);

const OPENING_STEPS = Object.freeze([
  { slot: "economics.opening.step.1", title: "Application & Feasibility" },
  { slot: "economics.opening.step.2", title: "Design & Standards" },
  { slot: "economics.opening.step.3", title: "Pre-Opening Planning" },
  { slot: "economics.opening.step.4", title: "Opening Support" },
  { slot: "economics.opening.step.5", title: "Stabilization" },
]);

function presentationRow(slotKey, title, body, sortOrder, extra = {}) {
  return { slotKey, title: title || "", body, sortOrder, ...extra };
}

function opsPack({
  primaryModel,
  managementOption,
  typicalOwnership,
  brandInvolvement,
  systemsIntegration,
  preOpening,
  staffingIntensity,
  fbComplexity,
  training,
  reportingDiscipline,
  qaRhythm,
  technology,
  philosophy,
  operatorSummary,
  operatorTags,
  operatorFit,
}) {
  return [
    ["operations.model.primary_model", primaryModel],
    ["operations.model.management_option", managementOption],
    ["operations.model.typical_ownership", typicalOwnership],
    ["operations.model.brand_involvement", brandInvolvement],
    ["operations.model.systems_integration", systemsIntegration],
    ["operations.model.pre_opening", preOpening],
    ["operations.model.staffing_intensity", staffingIntensity],
    ["operations.model.fb_complexity", fbComplexity],
    ["operations.model.training", training],
    ["operations.model.reporting_discipline", reportingDiscipline],
    ["operations.model.qa_rhythm", qaRhythm],
    ["operations.model.technology", technology],
    ["operations.standards_philosophy", philosophy],
    ["operations.operator_compat.summary", operatorSummary],
    ["operations.operator_compat.tags", operatorTags],
    ["operations.operator_compat.fit", operatorFit],
  ].map(([slotKey, body], i) => presentationRow(slotKey, "", body, 100 + i));
}

function flexPack(entries, baseSort = 200) {
  return entries.map(([slotKey, body], i) => presentationRow(slotKey, "", body, baseSort + i));
}

function compliancePack(entries, baseSort = 210) {
  return entries.map(([slotKey, body], i) => presentationRow(slotKey, "", body, baseSort + i));
}

function lifecyclePack(phaseBodies, baseSort = 300) {
  return LIFECYCLE_PHASES.map((ph, idx) =>
    presentationRow(ph.slot, ph.title, phaseBodies[idx], baseSort + idx)
  );
}

function openingStepPack(stepBodies, baseSort = 400) {
  return OPENING_STEPS.map((step, idx) =>
    presentationRow(step.slot, step.title, stepBodies[idx], baseSort + idx)
  );
}

function similarBrandsPack(rows, baseSort = 500) {
  return rows.map(([slotKey, title, body], i) => presentationRow(slotKey, title, body, baseSort + i));
}

function buildBrandPack({
  brandSlug,
  ops,
  flex,
  compliance,
  lifecycle,
  openingSteps,
  portfolioMix,
  similar,
  featuredApplication,
  geoIntro,
  growthThemes,
  growthEditorial,
  growthFit,
  regions,
}) {
  // Never write untitled footprint.momentum here.
  // Permanent template: named momentumCards via buildRecentMomentumCard in
  // brand-explorer-section-pattern-parity-content-<slug>.js (+ CONTENT_BY_SLUG).
  // section_pattern_parity remediation writes cards when recent_momentum_pattern_pass fails.
  const regionRows = (regions || []).map(([slotKey, title, body], i) =>
    presentationRow(slotKey, title, body, 470 + i)
  );
  return {
    brandSlug,
    presentation: [
      ...opsPack(ops),
      ...flexPack(flex),
      ...compliancePack(compliance),
      ...lifecyclePack(lifecycle),
      ...openingStepPack(openingSteps),
      presentationRow(
        "footprint.portfolio_mix",
        "Portfolio mix",
        portfolioMix ||
          "Urban lifestyle\nFull-service\nConversion-friendly\nNeighborhood-led",
        460
      ),
      ...regionRows,
      ...(geoIntro
        ? [presentationRow("footprint.geo_intro", "Geographic footprint", geoIntro, 480)]
        : []),
      ...(growthThemes
        ? [presentationRow("footprint.growth_themes", "Growth themes", growthThemes, 481)]
        : []),
      ...(growthEditorial
        ? [presentationRow("footprint.growth_editorial", "Growth editorial", growthEditorial, 482)]
        : []),
      ...(growthFit
        ? [presentationRow("footprint.growth_fit", "Growth fit", growthFit, 483)]
        : []),
      ...(featuredApplication
        ? [
            presentationRow(
              "overview.featured_application",
              featuredApplication.title || "Featured application",
              featuredApplication.body,
              90,
              featuredApplication.caseSummaryOverview
                ? { caseSummaryOverview: featuredApplication.caseSummaryOverview }
                : {}
            ),
          ]
        : []),
      ...similarBrandsPack(similar),
    ],
  };
}

const HOTEL_INDIGO = buildBrandPack({
  brandSlug: "hotel-indigo",
  ops: {
    primaryModel:
      "Branded lifestyle hotel within IHG, typically delivered through franchise or management arrangements that owners must confirm for the specific market and asset.",
    managementOption:
      "Third-party management is common for full-service lifestyle assets; owner-operated paths require credible local storytelling and service capability.",
    typicalOwnership:
      "Owners evaluating urban, gateway, or culturally distinctive hotels who want a place-led guest proposition with IHG platform participation.",
    brandInvolvement:
      "IHG development and brand review typically touch design narrative, guest experience standards, and opening readiness. Confirm the current review process directly.",
    systemsIntegration:
      "Hotel Indigo participates in the IHG systems and loyalty ecosystem. Owners should validate PMS/CRS cutover, training, and commercial systems requirements for the specific deal.",
    preOpening:
      "Expect design and brand sign-off, training, and opening readiness work before soft opening. Sequence PIP and operating setup with financing and operator capacity.",
    staffingIntensity:
      "Full-service lifestyle staffing across front office, housekeeping, and F&B. Underwrite labor to the intended local experience, not a select-service skeleton.",
    fbComplexity:
      "Public spaces and F&B usually carry part of the local story. Kitchen scope, outlet mix, and service rhythm are material diligence items.",
    training:
      "IHG and Hotel Indigo opening/service training should be confirmed as part of pre-opening planning. Budget time and cost against the agreement path.",
    reportingDiscipline:
      "IHG reporting and revenue-management cadence typically apply. Confirm owner reporting expectations and system participation in diligence.",
    qaRhythm:
      "Brand QA and standards checks apply at opening and on an ongoing basis. Lifestyle presentation and service consistency are both in scope.",
    technology:
      "IHG technology participation is a diligence item beyond brand flag alone. Confirm systems, digital, and loyalty integration requirements for the asset.",
    philosophy:
      "Hotel Indigo standards should keep the property's local story visible while meeting IHG lifestyle brand expectations. Owners should underwrite to design narrative, public-space activation, and service delivery—not marketing language alone.\nDesign and conversion detail: Adaptive reuse and urban conversions can fit when the building and district support a coherent guest journey.\nPIP / lifecycle capital: Confirm opening and conversion scope directly; do not assume a light refresh is enough.\nLocalization: Local storytelling is encouraged within brand guardrails and must be operationally deliverable.",
    operatorSummary:
      "Operators need to deliver a neighborhood-led full-service stay with credible public spaces and local programming while operating inside IHG systems.",
    operatorTags: "IHG lifestyle\nLocal storytelling\nFull-service\nUrban / conversion",
    operatorFit:
      "Best fit: operators experienced with lifestyle or full-service urban hotels who can execute a place-specific narrative. Weaker fit: select-service-only operators without F&B or experiential capacity.",
  },
  flex: [
    ["operations.flexibility.design", "High"],
    ["operations.flexibility.conversion", "High"],
    ["operations.flexibility.localization", "Very high"],
    ["operations.flexibility.operational_rigidity", "Medium"],
    ["operations.flexibility.pip", "Medium"],
    ["operations.flexibility.prototype", "Moderate"],
  ],
  compliance: [
    [
      "operations.compliance.qa_cadence",
      "Periodic brand QA and lifestyle presentation reviews cover opening readiness and ongoing standards aligned with IHG rhythm. Confirm current cadence, scoring expectations, and remediation paths for the specific Hotel Indigo asset before underwriting affiliation support.",
    ],
    [
      "operations.compliance.training_rigor",
      "IHG and Hotel Indigo onboarding for opening teams should include lifestyle storytelling and guest-experience rituals. Confirm property-specific training scope, timing, and cost during pre-opening planning rather than treating training as optional after soft opening.",
    ],
    [
      "operations.compliance.reporting",
      "IHG reporting, revenue-management, and loyalty participation expectations typically apply to Hotel Indigo. Owners should confirm ownership reporting cadence, operator vs owner data responsibilities, and system participation for the specific deal rather than assuming independent reporting remains unchanged after affiliation.",
    ],
    [
      "operations.compliance.brand_interaction",
      "Development and brand touchpoints usually cover design narrative, guest experience, and opening milestones. Interaction frequency varies by project stage—confirm how often brand and owner teams meet during conversion, opening, and stabilized operations for this opportunity.",
    ],
  ],
  portfolioMix: "Urban lifestyle\nFull-service\nConversion-friendly\nNeighborhood-led",
  lifecycle: [
    "Start diligence with district authenticity, building suitability, and whether the asset can deliver a credible neighborhood-led full-service stay under Hotel Indigo and IHG public positioning. Confirm development interest, franchise or management path, and how local storytelling will appear in public spaces, F&B, and guest communications before committing design capital or relying on IHG One Rewards participation.",
    "Shape conversion and design around Indigo's place-led guest journey—facade, lobby, corridors, and activation spaces should read as locally rooted while meeting IHG lifestyle design review. Align PIP scope, operator capacity, and financing with the intended F&B and programming mix rather than treating the flag as cosmetic repositioning alone.",
    "Sequence IHG systems cutover, brand training, hiring, and soft-opening plans with design sign-off and operating readiness. Budget time for loyalty integration, commercial setup, and QA touchpoints. Third-party operators often lead daily pre-opening execution while the owner funds capital and confirms milestone approvals with IHG development.",
    "Execute opening with consistent neighborhood storytelling across service, F&B, and public-space programming while stabilizing IHG systems and guest-facing standards. Opening support typically blends brand QA, training reinforcement, and commercial launch coordination—confirm who leads each workstream in the specific agreement.",
    "During ramp-up, calibrate rate positioning, channel mix, and local programming against stabilized service delivery and IHG revenue-management participation. Owners should watch labor, F&B complexity, and guest-review themes tied to the district narrative—not only occupancy headlines.",
    "On an ongoing basis, refresh local programming and physical product within brand guardrails while meeting IHG reporting, QA, and loyalty obligations. Revisit PIP timing, operator fit, and district competitiveness when markets shift or ownership strategy changes.",
  ],
  openingSteps: [
    "Submit the asset for IHG development review with market context, ownership structure, and a candid read on district fit for Hotel Indigo's neighborhood storytelling. Confirm feasibility of franchise or management participation, initial term expectations, and whether the building supports full-service lifestyle operations before detailed design spend.",
    "Complete Hotel Indigo design and brand standards review with IHG—local narrative, public spaces, guest rooms, and F&B concept should cohere with hotelindigo.com positioning. Treat this as a lifestyle conversion design phase, not a light cosmetic reflag.",
    "Build pre-opening budgets for hiring, training, IHG systems, FF&E, and opening marketing aligned with approved design. Confirm operator responsibilities, opening timeline, and milestone approvals with brand development and your advisors.",
    "Coordinate soft opening, brand QA, and IHG commercial launch support with the operator. Ensure guest-facing teams can deliver the intended local experience while systems and loyalty participation go live on schedule.",
    "Stabilize operations with IHG revenue-management rhythm, guest feedback loops, and refinement of local programming. Use early performance to validate underwriting on labor, F&B, and capital—not as a substitute for agreement-level economics review.",
  ],
  similar: [
    [
      "insight.similar.1",
      "Kimpton Hotels & Restaurants",
      "IHG lifestyle peer with design-forward urban hotels—compare place-led positioning, full-service intensity, and IHG platform participation for owners weighing lifestyle flags within the same parent.",
    ],
    [
      "insight.similar.2",
      "voco by IHG",
      "IHG conversion-friendly lifestyle peer—compare flexibility, service model, and district storytelling expectations when the asset may not need full Hotel Indigo expression.",
    ],
    [
      "insight.similar.3",
      "MGallery Collection",
      "Accor soft-collection peer for story-led hotels—compare affiliation model, design individuality, and parent-system rigidity outside IHG.",
    ],
  ],
});

const MGALLERY = buildBrandPack({
  brandSlug: "mgallery-collection",
  ops: {
    primaryModel:
      "Accor soft collection affiliation for distinctive hotels. Exact participation structure and operating responsibilities must be confirmed property by property.",
    managementOption:
      "Independent ownership with Accor collection participation is common; management arrangements vary and should be confirmed for each opportunity.",
    typicalOwnership:
      "Owners of characterful city, heritage, resort, or destination hotels seeking Accor soft-collection reach without a uniform prototype conversion.",
    brandInvolvement:
      "Collection review typically examines story, design quality, guest experience, and fit for the curated portfolio. Confirm current acceptance criteria directly with Accor.",
    systemsIntegration:
      "MGallery sits within Accor's ecosystem. Owners should validate reservations, loyalty, and operating-system expectations for the specific affiliation path.",
    preOpening:
      "Expect collection onboarding, brand presentation alignment, and opening readiness work. Sequence physical-product improvements with acceptance and operating plans.",
    staffingIntensity:
      "Refined full-service staffing consistent with a distinctive upscale or luxury-leaning stay. Service rituals should reinforce the hotel's individual story.",
    fbComplexity:
      "F&B and public spaces often carry the hotel's character. Outlet mix and service style should support the collection narrative without becoming generic.",
    training:
      "Collection and Accor onboarding/training expectations should be confirmed during diligence. Plan for service consistency around the property story.",
    reportingDiscipline:
      "Accor reporting and commercial participation requirements should be confirmed directly. Treat any published ranges as directional only.",
    qaRhythm:
      "Collection quality expectations apply to both physical product and guest experience. Confirm review cadence and remediation expectations before relying on affiliation value.",
    technology:
      "Accor systems and digital participation should be validated for the asset. Do not assume independent systems can remain unchanged after affiliation.",
    philosophy:
      "MGallery standards protect individual hotel character while requiring a coherent, curated guest experience. Owners should underwrite to story, design, and service delivery together.\nConversion detail: Soft-collection conversion is strongest when the asset already has substance worth protecting.\nLocalization: Local design and rituals are part of the premise, not optional decoration.\nPIP / capital: Confirm what must change for acceptance versus what can remain as distinctive character.",
    operatorSummary:
      "Third-party operators must sustain a distinctive hotel identity inside Accor's MGallery soft collection—strong on design storytelling, service rituals, and day-to-day coherence with mgallery.accor.com positioning. Operators who default to prototype chain expression usually struggle; operators who protect individuality while meeting Accor participation requirements are a better fit.",
    operatorTags: "Accor soft collection\nStory-led\nFull-service\nConversion / repositioning",
    operatorFit:
      "Best fit: operators experienced with boutique, heritage, or lifestyle hotels who can protect individuality. Weaker fit: prototype-driven operators who default to standardized chain expression.",
  },
  flex: [
    ["operations.flexibility.design", "Very high"],
    ["operations.flexibility.conversion", "High"],
    ["operations.flexibility.localization", "Very high"],
    ["operations.flexibility.operational_rigidity", "Low"],
    ["operations.flexibility.pip", "Moderate"],
    ["operations.flexibility.prototype", "Low"],
  ],
  compliance: [
    [
      "operations.compliance.qa_cadence",
      "MGallery collection quality and guest-experience reviews typically intensify around onboarding, repositioning, and remediations. Owners should confirm cadence, scoring focus, and who owns corrective action plans before treating affiliation as durable value.",
    ],
    [
      "operations.compliance.training_rigor",
      "Accor and MGallery orientation for service rituals and collection presentation—confirm depth for opening vs steady-state teams.",
    ],
    [
      "operations.compliance.reporting",
      "Accor commercial and affiliation reporting typically applies once a hotel participates in MGallery. Owners should validate required metrics, system participation, and owner vs operator reporting responsibilities for the specific property agreement rather than assuming independent reporting remains unchanged.",
    ],
    [
      "operations.compliance.brand_interaction",
      "Curation and design touchpoints with collection brand teams—frequency varies by acceptance stage and renovation scope.",
    ],
  ],
  lifecycle: [
    "Evaluate whether the hotel's story, design quality, and guest experience already justify MGallery's curated positioning on mgallery.accor.com—not whether the asset matches a chain prototype. Confirm Accor collection interest, affiliation structure, and operating autonomy expectations before underwriting affiliation value or comparing to hard-brand conversion paths.",
    "Design conversion and repositioning to protect the property's individuality while closing gaps collection review may flag. Align narrative, public spaces, guest rooms, and F&B with a coherent story-led stay. Sequence capital so acceptance-critical work precedes decorative changes that do not improve guest-facing substance.",
    "Plan onboarding, training, and opening readiness alongside Accor systems and loyalty integration where applicable. Coordinate operator hiring, service ritual rollout, and any remaining physical product work with collection sign-off milestones—confirm responsibilities between owner, operator, and Accor representatives.",
    "Open with consistent story-led service and design presentation across every guest touchpoint. MGallery opening support typically focuses on guest-experience coherence and curated brand presentation rather than a standardized hard-brand prototype checklist—confirm support scope, staffing coverage, and acceptance residuals directly with Accor before launch week.",
    "During ramp-up, refine programming, F&B rhythm, and commercial positioning while protecting collection quality expectations. Monitor guest feedback for story authenticity and service consistency; soft-collection assets win when individuality is executed reliably rather than when novelty substitutes for operating discipline.",
    "Ongoing, refresh design and rituals within collection guardrails and reassess Accor participation value as markets, operators, and capital plans evolve. Confirm renewal, remediation, and reporting expectations before major repositioning, PIP scopes, or operator transitions so affiliation economics remain intentional.",
  ],
  openingSteps: [
    "Begin with collection fit conversation—share the hotel's story, design assets, and operating model for Accor review. Confirm affiliation pathway, acceptance criteria, and whether management or owner-operated structures are in scope before detailed commercial modeling.",
    "Complete MGallery design and experience review emphasizing individuality, heritage or destination narrative, and guest-journey coherence across arrival, rooms, F&B, and public spaces. Expect curation-oriented standards on mgallery.accor.com rather than a uniform franchise prototype package; document acceptance residuals before capital lock.",
    "Build pre-opening plans for training, Accor systems participation, staffing, and any acceptance-driven capital alongside operator onboarding. Confirm loyalty, distribution, and reporting requirements for the specific property agreement so launch readiness is not delayed by late systems surprises.",
    "Coordinate opening milestones with collection brand support and the operator—focus on story-led service delivery, stable guest-facing presentation, and residual punch-list closure at launch. Confirm who owns guest-facing storytelling versus Accor commercial activation during opening week.",
    "Stabilize with Accor commercial rhythm and collection QA touchpoints after opening. Use early operating data to validate service-ritual delivery and capital recovery assumptions—not as a substitute for confirmed affiliation economics—and reassess residuals before year-one repositioning spend.",
  ],
  portfolioMix: "Story-led collection\nFull-service\nHeritage / conversion\nDestination character",
  similar: [
    [
      "insight.similar.1",
      "Hotel Indigo",
      "IHG lifestyle peer emphasizing neighborhood storytelling—compare parent-system rigidity, full-service lifestyle expectations, and conversion paths.",
    ],
    [
      "insight.similar.2",
      "Tribute Portfolio",
      "Marriott soft-collection peer for independent-character hotels—compare curation model, loyalty participation, and design preservation tradeoffs.",
    ],
    [
      "insight.similar.3",
      "Unbound Collection by Hyatt",
      "Hyatt independent-character collection peer—compare experiential positioning, affiliation structure, and owner control for story-led assets.",
    ],
  ],
});

const SLH = buildBrandPack({
  brandSlug: "small-luxury-hotels-of-the-world",
  ops: {
    primaryModel:
      "Selective independent luxury affiliation / consortium membership rather than a conventional chain franchise conversion. Confirm current membership pathway directly with SLH.",
    managementOption:
      "Independently owned and operated hotels are central to the model. Affiliation does not replace the need for a capable luxury operator.",
    typicalOwnership:
      "Owner-operators and independent luxury owners seeking global recognition and distribution context without converting into a uniform chain flag.",
    brandInvolvement:
      "Membership review is selective and focuses on quality, service, design, and individuality. Confirm eligibility and regional support expectations directly.",
    systemsIntegration:
      "Affiliation may bring distribution and commercial tools; exact systems participation varies. Validate what membership requires versus what remains property-controlled.",
    preOpening:
      "For new members, expect quality review, presentation alignment, and onboarding before relying on collection visibility. Sequence any physical upgrades with membership timing.",
    staffingIntensity:
      "Luxury service intensity with personal recognition and consistency across the stay. Staffing must support the hotel's individual luxury promise.",
    fbComplexity:
      "F&B should match the property's luxury positioning and identity. Complexity varies by city hotel versus resort, but quality expectations remain high.",
    training:
      "Service standards and membership expectations should be confirmed during diligence. Luxury consistency is an operating requirement, not only a brand label.",
    reportingDiscipline:
      "Commercial and membership reporting expectations should be confirmed directly. Do not invent fee or performance assumptions.",
    qaRhythm:
      "Selective membership implies ongoing quality expectations. Confirm review, remediation, and continuation standards before treating affiliation as durable value.",
    technology:
      "Distribution and booking connectivity are diligence items. Confirm what affiliation provides and what the hotel must maintain independently.",
    philosophy:
      "SLH standards center on independent luxury quality, individuality, and service consistency. Affiliation should amplify a hotel that already has a clear reason to choose it.\nMembership detail: Selective acceptance is part of the value logic; not every upscale independent will fit.\nIndependence: The model is not a full chain conversion—preserve ownership character while meeting membership expectations.\nPIP / capital: Confirm any physical or service gaps required for acceptance without assuming a prototype rebuild.",
    operatorSummary:
      "Third-party operators must deliver a distinctive independent luxury stay with consistent recognition-level service while participating in selective SLH affiliation on slh.com. The operator—not the consortium—remains accountable for day-to-day luxury delivery; affiliation amplifies a property that already works rather than substituting for operating excellence.",
    operatorTags: "Independent luxury\nSelective membership\nService-led\nAffiliation without chain conversion",
    operatorFit:
      "Best fit: operators already delivering independent luxury with strong service culture. Weaker fit: chain-prototype operators seeking a soft flag without a property-specific luxury proposition.",
  },
  flex: [
    ["operations.flexibility.design", "Very high"],
    ["operations.flexibility.conversion", "High"],
    ["operations.flexibility.localization", "Very high"],
    ["operations.flexibility.operational_rigidity", "Low"],
    ["operations.flexibility.pip", "Moderate"],
    ["operations.flexibility.prototype", "Low"],
  ],
  compliance: [
    [
      "operations.compliance.qa_cadence",
      "Selective membership quality reviews—confirm cadence and remediation expectations directly with SLH; not a chain inspection calendar.",
    ],
    [
      "operations.compliance.training_rigor",
      "Luxury service consistency and membership orientation remain property-led. Owners should confirm how SLH onboarding supports teams without replacing the operator's training culture or implying a standardized chain academy model.",
    ],
    [
      "operations.compliance.reporting",
      "Membership and commercial reporting expectations are defined in affiliation agreements with SLH. Owners should confirm which metrics are required, who prepares them, and how often they are reviewed—without inferring fee schedules or confidential commercial terms from Explorer content alone.",
    ],
    [
      "operations.compliance.brand_interaction",
      "Membership and quality touchpoints with SLH teams—interaction intensity varies by onboarding stage and property profile.",
    ],
  ],
  lifecycle: [
    "Evaluate whether the hotel already delivers independent luxury with a clear guest promise worthy of selective SLH membership on slh.com—not whether the asset fits a standardized hard-brand prototype. Confirm eligibility, regional support, and membership economics directly; do not treat affiliation as a substitute for operating excellence or as a hard-brand conversion.",
    "If gaps exist, plan targeted design, service, and F&B improvements that preserve individuality while meeting membership quality expectations on slh.com. Avoid generic chain repositioning logic—capital should reinforce what makes the property choosable for selective luxury travelers, not erase identity in pursuit of a uniform flag look.",
    "Sequence membership onboarding, quality review, distribution connectivity, and staff readiness before relying on global SLH visibility. Confirm which commercial tools or systems membership requires versus what remains fully property-controlled, and assign owner vs operator responsibilities before go-live dates are locked.",
    "Launch membership visibility with consistent luxury service delivery and intact property identity across arrival, rooms, F&B, and recognition moments. Opening in this context means credible guest experience at affiliation go-live—not a standardized franchise opening-support package or prototype checklist.",
    "During ramp-up, monitor guest satisfaction, recognition-level service, and commercial contribution from affiliation channels. Validate that SLH membership amplifies an already-strong independent luxury operation rather than masking staffing, F&B, or product gaps that owners still need to fund.",
    "Ongoing, maintain independent luxury standards and reassess membership fit as the property, operator, and market evolve. Confirm continuation criteria, quality reviews, and commercial terms with SLH before major capex programs, operator transitions, or ownership changes so affiliation remains intentional.",
  ],
  openingSteps: [
    "Open membership dialogue with SLH using an honest asset profile—luxury positioning, service culture, design identity, and operating track record. Confirm selective eligibility and regional support; this step is affiliation feasibility, not a standardized chain application package.",
    "Complete membership quality and presentation review focused on independent luxury substance—service, design, F&B, and guest-experience coherence across the stay. Expect curation standards from slh.com membership logic, not a chain prototype compliance matrix; document any acceptance residuals before capital commitment.",
    "Plan onboarding, distribution connectivity, training, and any acceptance-driven upgrades with your operator before relying on SLH visibility. Confirm commercial participation and reporting obligations without inferring confidential fee terms from public materials, and sequence systems work so launch is not delayed by late connectivity gaps.",
    "Go live with membership visibility while protecting property-specific luxury delivery—guest teams should reinforce individuality, recognition, and place rather than a uniform chain script. Confirm who owns guest-facing storytelling versus affiliation channel activation during the first operating weeks.",
    "Stabilize service quality and affiliation channel contribution, then reassess membership value against owner strategy and operating reality. Use early performance to inform renewals or enhancements—not as a proxy for undisclosed economics—and revisit residuals before year-one repositioning capital.",
  ],
  featuredApplication: {
    title: "Selective independent luxury membership",
    body:
      "An independent luxury hotel or resort with a mature property identity, strong service culture, and clear reason to choose it can use SLH membership on slh.com to gain selective global visibility without converting into a hard-brand franchise prototype. Owners should underwrite membership as amplification of an already-credible luxury operation—confirm eligibility, quality residuals, distribution participation, and economics directly with SLH before treating affiliation as durable value.",
    caseSummaryOverview:
      "Asset pattern: Independent luxury city hotel or resort with established guest recognition and high service intensity. Owner question: Does selective membership improve distribution and brand awareness without eroding operating autonomy or forcing a chain rebuild? Diligence: Confirm acceptance criteria, continuation standards, commercial participation, and who funds any quality upgrades required for membership.",
  },
  portfolioMix: "Independent luxury\nSelective membership\nFull-service\nProperty-led identity",
  similar: [
    [
      "insight.similar.1",
      "Relais & Châteaux",
      "Independent luxury association peer—compare selective membership, operating autonomy, and distribution model without chain franchise conversion.",
    ],
    [
      "insight.similar.2",
      "Leading Hotels of the World",
      "Global independent luxury marketing consortium peer—compare membership positioning and owner control expectations.",
    ],
    [
      "insight.similar.3",
      "Design Hotels",
      "Design-led independent collection with Marriott affiliation context—compare curation vs pure consortium membership paths for distinctive assets.",
    ],
  ],
});

export const TAB_FACTORY_TARGET_BRANDS = Object.freeze([
  "hotel-indigo",
  "mgallery-collection",
  "small-luxury-hotels-of-the-world",
]);

export const TAB_FACTORY_REMEDIATION_CONTENT = Object.freeze({
  "hotel-indigo": Object.freeze(HOTEL_INDIGO),
  "mgallery-collection": Object.freeze(MGALLERY),
  "small-luxury-hotels-of-the-world": Object.freeze(SLH),
});

/**
 * @param {string} brandSlug
 * @returns {{ brandSlug: string, presentation: Array<{ slotKey: string, title: string, body: string, sortOrder: number }> } | null}
 */
export function getTabFactoryRemediationPack(brandSlug) {
  const key = String(brandSlug || "")
    .trim()
    .toLowerCase();
  const pack = TAB_FACTORY_REMEDIATION_CONTENT[key];
  if (!pack) return null;
  return pack;
}
