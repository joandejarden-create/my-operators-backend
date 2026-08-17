/**
 * Patch-plan content for rendered field-completeness remediation.
 * Directional, owner-facing operations copy — no invented fee/pipeline metrics.
 */

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
  ].map(([slotKey, body], i) => ({
    slotKey,
    title: "",
    body,
    sortOrder: 100 + i,
  }));
}

export const RENDERED_FIELD_REMEDIATION_CONTENT = Object.freeze({
  "hotel-indigo": {
    brandSlug: "hotel-indigo",
    presentation: opsPack({
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
    }),
  },
  "mgallery-collection": {
    brandSlug: "mgallery-collection",
    presentation: opsPack({
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
        "Operators must sustain a distinctive hotel identity inside an Accor soft collection—strong on design, service rituals, and story consistency.",
      operatorTags: "Accor soft collection\nStory-led\nFull-service\nConversion / repositioning",
      operatorFit:
        "Best fit: operators experienced with boutique, heritage, or lifestyle hotels who can protect individuality. Weaker fit: prototype-driven operators who default to standardized chain expression.",
    }),
  },
  "small-luxury-hotels-of-the-world": {
    brandSlug: "small-luxury-hotels-of-the-world",
    featuredApplication: {
      title: "Independent Luxury Affiliation Without Chain Conversion",
      body: "An independent luxury hotel or resort with a mature property identity, a service-led guest proposition, and an owner considering curated global affiliation while retaining the hotel's individual voice. Confirm membership eligibility, standards, commercial terms, and regional support directly before relying on affiliation value.",
    },
    presentation: opsPack({
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
        "Operators must deliver a distinctive independent luxury stay with consistent service quality while participating in a selective global affiliation.",
      operatorTags: "Independent luxury\nSelective membership\nService-led\nAffiliation without chain conversion",
      operatorFit:
        "Best fit: operators already delivering independent luxury with strong service culture. Weaker fit: chain-prototype operators seeking a soft flag without a property-specific luxury proposition.",
    }),
  },
});
