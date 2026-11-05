/**
 * AVA Hotel Facts Coverage Matrix V1
 *
 * Clarifies: hotelFactsRegister historically stores material exceptions /
 * conflicts — NOT a full property encyclopedia. This matrix synthesizes a
 * commercially relevant coverage view from property + DCR + platform discovery
 * + actions without inventing facts.
 */
export const HOTEL_FACTS_COVERAGE_MATRIX_V1 =
  "AVA_HOTEL_FACTS_COVERAGE_MATRIX_V1";

export const FACT_STATUS = Object.freeze({
  FACT_VERIFIED: "FACT_VERIFIED",
  FACT_PARTIALLY_VERIFIED: "FACT_PARTIALLY_VERIFIED",
  FACT_CONFLICTED: "FACT_CONFLICTED",
  FACT_NOT_VERIFIED: "FACT_NOT_VERIFIED",
  NOT_APPLICABLE: "NOT_APPLICABLE",
});

const CORE_ATTRIBUTES = Object.freeze([
  { id: "official_name", label: "Official hotel name", group: "identity", apply: () => true },
  { id: "brand_affiliation", label: "Brand affiliation", group: "identity", apply: () => true },
  { id: "address_geo", label: "Address / geographic identity", group: "identity", apply: () => true },
  { id: "brand_page", label: "Official brand page", group: "identity", apply: () => true },
  { id: "hotel_website", label: "Hotel-operated website", group: "identity", apply: () => true },
  { id: "phone_contact", label: "Telephone / contact", group: "identity", apply: () => true },
  { id: "positioning", label: "Current positioning", group: "identity", apply: () => true },
  { id: "rooms_positioning", label: "Accommodation positioning", group: "rooms", apply: () => true },
  { id: "adults_family_policy", label: "Adults-only / family policy", group: "rooms", apply: (p) => /castillo|adult|family|kids|condesa|all.?inclusive/i.test(p) },
  { id: "dining", label: "Restaurants and bars", group: "fbl", apply: () => true },
  { id: "spa_wellness", label: "Spa / wellness", group: "fbl", apply: (p) => /spa|castillo|sheraton|chapultepec|condesa|fiesta|waterstone/i.test(p) },
  { id: "pools", label: "Pools", group: "fbl", apply: (p) => /resort|castillo|sheraton|condesa|fiesta|waterstone|chapultepec/i.test(p) },
  { id: "golf_marina_beach", label: "Golf / marina / beach facilities", group: "fbl", apply: (p) => /golf|marina|beach|condesa|waterstone|arabella|son.?vida/i.test(p) },
  { id: "meeting_space", label: "Meeting / event space", group: "meetings", apply: () => true },
  { id: "meeting_capacity", label: "Meeting capacity", group: "meetings", apply: (p) => /meeting|event|chapultepec|ac hotel|waterstone|castillo|sheraton/i.test(p) },
  { id: "weddings", label: "Weddings / social events", group: "meetings", apply: (p) => /wedding|waterstone|castillo|son.?vida/i.test(p) },
  { id: "destination_positioning", label: "Destination / neighborhood positioning", group: "destination", apply: () => true },
  { id: "destination_directory", label: "Tourism / CVB directory coverage", group: "destination", apply: () => true },
]);

function blob(payload) {
  return JSON.stringify({
    name: payload?.property?.name,
    dcr: payload?.digitalContentReadiness,
    actions: payload?.actions,
    findings: payload?.keyFindings,
    platforms: payload?.platformListingDiscoveryV3 || payload?.platformListingDiscoveryV2,
  }).toLowerCase();
}

function factFromRegister(payload, id) {
  const rows = payload?.digitalContentReadiness?.hotelFactsRegister || [];
  return rows.find(
    (r) =>
      String(r.attributeId || "").toLowerCase() === id ||
      String(r.attribute || "")
        .toLowerCase()
        .includes(id.replace(/_/g, " "))
  );
}

function platformUrl(payload, category) {
  const assessments =
    payload?.platformListingDiscoveryV3?.assessments ||
    payload?.platformCoverageAssessment?.assessments ||
    [];
  const hit = assessments.find((a) => a.category === category || /GOOGLE/i.test(category) && /GOOGLE/i.test(a.category));
  if (category.startsWith("GOOGLE")) {
    return assessments.find((a) => /GOOGLE/i.test(a.category || "")) || null;
  }
  return assessments.find((a) => a.category === category) || null;
}

function classifyAttribute(payload, attr) {
  const b = blob(payload);
  const applicable = attr.apply(b);
  if (!applicable) {
    return {
      attributeId: attr.id,
      label: attr.label,
      group: attr.group,
      status: FACT_STATUS.NOT_APPLICABLE,
      value: null,
      source: null,
      sourceUrl: null,
      commercialRelevance: "Not material for this property profile",
      notes: "Applicability filter — not forced into audit",
      inHotelFactsRegister: false,
      reachedAnalysis: false,
      inCustomerReportLikely: false,
    };
  }

  const reg = factFromRegister(payload, attr.id);
  if (reg) {
    const conflicted = /conflict|vs |versus|reconcile|inconsist/i.test(
      `${reg.value || ""} ${reg.notes || ""} ${reg.status || ""}`
    );
    return {
      attributeId: attr.id,
      label: attr.label,
      group: attr.group,
      status: conflicted ? FACT_STATUS.FACT_CONFLICTED : FACT_STATUS.FACT_PARTIALLY_VERIFIED,
      value: reg.value || null,
      source: reg.source || "hotelFactsRegister",
      sourceUrl: reg.evidenceUrl || null,
      commercialRelevance: "Material — present in Hotel Facts Register (exception/conflict inventory)",
      notes:
        "Hotel Facts Register historically stores commercially material exceptions/conflicts, not full encyclopedic inventory.",
      inHotelFactsRegister: true,
      reachedAnalysis: true,
      inCustomerReportLikely: true,
    };
  }

  // Identity synthesis from known payload fields / platform discovery
  if (attr.id === "official_name" && payload?.property?.name) {
    return {
      attributeId: attr.id,
      label: attr.label,
      group: attr.group,
      status: FACT_STATUS.FACT_VERIFIED,
      value: payload.property.name,
      source: "payload.property",
      sourceUrl: payload.property.website || null,
      commercialRelevance: "Core identity",
      notes: "Stored on property profile; not duplicated into hotelFactsRegister unless conflicted.",
      inHotelFactsRegister: false,
      reachedAnalysis: true,
      inCustomerReportLikely: true,
    };
  }
  if (attr.id === "hotel_website" && payload?.property?.website) {
    return {
      attributeId: attr.id,
      label: attr.label,
      group: attr.group,
      status: FACT_STATUS.FACT_VERIFIED,
      value: payload.property.website,
      source: "payload.property.website",
      sourceUrl: payload.property.website,
      commercialRelevance: "Core identity",
      notes: "Canonical hotel site on property profile.",
      inHotelFactsRegister: false,
      reachedAnalysis: true,
      inCustomerReportLikely: true,
    };
  }
  if (attr.id === "address_geo" && (payload?.property?.market || payload?.property?.coverLocation)) {
    return {
      attributeId: attr.id,
      label: attr.label,
      group: attr.group,
      status: FACT_STATUS.FACT_PARTIALLY_VERIFIED,
      value: payload.property.coverLocation || payload.property.market,
      source: "payload.property.coverLocation/market",
      sourceUrl: null,
      commercialRelevance: "Market identity for discovery",
      notes: "Market/cover location present; street-level address may live only on brand/OTA pages.",
      inHotelFactsRegister: false,
      reachedAnalysis: true,
      inCustomerReportLikely: true,
    };
  }
  if (attr.id === "brand_page") {
    const plat = platformUrl(payload, "BRAND_PROPERTY_PAGE");
    if (plat?.url) {
      return {
        attributeId: attr.id,
        label: attr.label,
        group: attr.group,
        status:
          plat.listingExistence === "EXACT_LISTING_VERIFIED"
            ? FACT_STATUS.FACT_VERIFIED
            : FACT_STATUS.FACT_PARTIALLY_VERIFIED,
        value: plat.url,
        source: "platformListingDiscovery",
        sourceUrl: plat.url,
        commercialRelevance: "Brand distribution",
        notes: plat.notes || null,
        inHotelFactsRegister: false,
        reachedAnalysis: true,
        inCustomerReportLikely: true,
      };
    }
  }
  if (attr.id === "destination_directory") {
    const plat = platformUrl(payload, "DESTINATION_CVB");
    if (plat?.url) {
      return {
        attributeId: attr.id,
        label: attr.label,
        group: attr.group,
        status: FACT_STATUS.FACT_VERIFIED,
        value: plat.url,
        source: "platformListingDiscovery",
        sourceUrl: plat.url,
        commercialRelevance: "Destination distribution",
        notes: null,
        inHotelFactsRegister: false,
        reachedAnalysis: true,
        inCustomerReportLikely: true,
      };
    }
  }

  // Mentioned in actions/findings without register row
  const mentioned = new RegExp(attr.id.split("_")[0], "i").test(b) ||
    new RegExp(attr.label.split(" ")[0], "i").test(b);
  if (mentioned) {
    return {
      attributeId: attr.id,
      label: attr.label,
      group: attr.group,
      status: FACT_STATUS.FACT_PARTIALLY_VERIFIED,
      value: null,
      source: "actions_or_findings_or_dcr_narrative",
      sourceUrl: null,
      commercialRelevance: "Referenced in analysis narrative",
      notes:
        "Attribute appears in research narrative/actions but lacks a dedicated Hotel Facts Register row (register is exception-oriented).",
      inHotelFactsRegister: false,
      reachedAnalysis: true,
      inCustomerReportLikely: /meeting|domain|dining|adult|spa|golf|marina/i.test(attr.id),
    };
  }

  return {
    attributeId: attr.id,
    label: attr.label,
    group: attr.group,
    status: FACT_STATUS.FACT_NOT_VERIFIED,
    value: null,
    source: null,
    sourceUrl: null,
    commercialRelevance: "Applicable but not yet verified at field level",
    notes: "Gap for supplemental content verification — not proof the attribute is missing on the web.",
    inHotelFactsRegister: false,
    reachedAnalysis: false,
    inCustomerReportLikely: false,
  };
}

/**
 * Build coverage matrix + lineage explanation for one hotel payload.
 */
export function buildHotelFactsCoverageMatrix(payload) {
  const attributes = CORE_ATTRIBUTES.map((a) => classifyAttribute(payload, a));
  const registerRows = payload?.digitalContentReadiness?.hotelFactsRegister || [];
  const sourceInventory = payload?.digitalContentReadiness?.sourceInventory || [];

  const counts = {
    FACT_VERIFIED: attributes.filter((a) => a.status === FACT_STATUS.FACT_VERIFIED).length,
    FACT_PARTIALLY_VERIFIED: attributes.filter(
      (a) => a.status === FACT_STATUS.FACT_PARTIALLY_VERIFIED
    ).length,
    FACT_CONFLICTED: attributes.filter((a) => a.status === FACT_STATUS.FACT_CONFLICTED).length,
    FACT_NOT_VERIFIED: attributes.filter((a) => a.status === FACT_STATUS.FACT_NOT_VERIFIED)
      .length,
    NOT_APPLICABLE: attributes.filter((a) => a.status === FACT_STATUS.NOT_APPLICABLE).length,
    registerExceptionRows: registerRows.length,
    sourceInventoryRows: sourceInventory.length,
  };

  return {
    contract: HOTEL_FACTS_COVERAGE_MATRIX_V1,
    propertyId: payload?.property?.propertyId,
    hotelName: payload?.property?.name,
    generatedAt: new Date().toISOString(),
    lineageConclusion:
      "CAUSE_A_EXCEPTION_REGISTER: hotelFactsRegister stores material exceptions/conflicts (and a few key commercial attributes), not a full property-facts encyclopedia. Core identity also lives on payload.property, sourceInventory, websiteDiscoverability, platformConsistency, actions, and platformListingDiscovery*. Low register row counts do NOT alone prove shallow research.",
    counts,
    attributes,
    storageMap: {
      "payload.property": "Core identity (name, market, website)",
      "digitalContentReadiness.sourceInventory": "Discovered source URLs by category",
      "digitalContentReadiness.hotelFactsRegister": "Exception/conflict-oriented commercial facts",
      "digitalContentReadiness.platformConsistency": "Cross-platform attribute comparisons",
      "digitalContentReadiness.travelerQuestionCoverage": "Traveler-question answerability",
      "platformListingDiscoveryV3": "Exact listing URLs + listing/content dual status",
      "actions / keyFindings": "Customer-facing recommendations derived from the above",
      "PDF V3/V4": "Selected commercially material items — not the full research archive",
    },
  };
}
