/**
 * HE 5.0 — 12-tab field-to-Airtable gap matrix (Bethesda pilot seed + reusable schema).
 * Classifications per founder directive. No paid research from this module.
 */

import { FIELD_CLASS, HE50_TABS, HE50_BETHESDA_HPC_ID, HE50_CANONICAL_BASE_ID } from "./contract.js";

/**
 * @typedef {object} FieldRow
 * @property {string} tabId
 * @property {string} fieldKey
 * @property {string} label
 * @property {string} baseId
 * @property {string} table
 * @property {string} sourceField
 * @property {string} entityId
 * @property {string} classification
 * @property {boolean} uiConnected
 * @property {boolean} researchRequired
 * @property {string} evidenceRequirement
 * @property {string} verificationStatus
 * @property {string} persistenceDestination
 * @property {string} refreshRequirement
 * @property {string} [notes]
 */

/** @type {FieldRow[]} */
export const BETHESDA_FIELD_GAP_MATRIX = Object.freeze([
  // —— Hotel Overview ——
  row("hotel", "name", "Hotel name", "HPC", "Property Name / name", FIELD_CLASS.EXISTING_VERIFIED, true, "LIVE_HPC"),
  row("hotel", "brand", "Brand / affiliation", "HPC", "Current Brand / Affiliation", FIELD_CLASS.EXISTING_VERIFIED, true, "LIVE_HPC"),
  row("hotel", "status", "Hotel status", "HPC", "project_phase / status", FIELD_CLASS.EXISTING_VERIFIED, true, "LIVE_HPC"),
  row("hotel", "website", "Official website", "HPC", "Website", FIELD_CLASS.EXISTING_VERIFIED, true, "LIVE_HPC"),
  row("hotel", "telephone", "Telephone", "HPC", "Phone", FIELD_CLASS.EXISTING_VERIFIED, true, "LIVE_HPC"),
  row("hotel", "address1", "Address", "HPC", "Address 1", FIELD_CLASS.EXISTING_VERIFIED, true, "LIVE_HPC"),
  row("hotel", "city", "City", "HPC", "city", FIELD_CLASS.EXISTING_VERIFIED, true, "LIVE_HPC"),
  row("hotel", "country", "Country", "HPC", "country", FIELD_CLASS.EXISTING_VERIFIED, true, "LIVE_HPC"),
  row("hotel", "parentCompany", "Parent company (brand)", "HPC", "Brand Family", FIELD_CLASS.EXISTING_VERIFIED, true, "LIVE_HPC", "Semantic: brand parent, not economic owner"),
  row("hotel", "chainScale", "Chain scale / class", "HPC", "Hotel Class / Segment", FIELD_CLASS.EXISTING_VERIFIED, true, "LIVE_HPC"),
  row("hotel", "rooms", "Rooms / keys", "HPC", "Rooms / Keys", FIELD_CLASS.EXISTING_VERIFIED, true, "LIVE_HPC", "407 keys corroborated HWE + CMBS"),
  row("hotel", "totalMeetingSpace", "Total meeting space", "ADP/HI + events URL", "Total Meeting Space Sq Ft", FIELD_CLASS.EXISTING_NOT_CONNECTED, true, "VERIFIED_DERIVED", "Displayed; optional HPC null-fill staged"),
  row("hotel", "operationType", "Operation type", "HPC", "Management Model", FIELD_CLASS.EXISTING_VERIFIED, true, "LIVE_HPC"),
  row("hotel", "managementCompany", "Management company", "HPC + HI Rel", "Operator / Management Company", FIELD_CLASS.EXISTING_VERIFIED, true, "LIVE_HPC", "Marriott International managed"),
  row("hotel", "market", "Market", "HPC", "Market", FIELD_CLASS.EXISTING_VERIFIED, true, "LIVE_HPC"),
  row("hotel", "submarket", "Submarket", "HPC", "Submarket", FIELD_CLASS.EXISTING_VERIFIED, true, "LIVE_HPC"),
  row("hotel", "locationType", "Location type", "HPC", "Asset Context", FIELD_CLASS.EXISTING_VERIFIED, true, "LIVE_HPC"),
  row("hotel", "yearBuilt", "Year built", "HPC", "Year Built", FIELD_CLASS.EXISTING_NOT_CONNECTED, false, "NEWLY_VERIFIED_FREE", "1979 — Travel Weekly / CMBS / Cvent; staged for HPC"),
  row("hotel", "lastRenovation", "Last meaningful renovation", "HPC", "Last Renovation", FIELD_CLASS.SOURCE_CONFLICT, false, "CONFLICT", "2015 major reno (Travel Weekly/CMBS/Bethesda Mag) vs Cvent 2017 unveil — stage both with roles"),
  row("hotel", "hotelHeadline", "Hotel headline", "HPC enrichment", "Hotel Headline", FIELD_CLASS.RESEARCH_REQUIRED, false, "GAP"),
  row("hotel", "hotelDescription", "Property description", "HPC", "Short Property Summary", FIELD_CLASS.RESEARCH_REQUIRED, false, "GAP"),
  row("hotel", "starRating", "Star rating", "HPC", "Star Rating", FIELD_CLASS.RESEARCH_REQUIRED, false, "GAP"),
  row("hotel", "amenitiesDisplay", "Amenities", "HPC + ADP Attributes", "amenities", FIELD_CLASS.EXISTING_VERIFIED, true, "LIVE_MIXED"),

  // —— Rooms ——
  row("rooms", "guestroomInventory", "Guestroom inventory", "HPC + ADP", "Rooms / Keys + room mix", FIELD_CLASS.EXISTING_VERIFIED, true, "STAGED_AND_LIVE"),
  row("rooms", "suiteInventory", "Suite inventory", "ADP/events", "suites", FIELD_CLASS.EXISTING_NOT_CONNECTED, true, "PARTIAL", "Cvent: 399 guest rooms + 8 suites = 407"),
  row("rooms", "meetingRoomsCount", "Meeting rooms count", "ADP/events", "meeting room count", FIELD_CLASS.EXISTING_VERIFIED, true, "VERIFIED_DERIVED", "27 rooms"),
  row("rooms", "largestMeetingRoom", "Largest meeting room", "ADP/events", "Grand Ballroom", FIELD_CLASS.EXISTING_VERIFIED, true, "VERIFIED_DERIVED", "4,592 sq ft"),

  // —— Ownership ——
  row("ownership", "legalOwner", "Legal property owner", "HI Orgs + Rel", "Legal Owner → Marbeth Acquisition LLC", FIELD_CLASS.EXISTING_NOT_CONNECTED, true, "VERIFIED_STAGED", "MD SDAT; dry-run only"),
  row("ownership", "economicOwner", "Economic sponsor", "HI Orgs + Rel", "Economic Owner → Mullen REC", FIELD_CLASS.EXISTING_NOT_CONNECTED, true, "VERIFIED_STAGED_MEDIUM", "Portfolio + HQ match; UBO natural person unresolved"),
  row("ownership", "operator", "Operator", "HI Rel", "Operator → Marriott International", FIELD_CLASS.EXISTING_VERIFIED, true, "LIVE_HI"),
  row("ownership", "brandFranchisor", "Brand / franchisor", "HI Rel + HPC", "Brand", FIELD_CLASS.EXISTING_VERIFIED, true, "LIVE_HI"),
  row("ownership", "assetManager", "Asset manager", "HI Orgs", "Asset Manager → Mullen Hospitality Mgmt", FIELD_CLASS.EXISTING_NOT_CONNECTED, true, "STAGED_FIRM", "Property-assigned AM unresolved"),
  row("ownership", "currentLender", "Current lender", "HI Rel", "FINANCED_BY", FIELD_CLASS.UNRESOLVED_AFTER_RESEARCH, false, "GAP", "Ramsfield ~$63M historical only; CMBS $39.8M noted — servicer identity not confirmed for graph"),
  row("ownership", "equityJv", "Equity / JV partners", "HI Rel", "JV / Investor", FIELD_CLASS.UNRESOLVED_AFTER_RESEARCH, false, "GAP"),
  row("ownership", "developers", "Developers", "HI Rel", "Developer", FIELD_CLASS.NOT_APPLICABLE, false, "N_A", "Open operating hotel; material owner-side development N/A"),
  row("ownership", "advisorsCurrent", "Current advisors", "HI Rel", "ADVISED_BY", FIELD_CLASS.UNRESOLVED_AFTER_RESEARCH, false, "GAP", "HWE 2018 historical only"),

  // —— Contacts ——
  row("contacts", "keyPeople", "Named decision-makers", "HI Contacts", "Org Contacts", FIELD_CLASS.EXISTING_NOT_CONNECTED, true, "IDENTITY_VERIFIED_STAGED", "Mullen / Button / Reece — Surfe session-only"),
  row("contacts", "publicChannels", "Public email/phone/LinkedIn", "HI Contact Channels", "channels", FIELD_CLASS.EXISTING_NOT_CONNECTED, true, "PARTIAL_PUBLIC", "LinkedIn staged; no Surfe persistence"),
  row("contacts", "orgFallbackRoutes", "Organizational fallback", "HI Channels", "org routes", FIELD_CLASS.EXISTING_VERIFIED, true, "LIVE_STAGED"),

  // —— Organization & Relationships ——
  row("organization", "ownerPortfolio", "Owner portfolio", "HI Rel across HPC", "OWNED_BY / CONTROLLED_BY", FIELD_CLASS.EXISTING_NOT_CONNECTED, true, "PROPOSED_PORTFOLIO", "7 candidates not auto-verified holdings"),
  row("relationships", "ocnEdges", "OCN relationship edges", "HI Hotel–Org Rel", "role edges", FIELD_CLASS.EXISTING_NOT_CONNECTED, true, "STAGED_DRY_RUN"),

  // —— Submarket / Area ——
  row("submarket", "submarketSnapshot", "Submarket snapshot", "HPC + Radar", "Submarket", FIELD_CLASS.EXISTING_VERIFIED, true, "LIVE"),
  row("area", "areaHotels", "Area hotels", "HPC / Radar peers", "nearby HPC", FIELD_CLASS.EXISTING_VERIFIED, true, "LIVE"),

  // —— Demand / Access ——
  row("demand", "demandAnchors", "Demand drivers", "Demand Anchors (ALT/HI)", "anchors + hotel assoc", FIELD_CLASS.EXISTING_NOT_CONNECTED, true, "US_EMPTY_HONEST", "Infrastructure live; US Bethesda anchors sparse — no invented proximity demand"),
  row("access", "travelInfrastructure", "Access & connectivity", "Travel Infrastructure Data", "TI + hotel links", FIELD_CLASS.EXISTING_NOT_CONNECTED, true, "PARTIAL", "Airports/roads may exist; no invented drive times"),

  // —— Sentiment / Sources ——
  row("sentiment", "platformIdentities", "Seven platform identities", "Guest Reputation", "platform IDs", FIELD_CLASS.EXISTING_NOT_CONNECTED, true, "STAGED_FOUNDER_PREVIEW", "Rights-gated; live empty without founderPreview"),
  row("sentiment", "hotelNews", "Hotel News & Articles", "Market Alerts", "hotel associations", FIELD_CLASS.UNRESOLVED_AFTER_RESEARCH, true, "ABSENT", "Genuine absence for Bethesda — not filter bug"),
  row("sources", "evidenceRegister", "Sources & Evidence", "HI Graph Evidence", "evidence links", FIELD_CLASS.EXISTING_NOT_CONNECTED, true, "STAGED_REGISTER", "Phase 3 evidence register ready for persist"),
].map((r) => Object.freeze(r)));

function row(
  tabId,
  fieldKey,
  label,
  table,
  sourceField,
  classification,
  uiConnected,
  verificationStatus,
  notes
) {
  const researchRequired =
    classification === FIELD_CLASS.RESEARCH_REQUIRED ||
    classification === FIELD_CLASS.SOURCE_CONFLICT ||
    classification === FIELD_CLASS.UNRESOLVED_AFTER_RESEARCH;
  return {
    tabId,
    fieldKey,
    label,
    baseId: HE50_CANONICAL_BASE_ID,
    table,
    sourceField,
    entityId: HE50_BETHESDA_HPC_ID,
    classification,
    uiConnected,
    researchRequired,
    evidenceRequirement: researchRequired || classification === FIELD_CLASS.EXISTING_NOT_CONNECTED
      ? "URL + publisher + access_date + supported_facts"
      : "reuse_existing_provenance",
    verificationStatus,
    persistenceDestination: table,
    refreshRequirement: "on_canonical_change_or_annual",
    notes: notes || "",
  };
}

export function summarizeFieldGapMatrix(rows = BETHESDA_FIELD_GAP_MATRIX) {
  const byClass = {};
  const byTab = {};
  for (const r of rows) {
    byClass[r.classification] = (byClass[r.classification] || 0) + 1;
    if (!byTab[r.tabId]) byTab[r.tabId] = { tabId: r.tabId, fields: 0, verified: 0, gaps: 0 };
    byTab[r.tabId].fields += 1;
    if (r.classification === FIELD_CLASS.EXISTING_VERIFIED) byTab[r.tabId].verified += 1;
    if (r.researchRequired) byTab[r.tabId].gaps += 1;
  }
  return {
    totalFields: rows.length,
    tabsCovered: HE50_TABS.length,
    byClass,
    byTab: Object.values(byTab),
  };
}

export function fieldsNeedingPaidResearch(rows = BETHESDA_FIELD_GAP_MATRIX) {
  return rows.filter(
    (r) =>
      r.classification === FIELD_CLASS.RESEARCH_REQUIRED ||
      r.classification === FIELD_CLASS.UNRESOLVED_AFTER_RESEARCH
  );
}
