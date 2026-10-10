/**
 * resolveGdiDemandBuyer() — entity/function first, named person optional.
 */

export const BUYER_TYPE = Object.freeze({
  ASSOCIATION: "ASSOCIATION",
  CORPORATE_MEETINGS: "CORPORATE_MEETINGS",
  EVENT_ORGANIZER: "EVENT_ORGANIZER",
  HOUSING_BUREAU: "HOUSING_BUREAU",
  DMC: "DMC",
  TRAVEL_AGENCY: "TRAVEL_AGENCY",
  PROCUREMENT: "PROCUREMENT",
  SPORTS_FEDERATION: "SPORTS_FEDERATION",
  GOVERNMENT: "GOVERNMENT",
  CONTRACTOR: "CONTRACTOR",
  PROJECT_MANAGEMENT: "PROJECT_MANAGEMENT",
  PRODUCTION_COMPANY: "PRODUCTION_COMPANY",
  UNIVERSITY_PROGRAM: "UNIVERSITY_PROGRAM",
  NGO_SECRETARIAT: "NGO_SECRETARIAT",
  MEDICAL_COMMS_AGENCY: "MEDICAL_COMMS_AGENCY",
  CRO: "CRO",
  UNKNOWN: "UNKNOWN",
});

const TYPE_PATTERNS = [
  [BUYER_TYPE.HOUSING_BUREAU, /\bhousing bureau|official housing|hébergement officiel|alojamiento oficial\b/i],
  [BUYER_TYPE.DMC, /\bDMC\b|destination management/i],
  [BUYER_TYPE.TRAVEL_AGENCY, /\btravel agency|agence de voyage|agencia de viajes\b/i],
  [BUYER_TYPE.PROCUREMENT, /\bprocurement|RFP|tender|licitación|appel d['']offres\b/i],
  [BUYER_TYPE.SPORTS_FEDERATION, /\bfederation|fédérat|federación|olympic|FIFA|UEFA\b/i],
  [BUYER_TYPE.GOVERNMENT, /\bministry|government|département|ayuntamiento|conseil\b/i],
  [BUYER_TYPE.CONTRACTOR, /\bcontractor|general contractor|subcontractor|commissioning\b/i],
  [BUYER_TYPE.PROJECT_MANAGEMENT, /\bproject management|PMC\b|EPCM\b/i],
  [BUYER_TYPE.PRODUCTION_COMPANY, /\bproduction company|broadcast|promoter\b/i],
  [BUYER_TYPE.UNIVERSITY_PROGRAM, /\buniversity|executive education|program office|faculty\b/i],
  [BUYER_TYPE.NGO_SECRETARIAT, /\bNGO|secretariat|foundation|fondation\b/i],
  [BUYER_TYPE.MEDICAL_COMMS_AGENCY, /\bmedical communications|medcomms\b/i],
  [BUYER_TYPE.CRO, /\b\bCRO\b|clinical research organization\b/i],
  [BUYER_TYPE.ASSOCIATION, /\bassociation|society|congress|congrès|symposium\b/i],
  [BUYER_TYPE.EVENT_ORGANIZER, /\borganizer|organiser|event agency|agence événementielle\b/i],
  [BUYER_TYPE.CORPORATE_MEETINGS, /\bcorporate|sales kickoff|leadership meeting|dealer meeting\b/i],
];

function blob(c = {}) {
  return [
    c.title,
    c.organizationName || c.organization,
    c.summaryWhat,
    c.snippet,
    c.pageText,
    c.hotelOpportunityThesis,
  ]
    .map((x) => String(x || ""))
    .join(" ");
}

function inferType(b) {
  for (const [type, re] of TYPE_PATTERNS) {
    if (re.test(b)) return type;
  }
  return BUYER_TYPE.UNKNOWN;
}

function roleFor(type) {
  const map = {
    [BUYER_TYPE.ASSOCIATION]: "ASSOCIATION_SECRETARIAT_OR_MEETINGS",
    [BUYER_TYPE.CORPORATE_MEETINGS]: "CORPORATE_MEETINGS_OR_TRAVEL",
    [BUYER_TYPE.EVENT_ORGANIZER]: "EVENT_ORGANIZER",
    [BUYER_TYPE.HOUSING_BUREAU]: "HOUSING_BUREAU",
    [BUYER_TYPE.DMC]: "DMC",
    [BUYER_TYPE.TRAVEL_AGENCY]: "TRAVEL_AGENCY",
    [BUYER_TYPE.PROCUREMENT]: "PROCUREMENT_BUYER",
    [BUYER_TYPE.SPORTS_FEDERATION]: "FEDERATION_TRAVEL",
    [BUYER_TYPE.GOVERNMENT]: "GOVERNMENT_DEPARTMENT",
    [BUYER_TYPE.CONTRACTOR]: "CONTRACTOR_WORKFORCE_LODGING",
    [BUYER_TYPE.PROJECT_MANAGEMENT]: "PROJECT_LODGING_BUYER",
    [BUYER_TYPE.PRODUCTION_COMPANY]: "PRODUCTION_CREW_LODGING",
    [BUYER_TYPE.UNIVERSITY_PROGRAM]: "PROGRAM_OR_CONFERENCE_OFFICE",
    [BUYER_TYPE.NGO_SECRETARIAT]: "NGO_SECRETARIAT",
    [BUYER_TYPE.MEDICAL_COMMS_AGENCY]: "MEDICAL_COMMS_AGENCY",
    [BUYER_TYPE.CRO]: "CRO_MEETINGS",
    [BUYER_TYPE.UNKNOWN]: "UNRESOLVED_BUYER_FUNCTION",
  };
  return map[type] || "UNRESOLVED_BUYER_FUNCTION";
}

/**
 * @returns {{ buyerEntity, buyerType, buyerRole, organizer, agency, housingPartner, publicContactPath, source }}
 */
export function resolveGdiDemandBuyer(candidate = {}, pageEvidence = {}) {
  const b = `${blob(candidate)} ${blob(pageEvidence)}`;
  const buyerEntity =
    String(
      pageEvidence.organizerName ||
        candidate.organizationName ||
        candidate.organization ||
        pageEvidence.organizationName ||
        ""
    ).trim() || null;

  const buyerType = inferType(b);
  const buyerRole = roleFor(buyerType);

  const organizer =
    pageEvidence.organizerName ||
    (/\borganizer|organiser|secretariat\b/i.test(b) ? buyerEntity : null) ||
    null;

  let agency = null;
  const agencyMatch = b.match(
    /\b((?:[A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,3})\s+(?:Events|Agency|Communications|DMC|Travel))\b/
  );
  if (agencyMatch) agency = agencyMatch[1];
  if (/\bDMC\b/i.test(b) && !agency) agency = "DMC_DETECTED";

  let housingPartner = null;
  if (/\bhousing bureau|official housing|onPeak|Passkey|hotelblocks\b/i.test(b)) {
    housingPartner = "HOUSING_PARTNER_DETECTED";
  }

  const publicContactPath =
    pageEvidence.organizationContactUrl ||
    pageEvidence.functionalContactEmail ||
    candidate.organizationContactUrl ||
    candidate.functionalContactEmail ||
    (pageEvidence.contactPageUrl ? pageEvidence.contactPageUrl : null) ||
    null;

  const source =
    pageEvidence.sourceUrl ||
    candidate.officialSource ||
    candidate.source ||
    candidate.url ||
    null;

  return {
    buyerEntity,
    buyerType,
    buyerRole,
    organizer: organizer || buyerEntity,
    agency,
    housingPartner,
    publicContactPath,
    source,
    resolved: Boolean(buyerEntity) && buyerType !== BUYER_TYPE.UNKNOWN,
  };
}
