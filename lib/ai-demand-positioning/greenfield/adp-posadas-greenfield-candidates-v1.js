/**
 * ADP Posadas greenfield candidates — Grand Fiesta Americana Chapultepec
 * and Fiesta Americana Condesa Cancún All Inclusive.
 *
 * Draft identities for zero-paid preflight. No live HPC writes. No paid monitoring.
 */

export const ADP_POSADAS_GREENFIELD_CANDIDATES_V1 = "ADP_POSADAS_GREENFIELD_CANDIDATES_V1";

/** @type {readonly object[]} */
export const POSADAS_GREENFIELD_CANDIDATES = Object.freeze([
  Object.freeze({
    draftPropertyId: "adp_grand_fiesta_americana_chapultepec",
    sequence: 1,
    runOrder: "FIRST",
    officialName: "Grand Fiesta Americana Chapultepec",
    brand: "Grand Fiesta Americana",
    parentGroup: "Grupo Posadas",
    loyaltyProgram: "Fiesta Rewards",
    loyaltyEligibility: "VERIFY — Fiesta Rewards / Posadas loyalty; not Marriott/Hilton",
    propertyType: "full_service_urban_luxury",
    chainScaleHypothesis: "Luxury / Upper Upscale",
    city: "Mexico City",
    state: "Ciudad de México",
    country: "Mexico",
    countryCode: "MX",
    addressOfficial:
      "756 Calzada General Mariano Escobedo, Anzures, 11590 Mexico City, Mexico",
    knownHpcId: "recterKmNPdntCm0v",
    hpcReuseDecision: "REUSE_EXISTING_VERIFIED",
    officialUrl:
      "https://www.fiestamericanatravelty.com/en/grand-fiesta-americana/hotels/grand-fiesta-americana-chapultepec",
    operatingStatus: "ACTIVE_OPERATING",
    identityAliases: Object.freeze([
      "Grand Fiesta Americana Chapultepec",
      "Grand Fiesta Americana Chapultepec Mexico City",
      "GFA Chapultepec",
      "Fiesta Americana Chapultepec",
    ]),
    ownershipNote:
      "Grupo Posadas ownership/brand/distribution/management/loyalty are distinct — do not conflate.",
    demandHypotheses: Object.freeze([
      "executive_corporate_travel",
      "chapultepec_reforma_location",
      "meetings_events_up_to_220_auditorium",
      "luxury_urban_leisure",
      "cultural_attractions_chapultepec",
      "wellness_spa_19th_dining_azur",
    ]),
    demandHypothesisStatus: "RESEARCH_HYPOTHESES_NOT_CERTIFIED_CLAIMS",
  }),
  Object.freeze({
    draftPropertyId: "adp_fiesta_americana_condesa_cancun_ai",
    sequence: 2,
    runOrder: "SECOND_AFTER_CHAPULTEPEC_CERTIFIED",
    officialName: "Fiesta Americana Condesa Cancún All Inclusive",
    brand: "Fiesta Americana",
    parentGroup: "Grupo Posadas",
    loyaltyProgram: "Fiesta Rewards",
    loyaltyEligibility: "VERIFY — Fiesta Rewards; all-inclusive product distinct from Chapultepec urban",
    propertyType: "all_inclusive_beach_resort",
    chainScaleHypothesis: "Upper Upscale / Upscale Resort",
    city: "Cancún",
    state: "Quintana Roo",
    country: "Mexico",
    countryCode: "MX",
    addressOfficial: "Km 16.5 Blvd Kukulkan, Cancún, Quintana Roo, Mexico",
    knownHpcId: "recIEcSWuhSCoNLFN",
    hpcReuseDecision: "REUSE_EXISTING_VERIFIED",
    officialUrl:
      "https://www.fiestamericanatravelty.com/en/fiesta-americana/hotels/fiesta-americana-condesa-cancun-all-inclusive",
    operatingStatus: "ACTIVE_OPERATING",
    identityAliases: Object.freeze([
      "Fiesta Americana Condesa Cancún All Inclusive",
      "Fiesta Americana Condesa Cancun All Inclusive",
      "Fiesta Americana Condesa Cancún",
      "Condesa Cancún All Inclusive",
      "FA Condesa Cancun",
    ]),
    ownershipNote:
      "Same Grupo Posadas parent as Chapultepec — NOT an automatic BPP peer; product/lens differs (AI resort vs urban luxury).",
    demandHypotheses: Object.freeze([
      "all_inclusive_vacation",
      "family_leisure",
      "couples_romantic",
      "beach_holiday",
      "dining_entertainment",
      "weddings_incentives",
      "groups_meetings_verify_vs_leisure",
      "wellness_miiluma_spa",
      "cancun_destination_demand",
    ]),
    demandHypothesisStatus: "RESEARCH_HYPOTHESES_NOT_CERTIFIED_CLAIMS",
  }),
]);

function norm(s) {
  return String(s || "")
    .toLowerCase()
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

export function findPosadasGreenfieldCandidate(hotelQuery) {
  const q = norm(hotelQuery);
  if (!q) return null;
  for (const c of POSADAS_GREENFIELD_CANDIDATES) {
    if (norm(c.draftPropertyId) === q || norm(c.officialName) === q) return c;
    for (const a of c.identityAliases || []) {
      if (norm(a) === q || q.includes(norm(a)) || norm(a).includes(q)) return c;
    }
    // Token soft match
    const tokens = q.split(" ").filter((t) => t.length > 2);
    const nameTokens = norm(c.officialName).split(" ");
    const hit = tokens.filter((t) => nameTokens.includes(t)).length;
    if (hit >= 3 && (q.includes("chapultepec") || q.includes("condesa") || q.includes("cancun"))) {
      return c;
    }
  }
  return null;
}

export function listPosadasGreenfieldCandidates() {
  return [...POSADAS_GREENFIELD_CANDIDATES];
}
