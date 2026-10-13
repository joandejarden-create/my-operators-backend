/**
 * YOTEL Second-Generation Decomposition P0
 * Official-list named participating accounts → traveling entity → buyer function.
 * No Apify. No threshold lowering. Jev shadow only.
 */

import { YOTEL_ACCOUNT_RESEARCH } from "../expansion-pilot/yotel-account-expansion-recovery-v1.js";
import { BUYER_CONTACT_PATH_CLASS } from "../buyer-contact-path-taxonomy-v1.js";

export const YOTEL_SECOND_GEN_ENGINE_ID = "gdi_yotel_second_generation_decomposition_p0";
export const YOTEL_SECOND_GEN_ENV = "GDI_YOTEL_SECOND_GEN_DECOMP_P0";

/** Campaigns targeted for second-gen (organizer/venue-shell weakened). Skip AidEx/CHI/SETAC strong. */
export const YOTEL_SECOND_GEN_TARGET_CAMPAIGNS = Object.freeze([
  "ycamp_geneva_health_forum_2026",
  "ycamp_art_geneve_2027",
  "ycamp_watches_wonders_2027",
  "ycamp_who_eb_160_2027",
  "ycamp_wha_80_2027",
  "ycamp_ecosoc_has_2027",
  "ycamp_ai_for_good_2027",
]);

export const TRAVELING_ENTITY_TYPE = Object.freeze({
  EXHIBITOR_TEAM: "EXHIBITOR_TEAM",
  DELEGATION: "DELEGATION",
  SPEAKER_TEAM: "SPEAKER_TEAM",
  SPONSOR_TEAM: "SPONSOR_TEAM",
  PRODUCTION_CREW: "PRODUCTION_CREW",
  VENDOR_TEAM: "VENDOR_TEAM",
  MEDIA_CREW: "MEDIA_CREW",
  RESEARCH_DELEGATION: "RESEARCH_DELEGATION",
  UNIVERSITY_TEAM: "UNIVERSITY_TEAM",
  NGO_TEAM: "NGO_TEAM",
  CORPORATE_TEAM: "CORPORATE_TEAM",
  OTHER_VERIFIED_GROUP: "OTHER_VERIFIED_GROUP",
});

const SIGNAL_TO_CAMPAIGN = Object.freeze({
  yotel_signal_geneva_health_forum_2026: "ycamp_geneva_health_forum_2026",
  yotel_signal_art_geneve_2026: "ycamp_art_geneve_2027",
  yotel_signal_watches_wonders_2027: "ycamp_watches_wonders_2027",
  yotel_signal_ecosoc_ocha: "ycamp_ecosoc_has_2027",
  yotel_signal_setac_europe_37_2027: "ycamp_setac_europe_37_2027",
  yotel_signal_aidex_geneva_2026: "ycamp_aidex_geneva_2026",
  yotel_signal_chi_geneva_2026: "ycamp_chi_geneva_centennial_2026",
});

/** Skip re-running strong Ready campaigns' expansion children in this P0. */
const SKIP_SIGNAL_IDS = new Set([
  "yotel_signal_aidex_geneva_2026",
  "yotel_signal_chi_geneva_2026",
  "yotel_signal_setac_europe_37_2027",
]);

const COHORT_TO_TRAVELING = Object.freeze({
  EXHIBITOR_TEAM: TRAVELING_ENTITY_TYPE.EXHIBITOR_TEAM,
  SPEAKER_FACULTY: TRAVELING_ENTITY_TYPE.SPEAKER_TEAM,
  SPONSOR_ACTIVATION: TRAVELING_ENTITY_TYPE.SPONSOR_TEAM,
  DELEGATION: TRAVELING_ENTITY_TYPE.DELEGATION,
  PRODUCTION_CREW: TRAVELING_ENTITY_TYPE.PRODUCTION_CREW,
  VENDOR_CREW: TRAVELING_ENTITY_TYPE.VENDOR_TEAM,
  PROJECT_TEAM: TRAVELING_ENTITY_TYPE.CORPORATE_TEAM,
});

const ROLE_TO_PARTICIPANT = Object.freeze({
  EXHIBITOR: "EXHIBITOR",
  SPEAKER_ORG: "SPEAKER_ORG",
  SPONSOR: "SPONSOR",
  PARTICIPANT: "PARTICIPANT",
  GALLERY: "GALLERY",
});

/**
 * Classify seed for early contamination block.
 * @returns {{ blocked: boolean, failureType: string|null, allowOrganizerHousing: boolean }}
 */
export function classifySecondGenContamination(seed = {}) {
  const role = String(seed.role || seed.participantType || "").toUpperCase();
  const org = String(seed.organizationName || "");
  const lodging = String(seed.lodgingState || "").toUpperCase();
  const note = `${seed.lodgingNote || ""} ${seed.buyerRole || ""}`;

  if (/VENUE|PALEXPO\s*SA|FIERA|AUDITORIUM/i.test(`${role} ${org}`)) {
    return {
      blocked: true,
      failureType: "VENUE_AS_ACCOUNT",
      allowOrganizerHousing: false,
    };
  }
  if (/^EVENT$|EVENT_AS_ACCOUNT/i.test(role) && /watches and wonders|art genève|geneva health forum$/i.test(org)) {
    return {
      blocked: true,
      failureType: "EVENT_AS_ACCOUNT",
      allowOrganizerHousing: false,
    };
  }
  const isOrganizer =
    /ORGANIZER|SECRETARIAT|PARENT_SOCIETY|FAIR_MANAGEMENT/i.test(role) ||
    /^(Art Genève|Watches and Wonders|Geneva Health Forum|World Health Organization|SETAC Europe|United Nations ECOSOC)/i.test(
      org
    );
  if (isOrganizer) {
    // Require affirmative housing-control evidence — not mere mention of "housing"
    // (e.g. "official housing not confirmed" must NOT qualify).
    const housingControl =
      (/(controls?\s+housing|official\s+hotel\s+program|room\s+blocks?|exhibitor\s+housing|delegation\s+accommodation|crew\s+lodging|housing\s+liaison|accommodation\s+liaison)/i.test(
        note
      ) &&
        !/\b(not|no|un)\s*(confirmed|published|evidenced|proven)?\b/i.test(note)) ||
      lodging === "STRONG" ||
      lodging === "CREDIBLE" ||
      seed.organizerHousingControl === true;
    if (!housingControl) {
      return {
        blocked: true,
        failureType: "ORGANIZER_AS_ACCOUNT_WITHOUT_HOUSING_ROLE",
        allowOrganizerHousing: false,
      };
    }
    return {
      blocked: false,
      failureType: null,
      allowOrganizerHousing: true,
    };
  }
  if (/^(partner|sponsors?|exhibitors?|delegations?|tbd|unknown)$/i.test(org.trim())) {
    return {
      blocked: true,
      failureType: "GENERIC_ORG_SHELL",
      allowOrganizerHousing: false,
    };
  }
  return { blocked: false, failureType: null, allowOrganizerHousing: false };
}

/**
 * Map expansion traveling cohort → second-gen traveling entity fields.
 */
export function mapTravelingEntity(account = {}) {
  const type =
    COHORT_TO_TRAVELING[account.travelingCohortType] ||
    (/SPEAKER/i.test(account.accountRole)
      ? TRAVELING_ENTITY_TYPE.SPEAKER_TEAM
      : /SPONSOR/i.test(account.accountRole)
        ? TRAVELING_ENTITY_TYPE.SPONSOR_TEAM
        : /GALLERY|EXHIBITOR/i.test(account.accountRole)
          ? TRAVELING_ENTITY_TYPE.EXHIBITOR_TEAM
          : TRAVELING_ENTITY_TYPE.OTHER_VERIFIED_GROUP);
  const evidence =
    account.travelingCohortSummary ||
    (account.travelingCohortEvidence || [])
      .map((e) => e.excerpt)
      .filter(Boolean)
      .join("; ") ||
    null;
  const confidence = String(account.travelingCohortConfidence || "LOW").toUpperCase();
  const proven =
    Boolean(evidence) &&
    (confidence === "HIGH" || confidence === "MEDIUM") &&
    Boolean(type);
  return {
    travelingEntityType: type,
    travelingEntityEvidence: evidence,
    travelingEntityConfidence: confidence,
    travelingEntityProven: proven,
  };
}

/**
 * Buyer commercial-relevance for second-gen seeds.
 */
export function classifyBuyerCommercialRelevance(seed = {}) {
  const role = `${seed.buyerRole || ""} ${seed.buyerEntity || ""}`;
  const path = String(seed.publicContactPath || "");
  if (
    /event|field marketing|travel|mobility|hospitality|housing|delegation|secretariat|procurement|production|medical affairs|fair ops|exhibitor services|activations|aid.?relief/i.test(
      role
    )
  ) {
    if (seed.namedPerson?.name) {
      return {
        contactPathClass: BUYER_CONTACT_PATH_CLASS.NAMED_BUYER_PERSON,
        buyerPathCommercialRelevance: "STRONG_INTERNAL_ROUTE",
      };
    }
    return {
      contactPathClass: BUYER_CONTACT_PATH_CLASS.RELEVANT_FUNCTION_CONTACT,
      buyerPathCommercialRelevance: "STRONG_INTERNAL_ROUTE",
    };
  }
  if (/presse|press@|media@|pr@/i.test(`${path} ${role}`)) {
    return {
      contactPathClass: BUYER_CONTACT_PATH_CLASS.GENERAL_ORG_CONTACT,
      buyerPathCommercialRelevance: "GENERIC_INTERNAL_CONTACT",
    };
  }
  if (/contact|homepage|info@|customerservice/i.test(`${path} ${role}`) || !role.trim()) {
    return {
      contactPathClass: BUYER_CONTACT_PATH_CLASS.GENERAL_ORG_CONTACT,
      buyerPathCommercialRelevance: "GENERIC_INTERNAL_CONTACT",
    };
  }
  return {
    contactPathClass: BUYER_CONTACT_PATH_CLASS.SOURCE_PAGE,
    buyerPathCommercialRelevance: "GENERIC_INTERNAL_CONTACT",
  };
}

function expansionAccountToSeed(account) {
  const travel = mapTravelingEntity(account);
  const evidenceUrl =
    account.evidenceItems?.[0]?.sourceUrl || account.publicContactPath || null;
  const buyer = {
    buyerEntity: account.buyerEntity || `${account.accountName} — events`,
    buyerRole: account.buyerRole || "Events / Field operations",
    publicContactPath: account.publicContactPath || evidenceUrl,
  };
  const commercial = classifyBuyerCommercialRelevance(buyer);
  const lodgingState =
    account.lodgingControlHypothesis && account.lodgingControlHypothesis !== "UNKNOWN"
      ? "WEAK"
      : "UNKNOWN";

  return {
    organizationName: account.accountName,
    role: account.accountRole || "EXHIBITOR",
    participantType:
      ROLE_TO_PARTICIPANT[account.accountRole] || account.accountRole || "PARTICIPANT",
    travelingGroup: travel.travelingEntityEvidence || account.travelingCohortSummary,
    ...travel,
    ...buyer,
    ...commercial,
    lodgingState,
    lodgingNote: account.lodgingControlSummary || "Lodging controller not verified",
    hotelMotionClass:
      lodgingState === "UNKNOWN" ? "PLAUSIBLE_HOTEL_MOTION" : "PLAUSIBLE_HOTEL_MOTION",
    evidenceUrl,
    hqRegion: account.hqRegion || null,
    secondGeneration: true,
    secondGenEngineId: YOTEL_SECOND_GEN_ENGINE_ID,
    sourceFamily: "official_participant_list_or_first_party",
    recommendedAction: account.recommendedNextAction || null,
    forceClass: travel.travelingEntityProven ? undefined : "SIGNAL_ONLY",
  };
}

/**
 * Curated second-gen seeds for campaigns lacking expansion coverage
 * (WHO/WHA/ECOSOC often PUBLIC_DATA_CEILING — no invented member states).
 * AI for Good: official 2026 partner list → future 2027 cycle thesis (no invented orgs).
 */
function curatedCeilingSeeds(campaignId) {
  if (campaignId === "ycamp_ai_for_good_2027") {
    const src = "https://relvehq.com/events/ai-for-good-global-summit";
    const official2027 = "https://2027summit.aiforgood.itu.int/";
    const mk = (row) => {
      const travel = {
        travelingEntityType: row.travelingEntityType,
        travelingEntityEvidence: row.travelingEntityEvidence,
        travelingEntityConfidence: row.travelingEntityConfidence || "MEDIUM",
        travelingEntityProven: row.travelingEntityProven !== false,
      };
      const buyer = {
        buyerEntity: row.buyerEntity,
        buyerRole: row.buyerRole,
        publicContactPath: row.publicContactPath || official2027,
      };
      const commercial = classifyBuyerCommercialRelevance(buyer);
      return {
        organizationName: row.organizationName,
        role: row.role,
        participantType: row.participantType || row.role,
        travelingGroup: travel.travelingEntityEvidence,
        ...travel,
        ...buyer,
        ...commercial,
        lodgingState: row.lodgingState || "UNKNOWN",
        lodgingNote: row.lodgingNote || "Lodging controller not verified for 2027 cycle",
        hotelMotionClass: row.hotelMotionClass || "PLAUSIBLE_HOTEL_MOTION",
        evidenceUrl: row.evidenceUrl || src,
        secondGeneration: true,
        secondGenEngineId: YOTEL_SECOND_GEN_ENGINE_ID,
        sourceFamily: "official_participant_list_or_first_party",
        forceClass: travel.travelingEntityProven ? undefined : "SIGNAL_ONLY",
      };
    };
    return [
      mk({
        organizationName: "EY",
        role: "SILVER_SPONSOR_2026",
        participantType: "SPONSOR",
        travelingEntityType: TRAVELING_ENTITY_TYPE.SPONSOR_TEAM,
        travelingEntityEvidence:
          "2026 silver sponsor activation / client-hosting team at AI for Good Global Summit (Palexpo) — 2027 renewal unconfirmed",
        buyerEntity: "EY — corporate events / field marketing",
        buyerRole: "Corporate Events / Field Marketing",
        lodgingState: "WEAK",
        hotelMotionClass: "PLAUSIBLE_HOTEL_MOTION",
      }),
      mk({
        organizationName: "Ministry of Science and ICT of the Republic of Korea",
        role: "GOLD_SPONSOR_2026",
        participantType: "GOVERNMENT_DELEGATION",
        travelingEntityType: TRAVELING_ENTITY_TYPE.DELEGATION,
        travelingEntityEvidence:
          "2026 gold sponsor ministerial / agency delegation at AI for Good — international travel cohort for Geneva Palexpo cycle",
        buyerEntity: "MSIT — international events / delegation secretariat",
        buyerRole: "Delegation Secretariat",
        lodgingState: "WEAK",
        hotelMotionClass: "PLAUSIBLE_HOTEL_MOTION",
      }),
      mk({
        organizationName: "Ministry of Internal Affairs and Communications of Japan",
        role: "GOLD_SPONSOR_2026",
        participantType: "GOVERNMENT_DELEGATION",
        travelingEntityType: TRAVELING_ENTITY_TYPE.DELEGATION,
        travelingEntityEvidence:
          "2026 gold sponsor Japanese MIC delegation at AI for Good Geneva — traveling government team thesis",
        buyerEntity: "MIC Japan — international affairs / events",
        buyerRole: "Delegation Secretariat",
        lodgingState: "WEAK",
        hotelMotionClass: "PLAUSIBLE_HOTEL_MOTION",
      }),
      mk({
        organizationName: "PixVerse",
        role: "FILM_FESTIVAL_PARTNER_2026",
        participantType: "PRODUCTION_PARTNER",
        travelingEntityType: TRAVELING_ENTITY_TYPE.PRODUCTION_CREW,
        travelingEntityEvidence:
          "2026 film-festival / production partner crew at AI for Good — activation/production travel cohort",
        buyerEntity: "PixVerse — event production / partnerships",
        buyerRole: "Production Management",
        lodgingState: "WEAK",
        hotelMotionClass: "PLAUSIBLE_HOTEL_MOTION",
      }),
      mk({
        organizationName: "Microsoft",
        role: "SESSION_PARTNER_2026",
        participantType: "SPONSOR_PARTNER",
        travelingEntityType: TRAVELING_ENTITY_TYPE.SPONSOR_TEAM,
        travelingEntityEvidence:
          "2026 session partner booth/speaker support team at AI for Good Global Summit",
        buyerEntity: "Microsoft — event marketing / field ops",
        buyerRole: "Event / Field Marketing",
      }),
      mk({
        organizationName: "Cisco",
        role: "SESSION_PARTNER_2026",
        participantType: "SPONSOR_PARTNER",
        travelingEntityType: TRAVELING_ENTITY_TYPE.SPONSOR_TEAM,
        travelingEntityEvidence:
          "2026 session partner technical demo / sponsor team at AI for Good",
        buyerEntity: "Cisco — event operations",
        buyerRole: "Event Operations",
      }),
      mk({
        organizationName: "Google",
        role: "NETWORKING_PARTNER_2026",
        participantType: "SPONSOR_PARTNER",
        travelingEntityType: TRAVELING_ENTITY_TYPE.SPONSOR_TEAM,
        travelingEntityEvidence:
          "2026 networking partner activation / product demo team at AI for Good",
        buyerEntity: "Google — event marketing",
        buyerRole: "Event / Field Marketing",
      }),
      mk({
        organizationName: "Lenovo",
        role: "NETWORKING_PARTNER_2026",
        participantType: "SPONSOR_PARTNER",
        travelingEntityType: TRAVELING_ENTITY_TYPE.SPONSOR_TEAM,
        travelingEntityEvidence:
          "2026 networking partner product showcase team at AI for Good",
        buyerEntity: "Lenovo — event marketing",
        buyerRole: "Event / Field Marketing",
      }),
      mk({
        organizationName: "HP Inc.",
        role: "NETWORKING_PARTNER_2026",
        participantType: "SPONSOR_PARTNER",
        travelingEntityType: TRAVELING_ENTITY_TYPE.SPONSOR_TEAM,
        travelingEntityEvidence:
          "2026 networking partner activation team (HP Inc.) at AI for Good",
        buyerEntity: "HP Inc. — event marketing",
        buyerRole: "Event / Field Marketing",
      }),
      mk({
        organizationName: "TikTok",
        role: "NETWORKING_PARTNER_2026",
        participantType: "SPONSOR_PARTNER",
        travelingEntityType: TRAVELING_ENTITY_TYPE.MEDIA_CREW,
        travelingEntityEvidence:
          "2026 networking partner content / product team at AI for Good",
        buyerEntity: "TikTok — partnerships / events",
        buyerRole: "Corporate Events",
      }),
      mk({
        organizationName: "Access Partnership",
        role: "SESSION_PARTNER_2026",
        participantType: "CONSULTANCY_PARTNER",
        travelingEntityType: TRAVELING_ENTITY_TYPE.CORPORATE_TEAM,
        travelingEntityEvidence:
          "2026 session partner policy / consultancy team at AI for Good",
        buyerEntity: "Access Partnership — events / policy ops",
        buyerRole: "Event Operations",
      }),
      {
        organizationName: "International Telecommunication Union (ITU)",
        role: "ORGANIZER",
        participantType: "ORGANIZER",
        travelingGroup: "ITU / AI for Good secretariat — largely Geneva-local",
        travelingEntityType: TRAVELING_ENTITY_TYPE.OTHER_VERIFIED_GROUP,
        travelingEntityEvidence: null,
        travelingEntityConfidence: "LOW",
        travelingEntityProven: false,
        buyerEntity: "ITU AI for Good secretariat",
        buyerRole: "Secretariat",
        publicContactPath: official2027,
        contactPathClass: BUYER_CONTACT_PATH_CLASS.SOURCE_PAGE,
        buyerPathCommercialRelevance: "GENERIC_INTERNAL_CONTACT",
        lodgingState: "UNKNOWN",
        lodgingNote: "Organizer without published housing-control role for YOTEL motion",
        hotelMotionClass: "NONE",
        evidenceUrl: official2027,
        forceClass: "SIGNAL_ONLY",
        contaminationFailureType: "ORGANIZER_AS_ACCOUNT_WITHOUT_HOUSING_ROLE",
        secondGeneration: true,
        secondGenEngineId: YOTEL_SECOND_GEN_ENGINE_ID,
        sourceFamily: "official_event_pages",
      },
    ];
  }
  if (campaignId === "ycamp_who_eb_160_2027" || campaignId === "ycamp_wha_80_2027") {
    return [
      {
        organizationName: "World Health Organization",
        role: "SECRETARIAT",
        participantType: "SECRETARIAT",
        travelingGroup: "Secretariat largely Geneva-local — member-state lodgings unnamed without official list",
        travelingEntityType: TRAVELING_ENTITY_TYPE.DELEGATION,
        travelingEntityEvidence: null,
        travelingEntityConfidence: "LOW",
        travelingEntityProven: false,
        buyerEntity: "WHO Governing Bodies / Conference Services",
        buyerRole: "Governing Bodies / Conference Services",
        publicContactPath: "https://www.who.int/gb/gov/en/dates-of-meetings-eb_en.html",
        contactPathClass: BUYER_CONTACT_PATH_CLASS.SOURCE_PAGE,
        buyerPathCommercialRelevance: "GENERIC_INTERNAL_CONTACT",
        lodgingState: "UNKNOWN",
        lodgingNote: "Member-state lists not published for this cycle in pack — do not invent",
        hotelMotionClass: "NONE",
        evidenceUrl: "https://www.who.int/gb/gov/en/dates-of-meetings-eb_en.html",
        forceClass: "SIGNAL_ONLY",
        secondGeneration: true,
        secondGenEngineId: YOTEL_SECOND_GEN_ENGINE_ID,
        sourceFamily: "official_event_pages",
        publicDataCeiling: true,
      },
    ];
  }
  if (campaignId === "ycamp_ecosoc_has_2027") {
    return [
      {
        organizationName: "United Nations ECOSOC / OCHA",
        role: "SECRETARIAT",
        participantType: "SECRETARIAT",
        travelingGroup: "2027 Geneva HAS participant lists not yet public — 2026 HAS was New York",
        travelingEntityType: TRAVELING_ENTITY_TYPE.NGO_TEAM,
        travelingEntityEvidence: null,
        travelingEntityConfidence: "LOW",
        travelingEntityProven: false,
        buyerEntity: "ECOSOC / OCHA conference support",
        buyerRole: "Conference support",
        publicContactPath: "https://sdg.iisd.org/events/ecosoc-humanitarian-affairs-segment-2027/",
        contactPathClass: BUYER_CONTACT_PATH_CLASS.SOURCE_PAGE,
        buyerPathCommercialRelevance: "GENERIC_INTERNAL_CONTACT",
        lodgingState: "UNKNOWN",
        lodgingNote: "Do not use NY 2026 HAS participants as Geneva lodging accounts",
        hotelMotionClass: "NONE",
        evidenceUrl: "https://sdg.iisd.org/events/ecosoc-humanitarian-affairs-segment-2027/",
        forceClass: "SIGNAL_ONLY",
        secondGeneration: true,
        secondGenEngineId: YOTEL_SECOND_GEN_ENGINE_ID,
        sourceFamily: "official_event_pages",
        publicDataCeiling: true,
      },
    ];
  }
  return [];
}

/**
 * Official-list / first-party second-gen seeds for a YOTEL campaign.
 */
export function getYotelSecondGenerationSeeds(campaignId) {
  if (!YOTEL_SECOND_GEN_TARGET_CAMPAIGNS.includes(campaignId)) return [];

  const seeds = [];
  const accounts = Array.isArray(YOTEL_ACCOUNT_RESEARCH)
    ? YOTEL_ACCOUNT_RESEARCH
    : [];

  for (const account of accounts) {
    if (account.evidenceStatus !== "ACCEPTED") continue;
    if (SKIP_SIGNAL_IDS.has(account.parentDemandSignalId)) continue;
    const mapped = SIGNAL_TO_CAMPAIGN[account.parentDemandSignalId];
    if (mapped !== campaignId) continue;
    seeds.push(expansionAccountToSeed(account));
  }

  if (seeds.length === 0) {
    seeds.push(...curatedCeilingSeeds(campaignId));
  }

  // Deduplicate
  const seen = new Set();
  return seeds.filter((s) => {
    const k = String(s.organizationName || "")
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, " ")
      .trim();
    if (!k || seen.has(k)) return false;
    seen.add(k);
    return true;
  });
}

/**
 * Merge second-gen seeds into base pack; demote organizer/venue shells.
 */
export function mergeYotelEvidencePackWithSecondGen(campaignId, basePack = []) {
  const second = getYotelSecondGenerationSeeds(campaignId);
  const demotedBase = (basePack || []).map((s) => {
    const contam = classifySecondGenContamination(s);
    if (contam.blocked && contam.failureType === "VENUE_AS_ACCOUNT") {
      return { ...s, forceClass: "REJECTED", contaminationFailureType: contam.failureType };
    }
    if (contam.blocked && contam.failureType === "ORGANIZER_AS_ACCOUNT_WITHOUT_HOUSING_ROLE") {
      return {
        ...s,
        forceClass: "SIGNAL_ONLY",
        contaminationFailureType: contam.failureType,
      };
    }
    return s;
  });

  const byKey = new Map();
  for (const s of [...demotedBase, ...second]) {
    const k = String(s.organizationName || "")
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, " ")
      .trim();
    if (!k) continue;
    // Prefer second-gen over organizer shell
    if (!byKey.has(k) || s.secondGeneration) byKey.set(k, s);
  }
  return [...byKey.values()];
}

export function isYotelSecondGenEnabled(env = process.env) {
  const v = String(env[YOTEL_SECOND_GEN_ENV] ?? "1").trim();
  return v !== "0" && v.toLowerCase() !== "false";
}

export {
  SIGNAL_TO_CAMPAIGN,
  SKIP_SIGNAL_IDS,
};
