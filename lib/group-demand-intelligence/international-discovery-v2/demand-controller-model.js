/**
 * Demand Controller — canonical entity that influences lodging selection.
 * Not customer-facing as a technical object; surface via safe copy helpers.
 */

import {
  CONTROLLER_AUTHORITY,
  CONTROLLER_TYPE,
  IDV2_VERSION,
} from "./constants.js";

const CONTROLLER_TYPES = new Set(Object.values(CONTROLLER_TYPE));
const AUTHORITIES = new Set(Object.values(CONTROLLER_AUTHORITY));

export function normalizeControllerType(raw) {
  const t = String(raw || "")
    .trim()
    .toUpperCase()
    .replace(/\s+/g, "_");
  if (CONTROLLER_TYPES.has(t)) return t;
  const aliases = {
    TECHNICAL_SECRETARIAT: CONTROLLER_TYPE.ASSOCIATION_SECRETARIAT,
    SECRETARIAT: CONTROLLER_TYPE.ASSOCIATION_SECRETARIAT,
    SECRETARIA_TECNICA: CONTROLLER_TYPE.ASSOCIATION_SECRETARIAT,
    HOUSING: CONTROLLER_TYPE.HOUSING_BUREAU,
    TMC: CONTROLLER_TYPE.TRAVEL_MANAGEMENT_COMPANY,
    CVB: CONTROLLER_TYPE.CONVENTION_BUREAU,
    CONGRESS_OFFICE: CONTROLLER_TYPE.VENUE_CONGRESS_OFFICE,
    EVENT_ORGANIZER: CONTROLLER_TYPE.EVENT_AGENCY,
    REGISTRATION: CONTROLLER_TYPE.REGISTRATION_VENDOR,
  };
  return aliases[t] || CONTROLLER_TYPE.UNKNOWN;
}

export function normalizeControllerAuthority(raw) {
  const a = String(raw || "")
    .trim()
    .toUpperCase()
    .replace(/\s+/g, "_");
  return AUTHORITIES.has(a) ? a : CONTROLLER_AUTHORITY.UNCONFIRMED;
}

/**
 * Build a Demand Controller record. Does not invent authority from organizer status alone.
 */
export function buildDemandController(input = {}) {
  const controllerName = String(input.controllerName || input.name || "").trim();
  const controllerType = normalizeControllerType(input.controllerType || input.type);
  const authority = normalizeControllerAuthority(input.selectionAuthority || input.authority);
  const lodgingAuthority = normalizeControllerAuthority(
    input.lodgingAuthority || input.selectionAuthority || input.authority
  );

  if (!controllerName) {
    return { ok: false, reason: "missing_controller_name", controller: null };
  }

  // Generic organizer shell without lodging-role evidence → CONTACT_ONLY max
  const evidenceType = String(input.evidenceType || "").toUpperCase();
  const evidenceText = String(input.evidenceText || input.evidenceSnippet || "").toLowerCase();
  let resolvedAuthority = authority;
  if (
    resolvedAuthority === CONTROLLER_AUTHORITY.CONFIRMED_LODGING_CONTROLLER ||
    resolvedAuthority === CONTROLLER_AUTHORITY.STRONG_SELECTION_INFLUENCE
  ) {
    const hasLodgingCue =
      /housing|accommodation|hotel|alojamiento|unterkunft|kontingent|room.?block|preferred.?hotel|hotel.?oficial|offizielles.?hotel|reservation|reserva/i.test(
        `${evidenceType} ${evidenceText}`
      ) ||
      [
        "OFFICIAL_HOUSING_PAGE",
        "PCO_ACCOMMODATION",
        "HOUSING_PROVIDER",
        "REGISTRATION_HOUSING",
        "HOTEL_RESERVATION_PORTAL",
        "PROCUREMENT_NOTICE",
        "EVENT_MANUAL_HOUSING",
        "DELEGATE_GUIDE_HOUSING",
      ].includes(evidenceType);
    if (!hasLodgingCue && controllerType === CONTROLLER_TYPE.UNKNOWN) {
      resolvedAuthority = CONTROLLER_AUTHORITY.CONTACT_ONLY;
    }
  }

  const idSeed = [
    String(input.market || "").toLowerCase(),
    String(input.country || "").toLowerCase(),
    controllerType,
    controllerName.toLowerCase().replace(/[^a-z0-9]+/g, "_").slice(0, 48),
  ]
    .filter(Boolean)
    .join("__");

  const controller = {
    demandControllerId: String(input.demandControllerId || `dc_${idSeed}`).slice(0, 120),
    controllerType,
    controllerName,
    organizationId: input.organizationId || null,
    campaignId: input.campaignId || null,
    accountId: input.accountId || null,
    market: input.market || null,
    country: input.country || null,
    role: input.role || null,
    publicContactPath: input.publicContactPath || input.contactUrl || null,
    namedContactId: input.namedContactId || null,
    selectionAuthority: resolvedAuthority,
    lodgingAuthority,
    evidenceSource: input.evidenceSource || input.sourceUrl || null,
    evidenceType: evidenceType || "UNSPECIFIED",
    evidenceConfidence: input.evidenceConfidence || "MEDIUM",
    evidenceText: input.evidenceText || input.evidenceSnippet || null,
    validFrom: input.validFrom || null,
    validTo: input.validTo || null,
    provenance: input.provenance || "PUBLIC_WEB",
    historicalCampaigns: Array.isArray(input.historicalCampaigns) ? input.historicalCampaigns : [],
    markets: Array.isArray(input.markets) ? input.markets : input.market ? [input.market] : [],
    knownContacts: Array.isArray(input.knownContacts) ? input.knownContacts : [],
    lastVerified: input.lastVerified || new Date().toISOString(),
    modelVersion: IDV2_VERSION,
  };

  return { ok: true, reason: "ok", controller };
}

/** Customer-safe copy — never expose enums. */
export function customerSafeControllerCopy(controller) {
  if (!controller?.controllerName) return null;
  const name = controller.controllerName;
  const type = controller.controllerType;
  const typeLabel =
    {
      PCO: "professional congress organizer",
      DMC: "destination management company",
      HOUSING_BUREAU: "housing bureau",
      ASSOCIATION_SECRETARIAT: "association secretariat",
      HOST_INSTITUTION: "host institution",
      CORPORATE_TRAVEL: "corporate travel team",
      PROCUREMENT: "procurement team",
      EVENT_AGENCY: "event agency",
      TRAVEL_MANAGEMENT_COMPANY: "travel management company",
      CONVENTION_BUREAU: "convention bureau",
      VENUE_CONGRESS_OFFICE: "venue congress office",
      SPORTS_TRAVEL: "sports travel agency",
      PRODUCTION_COORDINATOR: "production coordinator",
      REGISTRATION_VENDOR: "registration / housing vendor",
    }[type] || "organizing partner";

  const auth = controller.lodgingAuthority || controller.selectionAuthority;
  let lodgingLine = `Hotel selection is managed through ${name} (${typeLabel}).`;
  if (auth === CONTROLLER_AUTHORITY.CONFIRMED_LODGING_CONTROLLER) {
    lodgingLine = `Lodging decision appears to be controlled by ${name}.`;
  } else if (auth === CONTROLLER_AUTHORITY.STRONG_SELECTION_INFLUENCE) {
    lodgingLine = `Hotel selection is strongly influenced by ${name}.`;
  } else if (auth === CONTROLLER_AUTHORITY.PLAUSIBLE_CONTROLLER) {
    lodgingLine = `Lodging coordination may run through ${name}.`;
  } else if (auth === CONTROLLER_AUTHORITY.CONTACT_ONLY) {
    lodgingLine = `Best route into the opportunity appears to be via ${name}.`;
  }

  return {
    lodgingDecisionLine: lodgingLine,
    hotelSelectionLine:
      controller.validTo || controller.validFrom
        ? `Selection timing evidence: ${[controller.validFrom, controller.validTo].filter(Boolean).join(" → ")}`
        : null,
    bestRouteIn: controller.publicContactPath
      ? `Contact path: ${controller.publicContactPath}`
      : `Best route in: ${name}`,
    nextActionHint: "Confirm lodging responsibility and ask for hotel-list / RFP timing.",
  };
}

/**
 * Classify authority from evidence text/type — never from generic organizer alone.
 */
export function classifyControllerAuthorityFromEvidence({
  evidenceType = "",
  evidenceText = "",
  controllerType = CONTROLLER_TYPE.UNKNOWN,
} = {}) {
  const et = String(evidenceType || "").toUpperCase();
  const blob = String(evidenceText || "").toLowerCase();
  const lodgingStrong =
    /official.?hotel|hotel.?oficial|offizielles.?hotel|preferred.?hotel|partner.?hotel|housing.?bureau|accommodation.?partner|room.?block|hotel.?allotment|hotelkontingent|zimmerkontingent|bloque.?de.?habitaciones|alojamiento.?oficial/i.test(
      blob
    ) ||
    [
      "OFFICIAL_HOUSING_PAGE",
      "PCO_ACCOMMODATION",
      "HOUSING_PROVIDER",
      "HOTEL_RESERVATION_PORTAL",
      "EVENT_MANUAL_HOUSING",
      "DELEGATE_GUIDE_HOUSING",
    ].includes(et);

  if (lodgingStrong) return CONTROLLER_AUTHORITY.CONFIRMED_LODGING_CONTROLLER;

  const strongInfluence =
    /pco|dmc|secretar[ií]a.?t[eé]cnica|kongressorganisation|housing|accommodation|alojamiento|unterkunft|travel.?partner|agencia.?de.?viajes/i.test(
      blob
    ) ||
    [
      CONTROLLER_TYPE.PCO,
      CONTROLLER_TYPE.DMC,
      CONTROLLER_TYPE.HOUSING_BUREAU,
      CONTROLLER_TYPE.ASSOCIATION_SECRETARIAT,
      CONTROLLER_TYPE.REGISTRATION_VENDOR,
    ].includes(normalizeControllerType(controllerType));

  if (strongInfluence && (et.includes("OFFICIAL") || et.includes("HOUSING") || et.includes("PCO"))) {
    return CONTROLLER_AUTHORITY.STRONG_SELECTION_INFLUENCE;
  }
  if (strongInfluence) return CONTROLLER_AUTHORITY.PLAUSIBLE_CONTROLLER;
  if (et || blob) return CONTROLLER_AUTHORITY.CONTACT_ONLY;
  return CONTROLLER_AUTHORITY.UNCONFIRMED;
}
