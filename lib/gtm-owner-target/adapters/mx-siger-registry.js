/**
 * Mexico commercial registry (SIGER/RPC) — optional fallback when corporate web path insufficient.
 */
import { isMxRntLookupEnabled } from "./mx-rnt-portal-config.js";

export const MX_SIGER_URL = "https://www.siger.gob.mx/";
export const MX_SAT_RFC_VALIDATOR_URL =
  "https://agsc.siat.sat.gob.mx/PTSC/ValidaRFC/index.jsf";
export const MX_SIEM_OPEN_DATA_URL =
  "https://www.datos.gob.mx/dataset/sistema-de-informacion-empresarial-mexicano-siem";

/** @deprecated Use resolveMexicoRegistryPath from mx-corporate-web-first.js */
export function resolveMexicoRegistryPath(bridgeStrategy) {
  if (bridgeStrategy === "direct_entity") return "siger_first";
  return "hotel_website_then_siger";
}

/**
 * Normalize CoStar True Owner into SIGER search variants.
 * @param {string} ownerName
 */
export function mxSigerSearchTerms(ownerName) {
  const raw = String(ownerName || "").trim();
  if (!raw) return [];

  /** @type {string[]} */
  const terms = [raw];
  const withoutLegal = raw
    .replace(/,?\s*S\.?\s*A\.?\s*(\s*de\s*C\.?\s*V\.?)?\.?$/i, "")
    .replace(/,?\s*S\.?\s*A\.?\s*P\.?\s*I\.?\s*(\s*de\s*C\.?\s*V\.?)?\.?$/i, "")
    .trim();
  if (withoutLegal && withoutLegal !== raw) terms.push(withoutLegal);

  const saDeCv = `${withoutLegal || raw} SA de CV`.replace(/\s+/g, " ").trim();
  if (!terms.includes(saDeCv)) terms.push(saDeCv);

  return [...new Set(terms.filter(Boolean))];
}

/**
 * @param {object} queueItem
 * @param {object} [property]
 */
export function buildMxSigerSearchPlan(queueItem, property) {
  const bridgeStrategy = queueItem.bridgeStrategy || "direct_entity";
  const registryPath = resolveMexicoRegistryPath(bridgeStrategy);
  const searchTerms = mxSigerSearchTerms(queueItem.entitySearchName || queueItem.ownerName);

  /** @type {string[]} */
  const manualSteps = [
    `Open SIGER: ${MX_SIGER_URL}`,
    `Search by razón social (try in order): ${searchTerms.map((t) => `"${t}"`).join(", ")}`,
    "Open folio / request certificado — extract representante legal + RFC if shown.",
    `Confirm RFC active: ${MX_SAT_RFC_VALIDATOR_URL}`,
    "Find corporate email on entity website or LinkedIn (legal rep / director general).",
  ];

  /** @type {string[]} */
  const alternateSteps = [];

  if (registryPath === "hotel_website_then_siger") {
    alternateSteps.push(
      "Bridge via hotel operating entity (RNT portals unavailable).",
      property?.buildingName
        ? `Check hotel website / footer for "${property.buildingName}" operating razón social.`
        : "Check hotel website footer for operating razón social.",
      "Search SIGER with entity name found on site or invoice.",
      `Optional: SIEM open data name search — ${MX_SIEM_OPEN_DATA_URL}`,
    );
  }

  if (!isMxRntLookupEnabled()) {
    alternateSteps.push("RNT lookup skipped (set MX_RNT_LOOKUP_ENABLED=1 only if portal works for you).");
  }

  return {
    registryPath,
    bridgeStrategy,
    commercialRegistryUrl: MX_SIGER_URL,
    satRfcValidatorUrl: MX_SAT_RFC_VALIDATOR_URL,
    siemOpenDataUrl: MX_SIEM_OPEN_DATA_URL,
    entitySearchTerms: searchTerms,
    ownerName: queueItem.ownerName,
    property: property || null,
    manualSteps,
    alternateSteps,
  };
}

/**
 * Draft enrichment skeleton for SIGER-first workflow.
 * @param {object} queueItem
 * @param {object} [property]
 */
export function buildDraftEnrichmentFromSigerPlan(queueItem, property) {
  const plan = buildMxSigerSearchPlan(queueItem, property);
  return {
    ownerName: queueItem.ownerName,
    ownerTargetId: queueItem.id || null,
    enrichedAt: null,
    enrichedBy: "mx_siger_search_plan",
    status: "draft",
    bridgeProperty: property
      ? {
          buildingName: property.buildingName,
          city: property.city,
          country: property.country,
        }
      : null,
    registry: {
      system: "MX_SIGER",
      country: "Mexico",
      entityName: queueItem.entitySearchName || queueItem.ownerName,
      entityId: null,
      entityIdLabel: "RFC",
      legalRepresentative: null,
      verificationUrl: null,
      lookupNotes: [...plan.manualSteps, ...plan.alternateSteps],
      registryPath: plan.registryPath,
    },
    contact: {
      verificationTier: "V3",
      verificationSource: "public_registry",
    },
    sigerSearchPlan: plan,
  };
}
