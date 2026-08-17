/**
 * V1.1 centralized qualification gate for Market Alerts intelligence.
 * Deterministic — no LLM.
 */

import { isGeographyOnlyLabel } from "./market-alerts-geo-keywords.js";

/** @typedef {'whole'|'partial'|'non-hotel'|'ambiguous'} AssetScope */

function escapeRegExp(s) {
  return String(s).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

/**
 * Infer publisher tokens from RSS source label and Google News title suffixes.
 * @param {string} title
 * @param {string} source
 * @returns {string[]}
 */
export function inferPublisherTokens(title = "", source = "") {
  const tokens = [];
  const src = String(source || "").trim();
  if (src && !/^google news\b/i.test(src)) {
    tokens.push(src.replace(/\s*\([^)]*\)\s*$/, "").trim());
  }
  const parts = String(title || "").split(/\s+[-–|]\s+/);
  if (parts.length >= 2) {
    const last = parts[parts.length - 1].trim();
    if (last.length >= 3 && last.length <= 48 && !/\b(20\d{2}|hotel|resort)\b/i.test(last)) {
      tokens.push(last);
    }
  }
  return [...new Set(tokens.filter((t) => t && t.length >= 3))];
}

/**
 * @param {string|null|undefined} name
 * @param {{ publishers?: string[] }} [opts]
 * @returns {boolean}
 */
export function isUsableEntityName(name, opts = {}) {
  const n = String(name || "").trim();
  if (!n || n.length < 3) return false;

  const reject = [
    /^this property$/i,
    /^the property$/i,
    /^this hotel$/i,
    /^the hotel$/i,
    /^former full-service$/i,
    /^former full-service hotel$/i,
    /^hotel property$/i,
    /^resort property$/i,
    /^unnamed hotel$/i,
    /^property$/i,
    /^hotel$/i,
    /^resort$/i,
    /^full-service$/i,
    /^full-service hotel$/i,
    /^new hotel$/i,
    /^a hotel$/i,
    /^the hotel asset$/i,
    /^(the\s+)?(proposed|planned|future|new|themed|boutique|luxury)\s+(hotel|resort)(\s+project)?$/i,
    /^(mixed use|room luxury|planned luxury)\s+(hotel|resort)?$/i,
    /^city hotel$/i,
    /\b(faces|hurdle|adds pakistan|launch hotel|refinances|themed hotel)\b/i,
    /^ihg signs\b/i,
    /^marriott signs\b/i,
    /^hilton signs\b/i,
    /^brings?\b/i,
    /^signs noted\b/i,
    /^noted collection hotel$/i,
    /^uber lands\b/i,
    /\blands?\s+hotel$/i,
  ];
  if (reject.some((re) => re.test(n))) return false;
  if (/\blands?\b/i.test(n) && !/\b(?:island|ireland|highlands|lowlands|mainland)\b/i.test(n)) return false;
  if (/^(signs?|signed|brings?|announces?|opens?|debuts?)\b/i.test(n)) return false;
  if (n.split(/\s+/).length === 1 && /^(former|full|service|downtown|new)$/i.test(n)) return false;
  if (/^[A-Z]{1,2}\s+Hotel$/i.test(n) && !/^AC Hotel$/i.test(n)) return false;
  if (/^room\s+/i.test(n)) return false;
  if (isGeographyOnlyLabel(n)) return false;

  const geoHotel = n.match(/^(.+?)\s+(Hotel|Resort|Inn|Lodge)$/i);
  if (geoHotel && isGeographyOnlyLabel(geoHotel[1])) return false;

  const publishers = opts.publishers || [];
  const core = n.replace(/\s+(hotel|resort|inn|lodge)$/i, "").trim();
  for (const p of publishers) {
    const tok = String(p || "").trim();
    if (tok.length < 4) continue;
    const re = new RegExp(`^${escapeRegExp(tok)}\\s+(hotel|resort|inn|lodge)$`, "i");
    if (re.test(n)) return false;
    if (core.length >= 4 && tok.toLowerCase().includes(core.toLowerCase())) return false;
  }
  return true;
}

const NON_ASSET_TRANSACTION_OBJECT_RE =
  /\b(sell(?:s|ing)?|sold|sale of|acquires?|acquired|acquisition of|buys?|purchased?|purchase of)\s+(?:[\w.'-]+\s+){0,10}(?:booking(?:s)?|platform|software|technology|tech|integration|partnership|distribution|loyalty|service(?:s)?|vendor|guest experience|reservation(?:s)?|data|solutions?|company|business|startup|provider(?:s)?|system(?:s)?|application(?:s)?|tool(?:s)?|stake|shares|equity|capabilities|content|access|inventory|listings?|offering(?:s)?)\b/i;

const PARTNERSHIP_TRANSACTION_CONTEXT_RE =
  /\b(lands?|landed|strikes?|struck|forges?|forged|seals?|sealed)\s+(?:[\w-]+\s+){0,4}(?:hotel\s+)?deals?\s+with\b|\b(hotel\s+deals?\s+with|partners?\s+with|partnership\s+with|distribution\s+(?:agreement|deal|partnership)|booking\s+(?:partnership|integration|platform|deal)|loyalty\s+partnership|platform\s+(?:agreement|partnership|deal)|integration\s+with|strategic\s+(?:alliance|partnership)|commercial\s+alliance|travel[\s-]platform|technology\s+partnership|content\s+partnership)\b/i;

const VENDOR_MA_CONTEXT_RE =
  /\b(?:hospitality|hotel)\s+(?:software|technology|tech|platform|booking|reservation|guest experience|data|services?)\s+(?:company|provider|startup|firm|vendor|platform)\b|\b(?:software|technology|tech|booking|reservation|guest experience|platform)\s+(?:company|provider|startup|firm|vendor)\s+(?:acquired|acquires|buys|sold|merges)\b/i;

const HOTEL_ASSET_TX_EVIDENCE_RE =
  /\b(?:(?:hotel|resort|lodge|motel|inn|suites|retreat center|retreat centre|\d{2,4}[-\s]?room(?:\s+hotel)?)\b[^.]{0,80}\b(?:sold|sells?|sale|for sale|acquired|acquires|purchase|purchased|buys?|ownership)|(?:sold|sells?|sale|acquired|acquires|purchase|purchased|buys?)\b[^.]{0,80}\b(?:hotel|resort|lodge|motel|inn|suites|\d{2,4}[-\s]?room))\b/i;

/**
 * V1.1.1 — partnership/platform/vendor context is not a hotel asset transaction.
 * @param {string} text
 * @param {string|null} [eventType]
 */
export function isNonHotelAssetTransactionContext(text, eventType = null) {
  const t = String(text || "");
  if (!["Sale", "Acquisition", "Portfolio Acquisition"].includes(eventType || "")) return false;

  const hasAssetEvidence =
    HOTEL_ASSET_TX_EVIDENCE_RE.test(t) ||
    /\b(portfolio|properties|property)\b[^.]{0,60}\b(?:sold|sells?|sale|acquired|acquires|purchase)\b/i.test(t);

  if (PARTNERSHIP_TRANSACTION_CONTEXT_RE.test(t)) return true;
  if (/\b(sell(?:s|ing)?|sold)\s+(?:[\w-]+\s+){0,6}(?:powered|enabled|integrated|embedded)\b/i.test(t)) {
    return true;
  }
  if (VENDOR_MA_CONTEXT_RE.test(t) && !hasAssetEvidence) return true;
  if (NON_ASSET_TRANSACTION_OBJECT_RE.test(t) && !hasAssetEvidence) return true;
  return false;
}

/**
 * @param {string} text
 * @param {string|null} [eventType]
 */
export function validateHotelAssetTransaction(text, eventType = null) {
  if (!["Sale", "Acquisition", "Portfolio Acquisition"].includes(eventType || "")) {
    return { valid: true, reason: null };
  }
  const t = String(text || "");
  if (isNonHotelAssetTransactionContext(t, eventType)) {
    return { valid: false, reason: "transaction:non_asset_context" };
  }
  if (!HOTEL_ASSET_TX_EVIDENCE_RE.test(t) && !/\b(portfolio|properties|property)\b[^.]{0,60}\b(?:sold|sale|acquired|acquires|purchase)\b/i.test(t)) {
    return { valid: false, reason: "transaction:insufficient_asset_evidence" };
  }
  return { valid: true, reason: null };
}

const NEGATION_RE =
  /\b(not sold|has(?:n't| not) sold|have(?:n't| not) sold|was(?:n't| not) sold|sale failed|sale falls through|sale fell through|deal collapsed|deal terminated|transaction terminated|acquisition abandoned|withdrawn from sale|taken off the market|no longer for sale|buyer pulled out|bidder withdrew|failed to sell|unable to sell|did not sell)\b/i;

const WITHDRAWN_SALE_RE =
  /\b(withdrawn from sale|taken off the market|no longer for sale|pulled from market)\b/i;

const NON_HOTEL_PRIMARY_RE =
  /\b(?:iconic )?(?:coastal )?(?:historic )?pub\b|\b(?:restaurant|tavern|bar and grill|nightclub|brewery)\b|\b(?:office building|warehouse|industrial|retail plaza|shopping center|parking garage)\b/i;

const PARTIAL_ASSET_RE =
  /\b(apartment|condo(?:minium)?|residential unit|branded residence|single unit|retail unit|office component|parking (?:structure|component)|development parcel)\b/i;

const ERSTWHILE_COMPONENT_RE =
  /\b(?:apartment|unit|condo|residence).*(?:erstwhile|former|ex-)\b|\b(?:erstwhile|former|ex-).*(?:hotel|resort).*(?:apartment|unit|condo|sold)\b/i;

const HOTEL_ASSET_RE =
  /\b(hotel|resort|lodge|motel|hostel|inn|suites|retreat center|retreat centre|aparthotel|serviced apartment hotel|\d+[-\s]?key)\b/i;

const MAJOR_TRANSACTION_RE =
  /\$\s*[\d.,]+\s*(?:billion|bn|b\b|million|mm|m\b)/i;

const BRAND_SIGNED_RE =
  /\b(signs?|signed|signing|joins?|debuts?|brings?|to bring|franchise agreement|management agreement|under (?:a )?(?:signed )?(?:franchise|brand|management))\b/i;

const OPERATOR_CONFIRMED_RE =
  /\b(management agreement|operator appointed|appointed (?:as )?(?:operator|manager)|named (?:as )?(?:operator|manager)|will (?:be )?managed by|under (?:a )?management contract|signed management)\b/i;

/**
 * @param {string} text
 * @param {string|null} [eventType]
 */
export function detectTransactionNegation(text, eventType = null) {
  const t = String(text || "");
  if (!NEGATION_RE.test(t) && !WITHDRAWN_SALE_RE.test(t)) {
    return { negated: false, reason: null };
  }

  const saleLike =
    !eventType ||
    ["Sale", "Acquisition", "Hotel For Sale", "Portfolio Acquisition"].includes(eventType);
  if (!saleLike && !/\b(sold|sale|for sale|acquired|acquisition)\b/i.test(t)) {
    return { negated: false, reason: null };
  }

  if (/\bnot sold\b/i.test(t)) return { negated: true, reason: "negation:not_sold" };
  if (WITHDRAWN_SALE_RE.test(t)) return { negated: true, reason: "negation:withdrawn_from_sale" };
  if (/\b(sale fell through|sale falls through|sale failed)\b/i.test(t)) {
    return { negated: true, reason: "negation:sale_failed" };
  }
  return { negated: true, reason: "negation:transaction_contradicted" };
}

/**
 * @param {string} text
 * @param {string|null} [eventType]
 * @returns {{ isHotelAsset: boolean, assetScope: AssetScope, reason: string|null }}
 */
export function validateHospitalityAsset(text, eventType = null) {
  const t = String(text || "");
  const txLike =
    eventType &&
    ["Sale", "Acquisition", "Hotel For Sale", "Portfolio Acquisition", "Distress"].includes(
      eventType
    );

  if (ERSTWHILE_COMPONENT_RE.test(t) || (PARTIAL_ASSET_RE.test(t) && /\b(sold|sale|acquired|purchase)\b/i.test(t))) {
    return { isHotelAsset: false, assetScope: "partial", reason: "asset:partial_component" };
  }

  if (NON_HOTEL_PRIMARY_RE.test(t)) {
    const hotelMentioned = HOTEL_ASSET_RE.test(t);
    const pubIsSubject = /\b(?:buy|buys|acquire|acquires|purchase|purchases|sold)\b[^.]{0,80}\bpub\b/i.test(t)
      || /\bpub\b[^.]{0,40}\b(?:sold|acquired|purchase)\b/i.test(t);
    if (pubIsSubject || (!hotelMentioned && /\bpub\b/i.test(t))) {
      return { isHotelAsset: false, assetScope: "non-hotel", reason: "asset:non_hotel_primary" };
    }
  }

  if (HOTEL_ASSET_RE.test(t)) {
    if (PARTIAL_ASSET_RE.test(t) && !/\b(hotel|resort)\s+(?:sold|sale|acquired|for sale)\b/i.test(t)) {
      return { isHotelAsset: false, assetScope: "partial", reason: "asset:partial_with_hotel_mention" };
    }
    return { isHotelAsset: true, assetScope: "whole", reason: null };
  }

  if (eventType === "Hotel For Sale") {
    return { isHotelAsset: true, assetScope: "whole", reason: null };
  }

  if (txLike) {
    return { isHotelAsset: false, assetScope: "ambiguous", reason: "asset:unconfirmed_hospitality" };
  }

  return { isHotelAsset: true, assetScope: "ambiguous", reason: null };
}

/**
 * @param {object} entities
 * @returns {object}
 */
export function sanitizeEntities(entities = {}, opts = {}) {
  const out = { ...entities };
  if (out.hotelProject && !isUsableEntityName(out.hotelProject, opts)) {
    out.hotelProject = null;
  }
  if (out.hotelProject) {
    out.hotelProject = out.hotelProject.replace(/\s{2,}/g, " ").trim();
  }
  if (!out.hotelProject) {
    out.entityKey = out.brandInvolved || out.ownerDeveloper
      ? [out.brandInvolved, out.ownerDeveloper, out.rooms ? `${out.rooms}-keys` : null]
          .filter(Boolean)
          .join("|")
          .toLowerCase()
          .replace(/[^a-z0-9|]+/g, "-") || null
      : out.rooms
        ? `${out.rooms}-keys`
        : null;
  }
  return out;
}

/**
 * Display-safe hotel label for templates.
 * @param {object} entities
 * @param {{ preferGeneric?: boolean }} [opts]
 */
export function displayHotelLabel(entities = {}, opts = {}) {
  if (entities.hotelProject && isUsableEntityName(entities.hotelProject)) {
    return entities.hotelProject;
  }
  if (opts.preferGeneric) return "A hotel asset";
  return null;
}

export function isMajorHotelTransaction(text, entities = {}) {
  if (entities.rooms && entities.rooms >= 75) return true;
  if (MAJOR_TRANSACTION_RE.test(String(text || ""))) return true;
  if (entities.hotelProject && isUsableEntityName(entities.hotelProject)) {
    return /\b(resort|hotel|inn|lodge|palace|tower)\b/i.test(entities.hotelProject);
  }
  return false;
}

export function isClosedBrandDecision(eventType, text) {
  return (
    ["Brand Signing", "Reflag", "Conversion", "Management Agreement"].includes(eventType || "") ||
    (BRAND_SIGNED_RE.test(String(text || "")) &&
      /\b(marriott|hilton|ihg|hyatt|accor|wyndham|choice|radisson|autograph|voco|noted collection|tapestry|curio|tribute|four seasons|fairmont)\b/i.test(
        String(text || "")
      ))
  );
}

export function isClosedOperatorDecision(eventType, text) {
  return (
    ["Operator Appointment", "Operator Change", "Management Agreement"].includes(eventType || "") ||
    OPERATOR_CONFIRMED_RE.test(String(text || ""))
  );
}

/**
 * Apply V1.1 gate — may null event, sanitize entities, and downgrade audience flags.
 * @param {{
 *   title?: string,
 *   summary?: string,
 *   event?: { eventType?: string|null, whatChanged?: string|null, matched?: boolean },
 *   entities?: object,
 *   audience?: object,
 * }} input
 */
export function applyQualificationGate(input = {}) {
  const title = input.title || "";
  const summary = input.summary || "";
  const text = `${title} ${summary}`.trim();
  let eventType = input.event?.eventType || null;
  let whatChanged = input.event?.whatChanged || null;
  const publishers = inferPublisherTokens(title, input.sourceName || input.source || "");
  let entities = sanitizeEntities(input.entities || {}, { publishers });
  const audience = input.audience || { owner: {}, brand: {}, operator: {}, treatment: "STANDARD" };

  const stats = {
    negationRejections: 0,
    assetTypeRejections: 0,
    partialAssetRejections: 0,
    entityQualityRejections: 0,
    closedDecisionDowngrades: 0,
    audienceQualificationDowngrades: 0,
    reasons: [],
  };

  const negation = detectTransactionNegation(text, eventType);
  if (negation.negated) {
    stats.negationRejections = 1;
    stats.reasons.push(negation.reason);
    eventType = null;
    whatChanged = null;
    audience.owner = { worthReviewing: false, signalType: null, decisionStage: null };
    audience.brand = { worthReviewing: false, signalType: null, decisionStage: null };
    audience.operator = { worthReviewing: false, signalType: null, decisionStage: null };
    audience.treatment = "STANDARD";
    return {
      eventType,
      whatChanged,
      entities,
      audience,
      stats,
      asset: { isHotelAsset: false, assetScope: "ambiguous", reason: negation.reason },
    };
  }

  const asset = validateHospitalityAsset(text, eventType);
  const txEvents = ["Sale", "Acquisition", "Hotel For Sale", "Portfolio Acquisition", "Distress"];

  if (eventType && ["Sale", "Acquisition", "Portfolio Acquisition"].includes(eventType)) {
    const txValidation = validateHotelAssetTransaction(text, eventType);
    if (!txValidation.valid) {
      stats.assetTypeRejections += 1;
      stats.reasons.push(txValidation.reason);
      eventType = null;
      whatChanged = null;
    }
  }

  if (eventType && txEvents.includes(eventType)) {
    if (asset.assetScope === "non-hotel") {
      stats.assetTypeRejections = 1;
      stats.reasons.push(asset.reason);
      eventType = null;
      whatChanged = null;
    } else if (asset.assetScope === "partial") {
      stats.partialAssetRejections = 1;
      stats.reasons.push(asset.reason);
      eventType = null;
      whatChanged = null;
    } else if (!asset.isHotelAsset && asset.assetScope === "ambiguous") {
      stats.assetTypeRejections = 1;
      stats.reasons.push(asset.reason);
      eventType = null;
      whatChanged = null;
    }
  }

  if (input.entities?.hotelProject && !entities.hotelProject) {
    stats.entityQualityRejections = 1;
    stats.reasons.push("entity:low_quality_name_rejected");
  }

  // Re-apply audience downgrades based on asset + event state
  if (!eventType) {
    audience.owner = { worthReviewing: false, signalType: null, decisionStage: null };
    audience.brand = { worthReviewing: false, signalType: null, decisionStage: null };
    audience.operator = { worthReviewing: false, signalType: null, decisionStage: null };
    audience.treatment = "STANDARD";
    return { eventType, whatChanged, entities, audience, stats, asset };
  }

  // Partial asset: no brand/operator on component sales even if event survived
  if (asset.assetScope === "partial") {
    if (audience.brand?.worthReviewing || audience.operator?.worthReviewing) {
      stats.partialAssetRejections += 1;
      stats.audienceQualificationDowngrades += 1;
    }
    audience.brand = { worthReviewing: false, signalType: null, decisionStage: null };
    audience.operator = { worthReviewing: false, signalType: null, decisionStage: null };
  }

  // Owner: generic remote sale needs credible hotel asset signal
  if (["Sale", "Acquisition"].includes(eventType) && audience.owner?.worthReviewing) {
    const major = isMajorHotelTransaction(text, entities);
    const hasIdentity = isUsableEntityName(entities.hotelProject) || entities.rooms;
    if (!major && !hasIdentity) {
      audience.owner = { worthReviewing: false, signalType: null, decisionStage: null };
      stats.audienceQualificationDowngrades += 1;
      stats.reasons.push("audience:owner_generic_transaction");
    } else if (eventType === "Sale") {
      audience.owner.signalType = "Capital / Transaction Signal";
      audience.owner.decisionStage = "Likely Decided";
    }
  }

  // Brand: closed affiliation decisions → competitive intelligence, not open opportunity
  if (audience.brand?.worthReviewing) {
    if (isClosedBrandDecision(eventType, text)) {
      if (
        ["Potential Conversion Opportunity", "Potential Development Opportunity", "Reflag Opportunity", "Owner Expansion Signal"].includes(
          audience.brand.signalType || ""
        )
      ) {
        stats.closedDecisionDowngrades += 1;
      }
      if (["Brand Signing", "Reflag", "Conversion"].includes(eventType)) {
        audience.brand.signalType = "Competitive Brand Move";
        audience.brand.decisionStage = "Likely Decided";
      }
    } else if (["Acquisition", "Sale"].includes(eventType)) {
      audience.brand = { worthReviewing: false, signalType: null, decisionStage: null };
      stats.audienceQualificationDowngrades += 1;
      stats.reasons.push("audience:brand_no_open_affiliation_window");
    }
  }

  // Operator: closed management decisions
  if (audience.operator?.worthReviewing) {
    if (isClosedOperatorDecision(eventType, text)) {
      if (audience.operator.signalType === "Potential Management Opportunity") {
        stats.closedDecisionDowngrades += 1;
      }
      audience.operator.signalType =
        eventType === "Management Agreement"
          ? "Management Agreement Announced"
          : "Competitive Operator Move";
      audience.operator.decisionStage = "Likely Decided";
    } else if (["Acquisition", "Sale"].includes(eventType)) {
      audience.operator = { worthReviewing: false, signalType: null, decisionStage: null };
      stats.audienceQualificationDowngrades += 1;
      stats.reasons.push("audience:operator_no_open_management_window");
    }
  }

  // Hotel For Sale requires validated hospitality asset
  if (eventType === "Hotel For Sale" && !asset.isHotelAsset && asset.assetScope !== "whole") {
    audience.owner = { worthReviewing: false, signalType: null, decisionStage: null };
    stats.audienceQualificationDowngrades += 1;
  }

  // Entity required for owner WR on for-sale when no rooms
  if (
    eventType === "Hotel For Sale" &&
    audience.owner?.worthReviewing &&
    !isUsableEntityName(entities.hotelProject) &&
    !entities.rooms &&
    !HOTEL_ASSET_RE.test(text)
  ) {
    // Still allow if clear hotel for sale in text
  }

  const anyReview =
    audience.owner?.worthReviewing ||
    audience.brand?.worthReviewing ||
    audience.operator?.worthReviewing;
  audience.treatment = anyReview ? "REVIEW" : "STANDARD";

  return { eventType, whatChanged, entities, audience, stats, asset };
}
