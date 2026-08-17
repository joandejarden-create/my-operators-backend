/**
 * Owner context for branding-decision targeting — separates asset owners evaluating
 * third-party flags from integrated operators on house brands.
 */
import { normalizeOwnerKey } from "./normalize.js";
import { INTEGRATED_OPERATOR_ALLOWLIST_KEYS } from "./icp-classify.js";

const INDEPENDENT_BRAND_RE =
  /^(independent|unbranded|unaffiliated|none|n\/a|na|null|boutique independent|no brand|non branded|non-branded|other)$/i;

function isIndependentOrUnbrandedBrand(brand) {
  const b = String(brand || "").trim();
  if (!b) return true;
  return INDEPENDENT_BRAND_RE.test(b);
}

/** @typedef {"third_party_brand_decision" | "integrated_operator_mixed" | "integrated_operator_house_brand_only" | "asset_owner"} BrandingOutreachTrack */

export const VAL_GTM_BRANDING_OUTREACH_TRACK = [
  "third_party_brand_decision",
  "integrated_operator_mixed",
  "integrated_operator_house_brand_only",
  "asset_owner",
];

/** CoStar operator-field values that often appear on house-brand assets (not real third-party mgmt). */
const OPERATOR_FIELD_ARTIFACT_RE =
  /\b(visit us|visitusa|visit u\.s\.|sales office|marketing office|corporate office)\b/i;

/** Known third-party hotel managers / franchisors (genuine operator ≠ owner). */
export const KNOWN_THIRD_PARTY_OPERATOR_RE =
  /\b(aimbridge|highgate|hotel equities|davidson|valiance|sage hospitality|apple leisure|alterra|pyramid|crestline|northland|staypineapple|remington|concept hospitality|yotel|g6 hospitality|extended stay america|la quinta|sonesta|groupe du louvre)\b/i;

/** Major franchise flags an integrated operator might license but does not own. */
const MAJOR_THIRD_PARTY_FRANCHISE_RE =
  /\b(marriott|hilton|hyatt|ihg|intercontinental|holiday inn|hampton|courtyard|fairfield|residence inn|springhill|towneplace|aloft|element|four points|sheraton|westin|le meridien|autograph collection|tapestry|curio|ac hotel|moxy|city express|one hotel|fiesta inn|live aqua|gamma|four seasons|radisson|best western|wyndham|choice hotels|comfort|quality inn|sleep inn|clarion|park plaza|crowne plaza|even hotels|indigo|voco|staybridge|candlewood|embassy suites|doubletree|conrad|waldorf|canopy|trademark|surestay|aloft|citizenm|motif|novotel|mercure|pullman|sofitel|mgallery|tribute portfolio|design hotels|unbound collection|handwritten collection|soft brand)\b/i;

const OWNER_TOKEN_STOP = new Set([
  "grupo",
  "group",
  "hotels",
  "hotel",
  "hoteles",
  "resorts",
  "resort",
  "hospitality",
  "gestion",
  "hotelera",
  "international",
  "collection",
  "company",
  "management",
  "hotels and",
  "and resorts",
  "de cv",
  "sa de",
  "sab de",
  "limited",
  "holdings",
]);

/**
 * @param {string} ownerName
 * @param {string} [icpSegment]
 */
export function isIntegratedOperatorOwner(ownerName, icpSegment = "") {
  if (icpSegment === "owner_operator") return true;
  const key = normalizeOwnerKey(ownerName);
  return INTEGRATED_OPERATOR_ALLOWLIST_KEYS.has(key);
}

/**
 * @param {string} ownerName
 */
export function ownerBrandTokens(ownerName) {
  return normalizeOwnerKey(ownerName)
    .split(/\s+/)
    .filter((t) => t.length >= 4 && !OWNER_TOKEN_STOP.has(t));
}

/**
 * Brand belongs to the owner's own platform (Iberostar Waves, Barceló Karmina, etc.).
 * @param {string} ownerName
 * @param {string} brand
 */
export function isHouseBrandForOwner(ownerName, brand) {
  const b = normalizeOwnerKey(brand);
  if (!b || isIndependentOrUnbrandedBrand(brand)) return false;
  const tokens = ownerBrandTokens(ownerName);
  if (tokens.some((t) => b.includes(t))) return true;

  // Sub-brands explicitly tied to owner in brand string.
  const ownerKey = normalizeOwnerKey(ownerName);
  if (ownerKey.length >= 5 && b.includes(ownerKey.replace(/\s+/g, " ").slice(0, 12))) return true;

  return false;
}

/**
 * Franchise / flag from a major platform the owner licenses but does not operate as house brand.
 * @param {string} brand
 * @param {string} ownerName
 */
export function isThirdPartyFranchiseBrand(brand, ownerName) {
  const b = String(brand || "").trim();
  if (!b || isIndependentOrUnbrandedBrand(b)) return false;
  if (isHouseBrandForOwner(ownerName, b)) return false;
  return MAJOR_THIRD_PARTY_FRANCHISE_RE.test(b);
}

/**
 * @param {string} owner
 * @param {string} operator
 */
export function isOwnerAffiliatedOperator(owner, operator) {
  const o = normalizeEntityKey(owner);
  const op = normalizeEntityKey(operator);
  if (!o || !op) return false;
  if (o === op || o.includes(op) || op.includes(o)) return true;
  return ownerBrandTokens(owner).some((t) => op.includes(t));
}

/**
 * @param {string} ownerName
 * @param {string} operator
 * @param {string} brand
 */
export function isOperatorFieldArtifact(ownerName, operator, brand) {
  const op = String(operator || "").trim();
  if (!op) return false;
  if (OPERATOR_FIELD_ARTIFACT_RE.test(op)) return true;
  if (isHouseBrandForOwner(ownerName, brand) && !KNOWN_THIRD_PARTY_OPERATOR_RE.test(op)) {
    return !isOwnerAffiliatedOperator(ownerName, op);
  }
  return false;
}

/**
 * @param {string} trueOwner
 * @param {string} operator
 * @param {string} brand
 * @param {string} [icpSegment]
 */
export function isGenuineThirdPartyOperatorMismatch(trueOwner, operator, brand, icpSegment = "") {
  const owner = String(trueOwner || "").trim();
  const op = String(operator || "").trim();
  const b = String(brand || "").trim();
  if (!b || isIndependentOrUnbrandedBrand(b) || !op) return false;
  if (normalizeEntityKey(owner) === normalizeEntityKey(op)) return false;
  if (isOwnerAffiliatedOperator(owner, op)) return false;
  if (isOperatorFieldArtifact(owner, op, b)) return false;

  if (isThirdPartyFranchiseBrand(b, owner)) return true;
  if (KNOWN_THIRD_PARTY_OPERATOR_RE.test(op)) return true;

  if (isIntegratedOperatorOwner(owner, icpSegment) && isHouseBrandForOwner(owner, b)) {
    return false;
  }

  return true;
}

/**
 * Whether this property should contribute to brand/franchise *decision* intent (not reposition-only).
 * @param {object} property
 * @param {{ ownerName?: string, icpSegment?: string }} ownerContext
 */
export function isBrandDecisionEligibleProperty(property, ownerContext = {}) {
  const ownerName = ownerContext.ownerName || String(property.trueOwner || "").trim();
  const icpSegment = ownerContext.icpSegment || "";
  const brand = String(property.brandAffiliation || property.brand || "").trim();
  const operator = String(property.hotelOperator || "").trim();
  const integrated = isIntegratedOperatorOwner(ownerName, icpSegment);

  if (isIndependentOrUnbrandedBrand(brand)) return true;
  if (isThirdPartyFranchiseBrand(brand, ownerName)) return true;
  if (isGenuineThirdPartyOperatorMismatch(ownerName, operator, brand, icpSegment)) return true;

  if (!integrated) {
    // Asset owners: aging branded assets may still be renewal/conversion conversations.
    return !isHouseBrandForOwner(ownerName, brand);
  }

  // Integrated operator on house brand only — not a pick-a-new-flag conversation.
  if (integrated && isHouseBrandForOwner(ownerName, brand)) return false;

  return true;
}

/**
 * @param {string} ownerName
 * @param {string} icpSegment
 * @param {object[]} propertyScores from scorePropertyBrandingSignals
 */
export function classifyOwnerOutreachTrack(ownerName, icpSegment, propertyScores = []) {
  const integrated = isIntegratedOperatorOwner(ownerName, icpSegment);
  if (!integrated) return "asset_owner";

  const eligible = propertyScores.filter((p) => p.brandDecisionEligible);
  const ineligible = propertyScores.filter((p) => !p.brandDecisionEligible);

  if (eligible.length === 0 && ineligible.length > 0) {
    return "integrated_operator_house_brand_only";
  }
  if (eligible.length > 0 && ineligible.length > 0) {
    return "integrated_operator_mixed";
  }
  if (eligible.length > 0) return "third_party_brand_decision";
  return "asset_owner";
}

/**
 * @param {string} track
 */
export function isBrandDecisionOutreachTrack(track) {
  return track === "third_party_brand_decision" || track === "integrated_operator_mixed" || track === "asset_owner";
}

function normalizeEntityKey(value) {
  return String(value || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}
