/**
 * Resolve target brand context for Partner Intelligence brand extraction.
 * @see docs/data-intelligence/curio-extraction-context-audit.md
 */
import { PILOT_BRANDS } from "../../api/lib/partner-intelligence-explorer-field-registry.js";

const IDENTITY_FIELD_KEYS = new Set([
  "be.identity.brandName",
  "be.identity.parentCompany",
]);

export function isIdentityFieldKey(fieldKey) {
  return IDENTITY_FIELD_KEYS.has(String(fieldKey || ""));
}

/**
 * @param {{ brandId?: string|null, brandKey?: string|null }} input
 */
export function resolveBrandExtractionContext(input = {}) {
  const brandId = input.brandId ? String(input.brandId).trim() : null;
  const brandKey = input.brandKey ? String(input.brandKey).trim() : null;

  let pilot = null;
  if (brandKey && PILOT_BRANDS[brandKey]) {
    pilot = PILOT_BRANDS[brandKey];
  } else if (brandId) {
    pilot = Object.values(PILOT_BRANDS).find((p) => p.recordId === brandId) || null;
  }

  if (!pilot) {
    return {
      brandId,
      brandKey: null,
      pilotKey: null,
      brandName: null,
      parentCompany: null,
      resolved: false,
      ambiguous: !brandId && !brandKey,
    };
  }

  return {
    brandId: pilot.recordId,
    brandKey: pilot.key,
    pilotKey: pilot.key,
    brandName: pilot.brandName || null,
    parentCompany: pilot.parentCompany || null,
    resolved: true,
    ambiguous: false,
  };
}

function escapeRegExp(text) {
  return String(text || "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

/**
 * @param {string} fieldKey
 * @param {{ brandName?: string|null, parentCompany?: string|null, pilotKey?: string|null }} brandContext
 */
export function buildIdentityFieldHint(fieldKey, brandContext) {
  if (!brandContext?.pilotKey) return null;

  if (fieldKey === "be.identity.brandName" && brandContext.brandName) {
    const name = brandContext.brandName;
    return {
      pilotKey: brandContext.pilotKey,
      identityFallback: true,
      patterns: [new RegExp(escapeRegExp(name), "i")],
      fixedValue: name,
    };
  }

  if (fieldKey === "be.identity.parentCompany" && brandContext.parentCompany) {
    const parent = brandContext.parentCompany;
    return {
      pilotKey: brandContext.pilotKey,
      identityFallback: true,
      patterns: [
        new RegExp(escapeRegExp(parent), "i"),
        /Hilton Worldwide Holdings/i,
        /\bHilton\b/i,
      ],
      fixedValue: parent,
    };
  }

  return null;
}

/**
 * @param {string} extractedValue
 * @param {{ brandName?: string|null, parentCompany?: string|null, pilotKey?: string|null }} brandContext
 */
export function identityValueMatchesBrandContext(fieldKey, extractedValue, brandContext) {
  if (!isIdentityFieldKey(fieldKey) || !brandContext?.resolved) return true;
  const value = String(extractedValue || "").trim();
  if (!value) return false;

  if (fieldKey === "be.identity.brandName") {
    const expected = String(brandContext.brandName || "").toLowerCase();
    const v = value.toLowerCase();
    return v.includes(expected) || expected.includes(v) || /curio/i.test(v) === /curio/i.test(expected);
  }

  if (fieldKey === "be.identity.parentCompany") {
    const expected = String(brandContext.parentCompany || "").toLowerCase();
    const v = value.toLowerCase();
    return v.includes("hilton") || v.includes(expected) || expected.includes(v);
  }

  return true;
}

/**
 * Block cross-brand identity leakage (Kimpton/IHG on non-Kimpton targets).
 */
export function isWrongBrandIdentityLeak(fieldKey, extractedValue, brandContext) {
  if (!isIdentityFieldKey(fieldKey)) return false;
  const value = String(extractedValue || "");
  const isKimptonTarget = brandContext?.pilotKey === "kimptonHotels";
  const hasKimpton = /kimpton/i.test(value);
  const hasIhg = /\bihg\b/i.test(value) || /intercontinental hotels group/i.test(value);

  if (isKimptonTarget) return false;
  if (brandContext?.pilotKey === "curioCollection") {
    return hasKimpton || hasIhg;
  }
  if (brandContext?.resolved && !isKimptonTarget) {
    return hasKimpton || hasIhg;
  }
  return hasKimpton || hasIhg;
}
