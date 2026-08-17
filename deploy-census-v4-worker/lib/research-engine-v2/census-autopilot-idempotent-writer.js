/**
 * Idempotent Autopilot write rules — re-read, compare, write blanks only.
 *
 * Exception: High-confidence core-identity normalizations (City / State /
 * Canonical Property Name) may overwrite populated values when the change is
 * a safe case/accent/split/membership-suffix cleanup — never a material rename.
 */

import {
  isAllowedAutopilotField,
  isForbiddenAutopilotField,
  isGeocodeField,
  sanitizeAutopilotPatch,
} from "./census-autopilot-field-allowlist.js";
import { isWritableConfidence, normalizeConfidence } from "./census-autopilot-confidence.js";
import {
  normalizePlaceKey,
  canonicalCalaCity,
  isDescriptorCity,
} from "./census-city-state-normalizer.js";
import {
  CANONICAL_PROPERTY_NAME_FIELD,
  namesAreEquivalent,
  stripSafeMembershipSuffixes,
} from "./census-canonical-property-name.js";

function isDescriptorCityPlaceholder(city) {
  return isDescriptorCity(city);
}

/** Queues / methods allowed to overwrite populated identity fields safely. */
export const CORE_IDENTITY_OVERWRITE_QUEUES = Object.freeze([
  "brand_normalization",
  "core_identity_quality",
  "core_identity_source_lookup",
  "city_state_normalization",
  "key_field_completion",
]);

export const CORE_IDENTITY_OVERWRITE_FIELDS = Object.freeze([
  "City",
  "State / Region",
  CANONICAL_PROPERTY_NAME_FIELD,
  "Current Brand",
  "Brand Family",
  "Source URL",
  "Official Property URL",
]);

function isSitemapOrDirectorySourceUrl(url) {
  const s = String(url || "").toLowerCase();
  if (!s) return true;
  return /hotel-sitemap|\/locations\/|sitemap\.xml|\/regional-hotels|\/directory|\/destination\/continent\//i.test(
    s
  );
}

function isMarriottOrOfficialPropertyUrl(url) {
  const s = String(url || "");
  return (
    /marriott\.com\/(?:en-us\/)?hotels\/[a-z0-9]+-/i.test(s) ||
    /hilton\.com\/en\/hotels\//i.test(s) ||
    /hoteldetail/i.test(s) ||
    /choicehotels\.com\/[a-z0-9-]+\/[a-z0-9-]+\/[a-z0-9-]+\/[a-z0-9]+/i.test(s) ||
    /ihg\.com\/[^/]+\/hotels\//i.test(s) ||
    /all\.accor\.com\/hotel\//i.test(s) ||
    /wyndhamhotels\.com\/[^/]+\/[^/]+\/[^/]+\/overview/i.test(s) ||
    /preferredhotels\.com\/hotels\//i.test(s)
  );
}

function isBlank(v) {
  if (v == null) return true;
  if (typeof v === "string" && v.trim() === "") return true;
  if (Array.isArray(v) && v.length === 0) return true;
  return false;
}

function normalizeComparable(v) {
  if (v == null) return "";
  if (typeof v === "number") return String(v);
  if (typeof v === "boolean") return v ? "true" : "false";
  if (Array.isArray(v)) return JSON.stringify(v);
  return String(v).trim();
}

/**
 * Compare current Airtable value vs proposed.
 * @param {unknown} current
 * @param {unknown} proposed
 * @returns {'write'|'skip'|'conflict'}
 */
export function compareFieldValues(current, proposed) {
  if (isBlank(current)) return "write";
  if (normalizeComparable(current) === normalizeComparable(proposed)) return "skip";
  return "conflict";
}

/**
 * True when a populated → proposed change is a safe identity normalization
 * (case/accent, High city/state split, exact membership-suffix strip).
 * @param {string} field
 * @param {unknown} current
 * @param {unknown} proposed
 */
/**
 * @param {string} field
 * @param {unknown} current
 * @param {unknown} proposed
 * @param {{ brandNormalization?: boolean }} [opts]
 */
export function isSafeCoreIdentityOverwrite(field, current, proposed, opts = {}) {
  if (!CORE_IDENTITY_OVERWRITE_FIELDS.includes(field)) return false;
  if (isBlank(current) || isBlank(proposed)) return false;
  const cur = String(current).trim();
  const prop = String(proposed).trim();
  if (!cur || !prop) return false;
  if (normalizeComparable(cur) === normalizeComparable(prop)) return false;

  // Case / accent / spelling equivalents (CANCUN → Cancún; Nuevo Leon → Nuevo León)
  if (normalizePlaceKey(cur) === normalizePlaceKey(prop)) return true;
  if (namesAreEquivalent(cur, prop)) return true;

  if (field === "City") {
    // High repair: replace Unknown / descriptor / blank-equivalent dirty placeholders
    if (
      /^unknown$/i.test(cur) ||
      /^n\/?a$/i.test(cur) ||
      isDescriptorCityPlaceholder(cur)
    ) {
      return Boolean(prop) && prop.length >= 3;
    }
    // High city/state split: "Cd. Guadalupe, Nuevo Leon" → "Guadalupe"
    if (cur.includes(",")) {
      let left = cur.split(",")[0].trim();
      left = left.replace(/^cd\.?\s+/i, "").replace(/^ciudad\s+(de\s+)?/i, "").trim();
      if (normalizePlaceKey(left) === normalizePlaceKey(prop)) return true;
      if (namesAreEquivalent(left, prop)) return true;
      const leftCanon = canonicalCalaCity(left);
      if (leftCanon && normalizePlaceKey(leftCanon) === normalizePlaceKey(prop)) return true;
    }
    // Known CALA map: raw folds to same key as proposed canonical
    const canon = canonicalCalaCity(cur);
    if (canon && normalizePlaceKey(canon) === normalizePlaceKey(prop)) return true;
  }

  // Upgrade sitemap/directory Source URL → property-level official URL only
  if (field === "Source URL" || field === "Official Property URL") {
    if (isSitemapOrDirectorySourceUrl(cur) && isMarriottOrOfficialPropertyUrl(prop)) {
      return true;
    }
  }

  if (field === CANONICAL_PROPERTY_NAME_FIELD) {
    const stripped = stripSafeMembershipSuffixes(cur);
    if (stripped && namesAreEquivalent(stripped, prop)) return true;
    if (stripped && normalizeComparable(stripped) === normalizeComparable(prop)) return true;
  }

  // Brand normalization: High canonical/alias fixes from Active Brand Setup dictionary
  if (
    (field === "Current Brand" || field === "Brand Family") &&
    opts.brandNormalization === true
  ) {
    return prop.length >= 2;
  }

  return false;
}

/**
 * Whether this proposal may use safe identity overwrites.
 * @param {object} proposal
 * @param {object} [opts]
 */
export function proposalAllowsCoreIdentityOverwrite(proposal = {}, opts = {}) {
  if (opts.allowCoreIdentityOverwrite === true) return true;
  if (proposal.allow_normalization_overwrite === true) return true;
  if (proposal.allow_overwrite_dirty === true) return true;
  const queue = String(proposal.queue || "");
  if (CORE_IDENTITY_OVERWRITE_QUEUES.includes(queue)) return true;
  const method = String(proposal.method || "");
  if (
    method === "core_identity_quality_gate" ||
    method === "key_field_completion_autofill" ||
    method === "brand_normalization" ||
    method === "canonical_casing" ||
    method === "alias" ||
    method === "misspelling"
  ) {
    return true;
  }
  return false;
}

/**
 * Build idempotent patch from live record fields + proposal.
 * @param {Record<string, unknown>} currentFields
 * @param {Record<string, unknown>} proposedPatch
 * @param {{
 *   confidence?: string,
 *   allowGeocode?: boolean,
 *   schemaV114Ready?: boolean,
 *   threshold?: string,
 *   allowCoreIdentityOverwrite?: boolean,
 *   brandNormalization?: boolean,
 * }} [opts]
 */
export function buildIdempotentPatch(currentFields = {}, proposedPatch = {}, opts = {}) {
  const conf = normalizeConfidence(opts.confidence || "Low");
  if (!isWritableConfidence(conf, { threshold: opts.threshold || "High" })) {
    return {
      action: "no_write",
      reason: `confidence_${conf.toLowerCase()}`,
      fields: {},
      skipped: [],
      conflicts: [],
      dropped: [],
    };
  }

  const sanitized = sanitizeAutopilotPatch(proposedPatch, {
    allowGeocode: opts.allowGeocode,
    schemaV114Ready: opts.schemaV114Ready,
  });

  const fields = {};
  const skipped = [];
  const conflicts = [];
  const allowIdentityOverwrite = opts.allowCoreIdentityOverwrite === true;
  let usedIdentityOverwrite = false;

  for (const [k, proposed] of Object.entries(sanitized.fields)) {
    if (isForbiddenAutopilotField(k)) {
      conflicts.push({ field: k, reason: "forbidden" });
      continue;
    }
    if (!isAllowedAutopilotField(k)) continue;

    const cmp = compareFieldValues(currentFields[k], proposed);
    if (cmp === "write") fields[k] = proposed;
    else if (cmp === "skip") skipped.push({ field: k, reason: "already_matches" });
    else if (
      allowIdentityOverwrite &&
      isSafeCoreIdentityOverwrite(k, currentFields[k], proposed, {
        brandNormalization: opts.brandNormalization === true,
      })
    ) {
      fields[k] = proposed;
      usedIdentityOverwrite = true;
    } else {
      conflicts.push({
        field: k,
        reason: "value_conflict",
        current: currentFields[k],
        proposed,
      });
    }
  }

  const providerDropped = sanitized.dropped.filter((d) => d.reason === "provider_decision_needed");

  return {
    action: Object.keys(fields).length
      ? "write"
      : conflicts.length
        ? "conflict"
        : providerDropped.length && !Object.keys(fields).length
          ? "provider_decision_needed"
          : "skip",
    reason:
      Object.keys(fields).length > 0
        ? usedIdentityOverwrite
          ? "blank_or_safe_identity_normalization"
          : "blank_or_safe_write"
        : conflicts.length
          ? "value_conflict"
          : providerDropped.length
            ? "provider_decision_needed"
            : "nothing_to_write",
    fields,
    skipped,
    conflicts,
    dropped: sanitized.dropped,
    confidence: conf,
    used_identity_overwrite: usedIdentityOverwrite,
  };
}

/**
 * Split a proposal list into write / skip / conflict / provider / steward buckets.
 * @param {Array<object>} proposals - each may include record_id, patch, confidence, current_fields
 * @param {object} [opts]
 */
export function classifyIdempotentProposals(proposals = [], opts = {}) {
  const writable = [];
  const skipped = [];
  const conflicts = [];
  const provider_decision_needed = [];
  const steward = [];
  const blocked = [];

  for (const p of proposals) {
    if (p.source === "webhound" || p.webhound_direct_write) {
      blocked.push({ ...p, block_reason: "webhound_direct_write_forbidden" });
      continue;
    }
    if (p.held || p.human_review_required) {
      steward.push({ ...p, block_reason: "held_record" });
      continue;
    }
    if (p.brand_unconfirmed) {
      steward.push({ ...p, block_reason: "brand_unconfirmed" });
      continue;
    }
    if (p.room_count_ambiguous) {
      steward.push({ ...p, block_reason: "room_count_ambiguous" });
      continue;
    }

    const result = buildIdempotentPatch(p.current_fields || {}, p.patch || p.fields || {}, {
      confidence: p.confidence,
      allowGeocode: opts.allowGeocode,
      schemaV114Ready: opts.schemaV114Ready,
      threshold: opts.threshold,
      allowCoreIdentityOverwrite: proposalAllowsCoreIdentityOverwrite(p, opts),
      brandNormalization:
        String(p.queue || "") === "brand_normalization" ||
        String(p.method || "").startsWith("brand_normalization"),
    });

    const enriched = { ...p, idempotent: result };
    if (result.action === "write") writable.push(enriched);
    else if (result.action === "skip") skipped.push(enriched);
    else if (result.action === "provider_decision_needed") provider_decision_needed.push(enriched);
    else if (result.action === "conflict") conflicts.push(enriched);
    else if (result.reason?.startsWith("confidence_")) steward.push(enriched);
    else blocked.push(enriched);
  }

  return { writable, skipped, conflicts, provider_decision_needed, steward, blocked };
}

/**
 * Detect invalid coordinate patches.
 * @param {Record<string, unknown>} fields
 */
export function validateCoordinatePatch(fields = {}) {
  if (fields.Latitude == null && fields.Longitude == null) return { ok: true };
  const lat = Number(fields.Latitude);
  const lng = Number(fields.Longitude);
  if (!Number.isFinite(lat) || !Number.isFinite(lng)) {
    return { ok: false, reason: "coordinate_invalid" };
  }
  if (lat === 0 && lng === 0) return { ok: false, reason: "zero_zero" };
  if (Math.abs(lat) > 90 || Math.abs(lng) > 180) return { ok: false, reason: "coordinate_out_of_range" };
  return { ok: true };
}

export { isBlank, isGeocodeField };
