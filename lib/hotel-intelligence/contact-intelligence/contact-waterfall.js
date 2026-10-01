/**
 * Contact Intelligence V1 — provider-neutral waterfall routing.
 * Stop when quality threshold met. Do not call every paid provider.
 */

import { VERIFICATION_STATUS, WATERFALL_LEVEL } from "./vocabulary.js";
import { isPaidEnrichmentEnabled } from "./policy.js";

export const CONTACT_WATERFALL_VERSION = "contact-waterfall-v1";

export const WATERFALL_ORDER = Object.freeze([
  WATERFALL_LEVEL.L0_EXISTING_EVIDENCE,
  WATERFALL_LEVEL.L1_OFFICIAL_SOURCE,
  WATERFALL_LEVEL.L2_NATIVE_PUBLIC_WEB,
  WATERFALL_LEVEL.L3_DOMAIN_PUBLIC_HARVEST,
  WATERFALL_LEVEL.L4_PATTERN_CANDIDATES,
  WATERFALL_LEVEL.L5_EMAIL_VERIFICATION,
  WATERFALL_LEVEL.L6_EXTERNAL_PROVIDER,
  WATERFALL_LEVEL.L7_MANUAL_EXCEPTION,
]);

const STOP_VERIFICATION = new Set([
  VERIFICATION_STATUS.VERIFIED_OFFICIAL,
  VERIFICATION_STATUS.VERIFIED_PUBLIC,
  VERIFICATION_STATUS.VERIFIED_MAILBOX,
  VERIFICATION_STATUS.PUBLICLY_PUBLISHED,
]);

/**
 * Whether current contact quality is good enough to stop the waterfall.
 */
export function shouldStopWaterfall({
  verification_status = null,
  has_usable_hotel_contact = false,
  has_usable_owner_path = false,
  require_owner_path = true,
  require_verified_person_email = false,
} = {}) {
  const status = String(verification_status || "").toUpperCase();
  if (status === VERIFICATION_STATUS.INFERRED || status === VERIFICATION_STATUS.INVALID) {
    return { stop: false, reason: "inferred_or_invalid_not_sufficient" };
  }
  if (require_verified_person_email && !STOP_VERIFICATION.has(status)) {
    return { stop: false, reason: "person_email_not_verified" };
  }
  if (has_usable_hotel_contact && (!require_owner_path || has_usable_owner_path)) {
    if (STOP_VERIFICATION.has(status) || status === VERIFICATION_STATUS.DOMAIN_PATTERN_SUPPORTED) {
      // Pattern-supported alone does not stop if we still need a verified email path
      if (status === VERIFICATION_STATUS.DOMAIN_PATTERN_SUPPORTED && require_verified_person_email) {
        return { stop: false, reason: "pattern_supported_needs_verification" };
      }
      return { stop: true, reason: "quality_threshold_met" };
    }
    if (has_usable_hotel_contact && has_usable_owner_path && !require_verified_person_email) {
      return { stop: true, reason: "usable_hotel_and_owner_path" };
    }
  }
  return { stop: false, reason: "continue" };
}

/**
 * Next waterfall level given last completed level and policy.
 */
export function nextWaterfallLevel(lastLevel = null, env = process.env) {
  const idx = lastLevel ? WATERFALL_ORDER.indexOf(lastLevel) : -1;
  const next = WATERFALL_ORDER[idx + 1] || null;
  if (!next) return { level: null, blocked: false, reason: "exhausted" };
  if (next === WATERFALL_LEVEL.L6_EXTERNAL_PROVIDER && !isPaidEnrichmentEnabled(env)) {
    return {
      level: WATERFALL_LEVEL.L7_MANUAL_EXCEPTION,
      blocked: true,
      reason: "paid_enrichment_disabled_skip_l6",
      skipped: WATERFALL_LEVEL.L6_EXTERNAL_PROVIDER,
    };
  }
  return { level: next, blocked: false, reason: null };
}

/**
 * Plan a research run for a hotel/owner contact job (no side effects).
 */
export function planContactWaterfall({
  has_existing_package = false,
  paid_enabled = false,
  start_at = null,
} = {}) {
  const levels = [];
  let startIdx = start_at ? WATERFALL_ORDER.indexOf(start_at) : 0;
  if (startIdx < 0) startIdx = 0;
  if (!has_existing_package && startIdx === 0) {
    // Still include L0 as check-empty
  }
  for (let i = startIdx; i < WATERFALL_ORDER.length; i += 1) {
    const level = WATERFALL_ORDER[i];
    if (level === WATERFALL_LEVEL.L6_EXTERNAL_PROVIDER && !paid_enabled) {
      levels.push({
        level,
        enabled: false,
        note: "paid_enrichment_disabled",
      });
      continue;
    }
    levels.push({ level, enabled: true, note: null });
  }
  return {
    version: CONTACT_WATERFALL_VERSION,
    levels,
    stop_rule: "quality_threshold_or_exhausted",
    parallel_role: "discovery_interpretation_support_only",
    parallel_is_verifier: false,
  };
}
