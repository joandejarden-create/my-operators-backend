/**
 * Contact Intelligence V1 — configurable freshness / TTL model.
 */

import { FRESHNESS, CONTACT_TYPE } from "./vocabulary.js";

export const FRESHNESS_MODEL_VERSION = "contact-freshness-v1";

/** Default TTLs (days) by contact family — configurable via opts.ttl_overrides */
export const DEFAULT_TTL_DAYS = Object.freeze({
  person_role: 75,
  person_email: 75,
  person_phone: 75,
  organization_phone: 120,
  organization_domain: 150,
  organization_email: 120,
  hotel_contact: 120,
  hotel_website: 180,
  default: 90,
});

function familyForContactType(contactType) {
  const t = String(contactType || "").toUpperCase();
  if (t.startsWith("PERSON_") && t.includes("EMAIL")) return "person_email";
  if (t.startsWith("PERSON_") && t.includes("PHONE")) return "person_phone";
  if (t.includes("LINKEDIN") || t.includes("PROFILE")) return "person_role";
  if (t.startsWith("ORG_") && t.includes("PHONE")) return "organization_phone";
  if (t.startsWith("ORG_") && t.includes("EMAIL")) return "organization_email";
  if (t.includes("WEBSITE") || t.includes("DOMAIN")) return "organization_domain";
  if (t.startsWith("HOTEL_")) return "hotel_contact";
  return "default";
}

export function ttlDaysForContactType(contactType, ttl_overrides = {}) {
  const family = familyForContactType(contactType);
  if (ttl_overrides[family] != null) return Number(ttl_overrides[family]);
  if (ttl_overrides[contactType] != null) return Number(ttl_overrides[contactType]);
  return DEFAULT_TTL_DAYS[family] ?? DEFAULT_TTL_DAYS.default;
}

/**
 * Build freshness block for a canonical contact.
 */
export function buildFreshnessModel({
  contact_type = null,
  first_seen_at = null,
  last_seen_at = null,
  last_verified_at = null,
  now = Date.now(),
  ttl_overrides = {},
} = {}) {
  const ttl_days = ttlDaysForContactType(contact_type, ttl_overrides);
  const anchor = last_verified_at || last_seen_at || first_seen_at;
  const anchorMs = anchor ? Date.parse(anchor) : NaN;
  let status = FRESHNESS.UNKNOWN;
  let stale_after = null;
  let age_days = null;

  if (Number.isFinite(anchorMs)) {
    age_days = (now - anchorMs) / (1000 * 60 * 60 * 24);
    stale_after = new Date(anchorMs + ttl_days * 86400000).toISOString();
    if (age_days <= ttl_days * 0.5) status = FRESHNESS.FRESH;
    else if (age_days <= ttl_days) status = FRESHNESS.AGING;
    else status = FRESHNESS.STALE;
  }

  return {
    version: FRESHNESS_MODEL_VERSION,
    first_seen_at: first_seen_at || null,
    last_seen_at: last_seen_at || null,
    last_verified_at: last_verified_at || null,
    verification_ttl_days: ttl_days,
    stale_after,
    freshness_status: status,
    age_days: age_days != null ? Number(age_days.toFixed(1)) : null,
    contact_type: contact_type || null,
  };
}

/** Change triggers that should queue contact refresh. */
export const FRESHNESS_CHANGE_TRIGGERS = Object.freeze([
  "owner_changes",
  "person_title_changes",
  "person_leaves_company",
  "organization_domain_changes",
  "email_verification_fails",
  "new_project_announcement",
  "new_press_release",
  "new_transaction",
  "new_management_or_brand_change",
  "owner_control_graph_changes",
]);

export { CONTACT_TYPE, FRESHNESS };
