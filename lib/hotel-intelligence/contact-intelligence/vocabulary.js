/**
 * Contact Intelligence V1 — vocabulary (canonical enums).
 * Dimensions stay separate; never collapse into one score.
 */

export const CONTACT_INTELLIGENCE_VERSION = "contact-intelligence-v1";

/** How a channel is bound to a person / org / property. */
export const ATTRIBUTION = Object.freeze({
  OFFICIAL: "OFFICIAL",
  NAMED_PERSON: "NAMED_PERSON",
  ROLE_MAILBOX: "ROLE_MAILBOX",
  ORGANIZATION: "ORGANIZATION",
  PROPERTY: "PROPERTY",
  INFERRED: "INFERRED",
  UNATTRIBUTED: "UNATTRIBUTED",
});

/** Operational likelihood the channel works — not attribution. */
export const DELIVERABILITY = Object.freeze({
  HIGH: "HIGH",
  MEDIUM: "MEDIUM",
  LOW: "LOW",
  UNKNOWN: "UNKNOWN",
});

/** Whether a person's role/title is still current. */
export const ROLE_CURRENCY = Object.freeze({
  CURRENT: "CURRENT",
  STALE: "STALE",
  UNKNOWN: "UNKNOWN",
});

/** Why this contact matters for the focus hotel. */
export const PROPERTY_RELEVANCE = Object.freeze({
  PROPERTY_DIRECT: "PROPERTY_DIRECT",
  OWNER_ORG: "OWNER_ORG",
  OPERATOR: "OPERATOR",
  PORTFOLIO_REUSED: "PORTFOLIO_REUSED",
  BRAND_NETWORK: "BRAND_NETWORK",
  UNRELATED: "UNRELATED",
});

export const FRESHNESS = Object.freeze({
  FRESH: "FRESH",
  AGING: "AGING",
  STALE: "STALE",
  UNKNOWN: "UNKNOWN",
});

export const USAGE_RIGHTS = Object.freeze({
  PUBLIC_SAFE: "PUBLIC_SAFE",
  INTERNAL_ONLY: "INTERNAL_ONLY",
  RESTRICTED: "RESTRICTED",
});

export const CHANNEL_KIND = Object.freeze({
  HOTEL_PHONE: "HOTEL_PHONE",
  HOTEL_EMAIL: "HOTEL_EMAIL",
  HOTEL_WEBSITE: "HOTEL_WEBSITE",
  ORG_PHONE: "ORG_PHONE",
  ORG_EMAIL: "ORG_EMAIL",
  ORG_WEBSITE: "ORG_WEBSITE",
  CONTACT_FORM: "CONTACT_FORM",
  PERSON_EMAIL: "PERSON_EMAIL",
  PERSON_PHONE: "PERSON_PHONE",
  PERSON_LINKEDIN: "PERSON_LINKEDIN",
  ROLE_MAILBOX: "ROLE_MAILBOX",
  SWITCHBOARD: "SWITCHBOARD",
});

export const CONTACT_SUBJECT_KIND = Object.freeze({
  HOTEL: "HOTEL",
  ORGANIZATION: "ORGANIZATION",
  PERSON: "PERSON",
});

export const UNRESOLVED_REASON = Object.freeze({
  OWNER_UNRESOLVED: "OWNER_UNRESOLVED",
  NO_HOTEL_CONTACT: "NO_HOTEL_CONTACT",
  NO_ORG_ROUTE: "NO_ORG_ROUTE",
  NO_PERSON_EVIDENCE: "NO_PERSON_EVIDENCE",
  PERSON_ROLE_STALE: "PERSON_ROLE_STALE",
  CHANNEL_INFERRED_ONLY: "CHANNEL_INFERRED_ONLY",
  USAGE_RESTRICTED: "USAGE_RESTRICTED",
  REFRESH_REQUIRED: "REFRESH_REQUIRED",
  EVIDENCE_CONFLICT: "EVIDENCE_CONFLICT",
  PAID_ENRICHMENT_DISABLED: "PAID_ENRICHMENT_DISABLED",
});

export const REFRESH_STATUS = Object.freeze({
  NEVER: "NEVER",
  QUEUED: "QUEUED",
  RUNNING: "RUNNING",
  COMPLETE: "COMPLETE",
  PARTIAL: "PARTIAL",
  FAILED: "FAILED",
  STALE: "STALE",
});

export const CONTACT_WRITE_GUARANTEES = Object.freeze({
  owner_name_auto_writes: 0,
  census_owner_mutations: 0,
  airtable_ownership_writes: 0,
  paid_enrichment_default: 0,
  webhound_calls_default: 0,
  inferred_labeled_as_official: 0,
  switchboard_labeled_as_direct_personal: 0,
});
