/**
 * GDI Contact Enrichment Airtable Schema V1
 *
 * Provider-agnostic usage ledger (GDI ops base) + HI Contact Channels
 * licensing/verification field additions (Hotel Intelligence base).
 *
 * No Findymail/Surfe API calls. No contact PII in the usage ledger.
 * Dry-run default for any future sync writers.
 */

export const GDI_CONTACT_ENRICHMENT_AIRTABLE_SCHEMA_V1 =
  "gdi-contact-enrichment-airtable-schema-v1";

/** GDI / AI Demand Positioning operational base (usage ledger). */
export const GDI_OPS_BASE = Object.freeze({
  baseId: "appa2cE7FTRmIbB32",
  name: "AI Demand Positioning / GDI ops",
  referenceTableId: "tblDwW2jpbKBBtpko", // Hotel Event Spaces — URL anchor confirming base
  groupDemandOpportunitiesTableId: "tblRuReslJMwsfRQj",
});

/** Hotel Intelligence / HPC base (durable contact channels). */
export const HI_BASE = Object.freeze({
  baseId: "appCCUsuGsE1ifoLk",
  name: "Hotel Intelligence / Platform",
  contactChannelsTableId: "tblqC7C4ypy5Zojyw",
  contactChannelsTableName: "HI Contact Channels",
});

export const USAGE_EVENTS_TABLE_NAME = "GDI Contact Enrichment Usage Events";

/**
 * Live table ID after founder-authorized create (2026-11-23).
 * Env override: GDI_CONTACT_ENRICHMENT_USAGE_AIRTABLE_TABLE_ID
 */
export const USAGE_EVENTS_TABLE_ID_LIVE = "tblvUJlik63pETitY";

/** Live field IDs for GDI Contact Enrichment Usage Events (read-back verified). */
export const USAGE_EVENTS_FIELD_IDS = Object.freeze({
  "Lookup Event ID": "fldJn6IMVoqNDxX9B",
  Provider: "fldanrtIK5UEYCHEM",
  "Lookup Outcome": "fldf1OIMoW51E93K2",
  "Credits Charged": "fldRnU1K2D4L5jedD",
  "Cost Estimate USD": "fldAMvuBbIG6UcYrj",
  "Person Identity Hash": "fldvw4DFnTfWX0gy1",
  "Hotel ID": "fldN9R6QRP3N0f0eS",
  "Opportunity ID": "fldA2aguKBXHhx1Y2",
  "Request Origin": "fldCtfG0PyCDvluPi",
  "Event At": "fldKOSQkzFHyGrkKJ",
  "Data Environment": "fldfs7i6Zb6T9hMtp",
  "Schema Version": "fldaGStWc66Q5W1fw",
  "Provider Request Reference": "fld5BTA9c0oVyby3Q",
  Notes: "fldU5KTLXOd7cCpEp",
});

/** Live field IDs added to HI Contact Channels (additive). */
export const HI_CONTACT_CHANNEL_ENRICHMENT_FIELD_IDS = Object.freeze({
  "Last Checked At": "fldyUP3ylAUP8uPUi",
  "License / Retention Class": "fldkJEwy47xROXmys",
  "Retention Expires At": "fldeIWS3vz8s4NyrE",
  "Provider Request ID": "fld4xwUDV3AQ2MBa8",
});

export const USAGE_PROVIDER_CHOICES = Object.freeze([
  "Findymail",
  "Surfe",
  "Public Research",
  "Reuse",
  "Other",
]);

export const USAGE_LOOKUP_OUTCOME_CHOICES = Object.freeze([
  "REUSE_HIT",
  "LOOKUP_FOUND",
  "LOOKUP_MISS",
  "LOOKUP_SUPPRESSED",
  "LOOKUP_STALE_REFRESH",
  "LOOKUP_BLOCKED",
  "LOOKUP_ERROR",
]);

export const USAGE_REQUEST_ORIGIN_CHOICES = Object.freeze([
  "GDI Opportunity",
  "Hotel Explorer",
  "Contact Intelligence",
  "Admin",
  "Other",
]);

export const USAGE_DATA_ENVIRONMENT_CHOICES = Object.freeze([
  "Development",
  "Staging",
  "Production",
]);

export const MAP_USAGE_EVENT = Object.freeze({
  lookupEventId: "Lookup Event ID",
  provider: "Provider",
  lookupOutcome: "Lookup Outcome",
  creditsCharged: "Credits Charged",
  costEstimateUsd: "Cost Estimate USD",
  personIdentityHash: "Person Identity Hash",
  hotelId: "Hotel ID",
  opportunityId: "Opportunity ID",
  requestOrigin: "Request Origin",
  eventAt: "Event At",
  dataEnvironment: "Data Environment",
  schemaVersion: "Schema Version",
  providerRequestReference: "Provider Request Reference",
  notes: "Notes",
});

/** Forbidden in usage-events table (privacy). */
export const USAGE_EVENTS_FORBIDDEN_FIELD_PATTERNS = Object.freeze([
  /^email$/i,
  /e-?mail/i,
  /phone/i,
  /mobile/i,
  /display.?name/i,
  /^name$/i,
  /raw.?response/i,
  /payload/i,
  /api.?key/i,
  /token/i,
  /password/i,
  /secret/i,
]);

export const LICENSE_RETENTION_CLASS_CHOICES = Object.freeze([
  "PUBLIC_LISTED",
  "PROVIDER_LICENSED_INTERNAL",
  "PROVIDER_LICENSED_CUSTOMER_DURABLE",
  "PROVIDER_LICENSED_EXPORT_OK",
  "SUPPRESSED",
  "OPT_OUT",
  "EXPIRED",
]);

export const MAP_HI_CONTACT_CHANNEL_ENRICHMENT_V1 = Object.freeze({
  lastCheckedAt: "Last Checked At",
  licenseRetentionClass: "License / Retention Class",
  retentionExpiresAt: "Retention Expires At",
  providerRequestId: "Provider Request ID",
});

/**
 * Map granular Findymail V1.1 outcomes → Airtable single-select values.
 */
export function mapUsageOutcomeToAirtable(outcome) {
  const o = String(outcome || "").trim();
  if (USAGE_LOOKUP_OUTCOME_CHOICES.includes(o)) return o;
  if (o.startsWith("LOOKUP_BLOCKED")) return "LOOKUP_BLOCKED";
  if (o === "LOOKUP_ERROR" || /ERROR|FAIL/i.test(o)) return "LOOKUP_ERROR";
  if (o === "REUSE_HIT") return "REUSE_HIT";
  if (o === "LOOKUP_FOUND") return "LOOKUP_FOUND";
  if (o === "LOOKUP_MISS") return "LOOKUP_MISS";
  if (o === "LOOKUP_SUPPRESSED") return "LOOKUP_SUPPRESSED";
  if (o === "LOOKUP_STALE_REFRESH" || o === "STALE_NEEDS_REFRESH") {
    return "LOOKUP_STALE_REFRESH";
  }
  return "LOOKUP_ERROR";
}

export function mapProviderToAirtable(provider) {
  const p = String(provider || "").trim().toLowerCase();
  if (p === "findymail") return "Findymail";
  if (p === "surfe") return "Surfe";
  if (p === "public" || p === "public research" || p === "public_research") {
    return "Public Research";
  }
  if (p === "reuse" || p === "existing" || p === "none") return "Reuse";
  if (!p) return "Other";
  const exact = USAGE_PROVIDER_CHOICES.find((c) => c.toLowerCase() === p);
  return exact || "Other";
}

export function resolveUsageEventsTableId(env = process.env, liveId = USAGE_EVENTS_TABLE_ID_LIVE) {
  const fromEnv = String(env.GDI_CONTACT_ENRICHMENT_USAGE_AIRTABLE_TABLE_ID || "").trim();
  if (fromEnv) return fromEnv;
  if (liveId) return liveId;
  return null;
}

export function assertUsageEventHasNoPii(fields = {}) {
  const keys = Object.keys(fields || {});
  const violations = [];
  for (const k of keys) {
    for (const re of USAGE_EVENTS_FORBIDDEN_FIELD_PATTERNS) {
      if (re.test(k) || re.test(String(fields[k] || ""))) {
        // Allow boolean "Email Found" style only if we never create those fields —
        // this ledger intentionally has none. Any match is a violation.
        violations.push(k);
        break;
      }
    }
  }
  return { ok: violations.length === 0, violations };
}

export default {
  GDI_CONTACT_ENRICHMENT_AIRTABLE_SCHEMA_V1,
  GDI_OPS_BASE,
  HI_BASE,
  USAGE_EVENTS_TABLE_NAME,
  MAP_USAGE_EVENT,
  MAP_HI_CONTACT_CHANNEL_ENRICHMENT_V1,
  mapUsageOutcomeToAirtable,
  mapProviderToAirtable,
  resolveUsageEventsTableId,
  assertUsageEventHasNoPii,
};
