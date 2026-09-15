/**
 * Contact Intelligence V1 — paid enrichment policy (disabled by default).
 */

export const CONTACT_PAID_ENRICHMENT_ENV = "CONTACT_INTELLIGENCE_PAID_ENRICHMENT_ENABLED";
export const CONTACT_NATIVE_RESEARCH_ENV = "CONTACT_INTELLIGENCE_NATIVE_RESEARCH_ENABLED";

export function isPaidEnrichmentEnabled(env = process.env) {
  const v = String(env[CONTACT_PAID_ENRICHMENT_ENV] || "false")
    .trim()
    .toLowerCase();
  return v === "1" || v === "true" || v === "yes";
}

/** Native research is allowed by default for bounded local methods; may be disabled. */
export function isNativeResearchEnabled(env = process.env) {
  const raw = env[CONTACT_NATIVE_RESEARCH_ENV];
  if (raw == null || String(raw).trim() === "") return true;
  const v = String(raw).trim().toLowerCase();
  return v === "1" || v === "true" || v === "yes";
}

export function assertPaidEnrichmentAllowed(env = process.env) {
  if (!isPaidEnrichmentEnabled(env)) {
    const err = new Error("paid_enrichment_disabled");
    err.code = "paid_enrichment_disabled";
    err.customer_safe = "Paid contact enrichment is disabled.";
    throw err;
  }
}
