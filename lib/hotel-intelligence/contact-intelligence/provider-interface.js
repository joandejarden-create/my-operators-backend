/**
 * Contact Intelligence V1 — provider-neutral ContactProvider interface.
 * Adapters stubbed; Parallel is discovery/interpretation support only — not a verifier.
 */

import { PROVIDER_ID, VERIFICATION_STATUS } from "./vocabulary.js";
import { isPaidEnrichmentEnabled } from "./policy.js";

export const CONTACT_PROVIDER_INTERFACE_VERSION = "contact-provider-interface-v1";

/**
 * Licensing flags — if unknown, do not assume client display is allowed.
 */
export function createProviderLicenseFlags(partial = {}) {
  return {
    provider: partial.provider || null,
    license_display_allowed:
      partial.license_display_allowed === true
        ? true
        : partial.license_display_allowed === false
          ? false
          : null,
    license_persistence_allowed:
      partial.license_persistence_allowed === true
        ? true
        : partial.license_persistence_allowed === false
          ? false
          : null,
    license_resale_allowed:
      partial.license_resale_allowed === true
        ? true
        : partial.license_resale_allowed === false
          ? false
          : null,
  };
}

export function createProviderResult(partial = {}) {
  const license = createProviderLicenseFlags(partial);
  return {
    provider: license.provider,
    provider_record_id: partial.provider_record_id || null,
    contacts: Array.isArray(partial.contacts) ? partial.contacts : [],
    verification_status: partial.verification_status || VERIFICATION_STATUS.UNRESOLVED,
    cost_usd: Number(partial.cost_usd || 0),
    ...license,
    client_display_allowed:
      license.license_display_allowed === true &&
      partial.verification_status !== VERIFICATION_STATUS.INFERRED,
    notes: partial.notes || null,
  };
}

/**
 * Native web adapter — discovery only (calls injected discover fn).
 */
export function createNativeWebProvider({ discover } = {}) {
  return {
    id: PROVIDER_ID.NATIVE_WEB,
    role: "discovery",
    is_verifier: false,
    async findContacts(query) {
      if (typeof discover !== "function") {
        return createProviderResult({
          provider: PROVIDER_ID.NATIVE_WEB,
          notes: "discover_fn_not_injected",
        });
      }
      const raw = await discover(query);
      return createProviderResult({
        provider: PROVIDER_ID.NATIVE_WEB,
        contacts: raw?.contacts || [],
        verification_status: raw?.verification_status || VERIFICATION_STATUS.PUBLICLY_PUBLISHED,
        license_display_allowed: true,
        license_persistence_allowed: true,
        license_resale_allowed: false,
        cost_usd: raw?.cost_usd || 0,
      });
    },
  };
}

/**
 * Parallel adapter — deep interpretation / escalation support only.
 * Must never be used as sole contact verifier.
 */
export function createParallelResearchProvider({ interpret } = {}) {
  return {
    id: PROVIDER_ID.PARALLEL_RESEARCH,
    role: "interpretation_support",
    is_verifier: false,
    async findContacts(query) {
      if (typeof interpret !== "function") {
        return createProviderResult({
          provider: PROVIDER_ID.PARALLEL_RESEARCH,
          notes: "parallel_not_configured_interpretation_only",
        });
      }
      const raw = await interpret(query);
      return createProviderResult({
        provider: PROVIDER_ID.PARALLEL_RESEARCH,
        contacts: raw?.contacts || [],
        verification_status: VERIFICATION_STATUS.UNRESOLVED,
        license_display_allowed: null,
        notes: "Parallel must not mark contacts VERIFIED_MAILBOX",
        cost_usd: raw?.cost_usd || 0,
      });
    },
  };
}

/**
 * Future paid provider stub — blocked unless paid enrichment enabled.
 */
export function createFuturePaidProviderStub(providerId) {
  return {
    id: providerId,
    role: "enrichment_fallback",
    is_verifier: false,
    async findContacts(_query, env = process.env) {
      if (!isPaidEnrichmentEnabled(env)) {
        return createProviderResult({
          provider: providerId,
          notes: "paid_enrichment_disabled",
          license_display_allowed: null,
        });
      }
      return createProviderResult({
        provider: providerId,
        notes: "adapter_interface_only_not_integrated",
        license_display_allowed: null,
      });
    },
  };
}

export function listContactProviderRegistry() {
  return [
    { id: PROVIDER_ID.NATIVE_WEB, status: "active_discovery" },
    { id: PROVIDER_ID.PARALLEL_RESEARCH, status: "interpretation_only" },
    { id: PROVIDER_ID.CONTEXT_DEV, status: "research_extraction" },
    { id: PROVIDER_ID.FUTURE_APOLLO, status: "interface_only_blocklisted_evidence" },
    { id: PROVIDER_ID.FUTURE_HUNTER, status: "interface_only" },
    { id: PROVIDER_ID.FUTURE_PROSPEO, status: "client_exists_gated" },
    { id: PROVIDER_ID.FUTURE_FULLENRICH, status: "gated_submit" },
    { id: PROVIDER_ID.FUTURE_PDL, status: "interface_only" },
    { id: PROVIDER_ID.FUTURE_ROCKETREACH, status: "interface_only_blocklisted_evidence" },
    { id: PROVIDER_ID.FUTURE_SURFE, status: "client_exists_not_default_enricher" },
  ];
}
