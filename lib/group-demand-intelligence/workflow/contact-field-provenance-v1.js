/**
 * Field-level contact provenance — backend only.
 * Tracks distinct origins for name / title / profile / email / phone.
 * Surfe corroboration of an existing value does not rewrite original discovery source.
 */

import { createHash } from "node:crypto";
import {
  FIELD_KINDS,
  CHANNEL_OUTCOME,
  PHONE_TYPE,
} from "./buyer-contact-enrichment-contract-v1.js";

export const CONTACT_FIELD_PROVENANCE_VERSION = "gdi-contact-field-provenance-v1";

function text(v) {
  return String(v || "")
    .replace(/\s+/g, " ")
    .trim();
}

export function personIdentityKey({ name, organization, domain } = {}) {
  const n = text(name).toLowerCase();
  const o = text(organization || domain).toLowerCase();
  return createHash("sha256").update(`${n}|${o}`).digest("hex").slice(0, 16);
}

/**
 * @param {object} opts
 * @param {string} opts.fieldKind — FIELD_KINDS.*
 * @param {*} opts.value
 * @param {string} opts.sourceProvider — e.g. official_page | public_profile | surfe | existing_canonical
 * @param {string} [opts.discoveryMethod]
 * @param {string} [opts.sourceUrl]
 * @param {string} [opts.providerRequestId] — internal; never customer-facing
 * @param {string} [opts.verificationStatus]
 * @param {boolean} [opts.selectedForCustomerDisplay]
 */
export function buildFieldProvenanceRecord(opts = {}) {
  const now = new Date().toISOString();
  return {
    version: CONTACT_FIELD_PROVENANCE_VERSION,
    fieldKind: opts.fieldKind,
    value: opts.value ?? null,
    originalSourceProvider: opts.sourceProvider || "unknown",
    discoveryMethod: opts.discoveryMethod || null,
    sourceUrl: opts.sourceUrl || null,
    // Protected provider-response reference (internal)
    providerResponseRef: opts.providerResponseRef || null,
    providerRequestId: opts.providerRequestId || null,
    retrievalDate: opts.retrievalDate || now.slice(0, 10),
    verificationStatus: opts.verificationStatus || "UNKNOWN",
    verificationDate: opts.verificationDate || null,
    selectedForCustomerDisplay: opts.selectedForCustomerDisplay !== false,
    phoneType: opts.phoneType || null,
    channelOutcome: opts.channelOutcome || null,
    supersessionHistory: Array.isArray(opts.supersessionHistory) ? opts.supersessionHistory : [],
    corroborations: Array.isArray(opts.corroborations) ? opts.corroborations : [],
  };
}

/**
 * If Surfe (or another provider) confirms an existing value, record corroboration
 * without changing originalSourceProvider.
 */
export function recordCorroboration(existingField, corroboration = {}) {
  if (!existingField) return existingField;
  const next = {
    ...existingField,
    corroborations: [
      ...(existingField.corroborations || []),
      {
        provider: corroboration.provider || "surfe",
        providerRequestId: corroboration.providerRequestId || null,
        at: new Date().toISOString(),
        verificationStatus: corroboration.verificationStatus || "PROVIDER_VALIDATED",
        note: "Corroboration only — original discovery attribution preserved",
      },
    ],
  };
  if (
    corroboration.verificationStatus &&
    (!existingField.verificationStatus || existingField.verificationStatus === "UNKNOWN")
  ) {
    next.verificationStatus = corroboration.verificationStatus;
    next.verificationDate = new Date().toISOString().slice(0, 10);
  }
  return next;
}

/**
 * Merge a newly discovered field. Does not overwrite richer official evidence
 * with older provider results when hotel corrections or newer official facts exist.
 */
export function mergeFieldProvenance(existing, incoming, opts = {}) {
  if (!incoming || incoming.value == null || incoming.value === "") {
    return existing || null;
  }
  if (!existing || existing.value == null || existing.value === "") {
    return { ...incoming };
  }

  const same = String(existing.value).toLowerCase() === String(incoming.value).toLowerCase();
  if (same) {
    return recordCorroboration(existing, {
      provider: incoming.originalSourceProvider,
      providerRequestId: incoming.providerRequestId,
      verificationStatus: incoming.verificationStatus,
    });
  }

  // Protect hotel corrections and official sources from older provider overwrite
  const existingOfficial = /official|hotel.?correct|canonical/i.test(
    String(existing.originalSourceProvider || "")
  );
  const incomingProvider = /surfe|provider/i.test(String(incoming.originalSourceProvider || ""));
  if (existingOfficial && incomingProvider && opts.preferOfficial !== false) {
    return {
      ...existing,
      supersessionHistory: [
        ...(existing.supersessionHistory || []),
        {
          rejectedValue: incoming.value,
          rejectedProvider: incoming.originalSourceProvider,
          at: new Date().toISOString(),
          reason: "official_or_hotel_correction_protected",
        },
      ],
    };
  }

  // Incoming wins — archive prior
  return {
    ...incoming,
    supersessionHistory: [
      ...(incoming.supersessionHistory || []),
      {
        priorValue: existing.value,
        priorProvider: existing.originalSourceProvider,
        at: new Date().toISOString(),
        reason: opts.replaceReason || "newer_evidence",
      },
    ],
  };
}

/**
 * Build internal provenance bag for a contact enrichment result.
 */
export function buildContactProvenanceBag({
  personName,
  organization,
  domain,
  fields = {},
  responsibilityClass = null,
  relevanceEvidence = null,
  fallbackRoute = null,
  enrichmentStatus = null,
  unresolvedGaps = [],
  lastChecked = null,
  nextRefreshDate = null,
  usageLedgerRefs = [],
} = {}) {
  const normalized = {};
  for (const kind of Object.values(FIELD_KINDS)) {
    if (fields[kind]) normalized[kind] = fields[kind];
  }
  return {
    version: CONTACT_FIELD_PROVENANCE_VERSION,
    personIdentityKey: personIdentityKey({
      name: personName,
      organization,
      domain,
    }),
    personName: personName || null,
    organization: organization || null,
    domain: domain || null,
    responsibilityClass,
    relevanceEvidence,
    fields: normalized,
    fallbackRoute: fallbackRoute || null,
    enrichmentStatus,
    unresolvedGaps: unresolvedGaps || [],
    lastChecked: lastChecked || new Date().toISOString().slice(0, 10),
    nextRefreshDate: nextRefreshDate || null,
    usageLedgerRefs: usageLedgerRefs || [],
    internalOnly: true,
  };
}

/**
 * Strip all provider attribution for customer/API/export/print surfaces.
 * Keeps displayable values (email/phone/name) — removes Surfe/cost/job metadata.
 */
function scrubProviderBrandCopy(text) {
  if (text == null || text === "") return text == null ? null : "";
  return String(text)
    .replace(/\bpreviously verified via Surfe\b/gi, "established from prior verified enrichment")
    .replace(/\bverified via Surfe\b/gi, "established from prior verified enrichment")
    .replace(/\bvia Surfe\b/gi, "from prior verified enrichment")
    .replace(/\bSurfe\b/gi, "enrichment provider")
    .replace(/\bWebhound\b/gi, "research provider")
    .replace(/\s+/g, " ")
    .trim();
}

export function sanitizeContactForCustomerDisplay(contact = {}) {
  if (!contact || typeof contact !== "object") return null;
  const phoneType = contact.phoneType || null;
  const unknownPhone =
    !phoneType || phoneType === PHONE_TYPE.UNKNOWN || /unknown/i.test(String(phoneType));
  const evidence = scrubProviderBrandCopy(
    contact.relevanceEvidence || contact.whyThisContact || null
  );
  return {
    name: contact.name || null,
    role: contact.role || contact.title || null,
    title: contact.title || contact.role || null,
    organization: contact.organization || contact.employer || null,
    email: contact.email || null,
    emailOwnership: contact.emailOwnership || null,
    phone: contact.phone || null,
    phoneOwnership: contact.phoneOwnership || null,
    phoneType: contact.phone ? (unknownPhone ? PHONE_TYPE.UNKNOWN : phoneType) : null,
    phoneTypeLabel: contact.phone
      ? unknownPhone
        ? "Phone"
        : contact.phoneTypeLabel || null
      : null,
    relevanceEvidence: evidence,
    whyThisContact: scrubProviderBrandCopy(
      contact.whyThisContact || contact.relevanceEvidence || null
    ),
    publicProfileUrl: contact.publicProfileUrl || null,
    verificationDate: contact.verificationDate || null,
    responsibilityUncertainty: contact.responsibilityUncertainty || null,
    suggestedContactLabel: contact.suggestedContactLabel || null,
    organizationContactRoute: contact.organizationContactRoute || null,
    organizationContactRouteLabel: contact.organizationContactRouteLabel || null,
    askToBeDirectedTo: contact.askToBeDirectedTo || null,
    // Explicit denylist — never copy Surfe/provider/cost keys through
  };
}

export function assertNoProviderMetadataLeak(payload, path = "root") {
  const leaks = [];
  const walk = (node, p) => {
    if (node == null) return;
    if (typeof node === "string") {
      if (
        /\b(surfe|webhound|apollo|credits?\s*spent|provider\s*cost|trun_|enrichmentID)\b/i.test(
          node
        )
      ) {
        leaks.push({ path: p, sample: node.slice(0, 80) });
      }
      return;
    }
    if (Array.isArray(node)) {
      node.forEach((v, i) => walk(v, `${p}[${i}]`));
      return;
    }
    if (typeof node === "object") {
      for (const [k, v] of Object.entries(node)) {
        if (
          /surfe|providerRequest|providerPayload|credits?|estimatedCost|actualBilled|enrichmentId|_internalProvider/i.test(
            k
          )
        ) {
          leaks.push({ path: `${p}.${k}`, sample: "key" });
        }
        walk(v, `${p}.${k}`);
      }
    }
  };
  walk(payload, path);
  return { ok: leaks.length === 0, leaks };
}

export { FIELD_KINDS, CHANNEL_OUTCOME, PHONE_TYPE };
