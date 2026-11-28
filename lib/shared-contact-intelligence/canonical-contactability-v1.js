/**
 * Shared contactability metrics V1 — same formula for GDI opportunities and HE hotels.
 * Deduplicate people before denominator. Exclude org channels and Surfe session results.
 */

import {
  computeContactabilityMetrics,
  formatContactabilityFactoryFields,
  HE_CONTACTABILITY_METRICS_V1,
} from "../hotel-intelligence/contact-intelligence/he-contactability-metrics-v1.js";
import { surfeCountsTowardContactability } from "./surfe-transient-policy-v1.js";
import { SHARED_CANONICAL_CONTACT_INTELLIGENCE_V1 } from "./architecture-constants-v1.js";

export const CANONICAL_CONTACTABILITY_V1 = "canonical-contactability-v1";

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

/**
 * Map shared resolver people into the contactability metrics input shape.
 */
export function sharedPeopleToContactabilityInput(people = []) {
  return (people || []).map((p) => ({
    name: p.displayName || p.name,
    fullName: p.displayName || p.name,
    title: p.title,
    organization: p.organizationName || p.organization,
    organizationName: p.organizationName || p.organization,
    verified: /validated|confirmed|verified/i.test(String(p.verificationState || "")),
    verificationStatus: p.verificationState || "VERIFIED",
    linkedinUrl: p.linkedinUrl || p.linkedin,
    channels: (p.channels || [])
      .filter((c) => !c.sessionOnly && !c.surfeDerived)
      .map((c) => ({
        kind: c.channelType || c.kind,
        value: c.channelValue || c.value,
        provider: c.provider,
        verificationStatus: c.verificationStatus,
        ephemeral: c.sessionOnly === true,
        surfeDerived: c.surfeDerived === true,
      })),
    emailContacts: p.email
      ? [{ value: p.email, provider: "HI_CONTACT_CHANNELS" }]
      : [],
    phoneContacts: p.phone
      ? [{ value: p.phone, provider: "HI_CONTACT_CHANNELS" }]
      : [],
  }));
}

/**
 * Deduplicate by canonicalPersonId, else normalized name|org.
 */
export function dedupePeopleForContactability(people = []) {
  const byKey = new Map();
  for (const p of people || []) {
    const id = text(p.canonicalPersonId || p.personId);
    const key =
      id ||
      `${text(p.displayName || p.name).toLowerCase()}|${text(
        p.organizationName || p.organization,
      ).toLowerCase()}`;
    if (!key || key === "|") continue;
    const prev = byKey.get(key);
    if (!prev) {
      byKey.set(key, p);
      continue;
    }
    // Prefer the row with more durable channels.
    const prevScore =
      (prev.email ? 2 : 0) + (prev.phone ? 1 : 0) + (prev.linkedinUrl ? 1 : 0);
    const nextScore = (p.email ? 2 : 0) + (p.phone ? 1 : 0) + (p.linkedinUrl ? 1 : 0);
    if (nextScore > prevScore) byKey.set(key, p);
  }
  return [...byKey.values()];
}

export function computeSharedContactability(people = [], opts = {}) {
  const deduped = dedupePeopleForContactability(people);
  const input = sharedPeopleToContactabilityInput(deduped);
  const base = computeContactabilityMetrics(input);
  return {
    ...base,
    version: CANONICAL_CONTACTABILITY_V1,
    underlyingMetric: HE_CONTACTABILITY_METRICS_V1,
    sharedContract: SHARED_CANONICAL_CONTACT_INTELLIGENCE_V1,
    hotelId: opts.hotelId || null,
    opportunityId: opts.opportunityId || null,
    peopleDeduped: deduped.length,
    peopleRaw: (people || []).length,
    surfeExcludedFromDurable: !surfeCountsTowardContactability(),
    metrics: {
      verifiedNamedContacts: base.verifiedNamedContacts,
      verifiedDirectEmails: base.verifiedDirectEmailCount,
      verifiedDirectPhones: base.verifiedDirectPhoneCount,
      verifiedLinkedInProfiles: base.verifiedLinkedInCount,
      directlyContactableProfessionals: base.directlyContactableProfessionals,
      verifiedDecisionMakers: base.relevantDecisionMakerCoverage,
      alternativeProfessionalEntryPoints: base.alternativeEntryPointCoverage,
      unresolvedDirectChannels: base.directChannelsUnresolved,
      directContactabilityRate: base.verifiedDirectContactabilityRate,
      directContactabilityStatus: base.directContactabilityStatus,
    },
  };
}

export { formatContactabilityFactoryFields, computeContactabilityMetrics };

export default {
  CANONICAL_CONTACTABILITY_V1,
  sharedPeopleToContactabilityInput,
  dedupePeopleForContactability,
  computeSharedContactability,
  formatContactabilityFactoryFields,
};
