/**
 * Contact card shaping for Market Alerts UI (Phase A).
 * Never expose provider raw payloads, API IDs, or internal scoring.
 */

import { shouldDisplayContactByDefault, CONTACT_CONFIDENCE } from "./confidence.js";

/**
 * Logical contact record (local cache / API response).
 */
export function createContactRecord(partial = {}) {
  const now = new Date().toISOString();
  return {
    alertId: partial.alertId || null,
    entityKey: partial.entityKey || null,
    personName: partial.personName || null,
    jobTitle: partial.jobTitle || null,
    companyName: partial.companyName || null,
    companyDomain: partial.companyDomain || null,
    stakeholderRole: partial.stakeholderRole || null,
    email: partial.email || null,
    emailStatus: partial.emailStatus || null,
    phone: partial.phone || null,
    phoneStatus: partial.phoneStatus || null,
    linkedinUrl: partial.linkedinUrl || null,
    provider: partial.provider || null,
    providerPersonId: partial.providerPersonId || null,
    matchConfidence: partial.matchConfidence || CONTACT_CONFIDENCE.LOW,
    reasonSelected: partial.reasonSelected || null,
    sourceArticle: partial.sourceArticle || null,
    lastVerifiedAt: partial.lastVerifiedAt || null,
    enrichmentStatus: partial.enrichmentStatus || "planned",
    articleDerived: partial.articleDerived === true,
    quoted: partial.quoted === true,
    stakeholderClassification: partial.stakeholderClassification || null,
    sourceSentence: partial.sourceSentence || null,
    createdAt: partial.createdAt || now,
    updatedAt: partial.updatedAt || now,
  };
}

/** Fields safe to return to authenticated Market Alerts UI. */
export function toContactCard(record = {}, { includeLow = false } = {}) {
  const confidence = record.matchConfidence || CONTACT_CONFIDENCE.LOW;
  if (!includeLow && !shouldDisplayContactByDefault(confidence)) {
    return null;
  }
  return {
    personName: record.personName || null,
    jobTitle: record.jobTitle || null,
    companyName: record.companyName || null,
    stakeholderRole: record.stakeholderRole || null,
    reasonSelected: record.reasonSelected || null,
    email: record.email || null,
    emailStatus: record.emailStatus || null,
    phone: record.phone || null,
    phoneStatus: record.phoneStatus || null,
    linkedinUrl: record.linkedinUrl || null,
    matchConfidence: confidence,
    lastVerifiedAt: record.lastVerifiedAt || null,
    enrichmentStatus: record.enrichmentStatus || null,
    articleDerived: record.articleDerived === true,
    quoted: record.quoted === true,
    stakeholderClassification: record.stakeholderClassification || null,
  };
}

export function toContactCards(records = [], opts = {}) {
  return (records || []).map((r) => toContactCard(r, opts)).filter(Boolean);
}
