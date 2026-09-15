/**
 * Contact Intelligence V1 — canonical record builders.
 */

import crypto from "node:crypto";
import {
  ATTRIBUTION,
  CHANNEL_KIND,
  CONTACT_SUBJECT_KIND,
  DELIVERABILITY,
  FRESHNESS,
  PROPERTY_RELEVANCE,
  ROLE_CURRENCY,
  USAGE_RIGHTS,
} from "./vocabulary.js";
import {
  classifyDeliverability,
  classifyEmailAttribution,
  classifyFreshness,
  classifyPhoneAttribution,
  classifyRoleCurrency,
  defaultUsageRights,
} from "./dimensions.js";

function newId(prefix) {
  return `${prefix}_${crypto.randomBytes(6).toString("hex")}`;
}

export function createEvidenceRef(partial = {}) {
  return {
    evidence_id: partial.evidence_id || newId("ev"),
    source_title: partial.source_title || null,
    source_url: partial.source_url || null,
    source_type: partial.source_type || null,
    observed_at: partial.observed_at || null,
    excerpt: partial.excerpt || null,
    authority: partial.authority || null,
  };
}

export function createChannel(partial = {}) {
  const kind = String(partial.kind || CHANNEL_KIND.HOTEL_PHONE).toUpperCase();
  let attribution = partial.attribution
    ? String(partial.attribution).toUpperCase()
    : null;

  if (!attribution) {
    if (kind.includes("EMAIL")) {
      attribution = classifyEmailAttribution(partial.value, {
        contactName: partial.person_name || null,
        claimedOfficial: partial.claimed_official === true,
      });
    } else if (kind.includes("PHONE") || kind === CHANNEL_KIND.SWITCHBOARD) {
      attribution = classifyPhoneAttribution(partial.value, {
        lineType: partial.line_type || (kind === CHANNEL_KIND.SWITCHBOARD ? "SWITCHBOARD" : null),
        claimedDirectPersonal: partial.claimed_direct_personal === true,
      });
    } else if (kind.includes("WEBSITE") || kind.includes("LINKEDIN")) {
      attribution = partial.claimed_official ? ATTRIBUTION.OFFICIAL : ATTRIBUTION.ORGANIZATION;
    } else {
      attribution = ATTRIBUTION.UNATTRIBUTED;
    }
  }

  // Hard rule: inferred can never be rewritten to OFFICIAL here
  if (partial.force_inferred === true) attribution = ATTRIBUTION.INFERRED;
  if (attribution === ATTRIBUTION.INFERRED && partial.claimed_official === true) {
    attribution = ATTRIBUTION.INFERRED;
  }

  const deliverability =
    partial.deliverability ||
    classifyDeliverability({
      attribution,
      verified: partial.verified === true,
      bounced: partial.bounced === true,
    });

  const usage_rights = partial.usage_rights || defaultUsageRights(attribution);
  const freshness =
    partial.freshness || classifyFreshness({ observedAt: partial.observed_at || null });

  return {
    channel_id: partial.channel_id || newId("ch"),
    kind,
    value: partial.value || null,
    display_label: partial.display_label || null,
    attribution,
    deliverability,
    property_relevance: partial.property_relevance || PROPERTY_RELEVANCE.PROPERTY_DIRECT,
    freshness,
    usage_rights,
    line_type: partial.line_type || null,
    verified: partial.verified === true,
    evidence: Array.isArray(partial.evidence) ? partial.evidence.map(createEvidenceRef) : [],
    customer_caveat: partial.customer_caveat || null,
    provenance: partial.provenance || null,
  };
}

export function createPersonContact(partial = {}) {
  const role_currency =
    partial.role_currency ||
    classifyRoleCurrency({ observedAt: partial.role_observed_at || partial.observed_at || null });

  return {
    person_id: partial.person_id || newId("person"),
    subject_kind: CONTACT_SUBJECT_KIND.PERSON,
    display_name: partial.display_name || null,
    title: partial.title || null,
    organization_entity_id: partial.organization_entity_id || null,
    organization_name: partial.organization_name || null,
    role_currency,
    property_relevance: partial.property_relevance || PROPERTY_RELEVANCE.OWNER_ORG,
    freshness: partial.freshness || classifyFreshness({ observedAt: partial.observed_at || null }),
    usage_rights: partial.usage_rights || USAGE_RIGHTS.PUBLIC_SAFE,
    channels: Array.isArray(partial.channels) ? partial.channels.map(createChannel) : [],
    evidence: Array.isArray(partial.evidence) ? partial.evidence.map(createEvidenceRef) : [],
    unresolved_reasons: Array.isArray(partial.unresolved_reasons) ? partial.unresolved_reasons : [],
    why_relevant: partial.why_relevant || null,
    contact_source_date: partial.contact_source_date || null,
    role_check_date: partial.role_check_date || partial.role_observed_at || null,
    affiliation_status: partial.affiliation_status || "CURRENT_UNKNOWN",
    former_affiliation: partial.former_affiliation === true,
    publication_label: partial.publication_label || null,
    customer_caveat: partial.customer_caveat || null,
    provenance: partial.provenance || null,
  };
}

export function createHotelContactPackage(partial = {}) {
  return {
    package_version: "contact-package-v1",
    hotel_id: partial.hotel_id || null,
    hotel_name: partial.hotel_name || null,
    owner_entity_id: partial.owner_entity_id || null,
    owner_display_name: partial.owner_display_name || null,
    owner_resolved: Boolean(partial.owner_entity_id),
    hotel_contact: partial.hotel_contact || { channels: [] },
    organization_contact_route: partial.organization_contact_route || { channels: [] },
    people: Array.isArray(partial.people) ? partial.people : [],
    channels: Array.isArray(partial.channels) ? partial.channels : [],
    refresh_status: partial.refresh_status || "NEVER",
    last_refreshed_at: partial.last_refreshed_at || null,
    unresolved_reasons: Array.isArray(partial.unresolved_reasons) ? partial.unresolved_reasons : [],
    portfolio_reuse: partial.portfolio_reuse || null,
    verification_history: Array.isArray(partial.verification_history)
      ? partial.verification_history
      : [],
    provenance: partial.provenance || {},
    write_guarantees: partial.write_guarantees || null,
  };
}

export { DELIVERABILITY, FRESHNESS, ROLE_CURRENCY, USAGE_RIGHTS };
