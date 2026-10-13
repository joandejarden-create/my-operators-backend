/**
 * Bridge Contact Intelligence V1.4 discovery outputs → Contact Resolution V2 shape.
 */

import crypto from "node:crypto";
import { CHANNEL_KIND } from "../vocabulary.js";
import { EMAIL_STATUS, PHONE_TYPE, CONTACT_STRENGTH } from "./vocabulary.js";
import { resolveOwnerPhones } from "./phone-resolution.js";
import { gateContactCandidate, retainRejected } from "./gates.js";

export const CI_ORCHESTRATION_BRIDGE_VERSION = "ci-orchestration-bridge-v1";

function channelEmail(channels = []) {
  return (channels || []).find((c) => c.kind === CHANNEL_KIND.PERSON_EMAIL);
}

function channelPhone(channels = []) {
  return (channels || []).find((c) => c.kind === CHANNEL_KIND.PERSON_PHONE);
}

function orgEmailFromRoute(orgRoute) {
  for (const ch of orgRoute?.channels || []) {
    if ([CHANNEL_KIND.ORG_EMAIL, CHANNEL_KIND.ROLE_MAILBOX].includes(ch.kind)) return ch.value;
  }
  return null;
}

function orgPhoneFromRoute(orgRoute) {
  for (const ch of orgRoute?.channels || []) {
    if ([CHANNEL_KIND.SWITCHBOARD, CHANNEL_KIND.ORG_PHONE].includes(ch.kind)) return ch.value;
  }
  return null;
}

function labelContactStrength(c) {
  if (c.email_publishable && c.phone && [PHONE_TYPE.DIRECT_MOBILE, PHONE_TYPE.DIRECT_OFFICE].includes(c.phone_type)) {
    return CONTACT_STRENGTH.VERIFIED_DIRECT_CONTACT;
  }
  if (c.email_publishable && c.email_status === EMAIL_STATUS.EXPLICIT_VERIFIED) return CONTACT_STRENGTH.VERIFIED_WORK_EMAIL;
  if (c.email_publishable) return CONTACT_STRENGTH.CORPORATE_CONTACT;
  if (c.phone_type === PHONE_TYPE.EXECUTIVE_OFFICE) return CONTACT_STRENGTH.EXECUTIVE_OFFICE;
  if (c.phone || c.email) return CONTACT_STRENGTH.CORPORATE_CONTACT;
  return CONTACT_STRENGTH.GENERAL_CONTACT;
}

/**
 * @param {object} person — CI createPersonContact shape
 * @param {object} ctx
 */
export function mapCiPersonToV2Contact(person, ctx = {}) {
  const emailCh = channelEmail(person.channels);
  const phoneCh = channelPhone(person.channels);
  const draft = {
    person_id: person.person_id || `ocr2_p_${crypto.randomBytes(4).toString("hex")}`,
    full_name: person.display_name,
    title: person.title,
    relationship_role: person.title,
    role_tier: ctx.role_tier || null,
    linkedin_url:
      (person.channels || []).find((c) => c.kind === CHANNEL_KIND.PERSON_LINKEDIN)?.value || null,
    email: emailCh?.value || null,
    email_status: emailCh
      ? EMAIL_STATUS.EXPLICIT_VERIFIED
      : person.indirect_corporate_route
        ? null
        : EMAIL_STATUS.INFERRED_UNVERIFIED,
    email_method: emailCh ? "owner_person_discovery_v1.4" : null,
    email_confidence: emailCh ? "MEDIUM" : "LOW",
    email_publishable: Boolean(emailCh?.value),
    phone: phoneCh?.value || null,
    phone_type: phoneCh ? resolveOwnerPhones({ person: { phone: phoneCh.value }, country: ctx.country }).phones[0]?.phone_type : null,
    phone_method: phoneCh ? "owner_person_discovery_v1.4" : null,
    phone_confidence: phoneCh ? "MEDIUM" : null,
    evidence: (person.evidence || []).map((e) => ({
      source_url: e.source_url,
      excerpt: e.excerpt,
      observed_at: e.observed_at,
    })),
    research_method_ids: ["discoverOwnerPersonPath", "leadership-extraction"],
    provider: "deterministic_public_web",
    last_verified_at: person.contact_source_date || person.role_observed_at || null,
    publication_label: person.publication_label,
    functionally_relevant: person.provenance?.functionally_relevant,
    indirect_corporate_route: person.indirect_corporate_route || null,
  };
  draft.contact_strength = labelContactStrength(draft);
  return draft;
}

/**
 * @param {object} discovery — discoverOwnerPersonPath result
 */
export function mapDiscoveryResultToV2(discovery, { country, owner_domain, brand_is_owner } = {}) {
  const contacts = [];
  const rejected_candidates = [];
  for (const p of discovery.people || []) {
    const draft = mapCiPersonToV2Contact(p, { country });
    const gate = gateContactCandidate(draft, { owner_domain, brand_is_owner });
    if (!gate.ok) {
      rejected_candidates.push(retainRejected(draft, gate.rejected));
      continue;
    }
    contacts.push(draft);
  }

  const orgRoute = discovery.organization_contact_route;
  const organization_contacts = {
    general_email: orgEmailFromRoute(orgRoute),
    corporate_phone: orgPhoneFromRoute(orgRoute),
    website: discovery.confirmed_company_domain?.url || (owner_domain ? `https://${owner_domain}` : null),
    contact_form_fallbacks: (discovery.contact_form_fallbacks || []).map((c) => c.value),
  };

  return { contacts, organization_contacts, rejected_candidates };
}

/**
 * Extract first-party domain hints from golden portfolio profile sources.
 */
export function extractDomainFromPortfolio(profile) {
  if (!profile) return null;
  for (const src of profile.sources || []) {
    const t = String(src.title || "");
    if (/^[a-z0-9.-]+\.[a-z]{2,}$/i.test(t)) return t.replace(/^www\./, "").toLowerCase();
    const m = t.match(/([a-z0-9-]+\.[a-z]{2,})/i);
    if (m && src.authority === "first_party") return m[1].toLowerCase();
  }
  return null;
}
