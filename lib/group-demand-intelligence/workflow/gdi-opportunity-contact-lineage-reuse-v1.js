/**
 * Opportunity contact lineage reuse V1.
 *
 * Joins LIVE_NEW deep-research IDs to prior inventory contacts via
 * organization / program identity keys — does not merge unrelated events.
 * Read-only helper for materialize / $0 recovery; does not delete history.
 */

import { isPlausibleBuyerPersonName } from "./gdi-original-full-e2e-execution-restore-v1.js";

export const GDI_OPPORTUNITY_CONTACT_LINEAGE_REUSE_V1 =
  "gdi-opportunity-contact-lineage-reuse-v1";

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function normOrg(v) {
  return text(v)
    .toLowerCase()
    .normalize("NFKD")
    .replace(/[^\w\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

/**
 * Conservative identity key: org + optional programme/title token.
 * Refuses empty org.
 */
export function buildDemandClusterKey(opp = {}) {
  const org = normOrg(
    opp.organizationName || opp.organization || opp.demandGenerator,
  );
  if (!org || org.length < 3) return null;
  const program = normOrg(
    opp.programmeName || opp.programName || opp.editionKey || "",
  );
  const title = normOrg(opp.title || "");
  // Prefer program when present; else short title token (first 6 words) for undated accounts.
  const edition =
    program ||
    title
      .split(" ")
      .slice(0, 6)
      .join(" ");
  return `${org}|${edition || "undated"}`;
}

export function extractBestNamedContact(opp = {}) {
  const candidates = [
    opp.primaryContact,
    ...(Array.isArray(opp.namedStakeholders) ? opp.namedStakeholders : []),
    ...(Array.isArray(opp.namedStakeholdersFound)
      ? opp.namedStakeholdersFound
      : []),
    opp.contactName
      ? { name: opp.contactName, role: opp.contactRole || opp.contactTitle }
      : null,
  ].filter(Boolean);

  for (const c of candidates) {
    const name = text(c?.name || c);
    if (!isPlausibleBuyerPersonName(name)) continue;
    return {
      name,
      role: text(c.role || c.title || c.professionalTitle || "") || null,
      title: text(c.title || c.role || "") || null,
      organization: text(
        c.organization || c.organizationName || opp.organizationName || "",
      ) || null,
      sourceUrl: c.sourceUrl || c.url || null,
      personIdentityKey: c.personIdentityKey || null,
      provenance: "PRIOR_INVENTORY_LINEAGE_REUSE",
    };
  }
  return null;
}

/**
 * Build a map demandClusterKey → best named contact from prior inventory.
 */
export function indexPriorNamedContactsByCluster(priorOpportunities = []) {
  const map = new Map();
  for (const opp of priorOpportunities) {
    const key = buildDemandClusterKey(opp);
    if (!key) continue;
    const contact = extractBestNamedContact(opp);
    if (!contact) continue;
    if (!map.has(key)) map.set(key, { contact, sourceOpportunityId: opp.opportunityId || opp.id });
  }
  return map;
}

/**
 * Apply reuse onto a LIVE_NEW (or any) opportunity when cluster matches and
 * current primary is missing/junk. Never overwrites a plausible existing person.
 */
export function applyContactLineageReuse(opportunity = {}, priorIndex = new Map()) {
  const currentName = text(
    opportunity.primaryContact?.name || opportunity.contactName,
  );
  if (isPlausibleBuyerPersonName(currentName)) {
    return {
      opportunity,
      reused: false,
      reason: "CURRENT_NAMED_CONTACT_PRESENT",
    };
  }
  const key = buildDemandClusterKey(opportunity);
  if (!key) {
    return { opportunity, reused: false, reason: "NO_CLUSTER_KEY" };
  }
  const hit = priorIndex.get(key);
  if (!hit?.contact) {
    return { opportunity, reused: false, reason: "NO_PRIOR_MATCH", key };
  }
  // Guard: require exact org norm match (already in key) — refuse if titles diverge wildly
  // when both have dated event titles with different years.
  const priorTitle = normOrg(hit.sourceTitle || "");
  const curTitle = normOrg(opportunity.title || "");
  if (
    priorTitle &&
    curTitle &&
    /\b20\d{2}\b/.test(priorTitle) &&
    /\b20\d{2}\b/.test(curTitle) &&
    priorTitle !== curTitle &&
    !curTitle.includes(priorTitle.slice(0, 12)) &&
    !priorTitle.includes(curTitle.slice(0, 12))
  ) {
    return {
      opportunity,
      reused: false,
      reason: "DATED_TITLE_MISMATCH_REFUSED",
      key,
    };
  }

  const contact = hit.contact;
  const next = {
    ...opportunity,
    primaryContact: {
      ...(opportunity.primaryContact && typeof opportunity.primaryContact === "object"
        ? opportunity.primaryContact
        : {}),
      name: contact.name,
      role: contact.role,
      title: contact.title,
      organization: contact.organization,
      sourceUrl: contact.sourceUrl,
      personIdentityKey: contact.personIdentityKey,
      lineageReuse: {
        version: GDI_OPPORTUNITY_CONTACT_LINEAGE_REUSE_V1,
        sourceOpportunityId: hit.sourceOpportunityId,
        demandClusterKey: key,
      },
    },
    contactName: contact.name,
    contactRole: contact.role,
    contactLineageReuseV1: {
      version: GDI_OPPORTUNITY_CONTACT_LINEAGE_REUSE_V1,
      reused: true,
      sourceOpportunityId: hit.sourceOpportunityId,
      demandClusterKey: key,
    },
  };
  return { opportunity: next, reused: true, reason: "REUSED_PRIOR_NAMED", key };
}

export default {
  GDI_OPPORTUNITY_CONTACT_LINEAGE_REUSE_V1,
  buildDemandClusterKey,
  extractBestNamedContact,
  indexPriorNamedContactsByCluster,
  applyContactLineageReuse,
};
