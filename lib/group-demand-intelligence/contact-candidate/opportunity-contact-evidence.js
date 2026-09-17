/**
 * Normalize opportunity research contact evidence → canonical WHO candidate inputs.
 * REUSABLE_PRODUCT_LOGIC — does not replace candidate discovery/scoring.
 *
 * Bridge gap (Waterstone A1): research labels on primaryContact never carried
 * org / eventRelationship / functionalEntity flags → candidate scores ~24 < 45 floor.
 */

import { hasNamedPerson } from "../contact-resolution.js";
import { GDI_CONTACT_ROLE } from "./ontology.js";
import { OPPORTUNITY_TYPE } from "../claim-types.js";

export const RESEARCH_IDENTITY_KIND = Object.freeze({
  PERSON: "PERSON",
  FUNCTIONAL_ENTITY: "FUNCTIONAL_ENTITY",
  GENERIC_DESK: "GENERIC_DESK",
  UNKNOWN: "UNKNOWN",
});

const GENERIC_DESK_RE =
  /\b(show management|hotel program|housing team|housing desk|organizers?|procurement team|corporate travel manager|commencement office|athletics administration|events? team|management)\b/i;

const HOUSING_ENTITY_RE =
  /\b(team travel source|hbc|housing bureau|onpeak|passtkey|roomvy|event services|official housing)\b/i;

const ORG_ENTITY_RE =
  /\b(association|university|federation|movements|inc\.?|llc|corp(?:oration)?|company|group|show|championships?|juniors|quantum|campus)\b/i;

function splitRoleOrg(roleRaw) {
  const role = String(roleRaw || "").trim();
  if (!role) return { role: null, organization: null };
  // "VP Business Development, FIA" / "VP — FIA"
  const m = role.match(/^(.+?)(?:\s*[,—–-]\s*|\s+at\s+)(.+)$/i);
  if (m) {
    return { role: m[1].trim(), organization: m[2].trim() };
  }
  return { role, organization: null };
}

/**
 * Classify a raw research label into identity kind.
 */
export function classifyResearchIdentityKind(raw = {}, opportunity = {}) {
  const name = String(raw.name || raw.fullName || raw.label || "").trim();
  if (!name) return RESEARCH_IDENTITY_KIND.UNKNOWN;

  if (GENERIC_DESK_RE.test(name) && !hasNamedPerson({ name })) {
    return RESEARCH_IDENTITY_KIND.GENERIC_DESK;
  }

  // Housing / travel-source labels are always functional entities — never promote to PERSON
  // even when Title Case looks person-shaped ("Team Travel Source").
  if (
    HOUSING_ENTITY_RE.test(name) ||
    HOUSING_ENTITY_RE.test(String(raw.organization || "")) ||
    HOUSING_ENTITY_RE.test(String(raw.role || ""))
  ) {
    return RESEARCH_IDENTITY_KIND.FUNCTIONAL_ENTITY;
  }

  if (raw.functionalEntity === true) {
    if (hasNamedPerson({ name }) && !ORG_ENTITY_RE.test(name)) {
      return RESEARCH_IDENTITY_KIND.PERSON;
    }
    return RESEARCH_IDENTITY_KIND.FUNCTIONAL_ENTITY;
  }

  if (hasNamedPerson({ name }) && !ORG_ENTITY_RE.test(name)) {
    return RESEARCH_IDENTITY_KIND.PERSON;
  }

  // Multi-token org that looks like a person shape ("Team Travel Source")
  if (ORG_ENTITY_RE.test(name) || /^(the\s+)/i.test(name)) {
    return RESEARCH_IDENTITY_KIND.FUNCTIONAL_ENTITY;
  }

  if (/^[A-Z][a-z]+(?:\s+[A-Z][a-z'.-]+){1,3}$/.test(name) && name.split(/\s+/).length >= 2) {
    // Heuristic person shape without org keywords
    return RESEARCH_IDENTITY_KIND.PERSON;
  }

  if (name.length > 2) return RESEARCH_IDENTITY_KIND.FUNCTIONAL_ENTITY;
  return RESEARCH_IDENTITY_KIND.UNKNOWN;
}

function inferGdiRole(kind, raw, opportunity, parsedRole) {
  if (raw.gdiContactRole && GDI_CONTACT_ROLE[raw.gdiContactRole]) return raw.gdiContactRole;
  const blob = `${parsedRole || ""} ${raw.role || ""} ${raw.title || ""} ${raw.name || ""}`.toLowerCase();
  const isOverflow =
    opportunity.opportunityType === OPPORTUNITY_TYPE.OVERFLOW_HOUSING ||
    /overflow|housing|stay-to-play/i.test(String(opportunity.title || ""));

  if (kind === RESEARCH_IDENTITY_KIND.FUNCTIONAL_ENTITY && HOUSING_ENTITY_RE.test(`${raw.name} ${blob}`)) {
    return GDI_CONTACT_ROLE.HOUSING_OWNER;
  }
  if (/housing|sourcing|travel source|room block/i.test(blob) || (isOverflow && kind === RESEARCH_IDENTITY_KIND.FUNCTIONAL_ENTITY)) {
    if (HOUSING_ENTITY_RE.test(`${raw.name} ${blob}`) || /housing|travel source/i.test(blob)) {
      return GDI_CONTACT_ROLE.HOUSING_OWNER;
    }
  }
  if (/vp.{0,24}(business development|sales)|business development|director of sales/i.test(blob)) {
    return isOverflow ? GDI_CONTACT_ROLE.MEETINGS_OWNER : GDI_CONTACT_ROLE.SALES_OWNER;
  }
  if (/tournament director|event director|vp.{0,24}events|director of meetings|meeting planner/i.test(blob)) {
    return /tournament/i.test(blob) ? GDI_CONTACT_ROLE.EVENT_OWNER : GDI_CONTACT_ROLE.MEETINGS_OWNER;
  }
  if (/commencement|athletics|procurement|federation/i.test(blob)) {
    return GDI_CONTACT_ROLE.GENERAL_ORGANIZATION_CONTACT;
  }
  return null;
}

/**
 * Normalize one research contact into a candidate-shaped object for discovery.
 * Returns null if evidence is too weak to hand off.
 */
export function normalizeOpportunityContactEvidence(raw = {}, opportunity = {}, opts = {}) {
  if (!raw || typeof raw !== "object") return null;
  const name = String(raw.name || raw.fullName || raw.label || "").trim();
  if (!name) return null;

  const kind = classifyResearchIdentityKind(raw, opportunity);
  if (kind === RESEARCH_IDENTITY_KIND.UNKNOWN) return null;
  if (kind === RESEARCH_IDENTITY_KIND.GENERIC_DESK && !opts.allowGenericDesk) {
    // Keep as audit-only weak signal — do not promote to WHO primary pool
    return {
      name,
      researchIdentityKind: kind,
      functionalEntity: false,
      genericOrgOnly: true,
      rejectReason: "generic_desk_not_probable_who",
      forceReject: true,
      rejected: true,
      claimKind: "INFERENCE",
      sourceUrl: raw.sourceUrl || raw.url || null,
      discoveryProvenance: "research_contact_evidence_bridge",
      __discoveryProvenance: "research_contact_evidence_bridge",
    };
  }

  const { role: parsedRole, organization: orgFromRole } = splitRoleOrg(raw.role || raw.title);
  const organization =
    raw.organization ||
    orgFromRole ||
    opportunity.organizationName ||
    null;

  const gdiContactRole = inferGdiRole(kind, raw, opportunity, parsedRole);
  const isPerson = kind === RESEARCH_IDENTITY_KIND.PERSON;
  const isEntity = kind === RESEARCH_IDENTITY_KIND.FUNCTIONAL_ENTITY;

  const sourceUrl = raw.sourceUrl || raw.url || raw.source || null;
  const why =
    raw.whyThisPerson ||
    raw.whyThisContact ||
    (isPerson
      ? `Named on opportunity research evidence for ${opportunity.title || "this event"}.`
      : `Functional entity identified in research as a contact path for ${opportunity.title || "this event"}.`);

  return {
    name,
    fullName: isPerson ? name : undefined,
    role: parsedRole || raw.role || raw.title || (isEntity ? "Organization / housing contact" : null),
    title: parsedRole || raw.title || null,
    organization,
    email: raw.email || null,
    phone: raw.phone || null,
    sourceUrl,
    source: sourceUrl,
    claimKind: sourceUrl ? "FACT" : "ESTIMATED",
    functionalEntity: isEntity,
    researchIdentityKind: kind,
    gdiContactRole: gdiContactRole || undefined,
    targetRoleMatch: raw.targetRoleMatch || undefined,
    eventRelationship: raw.eventRelationship || "CURRENT_EVENT_CONTACT",
    eventSpecificEvidence: true,
    relationshipToEvent:
      raw.relationshipToEvent ||
      (isEntity ? "Research-identified functional contact path" : "Research-identified event contact"),
    whyThisPerson: why,
    whyThisContact: why,
    accessDate: raw.accessDate || raw.sourceDate || null,
    sourceType: raw.sourceType || "opportunity_research",
    sourceAuthority: raw.sourceAuthority || "Tier_B",
    discoveryProvenance: "research_contact_evidence_bridge",
    __discoveryProvenance: "research_contact_evidence_bridge",
    evidenceSnippet: raw.evidenceSnippet || raw.extractedText || null,
    opportunityId: opportunity.id || null,
  };
}

/**
 * Collect all research contact evidence from an opportunity pack row.
 */
export function collectOpportunityResearchContacts(opportunity = {}) {
  const raws = [];
  if (opportunity.primaryContact) raws.push(opportunity.primaryContact);
  if (opportunity.primaryContactCandidate) raws.push(opportunity.primaryContactCandidate);
  for (const c of opportunity.contacts || []) raws.push(c);
  for (const b of opportunity.backupContacts || []) raws.push(b);
  for (const e of opportunity.researchContactEvidence || []) raws.push(e);
  return raws.filter(Boolean);
}

/**
 * Bridge: normalize research contacts → extraCandidates for discoverContactCandidates.
 */
export function bridgeResearchContactsToWhoCandidates(opportunity = {}, opts = {}) {
  const normalized = [];
  const rejected = [];
  for (const raw of collectOpportunityResearchContacts(opportunity)) {
    const n = normalizeOpportunityContactEvidence(raw, opportunity, opts);
    if (!n) continue;
    if (n.forceReject || n.rejected) {
      rejected.push(n);
      continue;
    }
    normalized.push(n);
  }
  return {
    extraCandidates: normalized,
    rejectedGeneric: rejected,
    bridgePassId: "gdi_research_contact_evidence_bridge_v1",
  };
}
