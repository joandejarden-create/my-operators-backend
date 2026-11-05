/**
 * Demand ownership for Event-to-Demand — who may control hotel purchasing.
 * Evidence-backed only; never invents participation or lodging responsibility.
 */

import { createHash } from "node:crypto";
import { normalizeOrgIdentityKey } from "../../workflow/reuse-prior-intelligence-v1.js";
import { EVENT_DEMAND_MECHANISM } from "./event-to-demand-constants-v1.js";

export const DEMAND_OWNER_ROLE = Object.freeze({
  EVENT_ORGANIZER: "EVENT_ORGANIZER",
  PARTICIPATING_COMPANY: "PARTICIPATING_COMPANY",
  SPONSOR: "SPONSOR",
  EXHIBITOR: "EXHIBITOR",
  ASSOCIATION_OR_EXEC_ED: "ASSOCIATION_OR_EXEC_ED",
  VIP_HOSPITALITY_PROGRAM: "VIP_HOSPITALITY_PROGRAM",
  EVENT_AGENCY_OR_TRAVEL_INTERMEDIARY: "EVENT_AGENCY_OR_TRAVEL_INTERMEDIARY",
});

export const BUYER_FUNCTION = Object.freeze({
  MEETINGS_EVENTS: "MEETINGS_EVENTS",
  CORPORATE_TRAVEL_PROCUREMENT: "CORPORATE_TRAVEL_PROCUREMENT",
  VIP_HOSPITALITY: "VIP_HOSPITALITY",
  EXHIBITOR_SERVICES: "EXHIBITOR_SERVICES",
  EXECUTIVE_EDUCATION_OPS: "EXECUTIVE_EDUCATION_OPS",
  AGENCY_HOUSING: "AGENCY_HOUSING",
  GENERIC_ORG_CONTACT: "GENERIC_ORG_CONTACT",
  UNKNOWN: "UNKNOWN",
});

export const BUYER_CONFIDENCE = Object.freeze({
  CONFIRMED_BUYER: "CONFIRMED_BUYER", // named person + role + route (source-backed)
  LIKELY_BUYER_FUNCTION: "LIKELY_BUYER_FUNCTION", // function evidenced, no named person
  GENERIC_ORG_CONTACT: "GENERIC_ORG_CONTACT", // org email/form only
  NONE: "NONE",
});

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function demandOwnerId({ organizationName, eventId, role }) {
  const seed = [
    normalizeOrgIdentityKey(organizationName || ""),
    eventId || "",
    String(role || ""),
  ].join("|");
  return `gdi_own_${createHash("sha256").update(seed).digest("hex").slice(0, 12)}`;
}

/**
 * Map mechanism → default demand-owner role when org is the prospect subject.
 */
export function roleFromMechanism(mechanism, { isOrganizer = false } = {}) {
  const m = String(mechanism || "").toUpperCase();
  if (isOrganizer) {
    if (m === EVENT_DEMAND_MECHANISM.VIP_HOSPITALITY) {
      return DEMAND_OWNER_ROLE.VIP_HOSPITALITY_PROGRAM;
    }
    if (m === EVENT_DEMAND_MECHANISM.SMALL_GROUP_LODGING) {
      return DEMAND_OWNER_ROLE.ASSOCIATION_OR_EXEC_ED;
    }
    if (m === EVENT_DEMAND_MECHANISM.ANCILLARY_EVENT_BUSINESS) {
      return DEMAND_OWNER_ROLE.EVENT_AGENCY_OR_TRAVEL_INTERMEDIARY;
    }
    return DEMAND_OWNER_ROLE.EVENT_ORGANIZER;
  }
  if (m === EVENT_DEMAND_MECHANISM.EXHIBITOR_SPONSOR_LODGING) {
    return DEMAND_OWNER_ROLE.EXHIBITOR;
  }
  if (m === EVENT_DEMAND_MECHANISM.VIP_HOSPITALITY) {
    return DEMAND_OWNER_ROLE.VIP_HOSPITALITY_PROGRAM;
  }
  if (m === EVENT_DEMAND_MECHANISM.EXECUTIVE_DELEGATION) {
    return DEMAND_OWNER_ROLE.PARTICIPATING_COMPANY;
  }
  if (m === EVENT_DEMAND_MECHANISM.SMALL_GROUP_LODGING) {
    return DEMAND_OWNER_ROLE.ASSOCIATION_OR_EXEC_ED;
  }
  if (m === EVENT_DEMAND_MECHANISM.ANCILLARY_EVENT_BUSINESS) {
    return DEMAND_OWNER_ROLE.EVENT_AGENCY_OR_TRAVEL_INTERMEDIARY;
  }
  return DEMAND_OWNER_ROLE.PARTICIPATING_COMPANY;
}

/**
 * Likely purchasing function for a demand-owner role (diagnostic, not a named buyer).
 */
export function buyerFunctionForRole(role, mechanism) {
  switch (role) {
    case DEMAND_OWNER_ROLE.VIP_HOSPITALITY_PROGRAM:
      return BUYER_FUNCTION.VIP_HOSPITALITY;
    case DEMAND_OWNER_ROLE.EXHIBITOR:
    case DEMAND_OWNER_ROLE.SPONSOR:
      return BUYER_FUNCTION.EXHIBITOR_SERVICES;
    case DEMAND_OWNER_ROLE.ASSOCIATION_OR_EXEC_ED:
      return BUYER_FUNCTION.EXECUTIVE_EDUCATION_OPS;
    case DEMAND_OWNER_ROLE.EVENT_AGENCY_OR_TRAVEL_INTERMEDIARY:
      return BUYER_FUNCTION.AGENCY_HOUSING;
    case DEMAND_OWNER_ROLE.EVENT_ORGANIZER:
      return /EXECUTIVE|OFFSITE|SMALL_GROUP/i.test(String(mechanism || ""))
        ? BUYER_FUNCTION.MEETINGS_EVENTS
        : BUYER_FUNCTION.MEETINGS_EVENTS;
    case DEMAND_OWNER_ROLE.PARTICIPATING_COMPANY:
      return BUYER_FUNCTION.CORPORATE_TRAVEL_PROCUREMENT;
    default:
      return BUYER_FUNCTION.UNKNOWN;
  }
}

/**
 * Build demand-owner records from a matured E2D prospect + optional seed event.
 */
export function extractDemandOwnersFromProspect({
  opportunity = {},
  seedEvent = null,
} = {}) {
  const owners = [];
  const rejected = [];
  const eventId = opportunity.eventId || seedEvent?.eventId || null;
  const eventName = opportunity.eventName || seedEvent?.eventName || null;
  const primaryOrg = text(opportunity.organizationName);
  if (!primaryOrg) {
    rejected.push({ reason: "missing_organization" });
    return { owners, rejected };
  }

  const mech = opportunity.eventDemandMechanism || opportunity.eventHotelFitClass;
  const organizerName = text(seedEvent?.organizer || "");
  const isOrganizer =
    !organizerName ||
    normalizeOrgIdentityKey(primaryOrg) === normalizeOrgIdentityKey(organizerName) ||
    primaryOrg.toLowerCase().includes(organizerName.toLowerCase().slice(0, 12));

  const primaryRole = roleFromMechanism(mech, { isOrganizer });
  owners.push({
    demandOwnerId: demandOwnerId({
      organizationName: primaryOrg,
      eventId,
      role: primaryRole,
    }),
    organizationName: primaryOrg,
    organizationIdentityKey: normalizeOrgIdentityKey(primaryOrg),
    role: primaryRole,
    mechanism: mech,
    eventId,
    eventName,
    evidenceBasis: text(opportunity.localConnection) || "prospect_mechanism_rationale",
    evidenceUrls: [
      opportunity.futureDemand?.sourceUrl,
      ...(opportunity.sources || []).map((s) => s.url),
    ].filter(Boolean),
    sourceProspectId: opportunity.id,
    isPrimaryProspectOrg: true,
  });

  // Seed stakeholders that are distinct orgs for the same event
  for (const s of seedEvent?.stakeholders || []) {
    const name = text(s.organizationName);
    if (!name) continue;
    if (!s.demandRationale) {
      rejected.push({
        reason: "stakeholder_missing_rationale",
        organizationName: name,
      });
      continue;
    }
    if (normalizeOrgIdentityKey(name) === normalizeOrgIdentityKey(primaryOrg)) {
      continue;
    }
    const role = roleFromMechanism(s.mechanism, {
      isOrganizer:
        organizerName &&
        normalizeOrgIdentityKey(name) === normalizeOrgIdentityKey(organizerName),
    });
    // Refine exhibitor vs sponsor from rationale (not from mechanism enum names)
    let refined = role;
    if (/\bsponsors?\b/i.test(s.demandRationale) && !/exhibit/i.test(s.demandRationale)) {
      refined = DEMAND_OWNER_ROLE.SPONSOR;
    } else if (/exhibit|gallery|booth|participating/i.test(s.demandRationale)) {
      refined = DEMAND_OWNER_ROLE.EXHIBITOR;
    } else if (/vip|collector|hospitality/i.test(s.demandRationale)) {
      refined = DEMAND_OWNER_ROLE.VIP_HOSPITALITY_PROGRAM;
    } else if (/agency|dmc|housing bureau|planner/i.test(s.demandRationale)) {
      refined = DEMAND_OWNER_ROLE.EVENT_AGENCY_OR_TRAVEL_INTERMEDIARY;
    }

    owners.push({
      demandOwnerId: demandOwnerId({
        organizationName: name,
        eventId,
        role: refined,
      }),
      organizationName: name,
      organizationIdentityKey: normalizeOrgIdentityKey(name),
      role: refined,
      mechanism: s.mechanism || mech,
      eventId,
      eventName,
      evidenceBasis: s.demandRationale,
      evidenceUrls: (seedEvent.sources || []).map((x) => x.url).filter(Boolean),
      sourceProspectId: opportunity.id,
      isPrimaryProspectOrg: false,
      fromSeedStakeholder: true,
    });
  }

  // Howard de Walden / estate intermediary when Marylebone Village is the face brand
  if (
    /marylebone village/i.test(primaryOrg) &&
    seedEvent?.organizer &&
    /howard de walden/i.test(seedEvent.organizer)
  ) {
    const estate = "Howard de Walden Estate";
    if (
      !owners.some(
        (o) =>
          normalizeOrgIdentityKey(o.organizationName) ===
          normalizeOrgIdentityKey(estate)
      )
    ) {
      owners.push({
        demandOwnerId: demandOwnerId({
          organizationName: estate,
          eventId,
          role: DEMAND_OWNER_ROLE.EVENT_AGENCY_OR_TRAVEL_INTERMEDIARY,
        }),
        organizationName: estate,
        organizationIdentityKey: normalizeOrgIdentityKey(estate),
        role: DEMAND_OWNER_ROLE.EVENT_AGENCY_OR_TRAVEL_INTERMEDIARY,
        mechanism: mech,
        eventId,
        eventName,
        evidenceBasis:
          "Seed organizer names Howard de Walden Estate / Marylebone Village as place-brand operator for festival hospitality.",
        evidenceUrls: (seedEvent.sources || []).map((x) => x.url).filter(Boolean),
        sourceProspectId: opportunity.id,
        isPrimaryProspectOrg: false,
        fromSeedOrganizerSplit: true,
      });
    }
  }

  return { owners, rejected };
}

/**
 * Parse sponsor/exhibitor org names from page text — only when explicitly labeled.
 * Does not invent participation.
 */
function isPlausibleOrgName(name = "") {
  const n = text(name);
  if (n.length < 3 || n.length > 80) return false;
  if (/[{}\[\]\\|"']/.test(n)) return false;
  if (/^values:/i.test(n)) return false;
  if (/subject area|research centre|research center|event organis/i.test(n)) {
    return false;
  }
  if (/click here|learn more|read more|privacy|cookie|javascript/i.test(n)) {
    return false;
  }
  // Require at least one letter space letter pattern (multi-word) or known single brand length
  if (!/[A-Za-z]{2,}/.test(n)) return false;
  if (n.split(/\s+/).length === 1 && n.length < 5) return false;
  if (/^(values|events?|home|menu|search|null|undefined)$/i.test(n)) return false;
  return true;
}

export function extractNamedDemandOwnersFromPageText(blob = {}, { eventId, eventName, sourceUrl, sourceProspectId } = {}) {
  // Prefer plain prose; strip obvious JSON/script noise before matching
  const raw = typeof blob === "string" ? blob : blob.text || "";
  const textBlob = String(raw)
    .replace(/\\+"/g, " ")
    .replace(/[{}\[\]|]/g, " ")
    .replace(/\s+/g, " ");
  const owners = [];
  const patterns = [
    {
      re: /(?:title\s+)?sponsors?\s*[:|-]\s*([A-Z][A-Za-z0-9 &.'-]{2,60})/gi,
      role: DEMAND_OWNER_ROLE.SPONSOR,
    },
    {
      re: /(?:presented|organised|organized)\s+by\s+([A-Z][A-Za-z0-9 &.'-]{2,60})/gi,
      role: DEMAND_OWNER_ROLE.EVENT_ORGANIZER,
    },
    {
      re: /(?:official\s+)?housing\s+(?:partner|bureau|agency)\s*[:|-]?\s*([A-Z][A-Za-z0-9 &.'-]{2,60})/gi,
      role: DEMAND_OWNER_ROLE.EVENT_AGENCY_OR_TRAVEL_INTERMEDIARY,
    },
  ];
  for (const { re, role } of patterns) {
    let m;
    const local = new RegExp(re.source, re.flags);
    while ((m = local.exec(textBlob))) {
      const name = text(m[1])
        .replace(/\s+and\s+(partners|more).*$/i, "")
        .replace(/\s{2,}/g, " ")
        .slice(0, 80);
      if (!isPlausibleOrgName(name)) continue;
      owners.push({
        demandOwnerId: demandOwnerId({ organizationName: name, eventId, role }),
        organizationName: name,
        organizationIdentityKey: normalizeOrgIdentityKey(name),
        role,
        eventId: eventId || null,
        eventName: eventName || null,
        evidenceBasis: `Labeled ${role} language on source page`,
        evidenceUrls: sourceUrl ? [sourceUrl] : [],
        sourceProspectId: sourceProspectId || null,
        isPrimaryProspectOrg: false,
        fromPageExtraction: true,
      });
    }
  }
  // Dedupe by org+role
  const seen = new Set();
  return owners.filter((o) => {
    const k = `${o.organizationIdentityKey}|${o.role}`;
    if (seen.has(k)) return false;
    seen.add(k);
    return true;
  });
}

/**
 * Classify buyer confidence from contact + optional named person fields.
 */
export function classifyBuyerConfidence(contact = {}, buyerRoleEvidence = null) {
  const c = contact || {};
  const named =
    Boolean(c.personName && (c.role || c.title)) &&
    Boolean(c.businessEmail || c.namedBuyerEmail || c.contactFormUrl || c.businessPhone);
  if (named) return BUYER_CONFIDENCE.CONFIRMED_BUYER;
  if (buyerRoleEvidence?.supported === true) {
    return BUYER_CONFIDENCE.LIKELY_BUYER_FUNCTION;
  }
  if (c.businessEmail || c.contactFormUrl || c.businessPhone) {
    return BUYER_CONFIDENCE.GENERIC_ORG_CONTACT;
  }
  return BUYER_CONFIDENCE.NONE;
}
