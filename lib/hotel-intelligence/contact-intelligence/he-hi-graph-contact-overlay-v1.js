/**
 * Overlay HI Airtable graph (orgs + org contacts) onto Contact Intelligence hotelGet.
 * Read-through only — no GET writes, no Surfe, no invented emails/phones.
 *
 * Organization integrity: each person resolves employer from their Organization ID.
 * Never default a person's employer to the economic sponsor. Never attach the
 * sponsor website as a person channel for employees of a different organization.
 */
import {
  createPersonContact,
  createHotelContactPackage,
  createChannel,
} from "./contact-record.js";
import {
  CHANNEL_KIND,
  REFRESH_STATUS,
  UNRESOLVED_REASON,
  PROPERTY_RELEVANCE,
} from "./vocabulary.js";
import { personBelongsToOrganization } from "./he-contact-org-route-guard-v1.js";

function extractLinkedInFromNotes(notes) {
  const text = String(notes || "");
  const m = text.match(/https?:\/\/(?:www\.)?linkedin\.com\/in\/[A-Za-z0-9\-_%]+\/?/i);
  return m ? m[0].replace(/\/$/, "") + "/" : null;
}

function websiteChannel(org) {
  const url = String(org?.officialWebsite || "").trim();
  if (!url) return [];
  return [
    createChannel({
      kind: CHANNEL_KIND.ORG_WEBSITE,
      value: url,
      claimed_official: true,
      provenance: { source: "HI_ORGANIZATIONS" },
    }),
  ];
}

function orgLookupMaps(organizations = []) {
  const byCanonical = new Map();
  const byAirtable = new Map();
  for (const o of organizations || []) {
    if (!o) continue;
    if (o.orgId) byCanonical.set(String(o.orgId), o);
    if (o.airtableId) byAirtable.set(String(o.airtableId), o);
  }
  return { byCanonical, byAirtable };
}

function resolvePersonOrganization(p, maps, ownerOrgId) {
  const orgIdText = p?.organizationId ? String(p.organizationId).trim() : "";
  const linkIds = Array.isArray(p?.organizationLink)
    ? p.organizationLink.map(String)
    : p?.organizationLink
      ? [String(p.organizationLink)]
      : [];

  let org = null;
  if (orgIdText && maps.byCanonical.has(orgIdText)) {
    org = maps.byCanonical.get(orgIdText);
  }
  if (!org) {
    for (const link of linkIds) {
      if (maps.byAirtable.has(link)) {
        org = maps.byAirtable.get(link);
        break;
      }
    }
  }
  // Do NOT fall back to economic sponsor — unknown employer stays unresolved.
  return {
    org,
    organizationEntityId: org?.orgId || orgIdText || null,
    organizationName: org
      ? org.tradingName || org.legalName || org.orgId
      : null,
    isOwnerOrg:
      Boolean(org?.orgId) &&
      Boolean(ownerOrgId) &&
      String(org.orgId) === String(ownerOrgId),
  };
}

function inferPropertyRelevance(p, resolved) {
  const blob = [
    p?.hotelResponsibility,
    p?.functionRole,
    p?.title,
    resolved.organizationName,
  ]
    .filter(Boolean)
    .join(" ");
  if (
    /general manager|property leadership|director of sales|director of marketing|hotel leadership|on[- ]site/i.test(
      blob
    )
  ) {
    return PROPERTY_RELEVANCE.PROPERTY_DIRECT;
  }
  if (/operator|management company|regional (vp|vice|director)/i.test(blob)) {
    return PROPERTY_RELEVANCE.OPERATOR;
  }
  if (/brand|franchise|development/i.test(blob)) {
    return PROPERTY_RELEVANCE.BRAND_NETWORK;
  }
  if (resolved.isOwnerOrg || /asset management|ownership|owner|ceo|president|principal/i.test(blob)) {
    return PROPERTY_RELEVANCE.OWNER_ORG;
  }
  return PROPERTY_RELEVANCE.OWNER_ORG;
}

/**
 * @param {object} baseResult - hotelGet result
 * @param {object} graph - readHiGraphForHotel result
 * @param {object[]} [orgContacts]
 */
export function overlayHiGraphOnContactHotelGet(baseResult, graph, orgContacts = []) {
  if (!graph?.ok || !(graph.relationshipCount > 0)) return baseResult;

  const economic = graph.projection?.economic_owner || null;
  const primary = economic || graph.primaryOrganization || null;
  const ownerOrgId = primary?.orgId || null;
  const ownerName =
    primary?.tradingName || primary?.legalName || null;
  const maps = orgLookupMaps(graph.organizations || []);

  const preferred = [];
  const seenName = new Map();
  for (const p of orgContacts || []) {
    if (!p?.displayName) continue;
    const key = String(p.displayName).trim().toLowerCase();
    const rank =
      (String(p.verificationState || "").toLowerCase() === "validated" ? 2 : 0) +
      (p.organizationId ? 1 : 0) +
      (extractLinkedInFromNotes(p.notes) ? 1 : 0);
    const prev = seenName.get(key);
    if (!prev || rank > prev.rank) seenName.set(key, { p, rank });
  }
  for (const { p } of seenName.values()) preferred.push(p);

  const people = preferred.map((p) => {
    const linkedin = extractLinkedInFromNotes(p.notes);
    const resolved = resolvePersonOrganization(p, maps, ownerOrgId);
    const channels = [];
    if (linkedin) {
      channels.push(
        createChannel({
          kind: CHANNEL_KIND.PERSON_LINKEDIN,
          value: linkedin,
          claimed_official: false,
          provenance: {
            source: "HI_ORG_CONTACTS_NOTES",
            note: "LinkedIn URL recovered from contact Notes (HI Contact Channels allowlist quarantined)",
          },
        })
      );
    }
    // Person-scoped org website: only from THEIR resolved employer org.
    if (resolved.org?.officialWebsite) {
      const personStub = {
        organization: resolved.organizationName,
        organizationName: resolved.organizationName,
      };
      if (personBelongsToOrganization(personStub, resolved.organizationName)) {
        for (const ch of websiteChannel(resolved.org)) {
          channels.push(ch);
        }
      }
    }
    const propertyRelevance = inferPropertyRelevance(p, resolved);
    return createPersonContact({
      person_id: p.personId || undefined,
      display_name: p.displayName,
      title: p.title || null,
      organization_entity_id: resolved.organizationEntityId || null,
      organization_name: resolved.organizationName,
      property_relevance: propertyRelevance,
      affiliation_status:
        String(p.roleCurrency || "").toLowerCase() === "current"
          ? "CURRENT"
          : "CURRENT_UNKNOWN",
      channels,
      why_relevant: p.functionRole || p.title || null,
      hotel_commercial_responsibility: p.hotelResponsibility || p.functionRole || null,
      provenance: {
        source: "HI_ORG_CONTACTS",
        airtable_id: p.airtableId,
        verification_state: p.verificationState || null,
        confidence: p.confidence || null,
        organization_id: resolved.organizationEntityId || null,
      },
    });
  });

  // Hotel-level organization_contact_route stays on the economic sponsor / primary org.
  // UI must not inherit these onto people employed by other organizations.
  const orgChannels = websiteChannel(primary);
  const unresolved = new Set(baseResult.unresolved_reasons || []);
  if (ownerOrgId) unresolved.delete(UNRESOLVED_REASON.OWNER_UNRESOLVED);
  if (orgChannels.length) unresolved.delete(UNRESOLVED_REASON.NO_ORG_ROUTE);
  if (people.length) {
    unresolved.delete(UNRESOLVED_REASON.NO_PERSON_EVIDENCE);
    unresolved.delete(UNRESOLVED_REASON.NO_HOTEL_CONTACT);
  }

  const basePkg = baseResult.package || {};
  const pkg = createHotelContactPackage({
    ...basePkg,
    hotel_id: graph.hpcHotelId || basePkg.hotel_id,
    hotel_name: basePkg.hotel_name || null,
    owner_entity_id: ownerOrgId || basePkg.owner_entity_id,
    owner_display_name: ownerName || basePkg.owner_display_name,
    people: people.length ? people : basePkg.people || [],
    organization_contact_route: {
      owner_entity_id: ownerOrgId,
      owner_display_name: ownerName,
      channels: orgChannels.length
        ? orgChannels
        : basePkg.organization_contact_route?.channels || [],
    },
    refresh_status: people.length
      ? REFRESH_STATUS.PARTIAL
      : basePkg.refresh_status || REFRESH_STATUS.NEVER,
    unresolved_reasons: [...unresolved].filter(
      (r) => !(people.length && r === UNRESOLVED_REASON.NO_HOTEL_CONTACT),
    ),
    provenance: {
      ...(basePkg.provenance || {}),
      hi_graph: {
        source: "HI_AIRTABLE_GRAPH",
        contract: graph.contract,
        relationship_count: graph.relationshipCount,
        contact_count: people.length,
      },
    },
  });

  // Keep hotel-level NO_HOTEL_CONTACT if we only have owner-org people (honest).
  if (!pkg.hotel_contact?.channels?.length) {
    const u = new Set(pkg.unresolved_reasons || []);
    u.add(UNRESOLVED_REASON.NO_HOTEL_CONTACT);
    pkg.unresolved_reasons = [...u];
  }

  return {
    ...baseResult,
    package: pkg,
    owner_anchor: ownerOrgId
      ? {
          owner_entity_id: ownerOrgId,
          owner_display_name: ownerName,
          owner_explorer_path: null,
          source: "HI_AIRTABLE_GRAPH",
        }
      : baseResult.owner_anchor,
    unresolved_reasons: pkg.unresolved_reasons,
    hi_graph_overlay: {
      applied: true,
      contact_count: people.length,
      relationship_count: graph.relationshipCount,
    },
  };
}

export default { overlayHiGraphOnContactHotelGet };
