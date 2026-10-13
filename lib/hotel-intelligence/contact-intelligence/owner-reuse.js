/**
 * Contact Intelligence V1 — owner-level contact route reuse.
 */

import { PROPERTY_RELEVANCE } from "./vocabulary.js";
import { createChannel } from "./contact-record.js";

/**
 * Build an organization contact route from owner portfolio / evidence.
 */
export function buildOrganizationContactRoute({
  owner_entity_id,
  owner_display_name,
  channels = [],
  sources = [],
} = {}) {
  return {
    owner_entity_id: owner_entity_id || null,
    owner_display_name: owner_display_name || null,
    channels: channels.map((c) =>
      createChannel({
        ...c,
        property_relevance: c.property_relevance || PROPERTY_RELEVANCE.OWNER_ORG,
      })
    ),
    sources,
    reuse_key: owner_entity_id ? `owner_route:${owner_entity_id}` : null,
  };
}

/**
 * Reuse an owner route onto a hotel package (marks PORTFOLIO_REUSED).
 */
export function applyOwnerRouteReuse(hotelPackage, ownerRoute) {
  if (!ownerRoute?.owner_entity_id) {
    return {
      ...hotelPackage,
      portfolio_reuse: { reused: false, reason: "no_owner_route" },
    };
  }

  const reusedChannels = (ownerRoute.channels || []).map((ch) =>
    createChannel({
      ...ch,
      channel_id: undefined,
      property_relevance: PROPERTY_RELEVANCE.PORTFOLIO_REUSED,
      display_label: ch.display_label || "Owner organization contact (portfolio reuse)",
    })
  );

  const existing = hotelPackage.organization_contact_route?.channels || [];
  const merged = [...existing];
  for (const ch of reusedChannels) {
    const dup = merged.some(
      (m) => m.kind === ch.kind && String(m.value || "").toLowerCase() === String(ch.value || "").toLowerCase()
    );
    if (!dup) merged.push(ch);
  }

  return {
    ...hotelPackage,
    owner_entity_id: hotelPackage.owner_entity_id || ownerRoute.owner_entity_id,
    owner_display_name: hotelPackage.owner_display_name || ownerRoute.owner_display_name,
    organization_contact_route: {
      ...ownerRoute,
      channels: merged,
    },
    portfolio_reuse: {
      reused: reusedChannels.length > 0,
      owner_entity_id: ownerRoute.owner_entity_id,
      reused_channel_count: reusedChannels.length,
      reuse_key: ownerRoute.reuse_key,
    },
  };
}
