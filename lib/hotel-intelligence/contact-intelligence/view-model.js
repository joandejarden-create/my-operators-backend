/**
 * Contact Intelligence V1 — shared ContactSummary view model.
 * UI must not inspect raw provider outputs.
 */

import { applyPublicationRules } from "./publication-rules.js";

export const CONTACT_VIEW_MODEL_VERSION = "contact-summary-view-v1";

function publishChannels(channels, audience) {
  const out = [];
  for (const ch of channels || []) {
    const pub = applyPublicationRules(ch, { audience });
    if (pub.ok) out.push(pub.publishable || ch);
  }
  return out;
}

/**
 * Build UI-safe summary from a hotel contact package.
 */
export function buildContactIntelligenceViewModel(pkg, { audience = "public" } = {}) {
  const hotelChannels = publishChannels(pkg?.hotel_contact?.channels, audience);
  const ownerChannels = publishChannels(pkg?.organization_contact_route?.channels, audience);
  const decision_makers = (pkg?.people || [])
    .filter((p) => !p.former_affiliation)
    .map((p) => ({
      person_id: p.person_id,
      display_name: p.display_name,
      title: p.title,
      why_relevant: p.why_relevant,
      role_currency: p.role_currency,
      channels: publishChannels(p.channels, audience),
      confidence: {
        identity: p.identity_confidence || null,
        role: p.role_currency || null,
        contact: p.contact_confidence || null,
      },
      last_verified: p.role_check_date || p.contact_source_date || null,
      unresolved_reasons: p.unresolved_reasons || [],
    }));

  const best_contact_path = {
    hotel: hotelChannels[0] || null,
    owner_org: ownerChannels[0] || null,
    primary_person: decision_makers[0] || null,
  };

  return {
    version: CONTACT_VIEW_MODEL_VERSION,
    hotel_id: pkg?.hotel_id || null,
    hotel_name: pkg?.hotel_name || null,
    owner_entity_id: pkg?.owner_entity_id || null,
    owner_display_name: pkg?.owner_display_name || null,
    hotel_contacts: hotelChannels,
    owner_contacts: ownerChannels,
    decision_makers,
    best_contact_path,
    freshness: {
      refresh_status: pkg?.refresh_status || null,
      last_refreshed_at: pkg?.last_refreshed_at || null,
    },
    confidence: {
      owner_resolved: Boolean(pkg?.owner_resolved),
      portfolio_reuse: pkg?.portfolio_reuse || null,
    },
    evidence_available: Boolean(
      (pkg?.people || []).some((p) => (p.evidence || []).length) ||
        hotelChannels.some((c) => (c.evidence || []).length)
    ),
    unresolved_reasons: pkg?.unresolved_reasons || [],
    write_guarantees: pkg?.write_guarantees || null,
    raw_provider_outputs_exposed: false,
  };
}
