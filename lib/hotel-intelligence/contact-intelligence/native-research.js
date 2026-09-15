/**
 * Contact Intelligence V1 — bounded native research (no paid providers).
 */

import { isNativeResearchEnabled, isPaidEnrichmentEnabled } from "./policy.js";
import { UNRESOLVED_REASON, REFRESH_STATUS, CHANNEL_KIND, ATTRIBUTION } from "./vocabulary.js";
import { createChannel, createEvidenceRef } from "./contact-record.js";

/**
 * Native refresh: merge fixture/seed evidence + ownership surface signals only.
 * Does not call Webhound / paid APIs.
 */
export function runNativeContactResearch({
  hotel_id,
  seedPackage = null,
  ownershipAnchor = null,
  ownerPeople = [],
  hotelPhone = null,
  hotelWebsite = null,
  env = process.env,
} = {}) {
  const unresolved = [];
  const started = Date.now();

  if (!isNativeResearchEnabled(env)) {
    return {
      ok: false,
      refresh_status: REFRESH_STATUS.FAILED,
      error: "native_research_disabled",
      unresolved_reasons: [UNRESOLVED_REASON.PAID_ENRICHMENT_DISABLED],
      elapsed_ms: Date.now() - started,
      paid_used: false,
    };
  }

  if (isPaidEnrichmentEnabled(env) === false) {
    // Expected default — native path continues without paid.
  }

  const channels = [];
  const people = [];

  if (seedPackage?.hotel_contact?.channels?.length) {
    channels.push(...seedPackage.hotel_contact.channels);
  } else {
    if (hotelPhone) {
      channels.push(
        createChannel({
          kind: CHANNEL_KIND.HOTEL_PHONE,
          value: hotelPhone,
          display_label: "Hotel phone",
          attribution: ATTRIBUTION.PROPERTY,
          line_type: "MAIN",
          verified: true,
          evidence: [
            createEvidenceRef({
              source_title: "Census / property listing",
              source_type: "census_or_listing",
              observed_at: new Date().toISOString(),
            }),
          ],
        })
      );
    }
    if (hotelWebsite) {
      channels.push(
        createChannel({
          kind: CHANNEL_KIND.HOTEL_WEBSITE,
          value: hotelWebsite,
          display_label: "Hotel website",
          attribution: ATTRIBUTION.OFFICIAL,
          claimed_official: true,
          verified: true,
          evidence: [
            createEvidenceRef({
              source_title: "First-party hotel website",
              source_type: "first_party",
              observed_at: new Date().toISOString(),
            }),
          ],
        })
      );
    }
  }

  if (!channels.length && !(seedPackage?.hotel_contact?.channels || []).length) {
    unresolved.push(UNRESOLVED_REASON.NO_HOTEL_CONTACT);
  }

  if (!ownershipAnchor?.ok || !ownershipAnchor?.anchor?.primary_owner_entity_id) {
    unresolved.push(UNRESOLVED_REASON.OWNER_UNRESOLVED);
  }

  const orgChannels = seedPackage?.organization_contact_route?.channels || [];
  if (!orgChannels.length && !(seedPackage?.organization_contact_route?.channels || []).length) {
    // may still fill via owner reuse later
  }

  const seedPeople = seedPackage?.people || [];
  if (seedPeople.length) {
    people.push(...seedPeople);
  } else if (Array.isArray(ownerPeople) && ownerPeople.length) {
    for (const p of ownerPeople.slice(0, 3)) {
      const name = p.display_name || p.name || null;
      if (!name) continue;
      const personChannels = [];
      if (p.linkedin || p.linkedin_url) {
        personChannels.push(
          createChannel({
            kind: CHANNEL_KIND.PERSON_LINKEDIN,
            value: p.linkedin || p.linkedin_url,
            display_label: "Professional profile",
            attribution: ATTRIBUTION.NAMED_PERSON,
            person_name: name,
            verified: p.professional_profile_verified === true,
            usage_rights: p.professional_profile_verified === true ? "PUBLIC_SAFE" : "INTERNAL_ONLY",
          })
        );
      }
      if (p.email) {
        personChannels.push(
          createChannel({
            kind: CHANNEL_KIND.PERSON_EMAIL,
            value: p.email,
            display_label: "Email",
            person_name: name,
            claimed_official: false,
            force_inferred: p.email_inferred === true,
          })
        );
      }
      people.push({
        person_id: p.person_id || p.id || null,
        display_name: name,
        title: p.title || p.role || null,
        organization_entity_id: ownershipAnchor?.anchor?.primary_owner_entity_id || null,
        organization_name: ownershipAnchor?.anchor?.owner_display_name || null,
        role_currency: p.role_currency || "UNKNOWN",
        property_relevance: "OWNER_ORG",
        channels: personChannels,
        evidence: p.evidence || [],
        unresolved_reasons: personChannels.length ? [] : [UNRESOLVED_REASON.NO_PERSON_EVIDENCE],
      });
    }
  }

  if (!people.length) unresolved.push(UNRESOLVED_REASON.NO_PERSON_EVIDENCE);

  // Strip any channel that would violate hard labeling if somehow marked official while inferred
  for (const ch of channels) {
    if (ch.attribution === "INFERRED" && /official/i.test(String(ch.display_label || ""))) {
      ch.display_label = "Inferred (not officially attributed)";
      unresolved.push(UNRESOLVED_REASON.CHANNEL_INFERRED_ONLY);
    }
  }

  return {
    ok: true,
    refresh_status: unresolved.length ? REFRESH_STATUS.PARTIAL : REFRESH_STATUS.COMPLETE,
    paid_used: false,
    elapsed_ms: Date.now() - started,
    hotel_contact: { channels: channels.filter((c) => String(c.kind).startsWith("HOTEL_")) },
    organization_contact_route: seedPackage?.organization_contact_route || { channels: orgChannels },
    people,
    channels,
    unresolved_reasons: [...new Set(unresolved)],
    provenance: {
      method: "native_bounded",
      summary: "Bounded native contact research (paid enrichment disabled by default)",
      researched_at: new Date().toISOString(),
    },
  };
}
