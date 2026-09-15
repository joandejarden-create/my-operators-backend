/**
 * Labeled packages for synthetic benchmark hotels (evaluation harness only).
 */

import {
  CHANNEL_KIND,
  ATTRIBUTION,
  PROPERTY_RELEVANCE,
  UNRESOLVED_REASON,
} from "../vocabulary.js";
import { createChannel, createEvidenceRef, createPersonContact, createHotelContactPackage } from "../contact-record.js";
import { buildOrganizationContactRoute } from "../owner-reuse.js";

export function buildBenchmarkLabeledPackage(row) {
  const labels = row.labels || {};
  const unresolved = [];
  const hotelChannels = [];
  const orgChannels = [];
  const people = [];

  if (labels.expect_hotel_contact) {
    hotelChannels.push(
      createChannel({
        kind: CHANNEL_KIND.HOTEL_PHONE,
        value: `+52 55 ${String(row.hotel_id).slice(-4)} 0000`,
        display_label: "Hotel phone",
        attribution: ATTRIBUTION.PROPERTY,
        line_type: "MAIN",
        verified: true,
        property_relevance: PROPERTY_RELEVANCE.PROPERTY_DIRECT,
        evidence: [
          createEvidenceRef({
            source_title: "Benchmark gold label",
            source_type: "benchmark_label",
            observed_at: "2026-09-14T00:00:00.000Z",
          }),
        ],
      })
    );
  } else {
    unresolved.push(UNRESOLVED_REASON.NO_HOTEL_CONTACT);
  }

  if (labels.expect_org_route) {
    orgChannels.push(
      createChannel({
        kind: CHANNEL_KIND.ORG_WEBSITE,
        value: `https://example.invalid/${row.owner_group}`,
        display_label: "Organization website",
        attribution: ATTRIBUTION.ORGANIZATION,
        property_relevance: PROPERTY_RELEVANCE.OWNER_ORG,
        verified: false,
        evidence: [
          createEvidenceRef({
            source_title: "Benchmark gold org route",
            source_type: "benchmark_label",
            observed_at: "2026-09-14T00:00:00.000Z",
          }),
        ],
      })
    );
  } else {
    unresolved.push(UNRESOLVED_REASON.NO_ORG_ROUTE);
  }

  if (labels.expect_person) {
    people.push(
      createPersonContact({
        display_name: `Benchmark Contact ${row.hotel_id}`,
        title: "Managing Director",
        organization_entity_id: row.owner_entity_id,
        role_observed_at: "2026-06-01T00:00:00.000Z",
        property_relevance: PROPERTY_RELEVANCE.OWNER_ORG,
        channels: [
          createChannel({
            kind: CHANNEL_KIND.PERSON_LINKEDIN,
            value: `https://www.linkedin.com/in/benchmark-${row.hotel_id}`,
            display_label: "Professional profile",
            attribution: ATTRIBUTION.NAMED_PERSON,
            verified: true,
          }),
        ],
      })
    );
  } else {
    unresolved.push(UNRESOLVED_REASON.NO_PERSON_EVIDENCE);
  }

  const orgRoute = buildOrganizationContactRoute({
    owner_entity_id: row.owner_entity_id,
    owner_display_name: `Owner ${row.owner_group}`,
    channels: orgChannels,
  });

  return createHotelContactPackage({
    hotel_id: row.hotel_id,
    hotel_name: row.hotel_name,
    owner_entity_id: row.owner_entity_id,
    owner_display_name: `Owner ${row.owner_group}`,
    hotel_contact: { channels: hotelChannels },
    organization_contact_route: orgRoute,
    people,
    channels: [...hotelChannels, ...orgChannels],
    refresh_status: "COMPLETE",
    last_refreshed_at: "2026-09-14T00:00:00.000Z",
    unresolved_reasons: unresolved,
    portfolio_reuse: {
      reused: String(row.owner_group || "").startsWith("held_") || String(row.owner_group || "").startsWith("dev_")
        ? labels.expect_org_route
        : false,
      owner_entity_id: row.owner_entity_id,
      reused_channel_count: labels.expect_org_route ? orgChannels.length : 0,
    },
    provenance: {
      summary: "Benchmark labeled package (harness)",
      split: row.split,
      owner_group: row.owner_group,
    },
  });
}
