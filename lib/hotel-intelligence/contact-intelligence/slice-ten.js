/**
 * Contact Intelligence V1 — frozen 10-hotel first slice.
 * Uses golden Hotel Intelligence / ownership cohort hotels only.
 */

import {
  CHANNEL_KIND,
  ATTRIBUTION,
  PROPERTY_RELEVANCE,
  USAGE_RIGHTS,
  UNRESOLVED_REASON,
} from "./vocabulary.js";
import { createChannel, createEvidenceRef, createPersonContact, createHotelContactPackage } from "./contact-record.js";
import { buildOrganizationContactRoute } from "./owner-reuse.js";

/** Frozen slice — do not expand without a new slice version. */
export const CONTACT_SLICE_TEN_VERSION = "contact-slice-ten-v1";

export const CONTACT_SLICE_TEN_HOTELS = Object.freeze([
  {
    hotel_id: "recUNycnMwOVFX0hc",
    hotel_name: "Krystal Grand Puerto Vallarta",
    owner_entity_id: "dle_06G6AB1VK0BCCD94DNN7W8DRWZ",
    owner_display_name: "Grupo Hotelero Santa Fe",
    owner_group: "gsf",
  },
  {
    hotel_id: "recIwaP1etgx2g9nA",
    hotel_name: "Cambridge Beaches Resort & Spa",
    owner_entity_id: "ent_dovetail-hospitality",
    owner_display_name: "Dovetail Hospitality",
    owner_group: "dovetail",
  },
  {
    hotel_id: "recsYJb2R1jarPpK3",
    hotel_name: "Sheraton Guadalajara Expo",
    owner_entity_id: "ent_inmobiliaria-hnf",
    owner_display_name: "Inmobiliaria HNF",
    owner_group: "hnf",
  },
  {
    hotel_id: "recTYaiA4S6fR6ixx",
    hotel_name: "Real Inn Cancún / voco Cancún",
    owner_entity_id: "ent_alliance-hotel-management",
    owner_display_name: "Alliance Hotel Management",
    owner_group: "alliance",
  },
  {
    hotel_id: "recGZZCek9vDQGG1L",
    hotel_name: "voco Guadalajara Expo",
    owner_entity_id: "ent_alliance-hotel-management",
    owner_display_name: "Alliance Hotel Management",
    owner_group: "alliance",
  },
  {
    hotel_id: "rec79Xs4mZkuiWnuN",
    hotel_name: "Real Inn Ciudad Juárez",
    owner_entity_id: "ent_alliance-hotel-management",
    owner_display_name: "Alliance Hotel Management",
    owner_group: "alliance",
  },
  {
    hotel_id: "recFspIiglYxJp1N1",
    hotel_name: "Real Inn San Luis Potosí",
    owner_entity_id: "ent_alliance-hotel-management",
    owner_display_name: "Alliance Hotel Management",
    owner_group: "alliance",
  },
  {
    hotel_id: "recogJrXdZHRV06Bl",
    hotel_name: "Real Inn Nuevo Laredo",
    owner_entity_id: "ent_alliance-hotel-management",
    owner_display_name: "Alliance Hotel Management",
    owner_group: "alliance",
  },
  {
    hotel_id: "recZxCHVNG0bDQhfG",
    hotel_name: "Real Inn Torreón",
    owner_entity_id: "ent_alliance-hotel-management",
    owner_display_name: "Alliance Hotel Management",
    owner_group: "alliance",
  },
  {
    hotel_id: "recSliceTenGsfOp01",
    hotel_name: "GSF Operated Portfolio Example (slice)",
    owner_entity_id: "dle_06G6AB1VK0BCCD94DNN7W8DRWZ",
    owner_display_name: "Grupo Hotelero Santa Fe",
    owner_group: "gsf",
    note: "Slice hotel for owner-level reuse demonstration; not a Census promotion claim.",
  },
]);

const GSF_ORG_ROUTE = buildOrganizationContactRoute({
  owner_entity_id: "dle_06G6AB1VK0BCCD94DNN7W8DRWZ",
  owner_display_name: "Grupo Hotelero Santa Fe",
  channels: [
    createChannel({
      kind: CHANNEL_KIND.ORG_WEBSITE,
      value: "https://www.gsfhotels.com",
      display_label: "Owner website",
      attribution: ATTRIBUTION.OFFICIAL,
      claimed_official: true,
      verified: true,
      property_relevance: PROPERTY_RELEVANCE.OWNER_ORG,
      evidence: [
        createEvidenceRef({
          source_title: "GSF first-party site",
          source_type: "first_party",
          source_url: "https://www.gsfhotels.com",
          observed_at: "2026-08-01T00:00:00.000Z",
        }),
      ],
    }),
    createChannel({
      kind: CHANNEL_KIND.ROLE_MAILBOX,
      value: "ir@gsfhotels.com",
      display_label: "Investor relations mailbox",
      attribution: ATTRIBUTION.ROLE_MAILBOX,
      verified: true,
      property_relevance: PROPERTY_RELEVANCE.OWNER_ORG,
      evidence: [
        createEvidenceRef({
          source_title: "GSF IR contact",
          source_type: "first_party",
          observed_at: "2026-08-01T00:00:00.000Z",
        }),
      ],
    }),
  ],
});

const DOVETAIL_ORG_ROUTE = buildOrganizationContactRoute({
  owner_entity_id: "ent_dovetail-hospitality",
  owner_display_name: "Dovetail Hospitality",
  channels: [
    createChannel({
      kind: CHANNEL_KIND.ORG_WEBSITE,
      value: "https://www.dovetailandco.com",
      display_label: "Sponsor website",
      attribution: ATTRIBUTION.OFFICIAL,
      claimed_official: true,
      verified: true,
      property_relevance: PROPERTY_RELEVANCE.OWNER_ORG,
      evidence: [
        createEvidenceRef({
          source_title: "Dovetail first-party",
          source_url: "https://www.dovetailandco.com",
          source_type: "first_party",
          observed_at: "2026-09-04T00:00:00.000Z",
        }),
      ],
    }),
  ],
});

const ALLIANCE_ORG_ROUTE = buildOrganizationContactRoute({
  owner_entity_id: "ent_alliance-hotel-management",
  owner_display_name: "Alliance Hotel Management",
  channels: [
    createChannel({
      kind: CHANNEL_KIND.ORG_WEBSITE,
      value: "https://www.alliancehm.com",
      display_label: "Organization website",
      attribution: ATTRIBUTION.ORGANIZATION,
      verified: false,
      property_relevance: PROPERTY_RELEVANCE.OWNER_ORG,
      usage_rights: USAGE_RIGHTS.PUBLIC_SAFE,
      evidence: [
        createEvidenceRef({
          source_title: "Alliance portfolio research",
          source_type: "research_compile",
          observed_at: "2026-08-15T00:00:00.000Z",
        }),
      ],
    }),
  ],
});

const HNF_ORG_ROUTE = buildOrganizationContactRoute({
  owner_entity_id: "ent_inmobiliaria-hnf",
  owner_display_name: "Inmobiliaria HNF",
  channels: [
    createChannel({
      kind: CHANNEL_KIND.SWITCHBOARD,
      value: "+52 33 0000 0000",
      display_label: "Organization switchboard",
      attribution: ATTRIBUTION.ORGANIZATION,
      line_type: "SWITCHBOARD",
      claimed_direct_personal: false,
      verified: false,
      property_relevance: PROPERTY_RELEVANCE.OWNER_ORG,
      evidence: [
        createEvidenceRef({
          source_title: "HNF research note (placeholder line class)",
          source_type: "research_compile",
          observed_at: "2026-08-15T00:00:00.000Z",
          excerpt: "Corporate line class only — not a direct personal number.",
        }),
      ],
    }),
  ],
});

const ORG_BY_OWNER = Object.freeze({
  dle_06G6AB1VK0BCCD94DNN7W8DRWZ: GSF_ORG_ROUTE,
  "ent_dovetail-hospitality": DOVETAIL_ORG_ROUTE,
  "ent_alliance-hotel-management": ALLIANCE_ORG_ROUTE,
  "ent_inmobiliaria-hnf": HNF_ORG_ROUTE,
});

const HOTEL_SEED = Object.freeze({
  recUNycnMwOVFX0hc: {
    phone: "+52 322 226 0700",
    website: "https://www.krystal-hotels.com/krystal-grand-puerto-vallarta",
    people: [
      createPersonContact({
        display_name: "Francisco Ancira Elizondo",
        title: "Board President",
        organization_entity_id: "dle_06G6AB1VK0BCCD94DNN7W8DRWZ",
        organization_name: "Grupo Hotelero Santa Fe",
        role_observed_at: "2025-12-01T00:00:00.000Z",
        property_relevance: PROPERTY_RELEVANCE.OWNER_ORG,
        channels: [
          createChannel({
            kind: CHANNEL_KIND.PERSON_LINKEDIN,
            value: null,
            display_label: "Professional profile",
            attribution: ATTRIBUTION.UNATTRIBUTED,
            usage_rights: USAGE_RIGHTS.INTERNAL_ONLY,
          }),
        ],
        unresolved_reasons: [UNRESOLVED_REASON.NO_PERSON_EVIDENCE],
        evidence: [
          createEvidenceRef({
            source_title: "GSF securities / people notes",
            source_type: "corporate_filings",
            observed_at: "2025-12-01T00:00:00.000Z",
          }),
        ],
      }),
    ],
  },
  recIwaP1etgx2g9nA: {
    phone: "+441234 0331",
    website: "https://www.cambridgebeaches.com",
    people: [],
  },
  recsYJb2R1jarPpK3: {
    phone: null,
    website: null,
    people: [],
  },
  recTYaiA4S6fR6ixx: {
    phone: null,
    website: null,
    people: [],
  },
});

function hotelChannelsFromSeed(seed) {
  const channels = [];
  if (seed?.website) {
    channels.push(
      createChannel({
        kind: CHANNEL_KIND.HOTEL_WEBSITE,
        value: seed.website,
        display_label: "Hotel website",
        attribution: ATTRIBUTION.OFFICIAL,
        claimed_official: true,
        verified: true,
        property_relevance: PROPERTY_RELEVANCE.PROPERTY_DIRECT,
        evidence: [
          createEvidenceRef({
            source_title: "First-party hotel",
            source_type: "first_party",
            source_url: seed.website,
            observed_at: "2026-09-01T00:00:00.000Z",
          }),
        ],
      })
    );
  }
  if (seed?.phone) {
    channels.push(
      createChannel({
        kind: CHANNEL_KIND.HOTEL_PHONE,
        value: seed.phone,
        display_label: "Hotel phone",
        attribution: ATTRIBUTION.PROPERTY,
        line_type: "MAIN",
        verified: true,
        property_relevance: PROPERTY_RELEVANCE.PROPERTY_DIRECT,
        evidence: [
          createEvidenceRef({
            source_title: "First-party / tourism listings",
            source_type: "listing",
            observed_at: "2026-09-01T00:00:00.000Z",
          }),
        ],
      })
    );
  }
  return channels;
}

export function buildSliceTenSeedPackage(hotelId) {
  const meta = CONTACT_SLICE_TEN_HOTELS.find((h) => h.hotel_id === hotelId);
  if (!meta) return null;
  const seed = HOTEL_SEED[hotelId] || {};
  const hotelChannels = hotelChannelsFromSeed(seed);
  const orgRoute = ORG_BY_OWNER[meta.owner_entity_id] || null;
  const unresolved = [];

  if (!hotelChannels.length) unresolved.push(UNRESOLVED_REASON.NO_HOTEL_CONTACT);
  if (!orgRoute?.channels?.length) unresolved.push(UNRESOLVED_REASON.NO_ORG_ROUTE);
  if (!(seed.people || []).length) unresolved.push(UNRESOLVED_REASON.NO_PERSON_EVIDENCE);

  return createHotelContactPackage({
    hotel_id: meta.hotel_id,
    hotel_name: meta.hotel_name,
    owner_entity_id: meta.owner_entity_id,
    owner_display_name: meta.owner_display_name,
    hotel_contact: { channels: hotelChannels },
    organization_contact_route: orgRoute || { channels: [] },
    people: seed.people || [],
    channels: [...hotelChannels, ...(orgRoute?.channels || [])],
    refresh_status: "COMPLETE",
    last_refreshed_at: "2026-09-14T00:00:00.000Z",
    unresolved_reasons: unresolved,
    provenance: {
      slice_version: CONTACT_SLICE_TEN_VERSION,
      owner_group: meta.owner_group,
      summary: "Frozen Contact Intelligence V1 ten-hotel slice seed",
    },
  });
}

export function listSliceTenPackages() {
  return CONTACT_SLICE_TEN_HOTELS.map((h) => buildSliceTenSeedPackage(h.hotel_id));
}

export function getOwnerRouteForSlice(ownerEntityId) {
  return ORG_BY_OWNER[ownerEntityId] || null;
}
