/**
 * Contact Intelligence V1.3 — four development hotels with ownership-surface anchors.
 * Distinct owners only; operator/brand never equated to ownership.
 */

export const OWNER_PERSON_SUBSET_VERSION = "contact-owner-person-subset-v1.3";

/** Hypotheses from ownership fixtures — must be corroborated live. */
export const EVIDENCED_OWNER_HOTELS = Object.freeze([
  {
    hotel_id: "recUNycnMwOVFX0hc",
    hotel_name: "Krystal Grand Puerto Vallarta",
    city: "Puerto Vallarta",
    country: "Mexico",
    language: "es",
    expected_owner_entity_id: "dle_06G6AB1VK0BCCD94DNN7W8DRWZ",
    domain_hypotheses: ["https://www.gsfhotels.com/", "https://gsf-hotels.com/"],
    person_hypotheses: [
      {
        display_name: "Carlos Gerardo Ancira Elizondo",
        title: "Presidente (Consejo de Administración) — verify on first-party gobierno page",
        why_relevant_hypothesis:
          "Current first-party consejo president candidate; board ownership leadership ≠ confirmed brand-conversion DM.",
        source: "first_party_consejo_hypothesis",
      },
      {
        display_name: "Francisco Ancira Elizondo",
        title: "Board President (cohort seed — verify against current consejo)",
        why_relevant_hypothesis:
          "Historical cohort seed for GSF board leadership — must be corroborated or superseded by current first-party consejo listing.",
        source: "cohort_seed_hypothesis",
      },
    ],
    deep_research_path: "fixtures/golden-demo/gsf-mexico-ownership-cohort-v1.json",
  },
  {
    hotel_id: "recIwaP1etgx2g9nA",
    hotel_name: "Cambridge Beaches Resort & Spa",
    city: "Sandys Parish",
    country: "Bermuda",
    language: "en",
    expected_owner_entity_id: "ent_dovetail-hospitality",
    domain_hypotheses: ["https://www.dovetailandco.com/"],
    person_hypotheses: [],
    deep_research_path: "fixtures/golden-demo/dovetail-hospitality-cohort-v1.json",
  },
  {
    hotel_id: "recsYJb2R1jarPpK3",
    hotel_name: "Sheraton Guadalajara Expo",
    city: "Guadalajara",
    country: "Mexico",
    language: "es",
    expected_owner_entity_id: "ent_inmobiliaria-hnf",
    domain_hypotheses: [],
    /** Deceased former chair — must not publish as current DM. */
    person_hypotheses: [
      {
        display_name: "Patricia Olivia Newton Frausto",
        title: "Former HNF chair/director (deceased Feb 2022)",
        why_relevant_hypothesis: "Historical ownership principal — NOT current decision maker.",
        source: "deep_research_former",
        former_affiliation: true,
        deceased: true,
      },
    ],
    deep_research_path: "fixtures/golden-demo/sheraton-gdl-expo-deep-research-v1.json",
    /** Aimbridge contacts must stay OPERATOR-classified, never HNF org route. */
    forbidden_org_hosts: [
      "aimbridge.com",
      "aimbridgelatam.com",
      "remax.com",
      "remax.com.mx",
      "emis.com",
    ],
  },
  {
    hotel_id: "recTYaiA4S6fR6ixx",
    hotel_name: "Real Inn Cancún / voco Cancún",
    city: "Cancún",
    country: "Mexico",
    language: "es",
    expected_owner_entity_id: "ent_alliance-hotel-management",
    domain_hypotheses: [],
    person_hypotheses: [
      {
        display_name: "Rolf Tweeten",
        title: "Chairman (until Jan 2026) / Investor, Alliance Hospitality Management LLC",
        why_relevant_hypothesis:
          "Senior Alliance principal on owner-side platform for six-hotel Mexico package — TITLE_NOT_AUTHORITY.",
        source: "deep_research_hypothesis",
        linkedin: "https://www.linkedin.com/in/rolftweeten",
      },
      {
        display_name: "Carlos Justo",
        title: "Named buyer principal (six Real Inn hotels)",
        why_relevant_hypothesis:
          "Press-named buyer; legal bridge to Alliance unresolved — PROBABLE only.",
        source: "deep_research_hypothesis",
      },
    ],
    deep_research_path: "fixtures/golden-demo/real-inn-cancun-deep-research-v1.json",
    forbidden_org_hosts: [
      "palladiumhotelgroup.com",
      "aimbridge.com",
      "aimbridgelatam.com",
      "ihg.com",
      "apexalliance.eu",
      "apexalliance.com",
      "alliancehm.com",
    ],
  },
]);

export function groupHotelsByOwner(hotels, ownershipAnchorsByHotelId) {
  const byOwner = new Map();
  for (const h of hotels) {
    const anchor = ownershipAnchorsByHotelId.get(h.hotel_id);
    const ownerId =
      anchor?.anchor?.primary_owner_entity_id || h.expected_owner_entity_id || null;
    const key = ownerId || `unresolved:${h.hotel_id}`;
    if (!byOwner.has(key)) {
      byOwner.set(key, {
        owner_entity_id: ownerId,
        owner_display_name: anchor?.anchor?.owner_display_name || null,
        hotels: [],
      });
    }
    byOwner.get(key).hotels.push(h);
  }
  return [...byOwner.values()];
}
