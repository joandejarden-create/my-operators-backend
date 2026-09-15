/**
 * Contact Intelligence V1.4 — frozen selection of 10 DEVELOPMENT hotels.
 * Selected from reports/contact-intelligence-benchmark-real-v1.2.json BEFORE research outcomes.
 * Denominator of real manifest unchanged. Held-out excluded.
 */

export const GENERALIZATION_TEN_VERSION = "contact-generalization-ten-v1.4";
export const MANIFEST_SOURCE = "reports/contact-intelligence-benchmark-real-v1.2.json";
export const MANIFEST_DENOMINATOR = 100;
export const HELD_OUT_COUNT = 25;
export const DEVELOPMENT_COUNT = 75;

/** Owners researched in V1.3 (exclude from "new owner" count when reused). */
export const V13_RESEARCHED_OWNER_GROUPS = Object.freeze(["gsf", "dovetail", "hnf", "alliance"]);

/**
 * Frozen before ownership/contact research. Do not retarget after observing outcomes.
 */
export const GENERALIZATION_TEN_HOTELS = Object.freeze([
  {
    hotel_id: "recId5nDFUgVbJnzH",
    hotel_name: "Krystal Grand Residences Villas San Miguel de Allende",
    city: "San Miguel de Allende",
    country: "Mexico",
    language: "es",
    owner_group: "gsf",
    owner_group_status: "PROVISIONAL_UNTIL_LEAKAGE_ASSESSED",
    selection_role: "portfolio_relationship_probe_gsf_not_auto_owned",
    case_type: "branded_portfolio_probe",
    v13_owner_researched: true,
    leakage_consideration:
      "GSF group development-exposed in V1.3. Live-slice notes operated_not_owned — do NOT auto-attach GSF owner route without ownership-surface evidence.",
  },
  {
    hotel_id: "recGZZCek9vDQGG1L",
    hotel_name: "voco Guadalajara Expo",
    city: "Guadalajara",
    country: "Mexico",
    language: "es",
    owner_group: "alliance",
    owner_group_status: "PROVISIONAL_UNTIL_LEAKAGE_ASSESSED",
    selection_role: "portfolio_reuse_alliance",
    case_type: "branded_portfolio_reuse",
    v13_owner_researched: true,
    cohort_owner_entity_id: "ent_alliance-hotel-management",
    cohort_owner_display_name: "Alliance Hotel Management",
    leakage_consideration:
      "Alliance development-exposed in V1.3. If ownership surface lacks this hotel, attach researched Alliance org route as COHORT_PORTFOLIO_REUSE with hotel↔owner relationship still unverified on surface.",
  },
  {
    hotel_id: "rec00Cp2AgpZAtp9Z",
    hotel_name: "Pousada Ceu De Estrelas",
    city: null,
    country: "Brazil",
    language: "pt",
    owner_group: "provisional_unassigned",
    owner_group_status: "PROVISIONAL_UNTIL_LEAKAGE_ASSESSED",
    selection_role: "brazil_pt_independent",
    case_type: "independent_pt",
    v13_owner_researched: false,
    leakage_consideration:
      "Owner group provisional_unassigned — mark development-exposed; held-out also uses this provisional label so leakage is uncertain.",
  },
  {
    hotel_id: "rec08YF1M2BOFBcYG",
    hotel_name: "TRYP by Wyndham Sao Paulo Paulista Paraiso",
    city: null,
    country: "Brazil",
    language: "pt",
    owner_group: "provisional_unassigned",
    owner_group_status: "PROVISIONAL_UNTIL_LEAKAGE_ASSESSED",
    selection_role: "brazil_pt_branded",
    case_type: "branded_pt",
    v13_owner_researched: false,
    leakage_consideration: "provisional_unassigned — development-exposed; leakage uncertain vs held-out.",
  },
  {
    hotel_id: "rec07GLNG5oNBkAj8",
    hotel_name: "ibis Canoas Shopping",
    city: null,
    country: "Brazil",
    language: "pt",
    owner_group: "provisional_unassigned",
    owner_group_status: "PROVISIONAL_UNTIL_LEAKAGE_ASSESSED",
    selection_role: "brazil_pt_branded_economy",
    case_type: "branded_pt",
    v13_owner_researched: false,
    leakage_consideration: "provisional_unassigned — development-exposed; leakage uncertain vs held-out.",
  },
  {
    hotel_id: "rectL8jThAojSv0pV",
    hotel_name: "Royalton Antigua, An Autograph Collection All-Inclusive Resort",
    city: null,
    country: "Antigua and Barbuda",
    language: "en",
    owner_group: "provisional_unassigned",
    owner_group_status: "PROVISIONAL_UNTIL_LEAKAGE_ASSESSED",
    selection_role: "branded_caribbean",
    case_type: "branded_en",
    v13_owner_researched: false,
    leakage_consideration: "provisional_unassigned — development-exposed; leakage uncertain vs held-out.",
  },
  {
    hotel_id: "recyhrugWAKzAYU2S",
    hotel_name: "Blue Waters Antigua",
    city: "St John´s",
    country: "Antigua and Barbuda",
    language: "en",
    owner_group: "provisional_unassigned",
    owner_group_status: "PROVISIONAL_UNTIL_LEAKAGE_ASSESSED",
    selection_role: "independent_resort",
    case_type: "independent_en",
    v13_owner_researched: false,
    leakage_consideration: "provisional_unassigned — development-exposed; leakage uncertain vs held-out.",
  },
  {
    hotel_id: "recTVAOs9msNiOjSJ",
    hotel_name: "Copper & Lumber Store Hotel Boutique",
    city: "English Harbour",
    country: "Antigua and Barbuda",
    language: "en",
    owner_group: "provisional_unassigned",
    owner_group_status: "PROVISIONAL_UNTIL_LEAKAGE_ASSESSED",
    selection_role: "boutique_independent",
    case_type: "independent_en",
    v13_owner_researched: false,
    leakage_consideration: "provisional_unassigned — development-exposed; leakage uncertain vs held-out.",
  },
  {
    hotel_id: "recaRtCFEPEm7Sfzu",
    hotel_name: "four seasons resort and residences anguilla",
    city: "West End",
    country: "Anguilla",
    language: "en",
    owner_group: "provisional_unassigned",
    owner_group_status: "PROVISIONAL_UNTIL_LEAKAGE_ASSESSED",
    selection_role: "branded_luxury",
    case_type: "branded_en",
    v13_owner_researched: false,
    leakage_consideration: "provisional_unassigned — development-exposed; leakage uncertain vs held-out.",
  },
  {
    hotel_id: "recBHFTZ3fw7cCdPg",
    hotel_name: "Hammock Cove Antigua - All Inclusive - Adults Only",
    city: "Willikies",
    country: "Antigua and Barbuda",
    language: "en",
    owner_group: "provisional_unassigned",
    owner_group_status: "PROVISIONAL_UNTIL_LEAKAGE_ASSESSED",
    selection_role: "branded_soft_all_inclusive",
    case_type: "branded_en",
    v13_owner_researched: false,
    leakage_consideration: "provisional_unassigned — development-exposed; leakage uncertain vs held-out.",
  },
]);

export function summarizeSelection() {
  const newOwners = GENERALIZATION_TEN_HOTELS.filter((h) => !h.v13_owner_researched);
  const portfolioReuse = GENERALIZATION_TEN_HOTELS.filter((h) =>
    String(h.selection_role || "").startsWith("portfolio_reuse")
  );
  const brazilPt = GENERALIZATION_TEN_HOTELS.filter((h) => h.language === "pt" || h.country === "Brazil");
  return {
    version: GENERALIZATION_TEN_VERSION,
    count: GENERALIZATION_TEN_HOTELS.length,
    new_owner_slots: newOwners.length,
    portfolio_reuse_slots: portfolioReuse.length,
    brazil_pt_slots: brazilPt.length,
    brazil_pt_in_manifest_denominator: true,
    held_out_excluded: true,
    selection_frozen_before_research: true,
  };
}
