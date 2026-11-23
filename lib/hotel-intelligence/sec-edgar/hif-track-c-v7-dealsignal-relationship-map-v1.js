/**
 * Track C V7 — DealSignal relationship repair map (documentation + resolvers).
 * CUSTOMER_BLOCKED. No Airtable writes. No HPC merges.
 *
 * Root cause (Monterrey / Puebla):
 * Track C invented finding IDs that were never emitted by Track A CALA Discovery V1.
 * Hotel identity inventory + HPC links are valid; DealSignal Finding rows do not exist
 * for those synthetic IDs. Navigation must use the parent FINN internalization finding
 * for commercial evidence, and disc_* → HPC for hotel identity.
 */

export const HIF_TRACK_C_V7_MAP_VERSION = "1.0.0";

/** Authoritative Track A CALA Discovery V1 finding IDs (live corpus). */
export const TRACK_A_CALA_V1_FINDING_IDS = Object.freeze([
  "find_ds_cala_v1_finn_internalization_franchise_2026",
  "find_ds_cala_v1_finn_courtyard_pdc_committed_2026",
  "find_ds_cala_v1_fiho_yucatan_resort_lease_2026",
  "find_ds_cala_v1_cabo_rojo_puntacana_committed_2026",
  "find_ds_cala_v1_santander_concession_watch_2026",
  "find_ds_cala_v1_br_fii_hotel_context_2026",
]);

/**
 * Synthetic finding IDs invented by Track C identity script — NOT DealSignal corpus.
 * Do not create duplicate Finding rows solely to make these resolve.
 */
export const TRACK_C_SYNTHETIC_FINDING_IDS = Object.freeze([
  "find_ds_cala_v1_finn_hi_monterrey_valle_2026",
  "find_ds_cala_v1_finn_hi_puebla_la_noria_2026",
]);

export const PARENT_FINN_INTERNALIZATION_FINDING_ID =
  "find_ds_cala_v1_finn_internalization_franchise_2026";

/**
 * Hotel identity + navigation map for the four verified Mexico hotels.
 * canonicalHpcRecordId = current brand identity for HE deep-link.
 */
export const VERIFIED_MEXICO_HOTELS_V7 = Object.freeze([
  {
    label: "Courtyard by Marriott Playa del Carmen",
    discoveryPropertyId: "disc_ds_cala_v1_finn_courtyard_pdc_2026",
    dealsignalFindingId: "find_ds_cala_v1_finn_courtyard_pdc_committed_2026",
    findingAlignment: "TRACK_A_CORPUS",
    canonicalHpcRecordId: "rechcqksn3VfFD3c6",
    historicalSiblingHpcRecordId: "recf65FZ1FuVVqIrz",
    historicalBrand: "Wyndham Garden",
    currentBrand: "Courtyard by Marriott",
    owner: "Fibra Inn (FINN13 / Trust DB/1616)",
    operator: "Servicios Hoteleros FINN",
    stewardship:
      "CANONICAL_CURRENT=Courtyard HPC; HISTORICAL_ALIAS=Wyndham Garden HPC; no merge without founder approval",
  },
  {
    label: "The Yucatán Resort Playa del Carmen",
    discoveryPropertyId: "disc_ds_cala_v1_fiho_yucatan_resort_2026",
    dealsignalFindingId: "find_ds_cala_v1_fiho_yucatan_resort_lease_2026",
    findingAlignment: "TRACK_A_CORPUS",
    canonicalHpcRecordId: "recY9Cxxo1iAbJgOt",
    historicalSiblingHpcRecordId: null,
    currentBrand: "Tapestry Collection by Hilton",
    owner: "FibraHotel (FIHO 12 / Trust F/1596)",
    operator: null,
    operatorGap: "Lease counterparty after 2026-04-30 suspension not named in tested disclosures",
    stewardship: "SINGLE_HPC_MATCHED",
  },
  {
    label: "Holiday Inn Monterrey Valle",
    discoveryPropertyId: "disc_ds_cala_v1_finn_hi_monterrey_valle_2026",
    /** Synthetic — not in Track A corpus; use parent for DealSignal card evidence */
    dealsignalFindingId: null,
    syntheticFindingIdNote: "find_ds_cala_v1_finn_hi_monterrey_valle_2026",
    dealsignalEvidenceFindingId: PARENT_FINN_INTERNALIZATION_FINDING_ID,
    findingAlignment: "SYNTHETIC_REDIRECT_TO_PARENT",
    canonicalHpcRecordId: "recJRNuLr8fEiGImt",
    currentBrand: "Holiday Inn",
    owner: "Fibra Inn (FINN13 / Trust DB/1616)",
    operator: "Servicios Hoteleros FINN",
    stewardship: "IDENTITY_VIA_INVENTORY_HPC; COMMERCIAL_EVIDENCE_VIA_PARENT_FINDING",
  },
  {
    label: "Holiday Inn Puebla La Noria",
    discoveryPropertyId: "disc_ds_cala_v1_finn_hi_puebla_la_noria_2026",
    dealsignalFindingId: null,
    syntheticFindingIdNote: "find_ds_cala_v1_finn_hi_puebla_la_noria_2026",
    dealsignalEvidenceFindingId: PARENT_FINN_INTERNALIZATION_FINDING_ID,
    findingAlignment: "SYNTHETIC_REDIRECT_TO_PARENT",
    canonicalHpcRecordId: "recfpT5RJBZAORXs2",
    currentBrand: "Holiday Inn",
    owner: "Fibra Inn (FINN13 / Trust DB/1616)",
    operator: "Servicios Hoteleros FINN",
    stewardship: "IDENTITY_VIA_INVENTORY_HPC; COMMERCIAL_EVIDENCE_VIA_PARENT_FINDING",
  },
]);

export const DEFERRED_ENTITIES_V7 = Object.freeze([
  {
    label: "Cabo Rojo destination",
    discoveryPropertyId: "disc_ds_cala_v1_cabo_rojo_puntacana_2026",
    findingId: "find_ds_cala_v1_cabo_rojo_puntacana_committed_2026",
    status: "PARENT_ONLY",
    evidenceRequired: "Named individual hotels within destination",
  },
  {
    label: "Santander Bucaramanga concesión",
    discoveryPropertyId: "disc_ds_cala_v1_santander_concession_watch_2026",
    findingId: "find_ds_cala_v1_santander_concession_watch_2026",
    status: "UNRESOLVED_HOTEL_IDENTITY",
    evidenceRequired: "Official SECOP II pliego confirming hotel use",
  },
  {
    label: "Fibra Inn internalization portfolio",
    discoveryPropertyId: "disc_ds_cala_v1_finn_internalization_parent_2026",
    findingId: PARENT_FINN_INTERNALIZATION_FINDING_ID,
    status: "PARENT_ONLY",
    evidenceRequired: "None for hotel identity — keep portfolio separate",
  },
]);

/**
 * Resolve a claimed finding ID for DealSignal navigation.
 * Synthetic Monterrey/Puebla IDs redirect to parent FINN finding (no duplicate create).
 */
export function resolveDealSignalFindingId(claimedFindingId) {
  const id = String(claimedFindingId || "").trim();
  if (!id) {
    return { ok: false, reason: "EMPTY_FINDING_ID" };
  }
  if (TRACK_C_SYNTHETIC_FINDING_IDS.includes(id)) {
    return {
      ok: true,
      claimedFindingId: id,
      resolvedFindingId: PARENT_FINN_INTERNALIZATION_FINDING_ID,
      resolution: "SYNTHETIC_TO_PARENT",
      createFinding: false,
    };
  }
  if (TRACK_A_CALA_V1_FINDING_IDS.includes(id)) {
    return {
      ok: true,
      claimedFindingId: id,
      resolvedFindingId: id,
      resolution: "CORPUS_DIRECT",
      createFinding: false,
    };
  }
  return {
    ok: false,
    claimedFindingId: id,
    reason: "UNKNOWN_FINDING_ID",
    createFinding: false,
  };
}

/**
 * Courtyard / Wyndham stewardship recommendation (no merge).
 */
export function getCourtyardWyndhamStewardship() {
  return {
    physicalProperty: "Playacar Fase II — Avenida Aviación Lote 5 y 6 Manzana 29, Playa del Carmen",
    canonicalCurrentHpc: "rechcqksn3VfFD3c6",
    canonicalCurrentBrand: "Courtyard by Marriott",
    historicalHpc: "recf65FZ1FuVVqIrz",
    historicalBrand: "Wyndham Garden",
    relationship: "SAME_PHYSICAL_PROPERTY_BRAND_CONVERSION",
    recommendedAction: "STEWARD_ALIAS_DOCUMENTATION",
    forbiddenWithoutFounderApproval: ["MERGE_HPC", "DELETE_HPC", "OVERWRITE_HISTORICAL_BRAND_ROW"],
    heDeepLinkPreferred: "rechcqksn3VfFD3c6",
  };
}

export function listHeDeepLinkTargets() {
  return VERIFIED_MEXICO_HOTELS_V7.map((h) => ({
    label: h.label,
    hpcRecordId: h.canonicalHpcRecordId,
    deepLinkTarget: `http://localhost:8080/app#/opportunity-radar (open HE by HPC ${h.canonicalHpcRecordId})`,
    researchApiPath: `/api/hotel-intelligence/hotels/${h.canonicalHpcRecordId}/research`,
  }));
}
