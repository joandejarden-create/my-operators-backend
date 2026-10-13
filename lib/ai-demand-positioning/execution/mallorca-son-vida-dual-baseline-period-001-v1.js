/**
 * Mallorca Son Vida dual hotels — thin instance configs over shared first-baseline executor.
 * Castillo (PMILC) and Sheraton Arabella Golf (PMISI) remain independent subjects.
 */

import { MALLORCA_SON_VIDA_ENTITY_VERSION } from "../metrics/mallorca-son-vida-entity-registry.js";
import {
  buildSharedFirstPropertyPreflight,
  executeSharedFirstPropertyBaselinePeriod001,
} from "./shared-first-property-baseline-period-001-v1.js";

export const CASTILLO_PROPERTY_ID = "adp_castillo_hotel_son_vida";
export const SHERATON_PROPERTY_ID = "adp_sheraton_mallorca_arabella_golf";

/** Populated after census shell apply — set via bindCensusIds(). */
export const MALLORCA_DUAL_BINDINGS = {
  castillo: {
    propertyId: CASTILLO_PROPERTY_ID,
    censusRecordId: "rec82D9zpB8fede1I",
    baselineMarker: "ADP_CASTILLO_HOTEL_SON_VIDA_BASELINE_PERIOD_001",
    entityVersion: MALLORCA_SON_VIDA_ENTITY_VERSION,
    costCapUsd: 12,
    certifyTrigger: "castillo_hotel_son_vida_baseline_period_001",
    reportFileName: "adp-castillo-hotel-son-vida-baseline-period-001-run.json",
  },
  sheraton: {
    propertyId: SHERATON_PROPERTY_ID,
    censusRecordId: "recjhQdAUiSyxfCqE",
    baselineMarker: "ADP_SHERATON_MALLORCA_ARABELLA_GOLF_BASELINE_PERIOD_001",
    entityVersion: MALLORCA_SON_VIDA_ENTITY_VERSION,
    costCapUsd: 12,
    certifyTrigger: "sheraton_mallorca_arabella_golf_baseline_period_001",
    reportFileName: "adp-sheraton-mallorca-arabella-golf-baseline-period-001-run.json",
  },
};

export function bindCensusIds({ castilloCensusId, sheratonCensusId }) {
  if (castilloCensusId) MALLORCA_DUAL_BINDINGS.castillo.censusRecordId = castilloCensusId;
  if (sheratonCensusId) MALLORCA_DUAL_BINDINGS.sheraton.censusRecordId = sheratonCensusId;
  return MALLORCA_DUAL_BINDINGS;
}

export function buildCastilloSonVidaPreflight() {
  return buildSharedFirstPropertyPreflight(MALLORCA_DUAL_BINDINGS.castillo);
}

export function buildSheratonMallorcaArabellaPreflight() {
  return buildSharedFirstPropertyPreflight(MALLORCA_DUAL_BINDINGS.sheraton);
}

export function executeCastilloSonVidaBaselinePeriod001(opts = {}) {
  return executeSharedFirstPropertyBaselinePeriod001(MALLORCA_DUAL_BINDINGS.castillo, opts);
}

export function executeSheratonMallorcaArabellaBaselinePeriod001(opts = {}) {
  return executeSharedFirstPropertyBaselinePeriod001(MALLORCA_DUAL_BINDINGS.sheraton, opts);
}
