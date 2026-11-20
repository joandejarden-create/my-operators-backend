/**
 * DISABLED MODULE — FOUNDER CORRECTION 2026-11-20
 *
 * Hotel Census must not be reconciled into HPC.
 * All exports throw or return DEPRECATED markers.
 * Historical audit artifacts remain on disk but must not feed pipelines.
 */

export const DEALALITY_LEGACY_HPC_RECONCILIATION_V1 = "DEALALITY_LEGACY_HPC_RECONCILIATION_V1_DISABLED";
export const LEGACY_RECONCILIATION_DISABLED = true;
export const DEPRECATION_BANNER = "DEPRECATED — PROHIBITED LEGACY DATA SOURCE";

export const RECON_DISPOSITION = Object.freeze({
  MATCHED_HPC_VERIFIED: "MATCHED_HPC_VERIFIED",
  MATCHED_HPC_ALIAS: "MATCHED_HPC_ALIAS",
  HPC_CREATE_CANDIDATE: "HPC_CREATE_CANDIDATE",
  AMBIGUOUS_MATCH: "AMBIGUOUS_MATCH",
  DUPLICATE_LEGACY_RECORD: "DUPLICATE_LEGACY_RECORD",
  HISTORICAL_OR_INACTIVE: "HISTORICAL_OR_INACTIVE",
  REQUIRES_EVIDENCE: "REQUIRES_EVIDENCE",
  NOT_APPLICABLE: "NOT_APPLICABLE",
  PROHIBITED: "PROHIBITED_LEGACY_DATA_SOURCE",
});

function prohibited(fn) {
  const err = new Error(
    `${DEPRECATION_BANNER}: ${fn} is disabled. Do not read or reconcile Hotel Census into HPC.`
  );
  err.code = "LEGACY_RECONCILIATION_DISABLED";
  throw err;
}

export async function fetchAllLegacyHotelCensusRecords() {
  prohibited("fetchAllLegacyHotelCensusRecords");
}

export async function classifyLegacyCensusRecord() {
  prohibited("classifyLegacyCensusRecord");
}

export function markDuplicateLegacyMatches() {
  prohibited("markDuplicateLegacyMatches");
}

export function summarizeReconciliation() {
  return {
    contract: DEALALITY_LEGACY_HPC_RECONCILIATION_V1,
    disabled: true,
    banner: DEPRECATION_BANNER,
    total: 0,
    byDisposition: { [RECON_DISPOSITION.PROHIBITED]: 0 },
    matched: 0,
    createCandidates: 0,
    ambiguous: 0,
    unresolved: 0,
  };
}
