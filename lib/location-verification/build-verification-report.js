/**
 * Build Google pre-import verification report + clean Airtable-ready fixture.
 */

import { VERIFICATION_METHOD_LABEL } from "./verified-fixture-gating.js";

function stripSourceRow(record) {
  if (!record || typeof record !== "object") return record;
  const { sourceRow, _cacheHit, _cacheVerifiedAt, ...rest } = record;
  return rest;
}

/**
 * @param {object} input
 */
export function buildVerificationReportPayload(input) {
  const records = (input.records || []).map(stripSourceRow);
  const counts = {
    verifiedHighConfidence: 0,
    mediumConfidence: 0,
    lowConfidence: 0,
    ambiguous: 0,
    noMatch: 0,
    excludedRecords: input.excludedRecords ?? 0,
  };

  for (const row of records) {
    const status = String(row.verificationStatus || "");
    const confidence = String(row.matchConfidence || "");
    if (status === "Verified" && confidence === "High") counts.verifiedHighConfidence += 1;
    else if (confidence === "Medium") counts.mediumConfidence += 1;
    else if (status === "Ambiguous Match") counts.ambiguous += 1;
    else if (status === "No Match") counts.noMatch += 1;
    else if (confidence === "Low") counts.lowConfidence += 1;
    else if (status === "Needs Review" && confidence === "Medium") counts.mediumConfidence += 1;
    else counts.lowConfidence += 1;
  }

  return {
    verification: {
      method: VERIFICATION_METHOD_LABEL,
      verifiedAt: input.verifiedAt || new Date().toISOString(),
      country: input.country || "",
      inputFile: input.inputFile || "",
      candidateCount: input.candidateCount ?? records.length,
      verifiedHighConfidence: counts.verifiedHighConfidence,
      mediumConfidence: counts.mediumConfidence,
      lowConfidence: counts.lowConfidence,
      ambiguous: counts.ambiguous,
      noMatch: counts.noMatch,
      excludedRecords: counts.excludedRecords,
      cacheHits: input.cacheHits ?? 0,
      cacheMisses: input.cacheMisses ?? 0,
      apiRequestsMade: input.apiRequestsMade ?? 0,
      allowMedium: input.allowMedium === true,
      notes: input.notes || [],
    },
    records,
  };
}

/**
 * @param {object} input
 */
export function buildCleanVerifiedFixturePayload(input) {
  return {
    market: input.market || "",
    country: input.country || "",
    region: input.region || "",
    verification: {
      method: VERIFICATION_METHOD_LABEL,
      verifiedAt: input.verifiedAt || new Date().toISOString(),
      verifiedRecords: input.verifiedRecords ?? (input.points || []).length,
      manuallyVerifiedRecords: input.manuallyVerifiedRecords ?? 0,
      excludedRecords: input.excludedRecords ?? 0,
      requirement:
        input.requirement ||
        "High confidence Google match or explicit manual verification",
      notes:
        input.verificationNotes ||
        "Broad corridors/zones may use manual verification with official/public sources; Google metadata is not stored on points.",
    },
    points: input.points || [],
  };
}
