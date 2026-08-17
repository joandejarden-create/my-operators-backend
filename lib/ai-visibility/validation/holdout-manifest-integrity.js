/**
 * Future holdout seal integrity — fail-closed uniqueness guards.
 * Does not mutate historical Holdout v1/v2 artifacts.
 */

/**
 * @param {object[]} cases
 * @returns {{
 *   ok: boolean,
 *   PAIR_N: number,
 *   UNIQUE_CASE_ID_COUNT: number,
 *   UNIQUE_ENTITY_RESPONSE_PAIR_COUNT: number,
 *   duplicateCaseIds: string[],
 *   duplicateEntityResponsePairs: string[],
 *   NO_DUPLICATE_MANIFEST_ROWS: boolean,
 *   errors: string[],
 * }}
 */
export function validateHoldoutManifestIntegrity(cases) {
  const rows = Array.isArray(cases) ? cases : [];
  const PAIR_N = rows.length;
  const caseIdCounts = new Map();
  const pairCounts = new Map();
  const errors = [];

  for (const c of rows) {
    const caseId = c?.caseId;
    if (!caseId) {
      errors.push("MISSING_CASE_ID");
      continue;
    }
    caseIdCounts.set(caseId, (caseIdCounts.get(caseId) || 0) + 1);

    const entityId = c.canonicalEntityId || c.entityId || "";
    const responseId = c.sourceResponseId || c.responseId || "";
    const pairKey = `${entityId}::${responseId}`;
    if (!entityId || !responseId) {
      errors.push(`MISSING_ENTITY_OR_RESPONSE:${caseId}`);
    } else {
      pairCounts.set(pairKey, (pairCounts.get(pairKey) || 0) + 1);
    }
  }

  const duplicateCaseIds = [...caseIdCounts.entries()]
    .filter(([, n]) => n > 1)
    .map(([id]) => id)
    .sort();
  const duplicateEntityResponsePairs = [...pairCounts.entries()]
    .filter(([, n]) => n > 1)
    .map(([id]) => id)
    .sort();

  const UNIQUE_CASE_ID_COUNT = caseIdCounts.size;
  const UNIQUE_ENTITY_RESPONSE_PAIR_COUNT = pairCounts.size;
  const NO_DUPLICATE_MANIFEST_ROWS =
    duplicateCaseIds.length === 0 && duplicateEntityResponsePairs.length === 0;

  if (duplicateCaseIds.length > 0) {
    errors.push(`DUPLICATE_CASE_ID:${duplicateCaseIds.join(",")}`);
  }
  if (duplicateEntityResponsePairs.length > 0) {
    errors.push(`DUPLICATE_ENTITY_RESPONSE_PAIR:${duplicateEntityResponsePairs.join(",")}`);
  }
  if (UNIQUE_CASE_ID_COUNT !== PAIR_N) {
    errors.push(`UNIQUE_CASE_ID_COUNT_NE_PAIR_N:${UNIQUE_CASE_ID_COUNT}!=${PAIR_N}`);
  }
  if (UNIQUE_ENTITY_RESPONSE_PAIR_COUNT !== PAIR_N && PAIR_N > 0) {
    // Only enforce when every row had entity+response
    const complete = rows.every(
      (c) => (c?.canonicalEntityId || c?.entityId) && (c?.sourceResponseId || c?.responseId)
    );
    if (complete) {
      errors.push(
        `UNIQUE_ENTITY_RESPONSE_PAIR_COUNT_NE_PAIR_N:${UNIQUE_ENTITY_RESPONSE_PAIR_COUNT}!=${PAIR_N}`
      );
    }
  }

  const ok =
    PAIR_N === 0 ||
    (NO_DUPLICATE_MANIFEST_ROWS &&
      UNIQUE_CASE_ID_COUNT === PAIR_N &&
      UNIQUE_ENTITY_RESPONSE_PAIR_COUNT === PAIR_N &&
      errors.filter((e) => e !== "MISSING_CASE_ID").length === 0);

  return {
    ok,
    DO_NOT_SEAL: !ok,
    PAIR_N,
    UNIQUE_CASE_ID_COUNT,
    UNIQUE_ENTITY_RESPONSE_PAIR_COUNT,
    duplicateCaseIds,
    duplicateEntityResponsePairs,
    NO_DUPLICATE_MANIFEST_ROWS,
    errors,
    FUTURE_HOLDOUT_SEAL_RULES: [
      "UNIQUE_CASE_ID_COUNT == PAIR_N",
      "UNIQUE_ENTITY_RESPONSE_PAIR_COUNT == PAIR_N",
      "NO_DUPLICATE_MANIFEST_ROWS",
      "each caseId appears exactly once",
      "if duplicate caseId > 0: DO_NOT_SEAL",
      "if duplicate entity-response pair > 0: DO_NOT_SEAL",
    ],
  };
}

/**
 * Human label wins over candidateType (fixes Holdout v2 double-bucket bug).
 * @param {object} c
 * @returns {"PRESENT"|"NOT_PRESENT"|null}
 */
export function resolvePresenceSelectionLabel(c) {
  if (c?.humanLabel === "PRESENT" || c?.humanLabel === "NOT_PRESENT") return c.humanLabel;
  if (c?.humanFinalLabel === "PRESENT" || c?.humanFinalLabel === "NOT_PRESENT") {
    return c.humanFinalLabel;
  }
  if (c?.candidateType === "PRESENCE_TRUE") return "PRESENT";
  if (c?.candidateType === "PRESENCE_FALSE") return "NOT_PRESENT";
  return null;
}

/**
 * Deduplicate selection rows by caseId (first wins).
 * @param {object[]} rows
 */
export function dedupeHoldoutSelectionByCaseId(rows) {
  const seen = new Set();
  const out = [];
  for (const row of rows || []) {
    const id = row?.caseId;
    if (!id || seen.has(id)) continue;
    seen.add(id);
    out.push(row);
  }
  return out;
}
