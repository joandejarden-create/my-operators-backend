/**
 * Deterministic Presentation slot resolution for duplicate Brand+Slot rows.
 * Contract: BE_FACTORY_DUPLICATE_SLOT_RESOLUTION_V1
 *
 * Never auto-deletes. Selects a primary record; flags conflicts for review.
 */
export const BE_FACTORY_DUPLICATE_SLOT_RESOLUTION_V1 =
  "BE_FACTORY_DUPLICATE_SLOT_RESOLUTION_V1";

function scoreRow(row = {}) {
  let score = 0;
  if (row.hasAttachment) score += 40;
  if (row.technicallyValid) score += 30;
  if (row.exactBrandOk) score += 20;
  if (row.browserOk) score += 10;
  if (row.rightsOk) score += 5;
  // Prefer lower record id stability when tied — Airtable rec ids are opaque;
  // use createdTime if present else recordId lexical.
  return score;
}

/**
 * @param {Array<{recordId:string,hasAttachment?:boolean,technicallyValid?:boolean,exactBrandOk?:boolean,browserOk?:boolean,rightsOk?:boolean,createdTime?:string}>} rows
 */
export function resolveDuplicateSlotRows(rows = []) {
  const list = Array.isArray(rows) ? rows.filter((r) => r && r.recordId) : [];
  if (list.length <= 1) {
    return {
      contract: BE_FACTORY_DUPLICATE_SLOT_RESOLUTION_V1,
      conflict: false,
      primaryRecordId: list[0]?.recordId || null,
      secondaryRecordIds: [],
      resolution: "SINGLE_OR_EMPTY",
      reviewRequired: false,
    };
  }
  const ranked = [...list].sort((a, b) => {
    const ds = scoreRow(b) - scoreRow(a);
    if (ds !== 0) return ds;
    const ta = a.createdTime || "";
    const tb = b.createdTime || "";
    if (ta && tb && ta !== tb) return ta < tb ? -1 : 1;
    return String(a.recordId).localeCompare(String(b.recordId));
  });
  const primary = ranked[0];
  const secondary = ranked.slice(1).map((r) => r.recordId);
  const scores = ranked.map((r) => scoreRow(r));
  const tied = scores.length > 1 && scores[0] === scores[1];
  return {
    contract: BE_FACTORY_DUPLICATE_SLOT_RESOLUTION_V1,
    conflict: true,
    primaryRecordId: primary.recordId,
    secondaryRecordIds: secondary,
    resolution: tied ? "TIE_LEXICAL_PRIMARY" : "SCORE_PRIMARY",
    reviewRequired: tied || secondary.length > 0,
    rankedRecordIds: ranked.map((r) => r.recordId),
    note: "Do not delete secondary rows automatically — flag for founder/IC review.",
  };
}

export default {
  BE_FACTORY_DUPLICATE_SLOT_RESOLUTION_V1,
  resolveDuplicateSlotRows,
};
