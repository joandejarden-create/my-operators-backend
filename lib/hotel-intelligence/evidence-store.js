/**
 * Field-level evidence store (local JSON). Preserves agreeing + conflicting observations.
 */

import { createLocalStore } from "./local-store.js";
import { scoreFieldConfidence, preferCanonicalValue } from "./confidence.js";

export const EVIDENCE_STORE_VERSION = "hotel-intelligence-evidence-store-v1";

/**
 * @param {ReturnType<typeof createLocalStore>} [store]
 */
export function createEvidenceStore(store = createLocalStore()) {
  function listAll() {
    return store.readEvidence().items || [];
  }

  function listForHotel(hotelId, field = null) {
    const hid = String(hotelId || "").trim();
    const f = field != null ? String(field).trim() : null;
    return listAll().filter(
      (e) => e.hotel_id === hid && (!f || e.field === f)
    );
  }

  /**
   * Append a field observation. Does not overwrite prior sources.
   */
  function addEvidence(entry) {
    const hotelId = String(entry.hotel_id || "").trim();
    const field = String(entry.field || "").trim();
    const source = String(entry.source || "").trim().toLowerCase();
    if (!hotelId || !field || !source) {
      throw new Error("evidence_requires_hotel_id_field_source");
    }
    const scored =
      entry.confidence != null
        ? {
            confidence: Number(entry.confidence),
            explanation: entry.explanation || "provided",
          }
        : scoreFieldConfidence(field, source, {
            completeness: entry.completeness,
          });

    const item = {
      hotel_id: hotelId,
      field,
      value: entry.value,
      source,
      source_record_id: entry.source_record_id || null,
      observed_at: entry.observed_at || new Date().toISOString().slice(0, 10),
      confidence: scored.confidence,
      explanation: scored.explanation || scored.tier,
      created_at: new Date().toISOString(),
    };

    const db = store.readEvidence();
    db.items = [...(db.items || []), item];
    store.writeEvidence(db);
    return item;
  }

  function summarizeHotel(hotelId) {
    const items = listForHotel(hotelId);
    const byField = new Map();
    for (const item of items) {
      if (!byField.has(item.field)) byField.set(item.field, []);
      byField.get(item.field).push(item);
    }
    const fields = [];
    const conflicts = [];
    for (const [field, rows] of byField) {
      const pref = preferCanonicalValue(rows);
      fields.push({
        field,
        preferred: pref.preferred,
        observation_count: rows.length,
        conflict_count: pref.conflicts.length,
      });
      conflicts.push(...pref.conflicts);
    }
    return { hotel_id: hotelId, fields, conflicts, evidence_count: items.length };
  }

  return {
    version: EVIDENCE_STORE_VERSION,
    listAll,
    listForHotel,
    addEvidence,
    summarizeHotel,
  };
}
