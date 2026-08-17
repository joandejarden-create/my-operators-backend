/**
 * Machine-readable review queue (local JSON). No UI in V1.
 */

import { createLocalStore } from "./local-store.js";
import { randomBytes } from "node:crypto";

export const REVIEW_QUEUE_VERSION = "hotel-intelligence-review-queue-v1";

export const ISSUE_TYPES = Object.freeze({
  POSSIBLE_DUPLICATE: "possible_duplicate",
  IDENTITY_AMBIGUOUS: "identity_ambiguous",
  ROOM_COUNT_CONFLICT: "room_count_conflict",
  BRAND_CONFLICT: "brand_conflict",
  LOCATION_CONFLICT: "location_conflict",
  MISSING_ROOM_COUNT: "missing_room_count",
  MISSING_COORDINATES: "missing_coordinates",
  PROVIDER_MISMATCH: "provider_mismatch",
});

export const SEVERITY = Object.freeze({
  LOW: "low",
  MEDIUM: "medium",
  HIGH: "high",
});

function newId() {
  return `rev_${randomBytes(8).toString("hex")}`;
}

/**
 * @param {ReturnType<typeof createLocalStore>} [store]
 */
export function createReviewQueue(store = createLocalStore()) {
  function list(filters = {}) {
    let items = store.readReviewQueue().items || [];
    if (filters.status) {
      items = items.filter((i) => i.status === filters.status);
    }
    if (filters.issue_type) {
      items = items.filter((i) => i.issue_type === filters.issue_type);
    }
    if (filters.hotel_id) {
      items = items.filter((i) => i.hotel_id === filters.hotel_id);
    }
    return items;
  }

  function enqueue(item) {
    const row = {
      id: item.id || newId(),
      hotel_id: item.hotel_id || null,
      candidate: item.candidate || null,
      issue_type: item.issue_type,
      severity: item.severity || SEVERITY.MEDIUM,
      current_value: item.current_value ?? null,
      candidate_value: item.candidate_value ?? null,
      sources: item.sources || [],
      confidence: item.confidence ?? null,
      recommended_action: item.recommended_action || "manual_review",
      created_at: item.created_at || new Date().toISOString(),
      status: item.status || "open",
    };
    const db = store.readReviewQueue();
    const dup = (db.items || []).find(
      (i) =>
        i.status === "open" &&
        i.hotel_id === row.hotel_id &&
        i.issue_type === row.issue_type &&
        String(i.candidate_value) === String(row.candidate_value)
    );
    if (dup) return dup;
    db.items = [...(db.items || []), row];
    store.writeReviewQueue(db);
    return row;
  }

  function resolve(id, status = "resolved") {
    const db = store.readReviewQueue();
    const items = db.items || [];
    const idx = items.findIndex((i) => i.id === id);
    if (idx < 0) return null;
    items[idx] = {
      ...items[idx],
      status,
      resolved_at: new Date().toISOString(),
    };
    db.items = items;
    store.writeReviewQueue(db);
    return items[idx];
  }

  return {
    version: REVIEW_QUEUE_VERSION,
    ISSUE_TYPES,
    SEVERITY,
    list,
    enqueue,
    resolve,
  };
}

/**
 * Derive review items from identity + evidence conflicts.
 */
export function enqueueFromResolveResult(reviewQueue, resolveResult, opts = {}) {
  const created = [];
  if (!resolveResult) return created;
  if (resolveResult.match_status === "ambiguous" || resolveResult.review_required) {
    created.push(
      reviewQueue.enqueue({
        hotel_id: resolveResult.hotel_id,
        candidate: resolveResult.candidate_matches,
        issue_type:
          resolveResult.match_status === "ambiguous"
            ? ISSUE_TYPES.IDENTITY_AMBIGUOUS
            : ISSUE_TYPES.POSSIBLE_DUPLICATE,
        severity: SEVERITY.HIGH,
        recommended_action: "do_not_auto_merge",
        sources: opts.sources || [],
        confidence: resolveResult.match_score,
      })
    );
  }
  for (const c of opts.conflicts || []) {
    let issue = ISSUE_TYPES.PROVIDER_MISMATCH;
    if (c.field === "room_count") issue = ISSUE_TYPES.ROOM_COUNT_CONFLICT;
    else if (c.field === "brand_name") issue = ISSUE_TYPES.BRAND_CONFLICT;
    else if (c.field === "latitude" || c.field === "longitude" || c.field === "address_line_1") {
      issue = ISSUE_TYPES.LOCATION_CONFLICT;
    }
    created.push(
      reviewQueue.enqueue({
        hotel_id: opts.hotel_id || resolveResult.hotel_id,
        issue_type: issue,
        severity: SEVERITY.MEDIUM,
        current_value: c.preferred_value,
        candidate_value: c.alternate_value,
        sources: [c.preferred_source, c.alternate_source].filter(Boolean),
        confidence: c.alternate_confidence,
        recommended_action: "compare_sources",
      })
    );
  }
  return created;
}
