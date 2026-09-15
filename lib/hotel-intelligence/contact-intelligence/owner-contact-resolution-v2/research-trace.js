/**
 * Structured research trace + teacher-loop markers.
 */

import crypto from "node:crypto";

export function createResearchTrace({
  owner_entity_id,
  hotel_context_ids = [],
  country = null,
} = {}) {
  return {
    run_id: `ocr2_${crypto.randomBytes(6).toString("hex")}`,
    owner_entity_id,
    hotel_context_ids: [...hotel_context_ids],
    country,
    methods_attempted: [],
    queries: [],
    sources_opened: [],
    entities_discovered: [],
    contacts_considered: [],
    successful_path: [],
    final_contacts: [],
    cost_usd: 0,
    cost_breakdown: {},
    elapsed_ms: 0,
    result_status: null,
    TEACHER_SUCCESS: false,
    webhound: null,
    started_at: new Date().toISOString(),
  };
}

export function finalizeTrace(trace, { result_status, final_contacts = [], elapsed_ms = 0 } = {}) {
  trace.result_status = result_status;
  trace.final_contacts = final_contacts;
  trace.elapsed_ms = elapsed_ms;
  trace.finished_at = new Date().toISOString();
  return trace;
}
