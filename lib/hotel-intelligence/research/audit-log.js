/**
 * Packet 2.6C-R2 — audit log for paid / live research runs.
 * Never stores WEBHOUND_KEY or provider credentials.
 */

import path from "node:path";
import { resolveDataRoot, ensureDir, readJsonFile, writeJsonFile } from "../local-store.js";

function auditPath(env) {
  const root = path.join(resolveDataRoot(env), "research");
  ensureDir(root);
  return path.join(root, "audit-log.json");
}

export function appendResearchAuditEvent(event = {}, env = process.env) {
  const file = auditPath(env);
  const doc = readJsonFile(file, {
    version: "hi-research-audit-v1",
    events: [],
  });
  const row = {
    at: new Date().toISOString(),
    event: String(event.event || "unknown"),
    triggered_by: event.triggered_by || event.requested_by || null,
    hotel_id: event.hotel_id || null,
    template_id: event.template_id || null,
    template_version: event.template_version || null,
    request_id: event.request_id || null,
    run_id: event.run_id || null,
    provider: event.provider || null,
    provider_run_id: event.provider_run_id || null,
    budget_usd: event.budget_usd != null ? Number(event.budget_usd) : null,
    actual_cost_usd: event.actual_cost_usd != null ? Number(event.actual_cost_usd) : null,
    status: event.status || null,
    report_id: event.report_id || null,
    error_code: event.error_code || null,
    note: event.note ? String(event.note).slice(0, 240) : null,
  };
  doc.events.push(row);
  // Keep last 2000 events
  if (doc.events.length > 2000) doc.events = doc.events.slice(-2000);
  doc.updated_at = row.at;
  writeJsonFile(file, doc);
  return row;
}

export function listResearchAuditEvents(env = process.env, limit = 100) {
  const doc = readJsonFile(auditPath(env), { events: [] });
  return (doc.events || []).slice(-limit).reverse();
}
