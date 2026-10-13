/**
 * Filesystem store for publication monitors (hotel-local).
 * Path: data/group-demand-intelligence/hotels/{hotelId}/publication-monitors.json
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { PUBLICATION_MONITOR_VERSION } from "./constants.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..", "..", "..", "data", "group-demand-intelligence", "hotels");

function monitorsPath(hotelId) {
  return path.join(ROOT, hotelId, "publication-monitors.json");
}

function auditPath(hotelId) {
  return path.join(ROOT, hotelId, "publication-monitor-audit.jsonl");
}

export function loadPublicationMonitors(hotelId) {
  const p = monitorsPath(hotelId);
  if (!fs.existsSync(p)) {
    return {
      hotelId,
      schemaVersion: PUBLICATION_MONITOR_VERSION,
      updatedAt: null,
      monitors: [],
    };
  }
  const doc = JSON.parse(fs.readFileSync(p, "utf8"));
  return {
    hotelId,
    schemaVersion: doc.schemaVersion || PUBLICATION_MONITOR_VERSION,
    updatedAt: doc.updatedAt || null,
    monitors: Array.isArray(doc.monitors) ? doc.monitors : [],
    note: doc.note || null,
  };
}

export function savePublicationMonitors(hotelId, monitors, meta = {}) {
  const dir = path.join(ROOT, hotelId);
  fs.mkdirSync(dir, { recursive: true });
  const doc = {
    hotelId,
    schemaVersion: PUBLICATION_MONITOR_VERSION,
    updatedAt: new Date().toISOString(),
    note:
      meta.note ||
      "Campaign publication-trigger monitors — known official sources only. Not customer campaigns.",
    monitors,
  };
  fs.writeFileSync(monitorsPath(hotelId), JSON.stringify(doc, null, 2), "utf8");
  return doc;
}

export function upsertPublicationMonitors(hotelId, incoming = [], opts = {}) {
  const existing = loadPublicationMonitors(hotelId);
  const byId = new Map(existing.monitors.map((m) => [m.monitorId, m]));
  const now = new Date().toISOString();
  for (const row of incoming) {
    const id = row.monitorId;
    if (!id) continue;
    const prev = byId.get(id) || {};
    byId.set(id, {
      ...prev,
      ...row,
      monitorId: id,
      hotelId,
      createdAt: prev.createdAt || row.createdAt || now,
      updatedAt: now,
      // Preserve entity tracking unless replaced
      previouslySeenEntityIds:
        row.previouslySeenEntityIds ?? prev.previouslySeenEntityIds ?? [],
      checkHistory: Array.isArray(row.checkHistory)
        ? row.checkHistory
        : prev.checkHistory || [],
    });
  }
  return savePublicationMonitors(hotelId, [...byId.values()], opts);
}

export function getMonitorById(hotelId, monitorId) {
  return loadPublicationMonitors(hotelId).monitors.find((m) => m.monitorId === monitorId) || null;
}

export function listActiveMonitors(hotelId) {
  return loadPublicationMonitors(hotelId).monitors.filter(
    (m) => m.monitoringStatus === "ACTIVE" || m.monitoringStatus === "TRIGGERED"
  );
}

export function appendMonitorAudit(hotelId, entry) {
  const dir = path.join(ROOT, hotelId);
  fs.mkdirSync(dir, { recursive: true });
  const line = JSON.stringify({
    ...entry,
    hotelId,
    at: entry.at || new Date().toISOString(),
  });
  fs.appendFileSync(auditPath(hotelId), line + "\n", "utf8");
}

export function readMonitorAuditTail(hotelId, limit = 50) {
  const p = auditPath(hotelId);
  if (!fs.existsSync(p)) return [];
  const lines = fs.readFileSync(p, "utf8").trim().split("\n").filter(Boolean);
  return lines.slice(-limit).map((l) => {
    try {
      return JSON.parse(l);
    } catch {
      return { raw: l };
    }
  });
}
