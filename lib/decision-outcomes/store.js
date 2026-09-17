/**
 * Filesystem persistence for Decision & Outcome layer.
 * Follows GDI pilot pattern: data/decision-outcomes/hotels/{hotelId}/
 * Decisions are mutable metadata; events are append-only (never rewritten).
 */

import fs from "node:fs";
import path from "node:path";
import { createHash, randomBytes } from "node:crypto";
import { fileURLToPath } from "node:url";
import { SCHEMA_VERSION } from "./types.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../data/decision-outcomes");

export function getDecisionOutcomesRoot() {
  return ROOT;
}

export function hotelOutcomesDir(hotelId) {
  const id = String(hotelId || "").trim();
  if (!id) throw new Error("hotelId_required");
  return path.join(ROOT, "hotels", id);
}

function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
}

function readJson(filePath, fallback = null) {
  if (!fs.existsSync(filePath)) return fallback;
  return JSON.parse(fs.readFileSync(filePath, "utf8"));
}

function writeJson(filePath, data) {
  ensureDir(path.dirname(filePath));
  fs.writeFileSync(filePath, JSON.stringify(data, null, 2) + "\n", "utf8");
}

export function createDecisionId(prefix = "dec") {
  return `${prefix}_${Date.now().toString(36)}_${randomBytes(4).toString("hex")}`;
}

export function createEventId(prefix = "evt") {
  return `${prefix}_${Date.now().toString(36)}_${randomBytes(3).toString("hex")}`;
}

/** Deterministic idempotency key for Decision creation. */
export function buildDecisionIdempotencyKey({
  hotelId,
  productModule,
  decisionType,
  subjectId,
  recommendationVersion = 1,
} = {}) {
  const raw = [
    String(hotelId || "").trim(),
    String(productModule || "").trim(),
    String(decisionType || "").trim(),
    String(subjectId || "").trim(),
    String(recommendationVersion || 1),
  ].join("|");
  return createHash("sha256").update(raw).digest("hex").slice(0, 24);
}

function decisionsDir(hotelId) {
  return path.join(hotelOutcomesDir(hotelId), "decisions");
}

function eventsPath(hotelId, decisionId) {
  return path.join(hotelOutcomesDir(hotelId), "events", `${decisionId}.jsonl`);
}

function indexPath(hotelId) {
  return path.join(hotelOutcomesDir(hotelId), "index.json");
}

function loadIndex(hotelId) {
  return readJson(indexPath(hotelId), {
    hotelId,
    schemaVersion: SCHEMA_VERSION,
    decisions: [],
    updatedAt: null,
  });
}

function saveIndex(hotelId, index) {
  writeJson(indexPath(hotelId), {
    ...index,
    hotelId,
    schemaVersion: SCHEMA_VERSION,
    updatedAt: new Date().toISOString(),
  });
}

export function loadDecision(hotelId, decisionId) {
  const p = path.join(decisionsDir(hotelId), `${decisionId}.json`);
  return readJson(p, null);
}

export function findDecisionByIdempotencyKey(hotelId, idempotencyKey) {
  const index = loadIndex(hotelId);
  const hit = (index.decisions || []).find((d) => d.idempotencyKey === idempotencyKey);
  if (!hit) return null;
  return loadDecision(hotelId, hit.decisionId);
}

export function findDecisionBySubject(hotelId, { productModule, subjectId, decisionType = null } = {}) {
  const index = loadIndex(hotelId);
  const hits = (index.decisions || []).filter((d) => {
    if (d.productModule !== productModule) return false;
    if (d.subjectId !== subjectId) return false;
    if (decisionType && d.decisionType !== decisionType) return false;
    return true;
  });
  if (!hits.length) return null;
  // Prefer highest recommendationVersion
  hits.sort((a, b) => (b.recommendationVersion || 1) - (a.recommendationVersion || 1));
  return loadDecision(hotelId, hits[0].decisionId);
}

export function saveDecisionRecord(decision) {
  const hotelId = decision.hotelId;
  const decisionId = decision.decisionId;
  if (!hotelId || !decisionId) throw new Error("decision_hotel_and_id_required");
  const p = path.join(decisionsDir(hotelId), `${decisionId}.json`);
  writeJson(p, decision);

  const index = loadIndex(hotelId);
  const summary = {
    decisionId,
    idempotencyKey: decision.idempotencyKey,
    productModule: decision.productModule,
    decisionType: decision.decisionType,
    subjectType: decision.subjectType,
    subjectId: decision.subjectId,
    recommendationVersion: decision.recommendationVersion || 1,
    decisionStatus: decision.decisionStatus,
    createdAt: decision.createdAt,
    updatedAt: decision.updatedAt,
  };
  const idx = (index.decisions || []).findIndex((d) => d.decisionId === decisionId);
  if (idx >= 0) index.decisions[idx] = summary;
  else index.decisions.push(summary);
  saveIndex(hotelId, index);
  return decision;
}

export function listDecisionSummaries(hotelId) {
  return loadIndex(hotelId).decisions || [];
}

export function listHotelsWithDecisions() {
  const hotelsRoot = path.join(ROOT, "hotels");
  if (!fs.existsSync(hotelsRoot)) return [];
  return fs
    .readdirSync(hotelsRoot)
    .filter((id) => fs.existsSync(path.join(hotelsRoot, id, "index.json")));
}

/** Append-only event write. Never mutates prior lines. */
export function appendEvent(hotelId, decisionId, event) {
  const p = eventsPath(hotelId, decisionId);
  ensureDir(path.dirname(p));
  const line = JSON.stringify(event) + "\n";
  fs.appendFileSync(p, line, "utf8");
  return event;
}

export function loadEvents(hotelId, decisionId) {
  const p = eventsPath(hotelId, decisionId);
  if (!fs.existsSync(p)) return [];
  const text = fs.readFileSync(p, "utf8");
  return text
    .split("\n")
    .map((l) => l.trim())
    .filter(Boolean)
    .map((l) => {
      try {
        return JSON.parse(l);
      } catch {
        return null;
      }
    })
    .filter(Boolean);
}

export function assertHotelBoundary(requestedHotelId, resourceHotelId) {
  if (String(requestedHotelId) !== String(resourceHotelId)) {
    const err = new Error("hotel_boundary_violation");
    err.code = "hotel_boundary_violation";
    throw err;
  }
}
