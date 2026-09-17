/**
 * Decision & Outcome persistence facade.
 *
 * Modes (DECISION_OUTCOMES_PERSISTENCE):
 *   airtable   — Airtable primary (default when configured)
 *   filesystem — local FS only (tests / offline)
 *
 * Mirror (DECISION_OUTCOMES_FS_MIRROR):
 *   default "1" when mode=airtable — write FS after successful Airtable write
 *   set "0" to disable mirror
 *
 * Fallback (DECISION_OUTCOMES_FS_FALLBACK):
 *   default "0" — do NOT silently save to FS when Airtable fails
 *   set "1" only for explicitly audited emergency recovery
 *
 * Read fallback (DECISION_OUTCOMES_READ_FS_FALLBACK):
 *   default "1" during migration — if Airtable miss, try FS
 *   set "0" once migration verified
 */

import * as fsStore from "./store.js";
import * as atStore from "./airtable-store.js";

export function getPersistenceMode() {
  const explicit = String(process.env.DECISION_OUTCOMES_PERSISTENCE || "")
    .trim()
    .toLowerCase();
  if (explicit === "filesystem" || explicit === "fs") return "filesystem";
  if (explicit === "airtable" || explicit === "at") return "airtable";
  // Auto: Airtable when configured, else filesystem (keeps unit tests offline).
  return atStore.isAirtableConfigured() ? "airtable" : "filesystem";
}

export function isFsMirrorEnabled() {
  if (getPersistenceMode() !== "airtable") return false;
  const v = String(process.env.DECISION_OUTCOMES_FS_MIRROR ?? "1").trim();
  return v !== "0" && v.toLowerCase() !== "false";
}

export function isFsWriteFallbackEnabled() {
  const v = String(process.env.DECISION_OUTCOMES_FS_FALLBACK || "0").trim();
  return v === "1" || v.toLowerCase() === "true";
}

export function isFsReadFallbackEnabled() {
  const v = String(process.env.DECISION_OUTCOMES_READ_FS_FALLBACK ?? "1").trim();
  return v !== "0" && v.toLowerCase() !== "false";
}

function mirrorDecision(decision) {
  if (!isFsMirrorEnabled()) return;
  try {
    fsStore.saveDecisionRecord(decision);
  } catch (err) {
    console.error(
      "[decision-outcomes] FS mirror decision failed",
      err?.message || err
    );
  }
}

function mirrorEvent(hotelId, decisionId, event) {
  if (!isFsMirrorEnabled()) return;
  try {
    fsStore.appendEvent(hotelId, decisionId, event);
  } catch (err) {
    console.error(
      "[decision-outcomes] FS mirror event failed",
      err?.message || err
    );
  }
}

export async function saveDecisionRecord(decision) {
  const mode = getPersistenceMode();
  if (mode === "filesystem") {
    return fsStore.saveDecisionRecord(decision);
  }
  try {
    const saved = await atStore.saveDecisionRecord(decision);
    mirrorDecision(decision);
    return saved;
  } catch (err) {
    if (isFsWriteFallbackEnabled()) {
      console.error(
        "[decision-outcomes] Airtable decision write failed; FS fallback enabled",
        err?.message || err
      );
      return fsStore.saveDecisionRecord(decision);
    }
    throw err;
  }
}

export async function appendEvent(hotelId, decisionId, event, meta = {}) {
  const mode = getPersistenceMode();
  if (mode === "filesystem") {
    fsStore.appendEvent(hotelId, decisionId, event);
    return { event, created: true };
  }
  try {
    const result = await atStore.appendEvent(hotelId, decisionId, event, meta);
    if (result.created) mirrorEvent(hotelId, decisionId, event);
    return result;
  } catch (err) {
    if (isFsWriteFallbackEnabled()) {
      console.error(
        "[decision-outcomes] Airtable event write failed; FS fallback enabled",
        err?.message || err
      );
      fsStore.appendEvent(hotelId, decisionId, event);
      return { event, created: true, fallback: "filesystem" };
    }
    throw err;
  }
}

export async function loadDecision(hotelId, decisionId) {
  const mode = getPersistenceMode();
  if (mode === "filesystem") {
    return fsStore.loadDecision(hotelId, decisionId);
  }
  try {
    const d = await atStore.loadDecision(hotelId, decisionId);
    if (d) return d;
  } catch (err) {
    if (!isFsReadFallbackEnabled()) throw err;
    console.error(
      "[decision-outcomes] Airtable loadDecision failed; trying FS",
      err?.message || err
    );
  }
  if (isFsReadFallbackEnabled()) {
    return fsStore.loadDecision(hotelId, decisionId);
  }
  return null;
}

export async function loadEvents(hotelId, decisionId) {
  const mode = getPersistenceMode();
  if (mode === "filesystem") {
    return fsStore.loadEvents(hotelId, decisionId);
  }
  try {
    const events = await atStore.loadEvents(hotelId, decisionId);
    if (events.length || !isFsReadFallbackEnabled()) return events;
  } catch (err) {
    if (!isFsReadFallbackEnabled()) throw err;
    console.error(
      "[decision-outcomes] Airtable loadEvents failed; trying FS",
      err?.message || err
    );
  }
  if (isFsReadFallbackEnabled()) {
    return fsStore.loadEvents(hotelId, decisionId);
  }
  return [];
}

export async function findDecisionByIdempotencyKey(hotelId, idempotencyKey) {
  const mode = getPersistenceMode();
  if (mode === "filesystem") {
    return fsStore.findDecisionByIdempotencyKey(hotelId, idempotencyKey);
  }
  try {
    const d = await atStore.findDecisionByIdempotencyKey(hotelId, idempotencyKey);
    if (d) return d;
  } catch (err) {
    if (!isFsReadFallbackEnabled()) throw err;
  }
  if (isFsReadFallbackEnabled()) {
    return fsStore.findDecisionByIdempotencyKey(hotelId, idempotencyKey);
  }
  return null;
}

export async function findDecisionBySubject(hotelId, opts) {
  const mode = getPersistenceMode();
  if (mode === "filesystem") {
    return fsStore.findDecisionBySubject(hotelId, opts);
  }
  try {
    const d = await atStore.findDecisionBySubject(hotelId, opts);
    if (d) return d;
  } catch (err) {
    if (!isFsReadFallbackEnabled()) throw err;
  }
  if (isFsReadFallbackEnabled()) {
    return fsStore.findDecisionBySubject(hotelId, opts);
  }
  return null;
}

export async function listDecisionSummaries(hotelId) {
  const mode = getPersistenceMode();
  if (mode === "filesystem") {
    return fsStore.listDecisionSummaries(hotelId);
  }
  try {
    return await atStore.listDecisionSummaries(hotelId);
  } catch (err) {
    if (!isFsReadFallbackEnabled()) throw err;
    return fsStore.listDecisionSummaries(hotelId);
  }
}

export async function listHotelsWithDecisions() {
  const mode = getPersistenceMode();
  if (mode === "filesystem") {
    return fsStore.listHotelsWithDecisions();
  }
  try {
    const hotels = await atStore.listHotelsWithDecisions();
    if (hotels.length || !isFsReadFallbackEnabled()) return hotels;
  } catch (err) {
    if (!isFsReadFallbackEnabled()) throw err;
  }
  if (isFsReadFallbackEnabled()) {
    return fsStore.listHotelsWithDecisions();
  }
  return [];
}

export {
  createDecisionId,
  createEventId,
  buildDecisionIdempotencyKey,
  assertHotelBoundary,
  getDecisionOutcomesRoot,
} from "./store.js";

export {
  getDecisionOutcomesAirtableBaseId,
  isAirtableConfigured,
} from "./airtable-store.js";
