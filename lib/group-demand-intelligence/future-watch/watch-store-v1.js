/**
 * Filesystem persistence for future-watch corpus (append-safe history).
 * No new Airtable table — hotel-scoped JSON under GDI data root.
 */

import fs from "node:fs";
import path from "node:path";
import { getGdiDataRoot, hotelDir } from "../repository.js";
import { FUTURE_WATCH_VERSION } from "./constants.js";

export function futureWatchPath(hotelId) {
  return path.join(hotelDir(hotelId), "future-watches.json");
}

export function loadFutureWatches(hotelId) {
  const p = futureWatchPath(hotelId);
  try {
    if (fs.existsSync(p)) {
      return JSON.parse(fs.readFileSync(p, "utf8"));
    }
  } catch {
    /* ignore */
  }
  return {
    hotelId,
    version: FUTURE_WATCH_VERSION,
    updatedAt: null,
    watches: [],
  };
}

export function saveFutureWatches(hotelId, doc) {
  const p = futureWatchPath(hotelId);
  fs.mkdirSync(path.dirname(p), { recursive: true });
  const out = {
    ...doc,
    hotelId,
    version: FUTURE_WATCH_VERSION,
    updatedAt: new Date().toISOString(),
  };
  fs.writeFileSync(p, JSON.stringify(out, null, 2), "utf8");
  return out;
}

/**
 * Upsert one watch by watchId / candidateId — preserves prior watchHistory.
 */
export function upsertFutureWatch(hotelId, watch) {
  const doc = loadFutureWatches(hotelId);
  const idx = doc.watches.findIndex(
    (w) =>
      w.watchId === watch.watchId ||
      (watch.candidateId && w.candidateId === watch.candidateId)
  );
  if (idx >= 0) {
    const prev = doc.watches[idx];
    const mergedHistory = [
      ...(Array.isArray(prev.watchHistory) ? prev.watchHistory : []),
      ...(Array.isArray(watch.watchHistory) ? watch.watchHistory : []).filter(
        (h) =>
          !prev.watchHistory?.some(
            (p) => p.at === h.at && p.jevAction === h.jevAction && p.result === h.result
          )
      ),
    ];
    doc.watches[idx] = {
      ...prev,
      ...watch,
      watchHistory: mergedHistory.length ? mergedHistory : watch.watchHistory || prev.watchHistory,
      createdAt: prev.createdAt || watch.createdAt,
    };
  } else {
    doc.watches.push(watch);
  }
  return saveFutureWatches(hotelId, doc);
}

export function listAllFutureWatches(hotelIds = []) {
  const out = [];
  for (const id of hotelIds) {
    const doc = loadFutureWatches(id);
    for (const w of doc.watches || []) out.push(w);
  }
  return out;
}

export { getGdiDataRoot };
