/**
 * GDI opportunity persistence facade.
 *
 * Modes (GDI_OPPORTUNITIES_PERSISTENCE):
 *   airtable   — Airtable primary (default when configured)
 *   filesystem — local FS only (tests / offline)
 *
 * Mirror (GDI_OPPORTUNITIES_FS_MIRROR): default "1" when airtable
 * Fallback write (GDI_OPPORTUNITIES_FS_FALLBACK): default "0"
 * Read FS fallback (GDI_OPPORTUNITIES_READ_FS_FALLBACK): default "1" during cutover
 */

import fs from "node:fs";
import path from "node:path";
import * as fsRepo from "./repository.js";
import * as atStore from "./airtable-opportunity-store.js";
import {
  getCachedOpportunityDoc,
  setCachedOpportunityDoc,
  invalidateGdiHotelReadCache,
} from "./read-cache.js";

export function getGdiOpportunityPersistenceMode() {
  const explicit = String(process.env.GDI_OPPORTUNITIES_PERSISTENCE || "")
    .trim()
    .toLowerCase();
  if (explicit === "filesystem" || explicit === "fs") return "filesystem";
  if (explicit === "airtable" || explicit === "at") return "airtable";
  return atStore.isGdiOpportunityAirtableConfigured()
    ? "airtable"
    : "filesystem";
}

function mirrorEnabled() {
  if (getGdiOpportunityPersistenceMode() !== "airtable") return false;
  const v = String(process.env.GDI_OPPORTUNITIES_FS_MIRROR ?? "1").trim();
  return v !== "0" && v.toLowerCase() !== "false";
}

function writeFallbackEnabled() {
  const v = String(process.env.GDI_OPPORTUNITIES_FS_FALLBACK || "0").trim();
  return v === "1" || v.toLowerCase() === "true";
}

function readFallbackEnabled() {
  const v = String(
    process.env.GDI_OPPORTUNITIES_READ_FS_FALLBACK ?? "1"
  ).trim();
  return v !== "0" && v.toLowerCase() !== "false";
}

function mirrorSave(hotelId, payload) {
  if (!mirrorEnabled()) return;
  try {
    fsRepo.saveOpportunities(hotelId, payload);
  } catch (err) {
    console.error(
      "[gdi] FS mirror opportunities failed",
      err?.message || err
    );
  }
}

/**
 * In-flight canonical loads, keyed by normalized hotelId.
 * Prevents concurrent GDI page reads from issuing duplicate Airtable requests.
 */
const inFlightOpportunityLoads = new Map();

/**
 * Document-level metadata that Airtable row loads omit (import stamps,
 * durable hotel activity summary). Merge from FS mirror when present so
 * production summary headers can use verifiable history without inventing dates.
 */
function mergeFsDocumentMetadata(hotelId, doc) {
  try {
    const fsDoc = fsRepo.loadOpportunities(hotelId);
    if (!fsDoc || typeof fsDoc !== "object") return doc;
    const next = { ...doc };
    if (!next.reviewedProspectImport && fsDoc.reviewedProspectImport) {
      next.reviewedProspectImport = fsDoc.reviewedProspectImport;
    }
    if (!next.hotelActivitySummary && fsDoc.hotelActivitySummary) {
      next.hotelActivitySummary = fsDoc.hotelActivitySummary;
    }
    if (!next.lastReviewedPackageImportAt && fsDoc.lastReviewedPackageImportAt) {
      next.lastReviewedPackageImportAt = fsDoc.lastReviewedPackageImportAt;
    }
    if (!next.runId && fsDoc.runId) next.runId = fsDoc.runId;
    if (!next.researchVersion && fsDoc.researchVersion) {
      next.researchVersion = fsDoc.researchVersion;
    }
    return next;
  } catch {
    return doc;
  }
}

function canonicalReadError(err) {
  const error = new Error("canonical_opportunity_read_failed");
  error.code = "canonical_opportunity_read_failed";
  error.cause = err;
  return error;
}

function airtableOpportunityDoc(id, opportunities) {
  const updatedAt =
    opportunities
      .map((o) => o.updatedAt || "")
      .filter(Boolean)
      .sort()
      .at(-1) || null;
  return {
    hotelId: id,
    opportunities,
    updatedAt,
    persistence: "airtable",
    source: "airtable",
    readFailed: false,
  };
}

/**
 * @param {string} id
 * @param {{ strictCanonical?: boolean, loadAirtable?: Function }} [opts]
 * strictCanonical: honor the configured source only. Airtable mode must not
 * substitute opportunities.json when the canonical read is empty or fails.
 */
async function loadUncachedOpportunityDoc(id, opts = {}) {
  const mode = getGdiOpportunityPersistenceMode();
  const strict = opts.strictCanonical === true;
  let doc;
  if (mode === "filesystem") {
    try {
      doc = {
        ...fsRepo.loadOpportunities(id),
        persistence: "filesystem",
        source: "filesystem",
        readFailed: false,
      };
    } catch (err) {
      if (strict) throw canonicalReadError(err);
      throw err;
    }
  } else {
    const listAirtable =
      typeof opts.loadAirtable === "function"
        ? opts.loadAirtable
        : atStore.listOpportunitiesForHotel;
    try {
      const opportunities = await listAirtable(id);
      const rows = Array.isArray(opportunities) ? opportunities : [];
      if (strict || rows.length || !readFallbackEnabled()) {
        doc = strict
          ? airtableOpportunityDoc(id, rows)
          : mergeFsDocumentMetadata(id, airtableOpportunityDoc(id, rows));
      }
    } catch (err) {
      if (strict || !readFallbackEnabled()) throw canonicalReadError(err);
      console.error(
        "[gdi] Airtable loadOpportunities failed; trying FS",
        err?.message || err
      );
    }
    if (!doc && !strict && readFallbackEnabled()) {
      doc = {
        ...fsRepo.loadOpportunities(id),
        persistence: "filesystem_fallback",
        source: "filesystem",
        readFailed: false,
      };
    }
    if (!doc) {
      doc = airtableOpportunityDoc(id, []);
    }
  }
  return doc;
}

/**
 * Load hotel opportunity document (canonical).
 * Shape: { hotelId, opportunities[], updatedAt, runId?, persistence }
 */
export async function loadOpportunitiesCanonical(hotelId, opts = {}) {
  const id = String(hotelId || "").trim();
  const bypassCache = opts.bypassCache === true;
  if (!bypassCache) {
    const cached = getCachedOpportunityDoc(id);
    if (cached) {
      return { ...cached, cacheHit: true };
    }

    const pending = inFlightOpportunityLoads.get(id);
    if (pending) {
      return await pending;
    }
  }

  const loadPromise = (async () => {
    try {
      const doc = await loadUncachedOpportunityDoc(id, opts);
      setCachedOpportunityDoc(id, doc);
      return { ...doc, cacheHit: false };
    } finally {
      inFlightOpportunityLoads.delete(id);
    }
  })();

  inFlightOpportunityLoads.set(id, loadPromise);
  return await loadPromise;
}

/**
 * Save full opportunity set for a hotel (upsert each by opportunityId).
 */
export async function saveOpportunitiesCanonical(hotelId, payload) {
  const mode = getGdiOpportunityPersistenceMode();
  const opportunities = payload?.opportunities || [];
  let doc = {
    ...payload,
    hotelId,
    opportunities,
    updatedAt: new Date().toISOString(),
  };
  try {
    const { attachHotelActivitySummaryToDoc } = await import(
      "./hotel-activity-history-v1.js"
    );
    doc = attachHotelActivitySummaryToDoc(hotelId, doc);
  } catch (err) {
    console.error(
      "[gdi] hotel activity summary attach failed",
      err?.message || err
    );
  }

  if (mode === "filesystem") {
    fsRepo.saveOpportunities(hotelId, doc);
    invalidateGdiHotelReadCache(hotelId);
    return { ...doc, persistence: "filesystem" };
  }

  try {
    await atStore.upsertOpportunitiesBatch(hotelId, opportunities, {
      hotelId,
      runId: payload?.runId || null,
      researchVersion: payload?.researchVersion || payload?.runId || null,
      hotelIdentityStatus: payload?.hotelIdentityStatus || null,
    });
    mirrorSave(hotelId, doc);
    invalidateGdiHotelReadCache(hotelId);
    return { ...doc, persistence: "airtable" };
  } catch (err) {
    if (writeFallbackEnabled()) {
      console.error(
        "[gdi] Airtable saveOpportunities failed; FS fallback",
        err?.message || err
      );
      fsRepo.saveOpportunities(hotelId, doc);
      invalidateGdiHotelReadCache(hotelId);
      return { ...doc, persistence: "filesystem_fallback", error: err.message };
    }
    throw err;
  }
}

export async function upsertSingleOpportunity(hotelId, opportunity, meta = {}) {
  const mode = getGdiOpportunityPersistenceMode();
  if (mode === "filesystem") {
    const doc = fsRepo.loadOpportunities(hotelId);
    const id = opportunity.id || opportunity.opportunityId;
    const idx = (doc.opportunities || []).findIndex((o) => o.id === id);
    const next = { ...opportunity, hotelId, id };
    if (idx >= 0) doc.opportunities[idx] = { ...doc.opportunities[idx], ...next };
    else doc.opportunities.push(next);
    fsRepo.saveOpportunities(hotelId, doc);
    invalidateGdiHotelReadCache(hotelId);
    return { opportunity: next, created: idx < 0, persistence: "filesystem" };
  }
  const result = await atStore.upsertOpportunity(
    { ...opportunity, hotelId },
    { hotelId, ...meta }
  );
  if (mirrorEnabled()) {
    const doc = fsRepo.loadOpportunities(hotelId);
    const id = result.opportunity.id;
    const idx = (doc.opportunities || []).findIndex((o) => o.id === id);
    if (idx >= 0) doc.opportunities[idx] = { ...doc.opportunities[idx], ...result.opportunity };
    else doc.opportunities.push(result.opportunity);
    fsRepo.saveOpportunities(hotelId, doc);
  }
  invalidateGdiHotelReadCache(hotelId);
  return { ...result, persistence: "airtable" };
}

export {
  getGdiOpportunitiesAirtableBaseId,
  isGdiOpportunityAirtableConfigured,
} from "./airtable-opportunity-store.js";

/** Sync FS helpers retained for evals / profile listing. */
export {
  loadOpportunities as loadOpportunitiesFs,
  saveOpportunities as saveOpportunitiesFs,
  getGdiDataRoot,
  hotelDir,
} from "./repository.js";

/** True if hotel has FS opportunities file (selector eligibility; not SoT). */
export function hotelHasFsOpportunities(hotelId) {
  try {
    return fs.existsSync(
      path.join(fsRepo.hotelDir(hotelId), "opportunities.json")
    );
  } catch {
    return false;
  }
}
