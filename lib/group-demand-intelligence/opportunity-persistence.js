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
 * Load hotel opportunity document (canonical).
 * Shape: { hotelId, opportunities[], updatedAt, runId?, persistence }
 */
export async function loadOpportunitiesCanonical(hotelId) {
  const id = String(hotelId || "").trim();
  const cached = getCachedOpportunityDoc(id);
  if (cached) {
    return { ...cached, cacheHit: true };
  }

  const mode = getGdiOpportunityPersistenceMode();
  let doc;
  if (mode === "filesystem") {
    doc = {
      ...fsRepo.loadOpportunities(id),
      persistence: "filesystem",
    };
  } else {
    try {
      const opportunities = await atStore.listOpportunitiesForHotel(id);
      if (opportunities.length || !readFallbackEnabled()) {
        const updatedAt = opportunities
          .map((o) => o.updatedAt || "")
          .filter(Boolean)
          .sort()
          .at(-1) || null;
        doc = {
          hotelId: id,
          opportunities,
          updatedAt,
          persistence: "airtable",
          source: "airtable",
        };
      }
    } catch (err) {
      if (!readFallbackEnabled()) throw err;
      console.error(
        "[gdi] Airtable loadOpportunities failed; trying FS",
        err?.message || err
      );
    }
    if (!doc && readFallbackEnabled()) {
      doc = {
        ...fsRepo.loadOpportunities(id),
        persistence: "filesystem_fallback",
        source: "filesystem",
      };
    }
    if (!doc) {
      doc = {
        hotelId: id,
        opportunities: [],
        updatedAt: null,
        persistence: "airtable",
      };
    }
  }

  setCachedOpportunityDoc(id, doc);
  return { ...doc, cacheHit: false };
}

/**
 * Save full opportunity set for a hotel (upsert each by opportunityId).
 */
export async function saveOpportunitiesCanonical(hotelId, payload) {
  const mode = getGdiOpportunityPersistenceMode();
  const opportunities = payload?.opportunities || [];
  const doc = {
    ...payload,
    hotelId,
    opportunities,
    updatedAt: new Date().toISOString(),
  };

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
