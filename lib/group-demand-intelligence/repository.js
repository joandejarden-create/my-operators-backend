/**
 * Group Demand Intelligence — filesystem repository.
 * V1 persistence: data/group-demand-intelligence/ (no Airtable writes; no ADP writes).
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { randomBytes } from "node:crypto";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../data/group-demand-intelligence");

export function getGdiDataRoot() {
  return ROOT;
}

export function hotelDir(hotelId) {
  const id = String(hotelId || "").trim();
  if (!id) throw new Error("hotelId_required");
  return path.join(ROOT, "hotels", id);
}

function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
}

/** mtime-keyed process cache — avoids re-parse of ~0.5–1MB opportunities on every request. */
const jsonReadCache = new Map();

function readJson(filePath, fallback = null) {
  if (!fs.existsSync(filePath)) return fallback;
  let st;
  try {
    st = fs.statSync(filePath);
  } catch {
    return fallback;
  }
  const hit = jsonReadCache.get(filePath);
  if (hit && hit.mtimeMs === st.mtimeMs) return hit.data;
  const data = JSON.parse(fs.readFileSync(filePath, "utf8"));
  jsonReadCache.set(filePath, { mtimeMs: st.mtimeMs, data });
  return data;
}

function writeJson(filePath, data) {
  ensureDir(path.dirname(filePath));
  fs.writeFileSync(filePath, JSON.stringify(data, null, 2) + "\n", "utf8");
  try {
    const st = fs.statSync(filePath);
    jsonReadCache.set(filePath, { mtimeMs: st.mtimeMs, data });
  } catch {
    jsonReadCache.delete(filePath);
  }
}

export function clearGdiRepositoryReadCache() {
  jsonReadCache.clear();
}

export function createId(prefix) {
  return `${prefix}_${Date.now().toString(36)}_${randomBytes(4).toString("hex")}`;
}

export function loadHotelProfile(hotelId) {
  return readJson(path.join(hotelDir(hotelId), "profile.json"), null);
}

export function saveHotelProfile(hotelId, profile) {
  writeJson(path.join(hotelDir(hotelId), "profile.json"), {
    ...profile,
    hotelId,
    updatedAt: new Date().toISOString(),
  });
}

export function loadOpportunities(hotelId) {
  return readJson(path.join(hotelDir(hotelId), "opportunities.json"), {
    hotelId,
    opportunities: [],
    updatedAt: null,
  });
}

export function saveOpportunities(hotelId, payload) {
  writeJson(path.join(hotelDir(hotelId), "opportunities.json"), {
    ...payload,
    hotelId,
    updatedAt: new Date().toISOString(),
  });
}

export function loadFeedback(hotelId) {
  return readJson(path.join(hotelDir(hotelId), "feedback.json"), {
    hotelId,
    items: [],
  });
}

export function saveFeedbackItem(hotelId, item) {
  const doc = loadFeedback(hotelId);
  const next = {
    ...item,
    id: item.id || createId("gdi_fb"),
    hotelId,
    createdAt: item.createdAt || new Date().toISOString(),
    updatedAt: new Date().toISOString(),
  };
  const idx = doc.items.findIndex(
    (x) => x.opportunityId === next.opportunityId && x.id === next.id
  );
  if (idx >= 0) doc.items[idx] = { ...doc.items[idx], ...next };
  else doc.items.push(next);
  writeJson(path.join(hotelDir(hotelId), "feedback.json"), doc);
  return next;
}

export function listResearchRuns(hotelId) {
  const runsDir = path.join(hotelDir(hotelId), "runs");
  if (!fs.existsSync(runsDir)) return [];
  return fs
    .readdirSync(runsDir)
    .filter((n) => fs.existsSync(path.join(runsDir, n, "run.json")))
    .map((n) => readJson(path.join(runsDir, n, "run.json")))
    .filter(Boolean)
    .sort((a, b) => String(b.startedAt || "").localeCompare(String(a.startedAt || "")));
}

export function loadResearchRun(hotelId, runId) {
  return readJson(path.join(hotelDir(hotelId), "runs", runId, "run.json"), null);
}

export function saveResearchRun(hotelId, run) {
  const runId = run.id || createId("gdi_run");
  const full = { ...run, id: runId, hotelId };
  writeJson(path.join(hotelDir(hotelId), "runs", runId, "run.json"), full);
  return full;
}

export function listRegisteredHotels() {
  const hotelsRoot = path.join(ROOT, "hotels");
  if (!fs.existsSync(hotelsRoot)) return [];
  return fs
    .readdirSync(hotelsRoot)
    .filter((id) => fs.existsSync(path.join(hotelsRoot, id, "profile.json")))
    .map((id) => loadHotelProfile(id))
    .filter(Boolean);
}

/**
 * Data-driven GDI property selector catalog.
 * REUSABLE_PRODUCT_LOGIC — unions config registry + materialized hotel data dirs.
 * No hotel-name hardcodes.
 *
 * Eligible when hotel has a GDI config AND (profile.json OR opportunities.json).
 */
export function listGdiSelectableHotels() {
  const byId = new Map();

  const configDir = path.join(
    path.resolve(__dirname, "../../config/group-demand-intelligence/hotels")
  );
  if (fs.existsSync(configDir)) {
    for (const file of fs.readdirSync(configDir)) {
      if (!file.endsWith(".json")) continue;
      const hotelId = file.replace(/\.json$/i, "");
      let config = null;
      try {
        config = JSON.parse(fs.readFileSync(path.join(configDir, file), "utf8"));
      } catch {
        continue;
      }
      if (!config || typeof config !== "object") continue;
      // Skip provisional redirect stubs (canonical file owns the hotel).
      if (config.redirectTo || config.deprecatedProvisionalId) continue;
      if (!config.displayName && !config.commercialPriorities && !config.gdiOnboarding) {
        continue;
      }
      const resolvedId = String(config.hotelId || hotelId).trim() || hotelId;
      // Prefer Airtable rec… keys; skip duplicate provisional filesystem keys.
      if (hotelId.startsWith("gdi_hotel_") && resolvedId.startsWith("rec")) {
        continue;
      }
      byId.set(resolvedId, {
        hotelId: resolvedId,
        config,
        profile: null,
        hasProfile: false,
        hasOpportunities: false,
      });
    }
  }

  const hotelsRoot = path.join(ROOT, "hotels");
  if (fs.existsSync(hotelsRoot)) {
    for (const hotelId of fs.readdirSync(hotelsRoot)) {
      if (hotelId.startsWith("gdi_hotel_")) continue; // legacy provisional dirs
      const dir = path.join(hotelsRoot, hotelId);
      if (!fs.statSync(dir).isDirectory()) continue;
      const hasProfile = fs.existsSync(path.join(dir, "profile.json"));
      const hasOpportunities = fs.existsSync(path.join(dir, "opportunities.json"));
      if (!hasProfile && !hasOpportunities) continue;
      const existing = byId.get(hotelId) || { hotelId, config: null };
      existing.hasProfile = hasProfile;
      existing.hasOpportunities = hasOpportunities;
      if (hasProfile) {
        try {
          existing.profile = loadHotelProfile(hotelId);
        } catch {
          existing.profile = null;
        }
      }
      // Materialized data without config still selectable (legacy pilot path)
      byId.set(hotelId, existing);
    }
  }

  const rows = [];
  for (const row of byId.values()) {
    // Require config OR materialized profile (Bethesda pilot had profile before strict config gate)
    if (!row.config && !row.hasProfile) continue;
    // Prefer hotels that are initialized (profile or opportunities). Config-only stays hidden
    // until first research materializes state — except allow config+onboarded for local prep.
    const onboarded =
      row.config?.gdiOnboarding?.status != null ||
      row.hasProfile ||
      row.hasOpportunities;
    if (!onboarded) continue;
    if (!row.hasProfile && !row.hasOpportunities && !row.config) continue;
    // Local selector: show when config exists AND (data exists OR onboarding marked ready)
    const status = String(row.config?.gdiOnboarding?.status || "").toUpperCase();
    const readyStatuses = new Set([
      "ONBOARDED",
      "FIRST_RUN_PORTABILITY",
      "READY",
      "ACTIVE",
      "PILOT",
    ]);
    if (!row.hasProfile && !row.hasOpportunities && !readyStatuses.has(status)) {
      continue;
    }

    const identity = row.profile?.identity || {};
    const censusId =
      row.profile?.canonicalIds?.censusRecordId ||
      row.config?.aliases?.censusRecordId ||
      (String(row.hotelId || "").startsWith("rec") ? row.hotelId : null);
    const hotelName =
      row.config?.displayName ||
      identity.hotelName ||
      row.hotelId;
    const city =
      identity.city ||
      row.config?.identity?.city ||
      row.config?.location?.city ||
      null;
    const state =
      identity.state ||
      row.config?.identity?.state ||
      row.config?.location?.state ||
      null;
    const locationLine = [city, state].filter(Boolean).join(", ");
    const optionLabel = locationLine ? `${hotelName} — ${locationLine}` : hotelName;

    rows.push({
      hotelId: row.hotelId,
      hotelName,
      displayName: row.config?.displayName || hotelName,
      brand: identity.brand || row.config?.capabilityProfile?.softBrand || null,
      city,
      state,
      locationLine,
      optionLabel,
      hasProfile: row.hasProfile,
      hasOpportunities: row.hasOpportunities,
      identityStatus: censusId
        ? "canonical_census"
        : "provisional_gdi_key",
      censusRecordId: censusId,
      provisionalNote: censusId
        ? null
        : "Local/dev: provisional GDI hotel key — Census hotelId unresolved",
    });
  }

  rows.sort((a, b) =>
    String(a.hotelName || a.hotelId).localeCompare(String(b.hotelName || b.hotelId))
  );
  return rows;
}

export function loadLatestSummary(hotelId) {
  const opp = loadOpportunities(hotelId);
  const runs = listResearchRuns(hotelId);
  const latest = runs[0] || null;
  const opportunities = opp.opportunities || [];
  const active = opportunities.filter((o) => o.priority !== "DISQUALIFIED");
  return {
    hotelId,
    lastResearchAt: latest?.completedAt || latest?.startedAt || null,
    runStatus: latest?.status || "NEVER_RUN",
    latestRunId: latest?.id || null,
    sourceCount: latest?.metrics?.sourceCount ?? 0,
    opportunitiesDiscovered: opportunities.length,
    qualifiedCount: active.length,
    highPriorityCount: active.filter((o) => o.priority === "HIGH_PRIORITY").length,
    mediumPriorityCount: active.filter((o) => o.priority === "MEDIUM_PRIORITY").length,
    watchlistCount: active.filter((o) => o.priority === "WATCHLIST").length,
    researchCostUsd: latest?.cost?.totalUsd ?? 0,
    costBreakdown: latest?.cost || null,
  };
}
