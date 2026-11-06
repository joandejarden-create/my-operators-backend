/**
 * GDI_UNIVERSAL_NEED_DATES_AND_FULL_E2E_RESEARCH_V2 — three-layer hotel model.
 *
 * A. HOTEL INVENTORY — canonical GDI-eligible hotels (config registry)
 * B. GDI RESEARCH STATUS — independent of need dates
 * C. NEED-DATE CONFIGURATION STATUS — independent of research
 *
 * Does NOT use listGdiSelectableHotels() as the universal inventory denominator.
 * That selector remains for GDI research UI only.
 *
 * No Airtable writes. No paid research. No customer publish.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { listGdiSelectableHotels } from "../repository.js";
import {
  HOTEL_NEED_DATE_CONFIG_STATUS,
  resolveHotelNeedDateConfiguration,
} from "./gdi-universal-need-date-intelligence-v1.js";

export const GDI_HOTEL_LAYER_MODEL_V2 = "GDI_HOTEL_LAYER_MODEL_V2";

export const GDI_RESEARCH_STATUS = Object.freeze({
  NOT_STARTED: "NOT_STARTED",
  RESEARCH_IN_PROGRESS: "RESEARCH_IN_PROGRESS",
  RESEARCH_COMPLETED: "RESEARCH_COMPLETED",
  RESEARCH_UPDATE_REQUIRED: "RESEARCH_UPDATE_REQUIRED",
});

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");
const CONFIG_DIR = path.join(ROOT, "config/group-demand-intelligence/hotels");
const DATA_HOTELS = path.join(ROOT, "data/group-demand-intelligence/hotels");

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function readJson(filePath) {
  try {
    return JSON.parse(fs.readFileSync(filePath, "utf8"));
  } catch {
    return null;
  }
}

/**
 * Canonical GDI-eligible hotel inventory from config registry.
 * - Prefer Airtable census `rec…` identities
 * - Skip provisional redirect stubs and duplicate provisional filesystem keys
 * - Does not require completed GDI research or opportunity materialization
 */
export function listCanonicalGdiEligibleHotels(opts = {}) {
  const includeProvisional = opts.includeProvisional === true;
  const byId = new Map();
  if (!fs.existsSync(CONFIG_DIR)) return [];

  for (const file of fs.readdirSync(CONFIG_DIR)) {
    if (!file.endsWith(".json")) continue;
    const fileKey = file.replace(/\.json$/i, "");
    const config = readJson(path.join(CONFIG_DIR, file));
    if (!config || typeof config !== "object") continue;
    if (config.redirectTo || config.deprecatedProvisionalId) continue;

    const resolvedId = text(config.hotelId || fileKey);
    if (!resolvedId) continue;
    // Skip provisional filesystem keys that alias a rec… identity.
    if (fileKey.startsWith("gdi_hotel_") && resolvedId.startsWith("rec")) continue;
    if (!includeProvisional && resolvedId.startsWith("gdi_hotel_")) continue;
    if (!config.displayName && !config.gdiOnboarding && !config.commercialPriorities) {
      continue;
    }

    const hotelName = text(config.displayName) || resolvedId;
    byId.set(resolvedId, {
      hotelId: resolvedId,
      hotelName,
      displayName: hotelName,
      configPath: path.join(CONFIG_DIR, file),
      configFileKey: fileKey,
      onboardingStatus: text(config.gdiOnboarding?.status || "").toUpperCase() || null,
      city: config.identity?.city || config.location?.city || config.city || null,
      country:
        config.country ||
        config.capabilityProfile?.country ||
        config.identity?.country ||
        null,
      layer: "HOTEL_INVENTORY",
      researchRequiredForNeedDates: false,
    });
  }

  return [...byId.values()].sort((a, b) =>
    String(a.hotelName).localeCompare(String(b.hotelName))
  );
}

export function classifyGdiResearchStatus(hotelId, opts = {}) {
  const id = text(hotelId);
  const dir = path.join(DATA_HOTELS, id);
  const oppPath = path.join(dir, "opportunities.json");
  const profilePath = path.join(dir, "profile.json");
  const runsDir = path.join(dir, "research-runs");

  const hasOpportunitiesFile = fs.existsSync(oppPath);
  const hasProfile = fs.existsSync(profilePath);
  let opportunityCount = 0;
  if (hasOpportunitiesFile) {
    const doc = readJson(oppPath);
    const list = Array.isArray(doc)
      ? doc
      : Array.isArray(doc?.opportunities)
        ? doc.opportunities
        : [];
    opportunityCount = list.length;
  }

  let hasRuns = false;
  if (fs.existsSync(runsDir)) {
    try {
      hasRuns = fs.readdirSync(runsDir).some((f) => f.endsWith(".json"));
    } catch {
      hasRuns = false;
    }
  }

  // Selectable GDI research set is a subset — presence there implies research surface readiness.
  const selectableIds = new Set(
    (opts.selectableHotels || listGdiSelectableHotels()).map((h) => text(h.hotelId))
  );
  const onSelectable = selectableIds.has(id);

  if (opportunityCount > 0 || hasRuns) {
    return {
      status: GDI_RESEARCH_STATUS.RESEARCH_COMPLETED,
      opportunityCount,
      hasProfile,
      onSelectableResearchSet: onSelectable,
      note: null,
    };
  }
  if (hasProfile || onSelectable) {
    return {
      status: GDI_RESEARCH_STATUS.NOT_STARTED,
      opportunityCount: 0,
      hasProfile,
      onSelectableResearchSet: onSelectable,
      note: "Canonical hotel is GDI-eligible; research not materialized yet.",
    };
  }
  return {
    status: GDI_RESEARCH_STATUS.NOT_STARTED,
    opportunityCount: 0,
    hasProfile: false,
    onSelectableResearchSet: false,
    note: "No local research artifacts.",
  };
}

/**
 * Combined three-layer row for one hotel.
 * Need-date resolution stays independent of research status.
 */
export function resolveHotelLayerRow(hotel, opts = {}) {
  const hotelId = text(hotel.hotelId || hotel);
  const hotelName = text(hotel.hotelName || hotel.displayName || hotelId);
  const research = classifyGdiResearchStatus(hotelId, opts);
  const needDate = resolveHotelNeedDateConfiguration(hotelId, opts);
  return {
    hotelId,
    hotelName,
    inventoryLayer: "HOTEL_INVENTORY",
    gdiResearchStatus: research.status,
    gdiOpportunityCount: research.opportunityCount,
    onSelectableResearchSet: research.onSelectableResearchSet,
    needDateStatus: needDate.status,
    needDateStatusLabel: needDate.statusLabel,
    hotelConfirmedPeriods: needDate.counts?.hotelConfirmedPeriods || 0,
    marketEstimatedPeriods: needDate.counts?.marketEstimatedPeriods || 0,
    independence: {
      needDatesWithoutResearch: true,
      researchWithoutNeedDates: true,
      missingNeedDatesDoNotBlockResearch: true,
    },
  };
}

export function auditHotelLayerModelV2(opts = {}) {
  const inventory = listCanonicalGdiEligibleHotels(opts);
  const selectable = listGdiSelectableHotels();
  const rows = inventory.map((h) => resolveHotelLayerRow(h, { ...opts, selectableHotels: selectable }));

  const counts = {
    inventoryHotels: rows.length,
    gdiSelectableResearchHotels: selectable.length,
    researchCompleted: rows.filter(
      (r) => r.gdiResearchStatus === GDI_RESEARCH_STATUS.RESEARCH_COMPLETED
    ).length,
    researchNotStarted: rows.filter(
      (r) => r.gdiResearchStatus === GDI_RESEARCH_STATUS.NOT_STARTED
    ).length,
    HOTEL_CONFIRMED: rows.filter(
      (r) => r.needDateStatus === HOTEL_NEED_DATE_CONFIG_STATUS.HOTEL_CONFIRMED
    ).length,
    MARKET_ESTIMATED: rows.filter(
      (r) => r.needDateStatus === HOTEL_NEED_DATE_CONFIG_STATUS.MARKET_ESTIMATED
    ).length,
    NOT_ESTABLISHED: rows.filter(
      (r) => r.needDateStatus === HOTEL_NEED_DATE_CONFIG_STATUS.NOT_ESTABLISHED
    ).length,
    needDatesWithoutResearchCompleted: rows.filter(
      (r) =>
        r.gdiResearchStatus !== GDI_RESEARCH_STATUS.RESEARCH_COMPLETED &&
        r.needDateStatus !== HOTEL_NEED_DATE_CONFIG_STATUS.NOT_ESTABLISHED
    ).length,
    researchCompletedWithoutNeedDates: rows.filter(
      (r) =>
        r.gdiResearchStatus === GDI_RESEARCH_STATUS.RESEARCH_COMPLETED &&
        r.needDateStatus === HOTEL_NEED_DATE_CONFIG_STATUS.NOT_ESTABLISHED
    ).length,
  };

  return {
    version: GDI_HOTEL_LAYER_MODEL_V2,
    generatedAt: new Date().toISOString(),
    denominator: "canonical_gdi_eligible_config_registry",
    notDenominator: "listGdiSelectableHotels",
    counts,
    hotels: rows,
    selectableResearchHotels: selectable.map((h) => ({
      hotelId: h.hotelId,
      hotelName: h.hotelName || h.displayName,
    })),
  };
}
