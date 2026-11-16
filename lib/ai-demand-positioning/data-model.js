/**
 * AI Demand Positioning — Core Data Model.
 * Defines entities, period management, and observation storage.
 */

import { readFileSync, writeFileSync, existsSync, readdirSync, mkdirSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import {
  formatPropertyLocationLine,
  formatPropertySelectorLabel,
} from "../dealality/property-display-label.js";
import { createFilesystemDurablePublishedStore } from "./durable-publication/filesystem-durable-store-v1.js";
import crypto from "crypto";

const REPO_ROOT = join(dirname(fileURLToPath(import.meta.url)), "../..");
const RUNTIME_DIR = join(REPO_ROOT, "data/ai-demand-positioning/runtime");
const SEED_DIR = join(REPO_ROOT, "fixtures/ai-demand-positioning/seed-periods");

function listPeriodFiles(propertyId, dir) {
  if (!existsSync(dir)) return [];
  return readdirSync(dir)
    .filter((f) => f.startsWith("adp_period_") && f.includes(propertyId) && f.endsWith(".json"))
    .sort();
}

function readPeriodFile(dir, filename) {
  return JSON.parse(readFileSync(join(dir, filename), "utf-8"));
}

export function generatePeriodId(propertyId) {
  const ts = new Date().toISOString().replace(/[-:T]/g, "").slice(0, 14);
  const hash = crypto.randomBytes(3).toString("hex");
  return `adp_period_${propertyId}_${ts}_${hash}`;
}

export function createPeriod(propertyId, scenarios) {
  return {
    periodId: generatePeriodId(propertyId),
    propertyId,
    executionDate: new Date().toISOString(),
    scenarioCount: scenarios.length,
    providerCount: 4,
    status: "PENDING",
    observations: [],
  };
}

export function createObservation({ propertyId, scenarioId, provider, periodId, response }) {
  return {
    observationId: `obs_${crypto.randomBytes(6).toString("hex")}`,
    propertyId,
    scenarioId,
    provider,
    periodId,
    timestamp: new Date().toISOString(),
    mentioned: false,
    position: null,
    context: null,
    competitorsMentioned: [],
    attributesRecognized: [],
    sourcesCited: [],
    rawResponseLength: response?.length || 0,
  };
}

export function savePeriod(period) {
  mkdirSync(RUNTIME_DIR, { recursive: true });
  const path = join(RUNTIME_DIR, `${period.periodId}.json`);
  writeFileSync(path, JSON.stringify(period, null, 2));
  return path;
}

export function loadPeriod(periodId) {
  const path = join(RUNTIME_DIR, `${periodId}.json`);
  if (!existsSync(path)) return null;
  return JSON.parse(readFileSync(path, "utf-8"));
}

export function isTargetedMeasurementPeriod(period) {
  return period?.measurementScope?.type === "TARGETED_CORE_TRUTH_V1";
}

/** Dedicated LOYALTY_BPP measurement period. Core periods stay false. */
export function isLoyaltyMeasurementPeriod(period) {
  if (!period || typeof period !== "object") return false;
  const family =
    period.measurementFamily ||
    period.measurement?.measurementFamily ||
    period.measurementScope?.family ||
    period.measurementScope?.type;
  return String(family || "") === "LOYALTY_BPP";
}

export function loadLatestPeriod(propertyId, options = {}) {
  const { includeTargeted = false, includeLoyalty = false } = options;
  const runtimeFiles = listPeriodFiles(propertyId, RUNTIME_DIR);
  if (runtimeFiles.length) {
    for (let i = runtimeFiles.length - 1; i >= 0; i--) {
      const period = readPeriodFile(RUNTIME_DIR, runtimeFiles[i]);
      if (!includeTargeted && isTargetedMeasurementPeriod(period)) continue;
      // Loyalty BPP is a separate measurement family — never become the default
      // Core/customer evidence period just because its file sorts later.
      if (!includeLoyalty && isLoyaltyMeasurementPeriod(period)) continue;
      return period;
    }
  }

  const seedFiles = listPeriodFiles(propertyId, SEED_DIR);
  if (seedFiles.length) {
    for (let i = seedFiles.length - 1; i >= 0; i--) {
      const period = readPeriodFile(SEED_DIR, seedFiles[i]);
      if (!includeTargeted && isTargetedMeasurementPeriod(period)) continue;
      if (!includeLoyalty && isLoyaltyMeasurementPeriod(period)) continue;
      return period;
    }
  }

  return null;
}

/**
 * Customer-facing current period: latest certified official full-property period.
 * Falls back to official Core (FOUNDER_REVIEWED / pre-customer-cert) then latest
 * full-property Core. Loyalty BPP periods never win this selector.
 */
export function loadLatestCustomerPeriod(propertyId) {
  const all = loadAllPeriods(propertyId);
  const byDate = (a, b) =>
    String(a.executionDate || "").localeCompare(String(b.executionDate || ""));

  const officialCertified = all
    .filter(
      (p) =>
        p?.officialPeriod === true &&
        p?.measurementPhase === "OFFICIAL_PRODUCTION" &&
        p?.certified === true &&
        p?.customerVisible !== false &&
        !isTargetedMeasurementPeriod(p) &&
        !isLoyaltyMeasurementPeriod(p)
    )
    .sort(byDate);
  if (officialCertified.length) return officialCertified[officialCertified.length - 1];

  // Founder-reviewed / staged official Core (certified flag may still be false
  // until customer publication). Prefer this over a later Loyalty file.
  const officialCore = all
    .filter(
      (p) =>
        p?.officialPeriod === true &&
        p?.measurementPhase === "OFFICIAL_PRODUCTION" &&
        p?.customerVisible !== false &&
        !isTargetedMeasurementPeriod(p) &&
        !isLoyaltyMeasurementPeriod(p)
    )
    .sort(byDate);
  if (officialCore.length) return officialCore[officialCore.length - 1];

  return loadLatestPeriod(propertyId);
}

/** Compatibility alias used by ADP certification. Same selection as loadLatestCustomerPeriod. */
export function loadLatestCoreCustomerPeriod(propertyId) {
  return loadLatestCustomerPeriod(propertyId);
}

export function loadLatestLoyaltyPeriod(propertyId) {
  const matches = (loadAllPeriods(propertyId) || []).filter((period) =>
    isLoyaltyMeasurementPeriod(period)
  );
  if (!matches.length) return null;
  matches.sort((a, b) =>
    String(a.executionDate || "").localeCompare(String(b.executionDate || ""))
  );
  return matches[matches.length - 1];
}

export function loadLatestTargetedPeriod(propertyId) {
  const runtimeFiles = listPeriodFiles(propertyId, RUNTIME_DIR);
  for (let i = runtimeFiles.length - 1; i >= 0; i--) {
    const period = readPeriodFile(RUNTIME_DIR, runtimeFiles[i]);
    if (isTargetedMeasurementPeriod(period)) return period;
  }
  return null;
}

export function loadAllPeriods(propertyId) {
  const runtimeFiles = listPeriodFiles(propertyId, RUNTIME_DIR);
  if (runtimeFiles.length) {
    return runtimeFiles.map((f) => readPeriodFile(RUNTIME_DIR, f));
  }

  const seedFiles = listPeriodFiles(propertyId, SEED_DIR);
  return seedFiles.map((f) => readPeriodFile(SEED_DIR, f));
}

function safePropertyId(propertyId) {
  const id = String(propertyId || "").trim();
  if (!id || id.includes("..") || id.includes("/") || id.includes("\\")) return "";
  return id;
}

/**
 * Lean production images omit fixtures/. A published manifest is enough to
 * identify a hotel whose current report already shipped under data/.
 * This does not read Group Demand Intelligence files.
 */
export function profileFromPublishedManifest(propertyId, publishedDir) {
  const id = safePropertyId(propertyId);
  if (!id) return null;
  const root = publishedDir || join(REPO_ROOT, "data/ai-demand-positioning/published");
  const manifestPath = join(root, id, "manifest.json");
  if (!existsSync(manifestPath)) return null;
  let manifest;
  try {
    manifest = JSON.parse(readFileSync(manifestPath, "utf8"));
  } catch {
    return null;
  }
  if (manifest?.propertyId && manifest.propertyId !== id) return null;
  const name = String(manifest?.propertyName || "").trim();
  if (!name) return null;
  return {
    propertyId: id,
    name,
    city: manifest.city || "",
    state: manifest.state || "",
    market: manifest.market || "",
    country: manifest.country || "",
    customerDropdownVisible: false,
  };
}

function nonEmptyString(value) {
  const text = String(value || "").trim();
  return text || null;
}

export function profileFromDurablePublished(propertyId, opts = {}) {
  const id = safePropertyId(propertyId);
  if (!id) return null;
  if (opts.skipDurablePublished === true) return null;
  try {
    const env = opts.env || process.env;
    const root =
      opts.durableRoot ||
      String(env.ADP_DURABLE_PUBLISHED_ROOT || "").trim() ||
      undefined;
    const store = createFilesystemDurablePublishedStore({
      env,
      root: root || undefined,
    });
    const entry = store.getCurrentEntry(id);
    if (!entry?.periodId) return null;
    let manifest = null;
    try {
      manifest = store.readPublishedManifest(id);
    } catch {
      manifest = null;
    }
    let reportProperty = null;
    try {
      const resolved = store.readPublishedReport(id);
      const payload = resolved?.payload || resolved?.report?.payload || resolved?.report;
      reportProperty = payload?.property || null;
    } catch {
      reportProperty = null;
    }
    const name =
      nonEmptyString(manifest?.propertyName) ||
      nonEmptyString(entry.propertyName) ||
      nonEmptyString(reportProperty?.name);
    if (!name) return null;
    return {
      propertyId: id,
      name,
      city: nonEmptyString(manifest?.city) || nonEmptyString(reportProperty?.city) || "",
      state: nonEmptyString(manifest?.state) || nonEmptyString(reportProperty?.state) || "",
      market: nonEmptyString(manifest?.market) || nonEmptyString(reportProperty?.market) || "",
      country: nonEmptyString(manifest?.country) || nonEmptyString(reportProperty?.country) || "",
      customerDropdownVisible: false,
      _adpProfileSource: "durable_published",
      _adpDurablePeriodId: entry.periodId,
      _adpDurableReportSha256: entry.reportSha256 || null,
    };
  } catch {
    return null;
  }
}

export function loadPropertyProfile(propertyId, opts = {}) {
  const id = safePropertyId(propertyId);
  if (!id) return null;
  const fixturesDir = opts.fixturesDir || join(process.cwd(), "fixtures/ai-demand-positioning");
  if (existsSync(fixturesDir)) {
    const files = readdirSync(fixturesDir).filter(
      (f) => f.endsWith(".json") && !f.startsWith("adp_period_")
    );
    for (const file of files) {
      const data = JSON.parse(readFileSync(join(fixturesDir, file), "utf-8"));
      if (data.propertyId === id) return data;
    }
  }
  return (
    profileFromPublishedManifest(id, opts.publishedDir) ||
    profileFromDurablePublished(id, opts)
  );
}

/**
 * Customer dropdown + local founder-preview discovery.
 * - customerDropdownVisible === false → excluded from customer publish surface
 * - founderPreviewEligible === true → included for local/founder review only
 *   (never implies CUSTOMER_READY / customer publish)
 */
export function listPropertyProfiles(opts = {}) {
  const includeFounderPreview = opts.includeFounderPreview !== false;
  const fixturesDir = join(process.cwd(), "fixtures/ai-demand-positioning");
  if (!existsSync(fixturesDir)) return [];
  const files = readdirSync(fixturesDir).filter(
    (f) => f.endsWith("-property-profile.json") && f.endsWith(".json")
  );
  const profiles = [];
  for (const file of files) {
    const data = JSON.parse(readFileSync(join(fixturesDir, file), "utf-8"));
    if (!data.propertyId || !data.name) continue;
    const customerVisible = data.customerDropdownVisible !== false;
    const founderPreview =
      includeFounderPreview &&
      data.customerDropdownVisible === false &&
      data.founderPreviewEligible === true;
    // Dropdown release: certified customer-visible OR founder-preview staged.
    if (!customerVisible && !founderPreview) continue;
    const city = data.city || "";
    const state = data.state || "";
    const region = data.region || "";
    const country = data.country || "";
    const locationLine = formatPropertyLocationLine({ city, state, region, country });
    const baseLabel = formatPropertySelectorLabel({
      name: data.name,
      city,
      state,
      region,
      country,
    });
    const label = founderPreview
      ? `${baseLabel} · Founder Reviewed — Staged`
      : baseLabel;
    profiles.push({
      propertyId: data.propertyId,
      name: data.name,
      city,
      state,
      region,
      country,
      locationLine,
      label,
      optionLabel: label,
      chainScale: data.chainScale || "",
      affiliation: data.affiliation || "",
      customerDropdownVisible: customerVisible,
      founderPreviewEligible: data.founderPreviewEligible === true,
      founderPreview,
    });
  }
  return profiles.sort((a, b) => a.name.localeCompare(b.name));
}

export const PROVIDERS = Object.freeze(["openai", "gemini", "perplexity", "claude"]);

export const PROVIDER_LABELS = Object.freeze({
  openai: "OpenAI",
  gemini: "Gemini",
  perplexity: "Perplexity",
  claude: "Claude",
});
