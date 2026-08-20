/**
 * AI Demand Positioning — Core Data Model.
 * Defines entities, period management, and observation storage.
 */

import { readFileSync, writeFileSync, existsSync, readdirSync } from "fs";
import { join } from "path";
import crypto from "crypto";

const RUNTIME_DIR = join(process.cwd(), "data/ai-demand-positioning/runtime");
const SEED_DIR = join(process.cwd(), "fixtures/ai-demand-positioning/seed-periods");

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

export function loadLatestPeriod(propertyId, options = {}) {
  const { includeTargeted = false } = options;
  const runtimeFiles = listPeriodFiles(propertyId, RUNTIME_DIR);
  if (runtimeFiles.length) {
    for (let i = runtimeFiles.length - 1; i >= 0; i--) {
      const period = readPeriodFile(RUNTIME_DIR, runtimeFiles[i]);
      if (!includeTargeted && isTargetedMeasurementPeriod(period)) continue;
      return period;
    }
  }

  const seedFiles = listPeriodFiles(propertyId, SEED_DIR);
  if (seedFiles.length) {
    return readPeriodFile(SEED_DIR, seedFiles[seedFiles.length - 1]);
  }

  return null;
}

/**
 * Customer-facing current period: latest certified official full-property period.
 * Falls back to latest full-property only when no official period exists yet
 * (pre-baseline migration window). After Period 001 certification, official wins.
 */
export function loadLatestCustomerPeriod(propertyId) {
  const all = loadAllPeriods(propertyId);
  const official = all
    .filter(
      (p) =>
        p?.officialPeriod === true &&
        p?.measurementPhase === "OFFICIAL_PRODUCTION" &&
        p?.certified === true &&
        p?.customerVisible !== false &&
        !isTargetedMeasurementPeriod(p)
    )
    .sort((a, b) => String(a.executionDate || "").localeCompare(String(b.executionDate || "")));
  if (official.length) return official[official.length - 1];
  return loadLatestPeriod(propertyId);
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

export function loadPropertyProfile(propertyId) {
  const fixturesDir = join(process.cwd(), "fixtures/ai-demand-positioning");
  const files = readdirSync(fixturesDir).filter((f) => f.endsWith(".json") && !f.startsWith("adp_period_"));
  for (const file of files) {
    const data = JSON.parse(readFileSync(join(fixturesDir, file), "utf-8"));
    if (data.propertyId === propertyId) return data;
  }
  return null;
}

export function listPropertyProfiles() {
  const fixturesDir = join(process.cwd(), "fixtures/ai-demand-positioning");
  if (!existsSync(fixturesDir)) return [];
  const files = readdirSync(fixturesDir).filter(
    (f) => f.endsWith("-property-profile.json") && f.endsWith(".json")
  );
  const profiles = [];
  for (const file of files) {
    const data = JSON.parse(readFileSync(join(fixturesDir, file), "utf-8"));
    if (!data.propertyId || !data.name) continue;
    // Dropdown release only after certification (customerDropdownVisible !== false).
    if (data.customerDropdownVisible === false) continue;
    profiles.push({
      propertyId: data.propertyId,
      name: data.name,
      city: data.city || "",
      state: data.state || "",
      chainScale: data.chainScale || "",
      affiliation: data.affiliation || "",
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
