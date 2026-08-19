/**
 * AI Demand Positioning — Core Data Model.
 * Defines entities, period management, and observation storage.
 */

import { readFileSync, writeFileSync, existsSync, readdirSync } from "fs";
import { join } from "path";
import crypto from "crypto";

const RUNTIME_DIR = join(process.cwd(), "data/ai-demand-positioning/runtime");

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

export function loadLatestPeriod(propertyId) {
  if (!existsSync(RUNTIME_DIR)) return null;
  const files = readdirSync(RUNTIME_DIR)
    .filter((f) => f.startsWith("adp_period_") && f.includes(propertyId) && f.endsWith(".json"))
    .sort()
    .reverse();
  if (!files.length) return null;
  return JSON.parse(readFileSync(join(RUNTIME_DIR, files[0]), "utf-8"));
}

export function loadAllPeriods(propertyId) {
  if (!existsSync(RUNTIME_DIR)) return [];
  const files = readdirSync(RUNTIME_DIR)
    .filter((f) => f.startsWith("adp_period_") && f.includes(propertyId) && f.endsWith(".json"))
    .sort();
  return files.map((f) => JSON.parse(readFileSync(join(RUNTIME_DIR, f), "utf-8")));
}

export function loadPropertyProfile(propertyId) {
  const fixturesDir = join(process.cwd(), "fixtures/ai-demand-positioning");
  const files = readdirSync(fixturesDir).filter((f) => f.endsWith(".json"));
  for (const file of files) {
    const data = JSON.parse(readFileSync(join(fixturesDir, file), "utf-8"));
    if (data.propertyId === propertyId) return data;
  }
  return null;
}

export const PROVIDERS = Object.freeze(["openai", "gemini", "perplexity", "claude"]);

export const PROVIDER_LABELS = Object.freeze({
  openai: "OpenAI",
  gemini: "Gemini",
  perplexity: "Perplexity",
  claude: "Claude",
});
