/**
 * Baseline audit — federated Brand AI measured baseline (read-only).
 */

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { createBrandAiVisibilityReadStore } from "../storage/index.js";
import { resolveBrandAiVisibilityReadRoots } from "../storage/resolve-store-root.js";
import { readBaselineFreezeMarker, BASELINE_FREEZE_ID } from "../baseline-freeze.js";
import { loadShowcaseCompaniesConfig, getShowcasePortfolioBrandIds } from "../brand-ai-showcase-companies.js";
import { normalizeMeasurementDate } from "./grain.js";

export const PRIMARY_BASELINE_DATE = "2026-08-14";

export const BRAND_LONGITUDINAL_BASELINE_AUDIT_VERSION = "brand_longitudinal_baseline_audit_v1";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

function countPromptsInSummaries(summaries) {
  const promptIds = new Set();
  for (const s of summaries || []) {
    const slots = s.slots;
    if (Array.isArray(slots)) {
      for (const slot of slots) {
        if (typeof slot === "string") promptIds.add(slot);
        else if (slot?.promptId) promptIds.add(slot.promptId);
      }
    }
    const ids = s.promptIds;
    if (Array.isArray(ids)) {
      for (const id of ids) promptIds.add(id);
    }
  }
  return promptIds.size;
}

/**
 * Audit existing federated baseline from file store (no synthetic inference).
 */
export async function auditBrandLongitudinalBaseline(opts = {}) {
  const store = opts.store || createBrandAiVisibilityReadStore(opts);
  const roots = resolveBrandAiVisibilityReadRoots(opts);
  const summaries = (await store.listBatchSummaries?.()) || [];

  const measurementDates = new Set();
  const providers = new Set();
  const languages = new Set();
  const geographies = new Set();

  for (const s of summaries) {
    const d = normalizeMeasurementDate(s.completedAt || s.batchDate || s.startedAt || s.savedAt);
    if (d) measurementDates.add(d);
    const prov = s.provider?.name || s.provider;
    if (prov) providers.add(String(prov).toLowerCase());
    if (s.language) languages.add(s.language);
    if (s.commercialRegion || s.geographyScope) {
      geographies.add(s.commercialRegion || s.geographyScope);
    }
  }

  const freeze = readBaselineFreezeMarker(opts.baselineFreezeRoot);
  const showcase = getShowcasePortfolioBrandIds("marriott", loadShowcaseCompaniesConfig());

  const promptFixturePaths = [
    path.join(__dirname, "..", "..", "..", "fixtures", "ai-visibility", "phase3a9-showcase-prompt-seed.json"),
    path.join(__dirname, "..", "..", "..", "fixtures", "ai-visibility", "phase2d-prompt-seed.json"),
    path.join(__dirname, "..", "..", "..", "fixtures", "ai-visibility", "observed-demand-prompts-v1.json"),
  ];

  const promptMap = new Map();
  for (const fp of promptFixturePaths) {
    if (!fs.existsSync(fp)) continue;
    const j = JSON.parse(fs.readFileSync(fp, "utf8"));
    for (const p of j.prompts || j) {
      if (!p.promptId) continue;
      promptMap.set(p.promptId, p);
    }
  }

  const active = [...promptMap.values()].filter((p) => p.active !== false);
  const monitoringEligible = active.filter((p) => p.monitoringEligible !== false);
  const origins = { SCENARIO: 0, OBSERVED: 0, DERIVED: 0, LEGACY: 0 };
  for (const p of active) {
    const o = p.promptOrigin || "SCENARIO";
    if (o === "SCENARIO" && p.governanceStatus === "Legacy") origins.LEGACY += 1;
    else if (origins[o] != null) origins[o] += 1;
    else origins.SCENARIO += 1;
  }

  const distinctDates = [...measurementDates].sort();
  const realDistinctPeriods = distinctDates.length;

  let clientState = "BASELINE_ONLY";
  if (realDistinctPeriods >= 6) clientState = "TREND";
  else if (realDistinctPeriods >= 4) clientState = "TREND";
  else if (realDistinctPeriods === 3) clientState = "EARLY_TREND";
  else if (realDistinctPeriods === 2) clientState = "CURRENT_VS_PRIOR";

  return {
    version: BRAND_LONGITUDINAL_BASELINE_AUDIT_VERSION,
    REAL_MEASUREMENT_DATES: distinctDates,
    PRIMARY_BASELINE_DATE: distinctDates.includes(PRIMARY_BASELINE_DATE)
      ? PRIMARY_BASELINE_DATE
      : distinctDates[0] || null,
    PROMPTS: {
      TOTAL_ACTIVE: active.length,
      MONITORING_ELIGIBLE: monitoringEligible.length,
      OBSERVED_ACTIVE: active.filter((p) => p.promptOrigin === "OBSERVED").length,
      DERIVED_ACTIVE: active.filter((p) => p.promptOrigin === "DERIVED").length,
      SCENARIO_ACTIVE: active.filter((p) => !p.promptOrigin || p.promptOrigin === "SCENARIO").length,
      LEGACY_ACTIVE: origins.LEGACY,
      PROPOSED_LIBRARY_TOTAL: active.length,
    },
    PROMPTS_WITH_BASELINE: countPromptsInSummaries(summaries) || freeze?.observationCount / 4 || null,
    PROVIDER_COVERAGE: [...providers],
    BRAND_COVERAGE: showcase.ok ? showcase.brandIds : [],
    GEO_LANGUAGE_COVERAGE: {
      geographies: [...geographies],
      languages: [...languages],
    },
    REAL_DISTINCT_PERIODS: realDistinctPeriods,
    CLIENT_STATE: clientState,
    SYNTHETIC_POINTS: 0,
    BASELINE_FREEZE_ID: freeze?.freezeId || BASELINE_FREEZE_ID,
    STORE_MODE: roots.mode,
    STORE_ROOTS: roots.rootDirs || [roots.rootDir].filter(Boolean),
    DATASET_CLASS: "DEMO_VALIDATION",
    ENVIRONMENT: "railway_staging_brand_ai_service",
  };
}
