/**
 * Benchmark-eligible brand universe — distinct from customer-visible and peer v5.
 * Does not mutate peer sets v2–v5.
 */

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { listShowcaseMonitoringBrandIds } from "../brand-ai-showcase-companies.js";
import { IDS } from "./benchmark-brand-ids.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DEFAULT_PATH = path.join(
  __dirname,
  "..",
  "..",
  "..",
  "fixtures",
  "ai-visibility",
  "benchmark-eligible-universe-v1.json"
);

export const BENCHMARK_ELIGIBLE_UNIVERSE_ID = "benchmark_eligible_brands_v1";
export const CUSTOMER_VISIBLE_MUST_BE_ELIGIBLE = true;

export function loadBenchmarkEligibleUniverse(filePath = DEFAULT_PATH) {
  const raw = JSON.parse(fs.readFileSync(filePath, "utf8"));
  return {
    ...raw,
    configVersion: raw.configVersion || BENCHMARK_ELIGIBLE_UNIVERSE_ID,
  };
}

export function listBenchmarkEligibleMembers(config) {
  const cfg = config || loadBenchmarkEligibleUniverse();
  return [...(cfg.members || [])];
}

export function getBenchmarkEligibleMember(brandId, config) {
  return listBenchmarkEligibleMembers(config).find((m) => m.brandId === brandId) || null;
}

export function isBenchmarkEligible(brandId, config) {
  return Boolean(getBenchmarkEligibleMember(brandId, config));
}

export function listCustomerVisibleEligibleIds(config) {
  return listBenchmarkEligibleMembers(config)
    .filter((m) => m.customerVisible)
    .map((m) => m.brandId);
}

/**
 * Customer-visible brands must be allowed to serve as peers.
 * This is eligibility, not automatic comparability.
 */
export function auditCustomerVisibleBenchmarkEligibility(config) {
  const cfg = config || loadBenchmarkEligibleUniverse();
  const showcaseIds = listShowcaseMonitoringBrandIds();
  const eligible = new Set(listBenchmarkEligibleMembers(cfg).map((m) => m.brandId));
  const missing = showcaseIds.filter((id) => !eligible.has(id));
  const required = [IDS.VIGNETTE, IDS.VOCO, IDS.RADISSON, IDS.RAD_BLU];
  const requiredMissing = required.filter((id) => !eligible.has(id));
  return {
    ok: missing.length === 0 && requiredMissing.length === 0,
    customerVisibleCount: showcaseIds.length,
    eligibleCount: eligible.size,
    missingFromEligible: missing,
    requiredVisiblePeersPresent: requiredMissing.length === 0,
    requiredMissing,
    CUSTOMER_VISIBLE_MUST_BE_ELIGIBLE,
  };
}
