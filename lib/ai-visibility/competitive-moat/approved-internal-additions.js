/**
 * Founder-approved INTERNAL_BENCHMARK_ONLY brand additions.
 * Not customer-visible — used for AI Presence Index pilot cohort only.
 */

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DEFAULT_PATH = path.join(
  __dirname,
  "..",
  "..",
  "..",
  "fixtures",
  "ai-visibility",
  "internal-benchmark-additions-v1.json"
);

export const APPROVED_INTERNAL_ADDITION_COUNT = 7;

export function loadApprovedInternalAdditionsConfig(filePath = DEFAULT_PATH) {
  const raw = JSON.parse(fs.readFileSync(filePath, "utf8"));
  return raw;
}

export function listApprovedInternalAdditionIds(config) {
  const cfg = config || loadApprovedInternalAdditionsConfig();
  return (cfg.additions || []).map((a) => a.brandId).filter(Boolean);
}

/**
 * Verify all 7 approved brands against Active/Live universe inventory.
 */
export function verifyApprovedInternalAdditions(inventory = [], config) {
  const cfg = config || loadApprovedInternalAdditionsConfig();
  const additions = cfg.additions || [];
  const results = [];
  let allSafe = true;

  for (const add of additions) {
    const row =
      inventory.find((b) => b.recordId === add.brandId) ||
      inventory.find((b) => b.slug === add.slug) ||
      inventory.find((b) => b.brandName === add.brandName);

    const identitySafe =
      Boolean(row?.recordId) &&
      row.publicFull === true &&
      row.pvqlPass === true &&
      add.internalBenchmarkOnly === true &&
      add.brandId === row.recordId;

    if (!identitySafe) allSafe = false;

    results.push({
      brand: add.brandName,
      brandId: add.brandId,
      parent: add.canonicalParent,
      status: row ? (row.publicFull && row.pvqlPass ? "ACTIVE" : "OTHER") : "NOT_IN_ACTIVE_UNIVERSE",
      identitySafe: identitySafe ? "YES" : "NO",
      customerVisible: false,
      internalBenchmarkOnly: true,
      aliases: add.aliases || [],
      cohortTags: add.cohortTags || [],
    });
  }

  return {
    ok: allSafe && results.length === APPROVED_INTERNAL_ADDITION_COUNT,
    count: results.length,
    expectedCount: APPROVED_INTERNAL_ADDITION_COUNT,
    additions: results,
    excluded: results.filter((r) => r.identitySafe === "NO"),
  };
}
