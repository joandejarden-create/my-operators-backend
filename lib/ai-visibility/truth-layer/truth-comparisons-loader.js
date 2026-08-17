/**
 * Read-only loader for cached Truth Layer comparisons (P0E / P0D-A).
 * No provider calls. No rule changes.
 */

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.join(__dirname, "..", "..", "..");

export const DEFAULT_TRUTH_COMPARISONS_DIR = path.join(
  REPO_ROOT,
  "data",
  "ai-visibility",
  "truth-comparisons"
);

const CANDIDATE_FILES = [
  "latest-truth-comparisons-v1_1_semantic.json",
  "latest-truth-comparisons-v1.json",
];

/**
 * @param {string} [dir]
 * @returns {{ comparisons: object[], sourcePath: string|null, ruleVersion: string|null }}
 */
export function loadCachedTruthComparisons(dir = DEFAULT_TRUTH_COMPARISONS_DIR) {
  for (const file of CANDIDATE_FILES) {
    const p = path.join(dir, file);
    if (!fs.existsSync(p)) continue;
    try {
      const raw = JSON.parse(fs.readFileSync(p, "utf8"));
      const comparisons = Array.isArray(raw?.comparisons)
        ? raw.comparisons
        : Array.isArray(raw)
          ? raw
          : [];
      return {
        comparisons,
        sourcePath: p,
        ruleVersion: raw?.truthRuleVersion || raw?.ruleVersion || null,
      };
    } catch (err) {
      if (typeof console !== "undefined" && console.warn) {
        console.warn("[truth-comparisons-loader] parse failed:", p, err.message);
      }
    }
  }
  return { comparisons: [], sourcePath: null, ruleVersion: null };
}

/**
 * Filter comparisons for executive / detail intelligence cohort.
 * @param {object[]} comparisons
 * @param {object} [opts]
 */
export function filterTruthComparisonsForCohort(comparisons = [], opts = {}) {
  const language = String(opts.language || "en").toLowerCase().startsWith("es") ? "es" : "en";
  const geography = opts.geography || null;
  const brandIds = opts.brandIds?.length ? new Set(opts.brandIds) : null;

  return comparisons.filter((c) => {
    if (brandIds && !brandIds.has(c.subjectBrandId)) return false;
    const lang = String(c.language || "en").toLowerCase();
    const normLang = lang.startsWith("es") ? "es" : "en";
    if (normLang !== language) return false;
    if (geography && c.geography && c.geography !== geography && c.geography !== "Global") {
      return false;
    }
    return true;
  });
}
