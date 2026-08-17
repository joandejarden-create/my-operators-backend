/**
 * Geography eligibility for AI Visibility brands (Phase 3A.7 / 3A.8).
 * DEVELOPMENT_ELIGIBILITY (Region Offered) ≠ OPERATING_PRESENCE (Footprint).
 */

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

export const GEOGRAPHY_ELIGIBILITY_CONFIG_ID = "brand_ai_visibility_geography_eligibility_v1";
export const GEOGRAPHY_SCOPES = Object.freeze([
  "Global",
  "CALA",
  "Europe",
  "North America",
  "Mexico",
]);

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DEFAULT_PATH = path.join(
  __dirname,
  "..",
  "..",
  "fixtures",
  "ai-visibility",
  "brand-geography-eligibility-v1.json"
);

export function loadGeographyEligibilityConfig(filePath = DEFAULT_PATH) {
  return JSON.parse(fs.readFileSync(filePath, "utf8"));
}

/**
 * Structural development eligibility from Region Offered (not hotel count presence).
 */
export function getBrandGeographyEligibility(brandId, config) {
  const cfg = config || loadGeographyEligibilityConfig();
  const row = (cfg.brands || []).find((b) => b.brandId === brandId);
  if (!row) {
    return {
      brandId,
      GLOBAL: cfg.defaultState || "UNKNOWN",
      CALA: cfg.defaultState || "UNKNOWN",
      EUROPE: cfg.defaultState || "UNKNOWN",
      NORTH_AMERICA: cfg.defaultState || "UNKNOWN",
      MEXICO: cfg.defaultState || "UNKNOWN",
      SOURCE: cfg.defaultSource || "No geography eligibility row",
      QUALITY: "LOW",
      MEXICO_UNDER_CALA: cfg.mexicoUnderCala !== false,
      LANGUAGE_INDEPENDENT: true,
      OPERATING_PRESENCE: null,
      DEVELOPMENT_ELIGIBILITY: null,
    };
  }
  return {
    brandId,
    brandName: row.brandName || null,
    GLOBAL: row.Global || row.GLOBAL || cfg.defaultState || "UNKNOWN",
    CALA: row.CALA || cfg.defaultState || "UNKNOWN",
    EUROPE: row.Europe || row.EUROPE || cfg.defaultState || "UNKNOWN",
    NORTH_AMERICA:
      row["North America"] || row.NORTH_AMERICA || cfg.defaultState || "UNKNOWN",
    MEXICO: row.Mexico || row.MEXICO || cfg.defaultState || "UNKNOWN",
    SOURCE: row.source || cfg.defaultSource,
    QUALITY: row.quality || "LOW",
    MEXICO_UNDER_CALA: cfg.mexicoUnderCala !== false,
    LANGUAGE_INDEPENDENT: true,
    OPERATING_PRESENCE: row.OPERATING_PRESENCE || null,
    DEVELOPMENT_ELIGIBILITY: row.DEVELOPMENT_ELIGIBILITY || null,
    regionOffered: row.regionOffered || null,
  };
}

/**
 * Mexico showcase safety: UNKNOWN country eligibility must not auto-exclude.
 */
export function isSafeForMexicoShowcase(brandId, config) {
  const geo = getBrandGeographyEligibility(brandId, config);
  return {
    brandId,
    SAFE_FOR_MEXICO_SHOWCASE: geo.MEXICO !== "NOT_ELIGIBLE",
    MEXICO_DEVELOPMENT_ELIGIBILITY: geo.MEXICO,
    MEXICO_OPERATING_PRESENCE: geo.OPERATING_PRESENCE?.MEXICO || "UNKNOWN",
    MEXICO_UNDER_CALA: geo.MEXICO_UNDER_CALA,
    reason:
      geo.MEXICO === "NOT_ELIGIBLE"
        ? "Governed evidence excludes Mexico."
        : "Mexico UNKNOWN or eligible — UNKNOWN ≠ NOT_ELIGIBLE for showcase inclusion under CALA.",
  };
}
