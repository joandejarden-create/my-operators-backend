/**
 * Hilton directory → amenities index (ctyhocn → amenities text from hilton.com).
 */

import { readFileSync, existsSync } from "node:fs";
import { crawlHiltonBrandDirectory } from "../hilton-brand-directory-extract.js";
import { loadHiltonBrandDirectoryConfigs } from "../hilton-brand-registry.js";
import { formatAmenitiesText } from "../hilton-amenity-map.js";

/** Properties absent from directory crawl — amenities from hilton.com hotel pages (manual steward). */
export const HILTON_MANUAL_AMENITIES = {
  MIDYUGI:
    "Business center; Fitness center; Free WiFi; Meeting rooms; Non-smoking rooms; On-site restaurant; Outdoor pool; Pets not allowed",
  LRMFMHH:
    "All-inclusive; Beach access; Fitness center; Free WiFi; Meeting rooms; Non-smoking rooms; On-site restaurant; Outdoor pool; Spa; Tennis",
  LRMDOHH:
    "Adults only; All-inclusive; Beach access; Fitness center; Free WiFi; Non-smoking rooms; On-site restaurant; Outdoor pool; Spa",
};

/**
 * @param {string} planPath
 * @returns {Map<string, { amenitiesText: string, directoryName: string, sourceUrl: string }>}
 */
export function loadHiltonAmenityIndexFromEnrichmentPlan(planPath) {
  /** @type {Map<string, { amenitiesText: string, directoryName: string, sourceUrl: string }>} */
  const byCode = new Map();
  if (!existsSync(planPath)) return byCode;

  const plan = JSON.parse(readFileSync(planPath, "utf8"));
  for (const row of plan.planRows || []) {
    const code = String(row.directoryBrandPropertyCode || row.ctyhocn || "").trim().toUpperCase();
    const text = String(row.amenitiesTextSuggested || row.applyFields?.Amenities || "").trim();
    if (!code || !text) continue;
    byCode.set(code, {
      amenitiesText: text,
      directoryName: String(row.directoryName || "").trim(),
      sourceUrl: String(row.sourceUrl || "").trim(),
    });
  }
  return byCode;
}

/**
 * @param {object} [opts]
 * @param {(msg: string) => void} [opts.onProgress]
 * @param {number} [opts.crawlDelayMs]
 * @param {string[]} [opts.brandCodes]
 */
export async function buildHiltonAmenityIndexFromCrawl(opts = {}) {
  const configs = await loadHiltonBrandDirectoryConfigs({
    brandCodes: opts.brandCodes,
  });
  /** @type {Map<string, { amenitiesText: string, directoryName: string, sourceUrl: string }>} */
  const byCode = new Map();

  for (let i = 0; i < configs.length; i++) {
    const cfg = configs[i];
    if (opts.onProgress) {
      opts.onProgress(`[${i + 1}/${configs.length}] Crawl ${cfg.canonicalBrandName} (${cfg.brandCode})`);
    }
    const crawl = await crawlHiltonBrandDirectory({
      brandConfig: cfg,
      delayMs: opts.crawlDelayMs ?? 200,
    });
    for (const hotel of crawl.hotels) {
      const code = String(hotel.ctyhocn || "").trim().toUpperCase();
      const text = formatAmenitiesText(hotel.amenityIds || []);
      if (!code || !text) continue;
      byCode.set(code, {
        amenitiesText: text,
        directoryName: hotel.name,
        sourceUrl: hotel.sourceUrl,
      });
    }
  }

  return byCode;
}

function applyManualAmenities(index) {
  for (const [code, text] of Object.entries(HILTON_MANUAL_AMENITIES)) {
    if (!index.has(code)) {
      index.set(code, { amenitiesText: text, directoryName: "", sourceUrl: "manual_steward" });
    }
  }
  return index;
}

/**
 * @param {object} [opts]
 */
export async function loadHiltonAmenityIndex(opts = {}) {
  const planPath =
    opts.enrichmentPlanPath || "reports/hilton-census-enrichment-plan-all-brands.json";

  if (opts.refreshCrawl) {
    return applyManualAmenities(await buildHiltonAmenityIndexFromCrawl(opts));
  }

  const index = loadHiltonAmenityIndexFromEnrichmentPlan(planPath);
  if (index.size > 0) {
    return applyManualAmenities(index);
  }

  return applyManualAmenities(await buildHiltonAmenityIndexFromCrawl(opts));
}

/** Normalize amenities text for change detection. */
export function normalizeAmenitiesCompare(text) {
  return String(text || "")
    .split(/[;,]/)
    .map((s) => s.trim().toLowerCase())
    .filter(Boolean)
    .sort()
    .join("|");
}
