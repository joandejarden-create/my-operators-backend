/**
 * Plan Hilton website + property-code backfill for census rows missing hilton.com links.
 */

import { readFileSync, existsSync } from "node:fs";
import {
  fetchHiltonLocationsPage,
  extractHotelsFromPageData,
  normalizeHiltonDirectoryHotel,
  hiltonLocationsUrl,
} from "../hilton-brand-directory-extract.js";
import { loadHiltonBrandDirectoryConfigs, affiliationHintsForBrand } from "../hilton-brand-registry.js";
import { fetchHiltonHotelDescription, pickPrimaryHiltonDescription } from "../hilton-hotel-description-fetch.js";
import { fetchHiltonHotelStatus } from "../hilton-hotel-status-fetch.js";
import { nameSimilarity, normalizeKey, normalizeText, citiesMatch } from "../independent-census/match-current-census.js";
import { ctyhocnFromWebsite } from "./match-brand-directory-to-census.js";
import { MAP_DIRECTORY_ENRICHMENT } from "./brand-directory-enrichment-contract.js";
import {
  buildDescriptionEnrichmentFields,
  probeCensusDescriptionFields,
  CENSUS_DESCRIPTION_FIELD,
} from "./hilton-description-enrichment-contract.js";
import { normalizeCensusStatus, buildStatusCorrectionFields } from "./audit-hilton-census-status.js";
import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS, STATUS_OPEN } from "./fields.js";
import { getPlatformBase } from "./platform-base.js";
import { getCensusEnrichmentSelectFields } from "./probe-census-enrichment-fields.js";
import { HILTON_MANUAL_PROPERTY_LINKS } from "./hilton-manual-property-links.js";

const COUNTRY_SLUGS = {
  mexico: "mexico",
  "dominican republic": "dominican-republic",
  "costa rica": "costa-rica",
  panama: "panama",
  bahamas: "bahamas",
  jamaica: "jamaica",
  "trinidad and tobago": "trinidad-and-tobago",
};

const HILTON_PARENT_FORMULA = `FIND("Hilton", {${CENSUS_FIELDS.parentCompany}})`;

const MANUAL_LINKS = HILTON_MANUAL_PROPERTY_LINKS;

/** GraphQL misses — copy from hilton.com marketing pages. */
const MANUAL_DESCRIPTIONS = {
  PTYHXHX:
    "We're off Highway 3 in downtown Panama City, close to the financial district. Enjoy our rooftop pool, and excellent access throughout the city via the nearby metro station. The University of Panama is less than 10 minutes away. Visit the Biodiversity Museum, around 10km from the hotel. Tocumen International Airport is 25 minutes' drive. Daily hot breakfast and WiFi are on us.",
};

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function citySlug(city) {
  return normalizeKey(city).replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "");
}

function countrySlug(country) {
  const k = normalizeKey(country);
  return COUNTRY_SLUGS[k] || k.replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "");
}

function resolveBrandConfig(affiliation, configs) {
  const aff = normalizeText(affiliation);
  for (const cfg of configs) {
    const hints = affiliationHintsForBrand(cfg);
    if (hints.some((h) => aff === h || aff.includes(h) || h.includes(aff))) return cfg;
  }
  return null;
}

function hiltonHotelUrl(ctyhocn, website) {
  const w = String(website || "").trim();
  if (w.includes("hilton.com") && w.includes("/hotels/")) return w;
  const code = String(ctyhocn || "").trim().toLowerCase();
  if (!code) return "";
  return `https://www.hilton.com/en/hotels/${code}-hotel/`;
}

function loadEnrichmentLinkMap(planPath) {
  if (!existsSync(planPath)) return new Map();
  const plan = JSON.parse(readFileSync(planPath, "utf8"));
  /** @type {Map<string, { ctyhocn: string, website: string, source: string }>} */
  const map = new Map();
  for (const row of plan.planRows || []) {
    const id = String(row.censusRecordId || "").trim();
    const code = String(row.directoryBrandPropertyCode || "").trim().toUpperCase();
    if (!id || !code || row.matchConfidence === "none") continue;
    const website =
      row.applyFields?.Website ||
      row.sourceUrl ||
      hiltonHotelUrl(code, row.website);
    map.set(id, { ctyhocn: code, website: hiltonHotelUrl(code, website), source: "enrichment_plan" });
  }
  return map;
}

function hasHiltonWebsite(website) {
  return Boolean(ctyhocnFromWebsite(website));
}

/**
 * @param {{ openOnly?: boolean, includePipeline?: boolean }} [opts]
 */
export async function loadHiltonRowsMissingWebsite(opts = {}) {
  const base = getPlatformBase();
  const fields = await getCensusEnrichmentSelectFields(base);
  for (const f of ["Hotel Description", "Open Date", "projected_open_date"]) {
    if (!fields.includes(f)) {
      try {
        await base(HOTEL_CENSUS_TABLE).select({ fields: [f], maxRecords: 1 }).firstPage();
        fields.push(f);
      } catch {
        // optional
      }
    }
  }

  const records = await base(HOTEL_CENSUS_TABLE)
    .select({ fields, filterByFormula: HILTON_PARENT_FORMULA, pageSize: 100 })
    .all();

  let rows = records
    .filter((r) => !hasHiltonWebsite(r.fields?.Website))
    .map((r) => ({
      recordId: r.id,
      name: normalizeText(r.fields[CENSUS_FIELDS.name]),
      affiliation: normalizeText(r.fields[CENSUS_FIELDS.affiliation]),
      city: normalizeText(r.fields[CENSUS_FIELDS.city]),
      country: normalizeText(r.fields[CENSUS_FIELDS.country]),
      status: normalizeCensusStatus(r.fields[CENSUS_FIELDS.status]),
      fields: r.fields,
    }));

  if (opts.openOnly) rows = rows.filter((r) => r.status === STATUS_OPEN);
  if (!opts.includePipeline) {
    // default: process all missing website; caller can filter
  }
  return rows;
}

/**
 * @param {object[]} censusRows
 * @param {object} [opts]
 */
export async function planHiltonWebsiteBackfill(censusRows, opts = {}) {
  const enrichmentMap = loadEnrichmentLinkMap(
    opts.enrichmentPlanPath || "reports/hilton-census-enrichment-plan-all-brands.json"
  );
  const configs = await loadHiltonBrandDirectoryConfigs();
  const base = getPlatformBase();
  const presentDescFields = await probeCensusDescriptionFields(base);

  const planRows = [];
  const skipped = [];
  /** @type {Map<string, object[]>} */
  const cityCache = new Map();

  for (let i = 0; i < censusRows.length; i++) {
    const row = censusRows[i];
    let link = enrichmentMap.get(row.recordId);
    let matchSource = link?.source || "";

    if (!link) {
      const manual = MANUAL_LINKS.find((m) => m.recordId === row.recordId);
      if (manual) {
        link = { ...manual, source: "manual" };
        matchSource = "manual";
      }
    }

    if (!link) {
      const brandCfg = resolveBrandConfig(row.affiliation, configs);
      const cSlug = countrySlug(row.country);
      const ciSlug = citySlug(row.city);
      if (brandCfg && cSlug && ciSlug) {
        const pageUrl = hiltonLocationsUrl(`locations/${cSlug}/${ciSlug}/${brandCfg.locationsSlug}/`);
        let directoryHotels = cityCache.get(pageUrl);
        if (!directoryHotels) {
          if (opts.onProgress) opts.onProgress(`City page ${pageUrl}`);
          try {
            const page = await fetchHiltonLocationsPage(pageUrl);
            directoryHotels = extractHotelsFromPageData(page.pageData)
              .filter((h) => String(h?.brandCode || "").trim() === brandCfg.brandCode)
              .map((h) => normalizeHiltonDirectoryHotel(h, { sourceUrl: pageUrl }));
            cityCache.set(pageUrl, directoryHotels);
          } catch (err) {
            skipped.push({ ...row, reason: "city_page_error", error: err?.message || String(err) });
            if (opts.pageDelayMs) await sleep(opts.pageDelayMs);
            continue;
          }
          if (opts.pageDelayMs) await sleep(opts.pageDelayMs);
        }

        let best = null;
        for (const d of directoryHotels) {
          const sim = nameSimilarity(row.name, d.name);
          if (sim >= (opts.minNameSim ?? 0.5) && citiesMatch(row.city, d.city) && (!best || sim > best.sim)) {
            best = { hotel: d, sim };
          }
        }
        if (best) {
          link = {
            ctyhocn: best.hotel.ctyhocn,
            website: hiltonHotelUrl(best.hotel.ctyhocn, best.hotel.website),
            source: "city_page",
            directoryName: best.hotel.name,
            matchNameSim: best.sim,
          };
          matchSource = "city_page";
        }
      }
    }

    if (!link?.ctyhocn) {
      skipped.push({ ...row, reason: "no_link" });
      continue;
    }

    if (opts.onProgress) {
      opts.onProgress(`[${i + 1}/${censusRows.length}] ${link.ctyhocn} — ${row.name} (${matchSource})`);
    }

    const applyFields = {};
    if (!hasHiltonWebsite(row.fields?.Website) && link.website) {
      applyFields[MAP_DIRECTORY_ENRICHMENT.website] = link.website;
    }

    let hiltonStatus = null;
    let hiltonOpenDate = null;
    try {
      const statusRow = await fetchHiltonHotelStatus(link.ctyhocn, { refererUrl: link.website });
      hiltonStatus = statusRow.hiltonStatus;
      hiltonOpenDate = statusRow.openDate;
      const auditRow = {
        censusStatus: row.status,
        hiltonStatus,
        hiltonOpenDate,
        censusOpenDate: row.fields?.["Open Date"] || null,
        censusProjectedOpenDate: row.fields?.projected_open_date || null,
        suggestedStatus: hiltonStatus,
      };
      Object.assign(applyFields, buildStatusCorrectionFields(auditRow));
    } catch (err) {
      // website still useful without status
    }

    if (!String(row.fields?.["Hotel Description"] || "").trim()) {
      try {
        const desc = await fetchHiltonHotelDescription(link.ctyhocn, { refererUrl: link.website });
        Object.assign(
          applyFields,
          buildDescriptionEnrichmentFields(row.fields, desc, {
            fillBlankOnly: true,
            presentFields: presentDescFields,
          })
        );
      } catch {
        const manualDesc = MANUAL_DESCRIPTIONS[String(link.ctyhocn || "").toUpperCase()];
        if (manualDesc && presentDescFields.includes(CENSUS_DESCRIPTION_FIELD)) {
          applyFields[CENSUS_DESCRIPTION_FIELD] = manualDesc;
        }
      }
    }

    if (!Object.keys(applyFields).length) {
      skipped.push({ ...row, reason: "no_apply_fields", ctyhocn: link.ctyhocn });
      continue;
    }

    planRows.push({
      censusRecordId: row.recordId,
      censusName: row.name,
      ctyhocn: link.ctyhocn,
      website: link.website,
      matchSource,
      directoryName: link.directoryName || "",
      censusStatus: row.status,
      hiltonStatus,
      applyFields,
      status: "ready",
    });

    if (opts.fetchDelayMs) await sleep(opts.fetchDelayMs);
  }

  return { planRows, skipped };
}
