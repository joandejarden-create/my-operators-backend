/**
 * Phase 2F — Brand directory discovery / manual import (DRY-RUN ONLY).
 *
 * Not an aggressive scraper. Modes:
 * - manual-file: CSV/JSON provided by ops
 * - sitemap: fetch explicit sitemap/index URL only (no deep crawl)
 * - search-list: seed JSON with brand URLs (no crawling)
 *
 * Does NOT import Airtable. Reject apply in CLI.
 */

import { readFileSync } from "fs";
import { SOURCE_TYPES } from "../fields.js";
import { buildIndependentCandidate } from "../normalize-candidate.js";
import { getSourceProfile } from "../source-registry.js";
import { REVIEW_STATUS, RECOMMENDED_ACTION } from "../fields.js";

export const BRAND_DIRECTORY_LICENSE = "source_specific_terms";
export const DISCOVERY_STATUS = {
  NEEDS_RESEARCH: "needs_research",
  PARSED: "parsed",
};

const HOTEL_URL_HINTS = /hotel|resort|property|location|locations|hotels\//i;

/**
 * @param {object} row
 */
export function buildBrandDirectoryCandidate(row, ctx) {
  const profile = getSourceProfile(SOURCE_TYPES.BRAND_DIRECTORY);
  const sourceName = row.sourceName || `${row.brand || "Brand"} Directory`;
  const batchId = ctx.batchId;
  const importedAt = ctx.importedAt || new Date().toISOString();

  const rawPayload = {
    mode: ctx.mode,
    brand: row.brand || "",
    parentCompany: row.parentCompany || "",
    sourceNotes: row.sourceNotes || "",
    openingStatus: row.openingStatus || "",
    discoveryStatus: row.discoveryStatus || DISCOVERY_STATUS.NEEDS_RESEARCH,
    ...(row.extraPayload || {}),
  };

  return buildIndependentCandidate({
    sourceName,
    sourceType: SOURCE_TYPES.BRAND_DIRECTORY,
    sourceLicense: profile?.sourceLicense || BRAND_DIRECTORY_LICENSE,
    sourceUrl: row.sourceUrl || row.website || "",
    sourceRecordId: row.sourceRecordId || row.sourceUrl || `brand-dir-${batchId}-${ctx.seq ?? 0}`,
    rawHotelName: row.hotelName || row.hotel_name || "",
    rawAddress: row.address || "",
    rawCity: row.city || "",
    rawCountry: row.country || "",
    rawLatitude: row.latitude ?? row.lat ?? null,
    rawLongitude: row.longitude ?? row.lng ?? row.lon ?? null,
    rawWebsite: row.website || row.hotelUrl || "",
    rawPhone: row.phone || "",
    rawBrand: row.brand || "",
    rawPayload: rawPayload,
    importBatchId: batchId,
    importedAt,
    reviewStatus: REVIEW_STATUS.PENDING,
    recommendedAction: RECOMMENDED_ACTION.NEEDS_RESEARCH,
  });
}

/**
 * Discovery lead when name/address cannot be safely extracted.
 */
export function buildDiscoveryLead(row, ctx) {
  const candidate = buildBrandDirectoryCandidate(
    {
      ...row,
      hotelName: row.hotelName || "",
      discoveryStatus: DISCOVERY_STATUS.NEEDS_RESEARCH,
      extraPayload: {
        recordKind: "discovery_lead",
        leadUrl: row.hotelUrl || row.sourceUrl || "",
      },
    },
    ctx
  );
  return {
    ...candidate,
    recordKind: "discovery_lead",
    discoveryStatus: DISCOVERY_STATUS.NEEDS_RESEARCH,
  };
}

function parseCsvLine(line) {
  const out = [];
  let cur = "";
  let inQuotes = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (ch === '"') {
      inQuotes = !inQuotes;
      continue;
    }
    if (ch === "," && !inQuotes) {
      out.push(cur.trim());
      cur = "";
      continue;
    }
    cur += ch;
  }
  out.push(cur.trim());
  return out;
}

/**
 * @param {string} filePath
 */
export function parseManualCsv(filePath) {
  const text = readFileSync(filePath, "utf8");
  const lines = text.split(/\r?\n/).filter((l) => l.trim());
  if (lines.length < 2) return [];
  const headers = parseCsvLine(lines[0]).map((h) => h.replace(/^\uFEFF/, "").trim());
  const rows = [];
  for (let i = 1; i < lines.length; i++) {
    const vals = parseCsvLine(lines[i]);
    const row = {};
    headers.forEach((h, idx) => {
      row[h] = vals[idx] ?? "";
    });
    rows.push(normalizeManualRow(row));
  }
  return rows;
}

/**
 * @param {string} filePath
 */
export function parseManualJson(filePath) {
  const data = JSON.parse(readFileSync(filePath, "utf8"));
  const list = Array.isArray(data) ? data : data.records || data.seeds || data.hotels || [];
  return list.map(normalizeManualRow);
}

function normalizeManualRow(row) {
  return {
    brand: row.brand || row.Brand || "",
    parentCompany: row.parentCompany || row.parent_company || row["Parent Company"] || "",
    hotelName: row.hotelName || row.hotel_name || row["Hotel Name"] || row.name || "",
    address: row.address || row.Address || "",
    city: row.city || row.City || "",
    state: row.state || row.State || "",
    country: row.country || row.Country || "",
    postalCode: row.postalCode || row.postal_code || row["Postal Code"] || "",
    latitude: row.latitude ?? row.lat,
    longitude: row.longitude ?? row.lng ?? row.lon,
    website: row.website || row.Website || "",
    phone: row.phone || row.Phone || "",
    rooms: row.rooms || row.Rooms || "",
    openingStatus: row.openingStatus || row.opening_status || "",
    sourceUrl: row.sourceUrl || row.source_url || row.website || "",
    sourceRecordId: row.sourceRecordId || row.source_record_id || row.id || "",
    sourceName: row.sourceName || row.source_name || "",
    sourceNotes: row.sourceNotes || row.source_notes || "",
  };
}

/**
 * @param {Array<object>} seeds
 */
export function processSearchListSeeds(seeds, ctx) {
  const candidates = [];
  const discoveryLeads = [];
  let seq = 0;

  for (const seed of seeds) {
    const baseCtx = {
      ...ctx,
      mode: "search-list",
      brand: seed.brand,
      parentCompany: seed.parentCompany,
      sourceName: seed.sourceName || `${seed.brand} Directory`,
      country: seed.country,
    };

    const urls = seed.hotelUrls || seed.urls || [];
    for (const entry of urls) {
      seq++;
      const url = typeof entry === "string" ? entry : entry.url || entry.hotelUrl || "";
      const hotelName = typeof entry === "string" ? "" : entry.hotelName || entry.name || "";
      const row = {
        brand: seed.brand,
        parentCompany: seed.parentCompany,
        country: entry.country || seed.country,
        sourceUrl: seed.sourceUrl,
        sourceName: baseCtx.sourceName,
        hotelUrl: url,
        website: url,
        hotelName,
        city: entry.city || "",
        address: entry.address || "",
        sourceRecordId: entry.sourceRecordId || url,
        sourceNotes: seed.sourceNotes || "",
      };

      const hasName = String(hotelName).trim().length > 0;
      const itemCtx = { ...baseCtx, seq };
      if (hasName) {
        candidates.push(buildBrandDirectoryCandidate(row, itemCtx));
      } else {
        discoveryLeads.push(buildDiscoveryLead(row, itemCtx));
      }
    }

    if (!urls.length && seed.sourceUrl) {
      seq++;
      discoveryLeads.push(
        buildDiscoveryLead(
          {
            brand: seed.brand,
            parentCompany: seed.parentCompany,
            country: seed.country,
            sourceUrl: seed.sourceUrl,
            hotelUrl: seed.sourceUrl,
            sourceName: baseCtx.sourceName,
          },
          { ...baseCtx, seq }
        )
      );
    }
  }

  return { candidates, discoveryLeads, urlsInspected: seq };
}

export function extractSitemapLocs(xml) {
  return [...String(xml).matchAll(/<loc>\s*([^<\s]+)\s*<\/loc>/gi)].map((m) => m[1].trim());
}

function originOf(url) {
  const u = new URL(url);
  return `${u.protocol}//${u.host}`;
}

/**
 * Basic robots.txt check — if path disallowed, skip fetch.
 */
export async function isUrlAllowedByRobots(targetUrl, fetchFn = globalThis.fetch) {
  try {
    const origin = originOf(targetUrl);
    const robotsUrl = `${origin}/robots.txt`;
    const res = await fetchFn(robotsUrl, {
      headers: { "User-Agent": "DealalityBrandDirectoryDiscovery/1.0 (dry-run)" },
    });
    if (!res.ok) return true;
    const text = await res.text();
    const path = new URL(targetUrl).pathname;
    const disallows = [...text.matchAll(/^Disallow:\s*(.+)$/gim)].map((m) => m[1].trim());
    for (const rule of disallows) {
      if (rule && rule !== "/" && path.startsWith(rule)) return false;
    }
    return true;
  } catch {
    return true;
  }
}

/**
 * Fetch sitemap XML from explicit URL only (no recursive sitemap index expansion beyond one level).
 */
export async function fetchSitemapUrls(sitemapUrl, options = {}) {
  const fetchFn = options.fetchFn || globalThis.fetch;
  const maxPages = options.maxPages ?? 500;

  const allowed = await isUrlAllowedByRobots(sitemapUrl, fetchFn);
  if (!allowed) {
    throw new Error(`robots.txt disallows fetching sitemap: ${sitemapUrl}`);
  }

  const res = await fetchFn(sitemapUrl, {
    headers: { "User-Agent": "DealalityBrandDirectoryDiscovery/1.0 (dry-run)" },
  });
  if (!res.ok) throw new Error(`Sitemap HTTP ${res.status}: ${sitemapUrl}`);
  const xml = await res.text();
  let urls = extractSitemapLocs(xml).filter((u) => HOTEL_URL_HINTS.test(u));

  if (urls.length === 0) {
    const indexLocs = extractSitemapLocs(xml).filter((u) => /\.xml/i.test(u));
    if (options.maxPages > 0 && indexLocs.length) {
      const child = indexLocs.slice(0, 5);
      for (const childUrl of child) {
        const childAllowed = await isUrlAllowedByRobots(childUrl, fetchFn);
        if (!childAllowed) continue;
        const cr = await fetchFn(childUrl, {
          headers: { "User-Agent": "DealalityBrandDirectoryDiscovery/1.0 (dry-run)" },
        });
        if (cr.ok) {
          const childXml = await cr.text();
          urls.push(...extractSitemapLocs(childXml).filter((u) => HOTEL_URL_HINTS.test(u)));
        }
      }
    }
  }

  return urls.slice(0, maxPages);
}

/**
 * @param {object} opts
 */
export async function processSitemapMode(opts) {
  const { brand, parentCompany, sourceUrl, batchId, maxPages = 500 } = opts;
  const urls = await fetchSitemapUrls(sourceUrl, { maxPages });
  const ctx = {
    batchId,
    mode: "sitemap",
    importedAt: new Date().toISOString(),
  };

  const candidates = [];
  const discoveryLeads = [];
  let seq = 0;
  for (const url of urls) {
    seq++;
    const row = {
      brand,
      parentCompany,
      sourceUrl,
      sourceName: `${brand} Directory`,
      hotelUrl: url,
      website: url,
      country: opts.country || "",
      sourceRecordId: url,
    };
    discoveryLeads.push(buildDiscoveryLead(row, { ...ctx, seq }));
  }

  return {
    candidates,
    discoveryLeads,
    urlsInspected: urls.length,
  };
}

export function summarizeBrandDirectoryReport(candidates, discoveryLeads, meta = {}) {
  const all = [...candidates, ...discoveryLeads];
  const withName = all.filter((c) => String(c.rawHotelName || "").trim()).length;
  const withCity = all.filter((c) => String(c.rawCity || "").trim()).length;
  const withWebsite = all.filter((c) => String(c.rawWebsite || c.sourceUrl || "").trim()).length;
  const withAddress = all.filter((c) => String(c.rawAddress || "").trim()).length;

  return {
    ...meta,
    candidateCount: candidates.length,
    discoveryLeadCount: discoveryLeads.length,
    totalRecords: all.length,
    withHotelName: withName,
    withCity,
    withWebsite,
    withAddress,
    requiresManualReview: all.length,
    sourcePolicyRiskSummary: {
      sourceType: SOURCE_TYPES.BRAND_DIRECTORY,
      canUseInProduct: "review_required",
      canShowToUsers: "review_required",
      canUseForScoring: "review_required",
      requiresManualReview: true,
      riskLevel: "medium",
    },
  };
}
