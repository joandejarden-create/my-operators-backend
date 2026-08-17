/**
 * Brand Match — same-brand Open density in deal geography (Hotel Census).
 * Soft factor only: never a hard gate. Returns null when geography match is not confident.
 *
 * Schema (Platform base AIRTABLE_BASE_ID_ALT):
 * - Table: Hotel Census (CENSUS_FIELDS)
 * - Affiliation exact = Brand Setup Brand Name
 * - status = Open
 * - Geography: prefer Submarket vs deal "Hotel Submarket & Location";
 *   else Market / Dealality Market vs deal Primary Market Region / Market.
 * Country-only matches are intentionally excluded (not confident enough).
 */

import {
  HOTEL_CENSUS_TABLE,
  CENSUS_FIELDS,
  STATUS_OPEN,
} from "./hotel-census/fields.js";
import { BRAND_MATCH_SAME_BRAND_DENSITY } from "./brand-match-scoring-weight-config.js";

function str(v) {
  if (v == null) return "";
  if (typeof v === "string") return v.trim();
  if (typeof v === "number" && !Number.isNaN(v)) return String(v);
  return "";
}

function escapeFormulaValue(v) {
  return String(v ?? "").replace(/'/g, "\\'");
}

function norm(v) {
  return str(v).toLowerCase();
}

function geographyTokensOverlap(a, b) {
  const na = norm(a);
  const nb = norm(b);
  if (!na || !nb) return false;
  if (na === nb) return true;
  if (na.length >= 4 && nb.length >= 4 && (na.includes(nb) || nb.includes(na))) return true;
  return false;
}

/** @param {number} peerCount @returns {number} */
export function scoreSameBrandDensityFromPeerCount(peerCount) {
  const n = Number(peerCount);
  if (!Number.isFinite(n) || n < 0) return null;
  const bands = BRAND_MATCH_SAME_BRAND_DENSITY.bands || [];
  for (const band of bands) {
    if (n <= band.maxPeers) return band.score;
  }
  return BRAND_MATCH_SAME_BRAND_DENSITY.fallbackScore ?? 10;
}

/**
 * Resolve deal-side geography for density. Prefers submarket, then market-like signals.
 * @returns {{ grain: 'submarket'|'market'|null, value: string, country: string }}
 */
export function resolveDealDensityGeography(dealFields, locationData) {
  const loc = (airtableKey, normalizedKey) => {
    if (!locationData || typeof locationData !== "object") return undefined;
    const v = locationData[airtableKey] ?? locationData[normalizedKey];
    return v !== undefined && v !== null && v !== "" ? v : undefined;
  };

  const country = str(
    loc("Country", "country") || dealFields?.Country || dealFields?.country || ""
  );
  const submarket = str(
    loc("Hotel Submarket & Location", "submarket") ||
      dealFields?.["Hotel Submarket & Location"] ||
      ""
  );
  const market = str(
    loc("Primary Market Region", "primaryMarketRegion") ||
      dealFields?.["Primary Market Region"] ||
      loc("Market", "market") ||
      dealFields?.Market ||
      ""
  );

  if (country && submarket.length >= 3) {
    return { grain: "submarket", value: submarket, country };
  }
  if (country && market.length >= 3) {
    return { grain: "market", value: market, country };
  }
  return { grain: null, value: "", country };
}

/**
 * Count Open Hotel Census peers for exact Affiliation in confident geography.
 * @returns {Promise<{ score: number|null, peerCount: number|null, grain: string|null, reason: string|null }>}
 */
export async function evaluateSameBrandMarketDensity({
  brandName,
  dealFields,
  locationData,
  altBaseId = process.env.AIRTABLE_BASE_ID_ALT,
  apiKey = process.env.AIRTABLE_API_KEY,
}) {
  const affiliation = str(brandName);
  const geo = resolveDealDensityGeography(dealFields, locationData);

  if (!affiliation) {
    return { score: null, peerCount: null, grain: null, reason: "missing_brand" };
  }
  if (!geo.grain || !geo.country) {
    return {
      score: null,
      peerCount: null,
      grain: null,
      reason: "insufficient_deal_geography",
    };
  }
  if (!altBaseId || !apiKey) {
    return { score: null, peerCount: null, grain: geo.grain, reason: "census_unavailable" };
  }

  const formula = `AND({${CENSUS_FIELDS.affiliation}}='${escapeFormulaValue(affiliation)}',{${CENSUS_FIELDS.status}}='${escapeFormulaValue(STATUS_OPEN)}',LOWER({${CENSUS_FIELDS.country}})=LOWER('${escapeFormulaValue(geo.country)}'))`;

  const fieldParams = [CENSUS_FIELDS.name, CENSUS_FIELDS.submarket, CENSUS_FIELDS.market]
    .map((f) => `fields%5B%5D=${encodeURIComponent(f)}`)
    .join("&");

  try {
    let offset = null;
    const peers = [];
    const controller = typeof AbortController !== "undefined" ? new AbortController() : null;
    const timeoutMs = Number(process.env.BRAND_MATCH_DENSITY_TIMEOUT_MS) || 2500;
    const timer = controller
      ? setTimeout(() => {
          try {
            controller.abort();
          } catch (_) {
            /* ignore */
          }
        }, timeoutMs)
      : null;
    do {
      let url = `https://api.airtable.com/v0/${altBaseId}/${encodeURIComponent(HOTEL_CENSUS_TABLE)}?pageSize=100&filterByFormula=${encodeURIComponent(formula)}&${fieldParams}`;
      if (offset) url += `&offset=${encodeURIComponent(offset)}`;
      const res = await fetch(url, {
        headers: { Authorization: `Bearer ${apiKey}` },
        signal: controller ? controller.signal : undefined,
      });
      if (res.status === 429) {
        await new Promise((r) => setTimeout(r, 2000));
        continue;
      }
      const data = await res.json();
      if (data.error) {
        console.warn(
          "[brand-match-density] census peer fetch failed:",
          data.error.message || data.error.type
        );
        if (timer) clearTimeout(timer);
        return { score: null, peerCount: null, grain: geo.grain, reason: "census_api_error" };
      }
      for (const rec of data.records || []) {
        peers.push(rec.fields || {});
        if (peers.length > 50) break;
      }
      offset = data.offset || null;
      if (peers.length > 50) break;
    } while (offset);
    if (timer) clearTimeout(timer);

    if (peers.length === 0) {
      return {
        score: scoreSameBrandDensityFromPeerCount(0),
        peerCount: 0,
        grain: geo.grain,
        reason: null,
      };
    }

    let matched = [];
    let evaluable = [];

    if (geo.grain === "submarket") {
      evaluable = peers.filter((p) => str(p[CENSUS_FIELDS.submarket]));
      if (evaluable.length === 0) {
        // Brand has Open peers in country but Submarket not populated — do not guess.
        return {
          score: null,
          peerCount: null,
          grain: geo.grain,
          reason: "census_submarket_unpopulated",
        };
      }
      matched = evaluable.filter((p) =>
        geographyTokensOverlap(p[CENSUS_FIELDS.submarket], geo.value)
      );
    } else {
      // Fallback grain: census Market vs deal Primary Market Region / Market.
      // Prefer Submarket grain whenever deal Hotel Submarket & Location is present.
      evaluable = peers.filter((p) => str(p[CENSUS_FIELDS.market]));
      if (evaluable.length === 0) {
        return {
          score: null,
          peerCount: null,
          grain: geo.grain,
          reason: "census_market_unpopulated",
        };
      }
      matched = evaluable.filter((p) =>
        geographyTokensOverlap(p[CENSUS_FIELDS.market], geo.value)
      );
    }

    const peerCount = matched.length;
    return {
      score: scoreSameBrandDensityFromPeerCount(peerCount),
      peerCount,
      grain: geo.grain,
      reason: null,
    };
  } catch (e) {
    const msg = e && e.message ? e.message : String(e);
    if (/aborted|AbortError/i.test(msg) || (e && e.name === "AbortError")) {
      console.warn("[brand-match-density] census peer fetch timed out");
      return { score: null, peerCount: null, grain: geo.grain, reason: "census_timeout" };
    }
    console.warn("[brand-match-density] census peer fetch error:", msg);
    return { score: null, peerCount: null, grain: geo.grain, reason: "census_network_error" };
  }
}
