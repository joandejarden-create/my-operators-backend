/**
 * V1.3 family-specific Rooms / Keys resolvers.
 * Never Cvent / legacy / OTA inventory / room-type card counts.
 */

import { fetchText, sleep } from "../../adapters/adapter-utils.js";
import { IHG_FETCH_HEADERS } from "../../../ihg-brand-directory-extract.js";
import { HILTON_FETCH_HEADERS } from "../../../hilton-brand-directory-extract.js";
import { CHOICE_FETCH_HEADERS } from "../../../choice-regional-directory-extract.js";
import { HILTON_GRAPHQL_URL, HILTON_GRAPHQL_HEADERS } from "../../../hilton-hotel-description-fetch.js";
import {
  extractRoomsKeysFromOfficialHtml,
  selectBestRoomsHit,
} from "../../production-census-rooms-keys-extractor.js";
import { extractHiltonCtyhocn } from "../../census-autopilot-family-directory-adapters.js";
import { extractStandaloneWebsiteCandidate } from "../live-deep-research.js";

export const ROOMS_RESOLVER_VERSION = "census-autopilot-v1.3-rooms-family";

function headersForFamily(family) {
  const f = String(family || "").toLowerCase();
  if (f === "ihg") return IHG_FETCH_HEADERS;
  if (f === "hilton") return HILTON_FETCH_HEADERS;
  if (f === "choice") return CHOICE_FETCH_HEADERS;
  return {};
}

function claimFromHit(hit, meta) {
  if (!hit || hit.rejected || hit.count == null) return null;
  if (hit.confidence === "Hold") return null;
  const n = Number(hit.count);
  if (!Number.isFinite(n) || n < 20 || n > 2500) return null;
  return {
    rooms: n,
    confidence: hit.confidence || "Medium",
    source: meta.source_url || null,
    source_type: meta.source_type || "official_property_page",
    evidence_date: new Date().toISOString().slice(0, 10),
    method: hit.method || meta.method || "rooms_keys_extractor",
    family: meta.family,
    cvent_used: false,
    legacy_used: false,
    hotel_only: hit.hotel_only !== false,
  };
}

function extractFromHtml(html, url, family) {
  const extracted = extractRoomsKeysFromOfficialHtml(html, { url });
  const hits = extracted?.hits || [];
  // Prefer High, then Medium; reject Hold/false positives already filtered
  const best =
    (typeof selectBestRoomsHit === "function" && selectBestRoomsHit(hits)) ||
    hits.find((h) => !h.rejected && h.confidence === "High") ||
    hits.find((h) => !h.rejected && h.confidence === "Medium") ||
    null;
  return claimFromHit(best, {
    source_url: url,
    source_type: "official_property_page",
    method: best?.method,
    family,
  });
}

function roomsFromProse(text, url, family, method) {
  const t = String(text || "");
  const patterns = [
    /(\d{2,4})\s+(?:guest\s+)?rooms?\b/i,
    /(\d{2,4})\s+rooms and suites\b/i,
    /\bhotel with\s+(\d{2,4})\s+rooms\b/i,
    /(\d{2,4})\s+keys\b/i,
    /(\d{2,4})\s+habitaciones\b/i,
  ];
  for (const re of patterns) {
    const m = t.match(re);
    if (!m) continue;
    const n = Number(m[1]);
    if (n < 20 || n > 2500) continue;
    // Avoid booking widget max
    if (n <= 9) continue;
    return {
      rooms: n,
      confidence: "Medium",
      source: url,
      source_type: "official_description_prose",
      evidence_date: new Date().toISOString().slice(0, 10),
      method,
      family,
      cvent_used: false,
      legacy_used: false,
      hotel_only: true,
    };
  }
  return null;
}

/**
 * IHG: hoteldetail HTML + empty numberOfRooms handling + standalone ladder.
 */
export async function resolveIHGRooms(property, opts = {}) {
  const url = property.website || property.official_url;
  const attempts = [];
  if (!url) {
    return { ok: false, reason: "missing_url", attempts, claim: null };
  }
  if (opts.delayMs) await sleep(opts.delayMs);
  const page = await fetchText(url, {
    headers: headersForFamily("IHG"),
    timeoutMs: opts.timeoutMs || 25000,
  });
  attempts.push({ level: "A", kind: "ihg_hoteldetail", ok: page.ok, status: page.status, url });
  if (page.ok && page.text) {
    // Explicit empty numberOfRooms — do not invent
    if (/"numberOfRooms"\s*:\s*""/.test(page.text) && !/"numberOfRooms"\s*:\s*"?\d{2,4}/.test(page.text)) {
      attempts.push({ note: "ihg_empty_numberOfRooms" });
    }
    let claim = extractFromHtml(page.text, url, "IHG");
    if (claim) return { ok: true, claim, attempts };

    const standalone = extractStandaloneWebsiteCandidate(page.text, url);
    if (standalone && standalone !== url) {
      if (opts.delayMs) await sleep(opts.delayMs);
      const sp = await fetchText(standalone, {
        headers: headersForFamily("IHG"),
        timeoutMs: opts.timeoutMs || 25000,
      });
      attempts.push({
        level: "A",
        kind: "standalone_hotel_site",
        ok: sp.ok,
        status: sp.status,
        url: standalone,
      });
      if (sp.ok && sp.text) {
        claim = extractFromHtml(sp.text, standalone, "IHG");
        if (claim) return { ok: true, claim, attempts };
        claim = roomsFromProse(sp.text, standalone, "IHG", "ihg_standalone_prose");
        if (claim) return { ok: true, claim, attempts };
      }
    }
  }
  return { ok: false, reason: "ihg_rooms_not_found", attempts, claim: null };
}

/**
 * Hilton: GraphQL description prose + HTML (often 403) + standalone.
 */
export async function resolveHiltonRooms(property, opts = {}) {
  const attempts = [];
  const fields = {
    "Official Property URL": property.website,
    "Brand Property Code": property.property_ids?.[0],
  };
  const cty =
    property.ctyhocn ||
    extractHiltonCtyhocn(fields, property.independent_record_id);

  // GraphQL shortDesc — sometimes states room count
  if (cty) {
    try {
      if (opts.delayMs) await sleep(Math.min(opts.delayMs || 0, 200));
      const res = await fetch(HILTON_GRAPHQL_URL, {
        method: "POST",
        headers: {
          ...HILTON_GRAPHQL_HEADERS,
          Referer: property.website || `https://www.hilton.com/en/hotels/${cty.toLowerCase()}-hotel/`,
        },
        body: JSON.stringify({
          operationName: "hotelRoomsDesc",
          query: `query hotelRoomsDesc($ctyhocn: String!, $language: String!) {
            hotel(ctyhocn: $ctyhocn, language: $language) {
              name
              ctyhocn
              facilityOverview { shortDesc homeUrlTemplate }
            }
          }`,
          variables: { ctyhocn: cty, language: "en" },
        }),
      });
      const json = await res.json();
      const hotel = json?.data?.hotel;
      attempts.push({
        level: "structured",
        kind: "hilton_graphql_shortDesc",
        ok: Boolean(hotel),
        ctyhocn: cty,
      });
      if (hotel?.facilityOverview?.shortDesc) {
        const claim = roomsFromProse(
          hotel.facilityOverview.shortDesc,
          property.website || `hilton_graphql:${cty}`,
          "Hilton",
          "hilton_graphql_shortDesc_prose"
        );
        if (claim) return { ok: true, claim, attempts };
      }
    } catch (err) {
      attempts.push({
        level: "structured",
        kind: "hilton_graphql_shortDesc",
        ok: false,
        error: err?.message || String(err),
      });
    }
  }

  const url = property.website;
  if (url) {
    if (opts.delayMs) await sleep(opts.delayMs);
    const page = await fetchText(url, {
      headers: headersForFamily("Hilton"),
      timeoutMs: opts.timeoutMs || 25000,
    });
    attempts.push({
      level: "A",
      kind: "hilton_property_page",
      ok: page.ok,
      status: page.status,
      url,
      blocked: page.status === 403,
    });
    if (page.ok && page.text) {
      const claim = extractFromHtml(page.text, url, "Hilton");
      if (claim) return { ok: true, claim, attempts };
    }
  }

  return { ok: false, reason: "hilton_rooms_not_found", attempts, claim: null };
}

/**
 * Choice: pages often 403; reject sitewide numberOfRooms=25; try accessible HTML only.
 */
export async function resolveChoiceRooms(property, opts = {}) {
  const attempts = [];
  const url = property.website;
  if (!url) return { ok: false, reason: "missing_url", attempts, claim: null };

  if (opts.delayMs) await sleep(opts.delayMs);
  const page = await fetchText(url, {
    headers: headersForFamily("Choice"),
    timeoutMs: opts.timeoutMs || 25000,
  });
  attempts.push({
    level: "A",
    kind: "choice_property_page",
    ok: page.ok,
    status: page.status,
    url,
    blocked: page.status === 403 || page.status === 429,
  });
  if (page.ok && page.text) {
    const claim = extractFromHtml(page.text, url, "Choice");
    if (claim) return { ok: true, claim, attempts };
  }
  return {
    ok: false,
    reason: page.status === 403 ? "choice_page_blocked_403" : "choice_rooms_not_found",
    attempts,
    claim: null,
  };
}

/**
 * @param {object} property VIC record
 */
export async function resolveFamilyRooms(property, opts = {}) {
  const family = String(property.family || "").trim();
  if (family === "IHG") return resolveIHGRooms(property, opts);
  if (family === "Hilton") return resolveHiltonRooms(property, opts);
  if (family === "Choice") return resolveChoiceRooms(property, opts);
  return { ok: false, reason: "unsupported_family", attempts: [], claim: null };
}
