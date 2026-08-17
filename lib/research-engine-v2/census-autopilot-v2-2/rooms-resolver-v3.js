/**
 * Rooms Resolver V3 — forensic-informed; do not repeat V2 empty-numberOfRooms loop as sole path.
 * Evidence standard: explicit guest rooms / keys / unambiguous rooms+suites totals only.
 * NEVER infer. NEVER SerpApi. NEVER Cvent.
 */

import { fetchText, sleep } from "../adapters/adapter-utils.js";
import { IHG_FETCH_HEADERS } from "../../ihg-brand-directory-extract.js";
import { HILTON_FETCH_HEADERS } from "../../hilton-brand-directory-extract.js";
import { CHOICE_FETCH_HEADERS } from "../../choice-regional-directory-extract.js";
import { HILTON_GRAPHQL_URL, HILTON_GRAPHQL_HEADERS } from "../../hilton-hotel-description-fetch.js";
import {
  extractRoomsKeysFromOfficialHtml,
  selectBestRoomsHit,
} from "../production-census-rooms-keys-extractor.js";
import { extractHiltonCtyhocn } from "../census-autopilot-family-directory-adapters.js";
import { extractStandaloneWebsiteCandidate } from "../census-autopilot-v1/live-deep-research.js";
import { resolveFamilyRooms } from "../census-autopilot-v1/golden-gap-v13/rooms-family-resolvers.js";

export const ROOMS_RESOLVER_V3_VERSION = "census-autopilot-v2.2-rooms-resolver-v3";

export const ROOMS_CONFIDENCE = Object.freeze({
  HIGH: "HIGH",
  MEDIUM: "MEDIUM",
  LOW: "LOW",
});

const NEVER_FROM = Object.freeze([
  "room_types",
  "bedrooms",
  "meeting_rooms",
  "occupancy",
  "availability",
  "booking_inventory",
  "review_counts",
  "floor_count",
  "serpapi",
  "cvent",
  "legacy_census",
]);

function headersFor(family) {
  const f = String(family || "").toLowerCase();
  if (f === "ihg") return IHG_FETCH_HEADERS;
  if (f === "hilton") return HILTON_FETCH_HEADERS;
  if (f === "choice") return CHOICE_FETCH_HEADERS;
  return {
    "User-Agent":
      "Mozilla/5.0 (compatible; DealalityCensusBot/2.2; +https://dealality.com)",
    Accept: "text/html,application/xhtml+xml",
  };
}

function claimOk(n, conf) {
  return Number.isFinite(n) && n >= 20 && n <= 2500 && (conf === "HIGH" || conf === "MEDIUM" || conf === "High" || conf === "Medium");
}

function normalizeConf(c) {
  const s = String(c || "").toUpperCase();
  if (s === "HIGH") return ROOMS_CONFIDENCE.HIGH;
  if (s === "MEDIUM") return ROOMS_CONFIDENCE.MEDIUM;
  return ROOMS_CONFIDENCE.LOW;
}

/**
 * Extract rooms from JSON-LD Hotel / LodgingBusiness.
 */
export function extractRoomsFromJsonLd(html) {
  const out = [];
  const re = /<script[^>]*type=["']application\/ld\+json["'][^>]*>([\s\S]*?)<\/script>/gi;
  let m;
  while ((m = re.exec(html))) {
    try {
      const raw = m[1].trim();
      if (!raw) continue;
      const data = JSON.parse(raw);
      const nodes = Array.isArray(data) ? data : data["@graph"] ? data["@graph"] : [data];
      for (const node of nodes) {
        const n = Number(node?.numberOfRooms ?? node?.numberOfRooms?.value);
        if (Number.isFinite(n) && n >= 20 && n <= 2500) {
          out.push({
            rooms: n,
            confidence: ROOMS_CONFIDENCE.HIGH,
            method: "json_ld_numberOfRooms",
            source_type: "official_structured_json_ld",
          });
        }
      }
    } catch {
      // malformed ld+json — continue
    }
  }
  return out;
}

/**
 * Scan embedded application state blobs for durable room totals (not inventory).
 */
export function extractRoomsFromEmbeddedState(html) {
  const out = [];
  const patterns = [
    /"numberOfRooms"\s*:\s*"?(\d{2,4})"?/i,
    /"totalRooms"\s*:\s*"?(\d{2,4})"?/i,
    /"guestRooms"\s*:\s*"?(\d{2,4})"?/i,
    /"roomCount"\s*:\s*"?(\d{2,4})"?/i,
    /"totalKeys"\s*:\s*"?(\d{2,4})"?/i,
    /"keys"\s*:\s*"?(\d{2,4})"?/i,
  ];
  // Reject empty string form
  if (/"numberOfRooms"\s*:\s*""/.test(html) && !/"numberOfRooms"\s*:\s*"?\d{2,4}/.test(html)) {
    out.push({ empty_numberOfRooms: true });
  }
  for (const re of patterns) {
    const m = html.match(re);
    if (!m) continue;
    const n = Number(m[1]);
    if (n >= 20 && n <= 2500) {
      out.push({
        rooms: n,
        confidence: ROOMS_CONFIDENCE.HIGH,
        method: `embedded_state_${re.source.slice(0, 24)}`,
        source_type: "official_embedded_state",
      });
    }
  }
  return out;
}

function roomsFromProse(text, url, family, method, confidence = ROOMS_CONFIDENCE.MEDIUM) {
  const t = String(text || "");
  const patterns = [
    /(\d{2,4})\s+(?:guest\s+)?rooms?\b/i,
    /(\d{2,4})\s+rooms and suites\b/i,
    /\bhotel with\s+(\d{2,4})\s+rooms\b/i,
    /(\d{2,4})\s+keys\b/i,
    /(\d{2,4})\s+habitaciones\b/i,
    /(\d{2,4})\s+guestrooms?\b/i,
  ];
  for (const re of patterns) {
    const m = t.match(re);
    if (!m) continue;
    const n = Number(m[1]);
    if (!claimOk(n, confidence)) continue;
    return {
      rooms: n,
      confidence: normalizeConf(confidence),
      source: url,
      source_type: "official_description_prose",
      method,
      family,
      hotel_only: true,
    };
  }
  return null;
}

function claimFromHtmlExtractor(html, url, family) {
  const extracted = extractRoomsKeysFromOfficialHtml(html, { url });
  const hits = extracted?.hits || [];
  const best =
    (typeof selectBestRoomsHit === "function" && selectBestRoomsHit(hits)) ||
    hits.find((h) => !h.rejected && (h.confidence === "High" || h.confidence === "MEDIUM")) ||
    hits.find((h) => !h.rejected && h.confidence === "Medium") ||
    null;
  if (!best || best.count == null) return null;
  const conf = normalizeConf(best.confidence);
  if (conf === ROOMS_CONFIDENCE.LOW) return null;
  if (!claimOk(best.count, conf)) return null;
  return {
    rooms: best.count,
    confidence: conf,
    source: url,
    source_type: "official_property_page",
    method: best.method || "rooms_keys_extractor",
    family,
    hotel_only: best.hotel_only !== false,
  };
}

async function enrichFromPage(url, family, attempts, opts) {
  if (!url) return { claim: null, emptyField: false };
  if (opts.delayMs) await sleep(opts.delayMs);
  const page = await fetchText(url, {
    headers: headersFor(family),
    timeoutMs: opts.timeoutMs || 25000,
  });
  attempts.push({ kind: "official_page", ok: page.ok, status: page.status, url, family });
  if (!page.ok || !page.text) return { claim: null, emptyField: false, blocked: page.status === 403 };

  const emptyField = /"numberOfRooms"\s*:\s*""/.test(page.text);

  for (const hit of extractRoomsFromJsonLd(page.text)) {
    if (hit.rooms) {
      attempts.push({ kind: "json_ld", ok: true, rooms: hit.rooms });
      return {
        claim: {
          ...hit,
          source: url,
          family,
          hotel_only: true,
        },
        emptyField,
      };
    }
  }

  for (const hit of extractRoomsFromEmbeddedState(page.text)) {
    if (hit.rooms) {
      attempts.push({ kind: "embedded_state", ok: true, rooms: hit.rooms });
      return {
        claim: {
          ...hit,
          source: url,
          family,
          hotel_only: true,
        },
        emptyField,
      };
    }
    if (hit.empty_numberOfRooms) attempts.push({ note: "empty_numberOfRooms_confirmed_v3" });
  }

  let claim = claimFromHtmlExtractor(page.text, url, family);
  if (claim) return { claim, emptyField };

  claim = roomsFromProse(page.text.slice(0, 80000), url, family, "official_page_prose", ROOMS_CONFIDENCE.MEDIUM);
  if (claim) return { claim, emptyField };

  const standalone = extractStandaloneWebsiteCandidate(page.text, url);
  if (standalone && standalone !== url) {
    if (opts.delayMs) await sleep(opts.delayMs);
    const sp = await fetchText(standalone, {
      headers: headersFor(family),
      timeoutMs: opts.timeoutMs || 25000,
    });
    attempts.push({ kind: "standalone_owner", ok: sp.ok, status: sp.status, url: standalone });
    if (sp.ok && sp.text) {
      for (const hit of extractRoomsFromJsonLd(sp.text)) {
        if (hit.rooms) {
          return {
            claim: {
              ...hit,
              source: standalone,
              family,
              hotel_only: true,
              confidence: ROOMS_CONFIDENCE.MEDIUM,
              source_type: "official_owner_operator",
            },
            emptyField,
          };
        }
      }
      claim = claimFromHtmlExtractor(sp.text, standalone, family);
      if (claim) {
        claim.confidence = ROOMS_CONFIDENCE.MEDIUM;
        claim.source_type = "official_owner_operator";
        return { claim, emptyField };
      }
      claim = roomsFromProse(sp.text.slice(0, 80000), standalone, family, "owner_prose", ROOMS_CONFIDENCE.MEDIUM);
      if (claim) return { claim, emptyField };
    }
  }

  return { claim: null, emptyField, blocked: false };
}

async function hiltonGraphQLRooms(property, attempts, opts) {
  const fields = {
    "Official Property URL": property.website,
    "Brand Property Code": property.property_ids?.[0],
  };
  const cty =
    property.ctyhocn ||
    extractHiltonCtyhocn(fields, property.independent_record_id || property.property_identity_id);
  if (!cty) {
    attempts.push({ kind: "hilton_graphql", ok: false, reason: "no_ctyhocn" });
    return null;
  }
  try {
    if (opts.delayMs) await sleep(Math.min(opts.delayMs || 0, 200));
    const res = await fetch(HILTON_GRAPHQL_URL, {
      method: "POST",
      headers: {
        ...HILTON_GRAPHQL_HEADERS,
        Referer: property.website || `https://www.hilton.com/en/hotels/${cty.toLowerCase()}-hotel/`,
      },
      body: JSON.stringify({
        operationName: "hotelRoomsV3",
        query: `query hotelRoomsV3($ctyhocn: String!, $language: String!) {
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
    attempts.push({ kind: "hilton_graphql_shortDesc", ok: Boolean(hotel), ctyhocn: cty });
    if (hotel?.facilityOverview?.shortDesc) {
      return roomsFromProse(
        hotel.facilityOverview.shortDesc,
        property.website || `hilton_graphql:${cty}`,
        "Hilton",
        "hilton_graphql_shortDesc_prose",
        ROOMS_CONFIDENCE.MEDIUM
      );
    }
  } catch (err) {
    attempts.push({
      kind: "hilton_graphql_shortDesc",
      ok: false,
      error: String(err?.message || err).slice(0, 120),
    });
  }
  return null;
}

/**
 * @param {object} property
 * @param {{ delayMs?: number, timeoutMs?: number }} [opts]
 */
export async function resolveRoomsV3(property, opts = {}) {
  const family = String(property.family || "").trim();
  const attempts = [];
  const retrieved_at = new Date().toISOString();
  const base = {
    ok: false,
    rooms_value: null,
    rooms_source: null,
    rooms_source_type: null,
    retrieved_at,
    confidence: null,
    match_confidence: property.independent_record_id || property.property_identity_id || null,
    rights_class: "official_research",
    serpapi_used: false,
    inferred: false,
    cvent_used: false,
    legacy_used: false,
    never_from: NEVER_FROM,
    resolver_version: ROOMS_RESOLVER_V3_VERSION,
    attempts,
  };

  // 1) Family-native V1.3 path first (may still work for rare populated fields)
  if (["IHG", "Hilton", "Choice"].includes(family)) {
    try {
      const v13 = await resolveFamilyRooms(property, opts);
      attempts.push(...(v13.attempts || []), { kind: "v13_wrap", ok: Boolean(v13.ok) });
      if (v13.ok && v13.claim?.rooms != null) {
        const conf = normalizeConf(v13.claim.confidence);
        if (conf !== ROOMS_CONFIDENCE.LOW) {
          return {
            ...base,
            ok: true,
            rooms_value: v13.claim.rooms,
            rooms_source: v13.claim.source,
            rooms_source_type: v13.claim.source_type,
            confidence: conf,
            classification: "NATIVE RESOLVABLE",
            evidence_quote_or_structured_field: v13.claim.method,
          };
        }
      }
    } catch (err) {
      attempts.push({ kind: "v13_wrap", ok: false, error: String(err?.message || err).slice(0, 120) });
    }
  }

  // 2) Hilton GraphQL prose (occasional)
  if (family === "Hilton") {
    const gq = await hiltonGraphQLRooms(property, attempts, opts);
    if (gq?.rooms) {
      return {
        ...base,
        ok: true,
        rooms_value: gq.rooms,
        rooms_source: gq.source,
        rooms_source_type: gq.source_type,
        confidence: gq.confidence,
        classification: "NATIVE RESOLVABLE",
        evidence_quote_or_structured_field: gq.method,
      };
    }
  }

  // 3) Official page + JSON-LD + embedded + owner ladder
  const url = property.website || property.official_url;
  const pageResult = await enrichFromPage(url, family || "Unknown", attempts, opts);
  if (pageResult.claim?.rooms) {
    const conf = normalizeConf(pageResult.claim.confidence);
    if (conf !== ROOMS_CONFIDENCE.LOW) {
      return {
        ...base,
        ok: true,
        rooms_value: pageResult.claim.rooms,
        rooms_source: pageResult.claim.source,
        rooms_source_type: pageResult.claim.source_type,
        confidence: conf,
        classification: "NATIVE RESOLVABLE",
        evidence_quote_or_structured_field: pageResult.claim.method,
      };
    }
  }

  // 4) Classification — do not treat empty IHG field as "try same page harder"
  let classification = "PUBLIC-RESEARCH ESCALATION";
  let reason = "rooms_not_found_v3";
  if (pageResult.blocked) {
    classification = "PUBLIC-RESEARCH ESCALATION";
    reason = "access_blocked";
  } else if (pageResult.emptyField && family === "IHG") {
    classification = "FIRST-PARTY VALIDATION";
    reason = "ihg_empty_numberOfRooms_requires_first_party";
  } else if (["Marriott", "Accor", "Wyndham", "Hyatt", "Melia", "Minor", "Barcelo", "RIU", "Iberostar"].includes(family)) {
    classification = "FIRST-PARTY VALIDATION";
    reason = "family_rooms_not_in_public_structured_sources";
  } else if (family === "Choice") {
    classification = "FIRST-PARTY VALIDATION";
    reason = "choice_rooms_sparse_public";
  } else if (family === "Independent" || !family) {
    classification = "DEEP RESEARCH ESCALATION";
    reason = "independent_rooms_need_deep_or_first_party";
  }

  return {
    ...base,
    ok: false,
    classification,
    reason,
    evidence_quote_or_structured_field: pageResult.emptyField ? "numberOfRooms:\"\"" : null,
  };
}

export { NEVER_FROM as ROOMS_NEVER_FROM_V3 };
