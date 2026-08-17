/**
 * Strict Tripadvisor ↔ Dealality hotel match gates.
 */

import { MATCH_CONFIG, ROOM_COMPARE } from "./constants.js";

export const TRIPADVISOR_MATCH_VERSION = "tripadvisor-match-v1";

const STOP = new Set([
  "hotel",
  "hotels",
  "the",
  "and",
  "spa",
  "resort",
  "resorts",
  "by",
  "a",
  "an",
  "collection",
  "all",
  "inclusive",
  "adults",
  "only",
]);

const COUNTRY_ALIASES = {
  "dominican republic": ["dominican republic", "caribbean", "dominicana"],
  "costa rica": ["costa rica"],
  colombia: ["colombia"],
  mexico: ["mexico", "méxico"],
  panama: ["panama", "panamá"],
  brazil: ["brazil", "brasil"],
  argentina: ["argentina"],
  chile: ["chile"],
  peru: ["peru", "perú"],
  ecuador: ["ecuador"],
  jamaica: ["jamaica"],
  "puerto rico": ["puerto rico"],
  aruba: ["aruba"],
  bahamas: ["bahamas"],
  barbados: ["barbados"],
  belize: ["belize"],
  guatemala: ["guatemala"],
  honduras: ["honduras"],
  nicaragua: ["nicaragua"],
  "el salvador": ["el salvador"],
  venezuela: ["venezuela"],
  uruguay: ["uruguay"],
  bolivia: ["bolivia"],
  paraguay: ["paraguay"],
  suriname: ["suriname"],
  guyana: ["guyana"],
};

export function fold(s) {
  return String(s || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

export function tokens(s) {
  return fold(s)
    .split(/\s+/)
    .filter((t) => t.length > 2)
    .filter((t) => !STOP.has(t));
}

export function nameSimilarity(a, b) {
  const na = fold(a);
  const nb = fold(b);
  if (!na || !nb) return 0;
  if (na === nb) return 1;
  if (na.includes(nb) || nb.includes(na)) return 0.92;
  const ta = new Set(tokens(a));
  const tb = new Set(tokens(b));
  if (!ta.size || !tb.size) return 0;
  let inter = 0;
  for (const t of ta) if (tb.has(t)) inter += 1;
  return inter / Math.max(ta.size, tb.size);
}

export function haversineKm(lat1, lon1, lat2, lon2) {
  if ([lat1, lon1, lat2, lon2].some((v) => v == null || !Number.isFinite(Number(v))))
    return null;
  const toRad = (d) => (Number(d) * Math.PI) / 180;
  const R = 6371;
  const dLat = toRad(lat2 - lat1);
  const dLon = toRad(lon2 - lon1);
  const x =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLon / 2) ** 2;
  return 2 * R * Math.asin(Math.sqrt(x));
}

export function websiteHost(url) {
  try {
    return new URL(String(url)).hostname.replace(/^www\./, "").toLowerCase();
  } catch {
    return null;
  }
}

export function isHotelItem(it) {
  const type = String(it?.type || "").toUpperCase();
  const cat = String(it?.category || "").toLowerCase();
  if (["RESTAURANT", "ATTRACTION", "VACATION_RENTAL", "ACTIVITY"].includes(type))
    return false;
  if (["restaurant", "attraction", "vacation_rental"].includes(cat)) return false;
  return type === "HOTEL" || cat === "hotel";
}

export function usableTripadvisorRooms(it) {
  const t = Number(it?.numberOfRooms);
  if (!Number.isFinite(t) || t <= 0) return false;
  const reviews = Number(it?.numberOfReviews) || 0;
  const desc = String(it?.description || "");
  const descRooms = desc.match(/(\d{2,5})\s*(?:room|suite|key)/i);
  if (t <= 2 && (reviews >= 500 || (descRooms && Number(descRooms[1]) >= 50))) {
    return false;
  }
  return true;
}

export function classifyRoomCompare(authoritative, taRooms, taItem) {
  if (!usableTripadvisorRooms({ numberOfRooms: taRooms, ...taItem })) {
    return ROOM_COMPARE.MISSING;
  }
  const d = Number(authoritative);
  const t = Number(taRooms);
  if (!Number.isFinite(d)) return ROOM_COMPARE.MISSING;
  if (d === t) return ROOM_COMPARE.EXACT;
  const diff = Math.abs(d - t);
  const pct = diff / Math.max(d, 1);
  if (diff <= 5 || pct <= 0.05) return ROOM_COMPARE.NEAR_MATCH;
  return ROOM_COMPARE.CONFLICT;
}

function countryCompatible(dealalityCountry, taItem) {
  const dc = fold(dealalityCountry);
  if (!dc) return true;
  const taCountry = fold(taItem?.addressObj?.country || "");
  const loc = fold(
    [taItem?.locationString, taItem?.address, taCountry].filter(Boolean).join(" ")
  );
  if (
    dc !== "united states" &&
    (taCountry === "united states" ||
      /\bunited states\b|\busa\b|, (ga|nd|tx|fl|ca|ny)\b/.test(loc))
  ) {
    return false;
  }
  const aliases = COUNTRY_ALIASES[dc] || [dc.split(" ")[0]];
  if (aliases.some((a) => loc.includes(fold(a)))) return true;
  return null;
}

/**
 * Distinctive Dealality tokens missing from Tripadvisor name → sister collision risk.
 */
export function sisterBrandCollision(dealalityName, taName) {
  const distinctive = [
    "boutique",
    "selection",
    "waves",
    "tecnologico",
    "technologico",
    "dominicana",
    "collection",
    "palace",
    "mirage",
    "elegance",
    "colonial",
    "adults",
  ];
  const da = fold(dealalityName);
  const tb = fold(taName);
  const missing = distinctive.filter((t) => da.includes(t) && !tb.includes(t));
  const extra = distinctive.filter((t) => !da.includes(t) && tb.includes(t));
  if (missing.length >= 1 || extra.length >= 1) {
    // Only flag when base brand tokens still overlap (classic sister swap)
    const sim = nameSimilarity(dealalityName, taName);
    if (sim >= 0.55 && sim < 0.98) {
      return {
        collided: true,
        missing,
        extra,
        reason: "sister_brand_or_property_token_mismatch",
      };
    }
  }
  return { collided: false, missing, extra, reason: null };
}

/**
 * @param {object} hotel Dealality hotel
 * @param {object[]} pool Tripadvisor Actor items
 * @returns {{ match: object|null, rejection: object|null }}
 */
export function matchTripadvisorHotel(hotel, pool) {
  const lat = hotel.lat ?? hotel.latitude ?? null;
  const lng = hotel.lng ?? hotel.longitude ?? null;
  const dealHost = websiteHost(hotel.website);
  let best = null;
  const rejections = [];

  for (const it of pool || []) {
    if (!isHotelItem(it)) {
      rejections.push({ id: it?.id, reason: "not_hotel_type" });
      continue;
    }
    const compat = countryCompatible(hotel.country, it);
    if (compat === false) {
      rejections.push({ id: it?.id, reason: "country_mismatch" });
      continue;
    }
    const sim = nameSimilarity(hotel.name, it.name);
    if (sim < MATCH_CONFIG.minNameSimilarity) continue;

    const sister = sisterBrandCollision(hotel.name, it.name);
    if (sister.collided) {
      rejections.push({
        id: it?.id,
        reason: sister.reason,
        name_similarity: sim,
        missing_tokens: sister.missing,
      });
      continue;
    }

    const km = haversineKm(lat, lng, it.latitude, it.longitude);
    if (km != null && km > MATCH_CONFIG.maxGeoKmHard) {
      rejections.push({ id: it?.id, reason: "geo_too_far", km });
      continue;
    }
    if (compat === null && km == null && sim < 0.9) {
      rejections.push({ id: it?.id, reason: "country_undecided_weak_name" });
      continue;
    }

    let score = sim;
    if (km != null && km <= 1) score += 0.15;
    else if (km != null && km <= MATCH_CONFIG.maxGeoKmPreferred) score += 0.1;
    else if (km != null && km <= 10) score += 0.04;
    if (compat === true) score += 0.08;

    const taHost = websiteHost(it.website);
    if (dealHost && taHost) {
      if (dealHost === taHost || dealHost.endsWith(`.${taHost}`) || taHost.endsWith(`.${dealHost}`)) {
        score += 0.12;
      } else if (
        // major brand hosts may differ by path; soft penalty only when both look property-specific
        !/marriott|hilton|ihg|hyatt|accor|choice|wyndham|radisson|iberostar|riu|barcelo/.test(
          dealHost
        )
      ) {
        score -= 0.08;
      }
    }

    if (!best || score > best.score) {
      best = { item: it, score, sim, km, sister, website_host_match: Boolean(dealHost && taHost && dealHost === taHost) };
    }
  }

  if (!best || best.score < MATCH_CONFIG.minAcceptScore) {
    return {
      match: null,
      rejection: {
        reason: best ? "score_below_threshold" : "no_candidate",
        best_score: best?.score ?? 0,
        near_misses: rejections.slice(0, 8),
      },
    };
  }

  let confidence = "low";
  if (best.score >= 0.95 || (best.sim >= 0.92 && best.km != null && best.km <= 3))
    confidence = "high";
  else if (best.score >= 0.88) confidence = "medium";

  // Sister collision on accepted match with room conflict → false match
  if (
    best.sim < 0.92 &&
    hotel.rooms != null &&
    usableTripadvisorRooms(best.item) &&
    classifyRoomCompare(hotel.rooms, best.item.numberOfRooms, best.item) ===
      ROOM_COMPARE.CONFLICT
  ) {
    const pct =
      Math.abs(Number(hotel.rooms) - Number(best.item.numberOfRooms)) /
      Math.max(Number(hotel.rooms), 1);
    if (pct > MATCH_CONFIG.sisterBrandConflictPct) {
      return {
        match: null,
        rejection: {
          reason: "sister_brand_room_conflict",
          tripadvisor_id: best.item.id,
          name_similarity: best.sim,
          score: best.score,
        },
      };
    }
  }

  return {
    match: {
      item: best.item,
      score: Math.round(best.score * 1000) / 1000,
      name_similarity: Math.round(best.sim * 1000) / 1000,
      geo_km: best.km != null ? Math.round(best.km * 100) / 100 : null,
      confidence,
      website_host_match: best.website_host_match,
    },
    rejection: null,
  };
}
