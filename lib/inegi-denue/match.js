/**
 * Multi-signal DENUE establishment matching for Hotel Property Census rows.
 * Does not force ambiguous matches; preserves candidates + reasons.
 * DENUE razón social ≠ automatic economic / property owner.
 */

import { DENUE_HOTEL_ACTIVITY_RE, isDenueHotelEstablishment } from "./client.js";

export const DENUE_MATCH_VERSION = "inegi-denue-match-v1";

export const BUSINESS_RELATIONSHIP = Object.freeze({
  REGISTERED_BUSINESS: "REGISTERED_BUSINESS",
  OPERATOR: "OPERATOR",
  LESSEE: "LESSEE",
  PROPERTY_OWNER: "PROPERTY_OWNER",
  ECONOMIC_OWNER_OR_SPONSOR: "ECONOMIC_OWNER_OR_SPONSOR",
  RELATIONSHIP_UNRESOLVED: "RELATIONSHIP_UNRESOLVED",
});

function normText(s) {
  return String(s || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function tokens(s) {
  return normText(s)
    .split(" ")
    .filter((t) => t.length > 2 && !STOP.has(t));
}

const STOP = new Set([
  "the",
  "hotel",
  "hoteles",
  "resort",
  "resorts",
  "spa",
  "and",
  "del",
  "de",
  "la",
  "las",
  "los",
  "el",
  "by",
  "a",
  "an",
  "inn",
  "suites",
  "suite",
  "collection",
  "plaza",
  // Geography / market tokens — insufficient alone for identity
  "cabos",
  "cabo",
  "cancun",
  "cancún",
  "mexico",
  "mexicano",
  "playa",
  "mujeres",
  "riviera",
  "maya",
  "guadalajara",
  "zapopan",
  "reforma",
  "city",
  "all",
  "inclusive",
  "family",
  "golf",
  "ages",
  "express",
  "business",
]);

/** Tokens that are only geographic/generic — overlap on these alone is not identity evidence. */
function meaningfulShared(shared) {
  return (shared || []).filter((t) => !STOP.has(t) && t.length > 2);
}

export function haversineMeters(lat1, lon1, lat2, lon2) {
  const toRad = (d) => (d * Math.PI) / 180;
  const R = 6371000;
  const dLat = toRad(lat2 - lat1);
  const dLon = toRad(lon2 - lon1);
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLon / 2) ** 2;
  return 2 * R * Math.asin(Math.sqrt(a));
}

function tokenOverlap(a, b) {
  const A = new Set(tokens(a));
  const B = new Set(tokens(b));
  if (!A.size || !B.size) return { overlap: 0, ratio: 0, shared: [] };
  const shared = [...A].filter((t) => B.has(t));
  const ratio = shared.length / Math.min(A.size, B.size);
  return { overlap: shared.length, ratio, shared };
}

function addressSignals(hotel, denue) {
  const hAddr = normText([hotel.address, hotel.city].filter(Boolean).join(" "));
  const dAddr = normText(
    [denue.calle, denue.num_ext, denue.colonia, denue.cp, denue.ubicacion].filter(Boolean).join(" ")
  );
  const ov = tokenOverlap(hAddr, dAddr);
  return {
    address_token_overlap: ov.overlap,
    address_token_ratio: ov.ratio,
    shared_address_tokens: ov.shared,
  };
}

/**
 * Score one DENUE candidate against a frozen hotel.
 * Returns score 0–100 + reasons. Confidence bands:
 *   CONFIDENT >= 72, PROBABLE >= 55, AMBIGUOUS otherwise (do not auto-accept).
 */
export function scoreDenueCandidate(hotel, denue) {
  const reasons = [];
  let score = 0;
  const names = [hotel.property_name, hotel.canonical_name, ...(hotel.aliases || [])].filter(Boolean);
  const dName = denue.nombre || "";
  const dRazon = denue.razon_social || "";

  let bestName = { overlap: 0, ratio: 0, shared: [], against: null };
  for (const n of names) {
    for (const against of [dName, dRazon]) {
      const ov = tokenOverlap(n, against);
      if (ov.ratio > bestName.ratio || (ov.ratio === bestName.ratio && ov.overlap > bestName.overlap)) {
        bestName = { ...ov, against };
      }
    }
  }
  if (bestName.ratio >= 0.8 && bestName.overlap >= 2 && meaningfulShared(bestName.shared).length >= 1) {
    score += 40;
    reasons.push(`STRONG_NAME_OVERLAP:${bestName.shared.join(",")}`);
  } else if (bestName.ratio >= 0.5 && meaningfulShared(bestName.shared).length >= 1) {
    score += 25;
    reasons.push(`PARTIAL_NAME_OVERLAP:${bestName.shared.join(",")}`);
  } else if (meaningfulShared(bestName.shared).length >= 1) {
    score += 10;
    reasons.push(`WEAK_NAME_TOKEN:${bestName.shared.join(",")}`);
  } else if (bestName.overlap >= 1) {
    reasons.push(`GEO_OR_GENERIC_NAME_ONLY:${bestName.shared.join(",")}`);
    score -= 5;
  } else {
    reasons.push("NO_NAME_OVERLAP");
  }

  const hLat = Number(hotel.latitude);
  const hLng = Number(hotel.longitude);
  const dLat = Number(denue.latitud);
  const dLng = Number(denue.longitud);
  let distance_m = null;
  if ([hLat, hLng, dLat, dLng].every((n) => Number.isFinite(n))) {
    distance_m = haversineMeters(hLat, hLng, dLat, dLng);
    if (distance_m <= 80) {
      score += 35;
      reasons.push(`GEO_VERY_NEAR:${Math.round(distance_m)}m`);
    } else if (distance_m <= 250) {
      score += 25;
      reasons.push(`GEO_NEAR:${Math.round(distance_m)}m`);
    } else if (distance_m <= 600) {
      score += 12;
      reasons.push(`GEO_NEIGHBORHOOD:${Math.round(distance_m)}m`);
    } else if (distance_m <= 1500) {
      score += 4;
      reasons.push(`GEO_DISTANT:${Math.round(distance_m)}m`);
    } else {
      reasons.push(`GEO_FAR:${Math.round(distance_m)}m`);
      score -= 15;
    }
  } else {
    reasons.push("GEO_MISSING");
  }

  const addr = addressSignals(hotel, denue);
  if (addr.address_token_ratio >= 0.5 && addr.address_token_overlap >= 2) {
    score += 15;
    reasons.push(`ADDRESS_STRONG:${addr.shared_address_tokens.join(",")}`);
  } else if (addr.address_token_overlap >= 1) {
    score += 6;
    reasons.push(`ADDRESS_PARTIAL:${addr.shared_address_tokens.join(",")}`);
  }

  const act = String(denue.actividad || "");
  const code = String(denue.actividad_id || "");
  if (
    isDenueHotelEstablishment({
      codigo_act: code,
      nombre_act: act,
      nom_estab: denue.nombre,
    }) ||
    /^7211/.test(code) ||
    DENUE_HOTEL_ACTIVITY_RE.test(act)
  ) {
    score += 10;
    reasons.push("ACTIVITY_HOTEL_CLASS");
  } else if (act) {
    reasons.push(`ACTIVITY_OTHER:${act.slice(0, 60)}`);
    score -= 8;
  }

  // Same-address multi-business risk: if name is weak but geo very near, flag ambiguity.
  if (distance_m != null && distance_m <= 80 && meaningfulShared(bestName.shared).length < 1) {
    reasons.push("RISK_SAME_NEARBY_ADDRESS_WEAK_NAME");
    score -= 25;
  } else if (distance_m != null && distance_m <= 80 && bestName.ratio < 0.5) {
    reasons.push("RISK_SAME_NEARBY_ADDRESS_WEAK_NAME");
    score -= 10;
  }

  // Require at least one meaningful name token for CONFIDENT/PROBABLE acceptance upstream
  const identity_ok = meaningfulShared(bestName.shared).length >= 1;

  let band = "REJECT";
  if (score >= 72 && identity_ok) band = "CONFIDENT";
  else if (score >= 55 && identity_ok) band = "PROBABLE";
  else if (score >= 40) band = "AMBIGUOUS";

  return {
    score: Math.max(0, Math.min(100, score)),
    band,
    distance_m,
    name_overlap: bestName,
    identity_ok,
    address: addr,
    reasons,
    // Default business relationship from DENUE alone
    business_relationship_from_denue_alone: BUSINESS_RELATIONSHIP.REGISTERED_BUSINESS,
    note: "DENUE match establishes registered business association only — not HOTEL_TO_OWNER.",
  };
}

/**
 * Match hotel against DENUE hotel index rows within a geo/name prefilter.
 */
export function matchHotelToDenue(hotel, denueRows, { maxCandidates = 8, maxDistanceM = 2000 } = {}) {
  const hLat = Number(hotel.latitude);
  const hLng = Number(hotel.longitude);
  const pre = [];
  for (const d of denueRows || []) {
    const dLat = Number(d.latitud);
    const dLng = Number(d.longitud);
    let dist = null;
    if ([hLat, hLng, dLat, dLng].every((n) => Number.isFinite(n))) {
      dist = haversineMeters(hLat, hLng, dLat, dLng);
      if (dist > maxDistanceM) continue;
    } else {
      // keep only if name has any token overlap when geo missing
      const names = [hotel.property_name, hotel.canonical_name, ...(hotel.aliases || [])];
      const any = names.some((n) => tokenOverlap(n, d.nombre || "").overlap >= 1);
      if (!any) continue;
    }
    pre.push(d);
  }

  const scored = pre
    .map((d) => ({ denue: d, match: scoreDenueCandidate(hotel, d) }))
    .sort((a, b) => b.match.score - a.match.score || (a.match.distance_m ?? 9e9) - (b.match.distance_m ?? 9e9));

  const top = scored.slice(0, maxCandidates);
  const best = top[0] || null;
  const confident =
    best &&
    best.match.identity_ok &&
    (best.match.band === "CONFIDENT" || best.match.band === "PROBABLE") &&
    // require clear separation from runner-up when scores close
    (!top[1] || best.match.score - top[1].match.score >= 8 || best.match.band === "CONFIDENT");

  // Ambiguity: two strong candidates within 100m with close scores
  let ambiguous = false;
  if (top.length >= 2 && best) {
    const a = top[0];
    const b = top[1];
    if (
      a.match.score >= 55 &&
      b.match.score >= 50 &&
      a.match.score - b.match.score < 8 &&
      a.match.distance_m != null &&
      b.match.distance_m != null &&
      Math.abs(a.match.distance_m - b.match.distance_m) < 120
    ) {
      ambiguous = true;
    }
  }

  return {
    version: DENUE_MATCH_VERSION,
    accepted: Boolean(confident && !ambiguous && best && best.match.band !== "AMBIGUOUS" && best.match.band !== "REJECT"),
    ambiguous,
    best: best
      ? {
          clee: best.denue.clee,
          establecimiento_id: best.denue.establecimiento_id,
          nombre: best.denue.nombre,
          razon_social: best.denue.razon_social,
          telefono: best.denue.telefono,
          correo: best.denue.correo,
          website: best.denue.website,
          actividad: best.denue.actividad,
          latitud: best.denue.latitud,
          longitud: best.denue.longitud,
          provenance: best.denue.provenance,
          match: best.match,
        }
      : null,
    candidates: top.map((c) => ({
      clee: c.denue.clee,
      establecimiento_id: c.denue.establecimiento_id,
      nombre: c.denue.nombre,
      razon_social: c.denue.razon_social,
      telefono: c.denue.telefono,
      correo: c.denue.correo,
      actividad: c.denue.actividad,
      match: c.match,
    })),
    rejection_reason: !best
      ? "NO_CANDIDATES_IN_RADIUS"
      : ambiguous
        ? "AMBIGUOUS_MULTIPLE_NEARBY"
        : !confident
          ? `BAND_${best.match.band}`
          : null,
  };
}
