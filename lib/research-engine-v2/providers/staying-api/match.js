/**
 * Deterministic Census ↔ StayingAPI property identity matching.
 * EXACT / HIGH only for enrichment proposals. Geography is a hard constraint.
 */

function norm(s) {
  return String(s || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function tokens(s) {
  return norm(s)
    .split(" ")
    .filter((t) => t.length > 2 && !["the", "hotel", "by", "and", "del", "los", "las", "san"].includes(t));
}

function tokenOverlap(a, b) {
  const A = new Set(tokens(a));
  const B = new Set(tokens(b));
  if (!A.size || !B.size) return 0;
  let hit = 0;
  for (const t of A) if (B.has(t)) hit += 1;
  return hit / Math.max(A.size, B.size);
}

function haversineM(lat1, lng1, lat2, lng2) {
  const toRad = (d) => (d * Math.PI) / 180;
  const R = 6371000;
  const dLat = toRad(lat2 - lat1);
  const dLng = toRad(lng2 - lng1);
  const x =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLng / 2) ** 2;
  return 2 * R * Math.asin(Math.sqrt(x));
}

function countryOk(censusCountry, candCountry) {
  const a = norm(censusCountry);
  const b = norm(candCountry);
  if (!a || !b) return false;
  if (a === b) return true;
  if ((a === "mexico" || a === "mx") && (b === "mexico" || b === "mx")) return true;
  return false;
}

function cityOk(censusCity, candCity) {
  const a = norm(censusCity);
  const b = norm(candCity);
  if (!a || !b) return false;
  if (a === b) return true;
  if (a.includes(b) || b.includes(a)) return true;
  // common aliases
  const aliases = {
    cancun: ["cancun", "cancún"],
    "mexico city": ["cdmx", "ciudad de mexico", "mexico city"],
    "playa del carmen": ["playa del carmen", "riviera maya"],
    "puerto vallarta": ["vallarta", "puerto vallarta"],
  };
  for (const [k, vals] of Object.entries(aliases)) {
    if (vals.some((v) => norm(v) === a) && vals.some((v) => norm(v) === b)) return true;
  }
  return false;
}

/**
 * @param {object} censusHotel - Dealality truth
 * @param {object} candidate - normalized Staying candidate
 */
export function matchCensusProperty(censusHotel, candidate) {
  const reasons = [];
  const failures = [];

  if (!candidate?.name) {
    return { level: "REJECT", score: 0, reasons: ["missing_candidate_name"], failures: ["no_name"], distance_m: null };
  }

  // HARD: country
  if (!countryOk(censusHotel.country, candidate.country)) {
    return {
      level: "REJECT",
      score: 0,
      reasons: ["country_mismatch"],
      failures: ["country"],
      distance_m: null,
    };
  }
  reasons.push("country_ok");

  // HARD: city when both present
  const cityMatch = cityOk(censusHotel.city, candidate.city);
  if (censusHotel.city && candidate.city && !cityMatch) {
    return {
      level: "REJECT",
      score: 0,
      reasons: ["city_mismatch_hard"],
      failures: ["city"],
      distance_m: null,
    };
  }
  if (cityMatch) reasons.push("city_ok");

  const nameOverlap = tokenOverlap(censusHotel.name, candidate.name);
  const brand = norm(censusHotel.brand);
  const candName = norm(candidate.name);
  const brandInName = brand && candName.includes(brand.split(" ")[0]) && brand.split(" ")[0].length > 3;

  let distance_m = null;
  if (
    censusHotel.latitude != null &&
    censusHotel.longitude != null &&
    candidate.latitude != null &&
    candidate.longitude != null
  ) {
    distance_m = haversineM(
      Number(censusHotel.latitude),
      Number(censusHotel.longitude),
      Number(candidate.latitude),
      Number(candidate.longitude)
    );
  }

  let score = 0;
  score += nameOverlap * 40;
  if (brandInName) {
    score += 15;
    reasons.push("brand_token_in_name");
  }
  if (cityMatch) score += 20;
  if (distance_m != null) {
    if (distance_m <= 150) {
      score += 25;
      reasons.push("coords_within_150m");
    } else if (distance_m <= 500) {
      score += 15;
      reasons.push("coords_within_500m");
    } else if (distance_m <= 2000) {
      score += 5;
      reasons.push("coords_within_2km");
    } else {
      failures.push("coords_far");
      score -= 20;
    }
  }

  // Address token overlap soft bonus
  if (censusHotel.address && candidate.address) {
    const addrOverlap = tokenOverlap(censusHotel.address, candidate.address);
    score += addrOverlap * 10;
    if (addrOverlap >= 0.4) reasons.push("address_token_overlap");
  }

  // Sibling guard: same brand + same city + low name overlap → reject/low
  if (brandInName && cityMatch && nameOverlap < 0.35) {
    failures.push("possible_sibling_property");
    score -= 25;
  }

  let level;
  if (score >= 75 && nameOverlap >= 0.45 && cityMatch && (distance_m == null || distance_m <= 500)) {
    level = "EXACT";
  } else if (score >= 60 && nameOverlap >= 0.35 && cityMatch && (distance_m == null || distance_m <= 2000)) {
    level = "HIGH";
  } else if (score >= 45 && cityMatch) {
    level = "MEDIUM";
  } else if (score >= 30) {
    level = "LOW";
  } else {
    level = "REJECT";
  }

  // Material sibling / far pin → demote
  if (failures.includes("possible_sibling_property") && level === "EXACT") level = "MEDIUM";
  if (failures.includes("coords_far") && distance_m > 5000) level = "REJECT";

  return {
    level,
    score: Math.round(score * 10) / 10,
    name_overlap: Math.round(nameOverlap * 1000) / 1000,
    distance_m: distance_m != null ? Math.round(distance_m) : null,
    reasons,
    failures,
    enrichment_eligible: level === "EXACT" || level === "HIGH",
  };
}

export { haversineM, norm, tokenOverlap };
