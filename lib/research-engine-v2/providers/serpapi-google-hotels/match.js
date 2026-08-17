/**
 * Deterministic Census ↔ SerpApi Google Hotels property identity matching.
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
  if (!a) return true; // allow when candidate country parsed later from address
  if (!b) return true; // Google often omits separate country on search cards
  if (a === b) return true;
  if ((a === "mexico" || a === "mx") && (b === "mexico" || b === "mx" || b.includes("mexico"))) return true;
  return false;
}

function cityOk(censusCity, candCity) {
  const a = norm(censusCity);
  const b = norm(candCity);
  if (!a || !b) return false;
  if (a === b) return true;
  if (a.includes(b) || b.includes(a)) return true;
  const aliases = {
    cancun: ["cancun", "cancún"],
    "mexico city": ["cdmx", "ciudad de mexico", "mexico city"],
    "playa del carmen": ["playa del carmen", "riviera maya"],
    "puerto vallarta": ["vallarta", "puerto vallarta"],
    "riviera maya": ["riviera maya", "playa del carmen", "tulum", "paraiso", "paraíso"],
    "san luis potosi": ["san luis potosi", "san luis potosí", "slp"],
    queretaro: ["queretaro", "querétaro"],
    chilpancingo: ["chilpancingo"],
    puebla: ["puebla"],
    celaya: ["celaya"],
    oaxaca: ["oaxaca"],
    uruapan: ["uruapan"],
    monterrey: ["monterrey"],
    "punta de mita": ["punta de mita", "bahia de banderas", "bahía de banderas"],
    tulum: ["tulum", "riviera maya"],
  };
  for (const vals of Object.values(aliases)) {
    if (vals.some((v) => norm(v) === a || a.includes(norm(v))) && vals.some((v) => b.includes(norm(v)) || norm(v) === b))
      return true;
  }
  return false;
}

function cityInAddress(censusCity, address) {
  if (!censusCity || !address) return false;
  return norm(address).includes(norm(censusCity)) || cityOk(censusCity, address);
}

/**
 * @param {object} censusHotel - Dealality truth / challenge hint
 * @param {object} candidate - normalized SerpApi candidate
 */
export function matchCensusProperty(censusHotel, candidate) {
  const reasons = [];
  const failures = [];

  if (!candidate?.name) {
    return { level: "REJECT", score: 0, reasons: ["missing_candidate_name"], failures: ["no_name"], distance_m: null };
  }

  if (!countryOk(censusHotel.country || "Mexico", candidate.country)) {
    return {
      level: "REJECT",
      score: 0,
      reasons: ["country_mismatch"],
      failures: ["country"],
      distance_m: null,
    };
  }
  reasons.push("country_ok");

  const cityMatch =
    cityOk(censusHotel.city, candidate.city) || cityInAddress(censusHotel.city, candidate.address);
  const censusCityMissing = !String(censusHotel.city || "").trim();
  if (censusHotel.city && (candidate.city || candidate.address) && !cityMatch) {
    // Soft for discovery challenges where city unknown — still hard when both sides have city
    if (censusHotel.city && candidate.city) {
      return {
        level: "REJECT",
        score: 0,
        reasons: ["city_mismatch_hard"],
        failures: ["city"],
        distance_m: null,
      };
    }
  }
  if (cityMatch) reasons.push("city_ok");
  if (censusCityMissing) reasons.push("census_city_absent_soft");

  const nameOverlap = tokenOverlap(censusHotel.name || censusHotel.name_hint, candidate.name);
  const brand = norm(censusHotel.brand);
  const candName = norm(candidate.name);
  const brandFirst = brand.split(" ")[0];
  const brandInName = brandFirst && brandFirst.length > 3 && candName.includes(brandFirst);

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

  if (censusHotel.website && candidate.website) {
    try {
      const a = new URL(censusHotel.website).hostname.replace(/^www\./, "");
      const b = new URL(candidate.website).hostname.replace(/^www\./, "");
      if (a && b && (a === b || a.includes(b) || b.includes(a))) {
        score += 20;
        reasons.push("website_host_match");
      }
    } catch {
      /* ignore */
    }
  }

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

  if (censusHotel.address && candidate.address) {
    const addrOverlap = tokenOverlap(censusHotel.address, candidate.address);
    score += addrOverlap * 10;
    if (addrOverlap >= 0.4) reasons.push("address_token_overlap");
  }

  if (brandInName && cityMatch && nameOverlap < 0.35) {
    failures.push("possible_sibling_property");
    score -= 25;
  }

  let level;
  // When census city is absent (postal/admin labels stripped), allow HIGH/EXACT on strong name+country.
  const geoOk = cityMatch || censusCityMissing;
  if (score >= 75 && nameOverlap >= 0.45 && geoOk && (distance_m == null || distance_m <= 500)) {
    level = "EXACT";
  } else if (score >= 60 && nameOverlap >= 0.35 && geoOk && (distance_m == null || distance_m <= 2000)) {
    level = "HIGH";
  } else if (score >= 45 && (cityMatch || nameOverlap >= 0.5 || censusCityMissing)) {
    level = "MEDIUM";
  } else if (score >= 30) {
    level = "LOW";
  } else {
    level = "REJECT";
  }

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

export { haversineM, norm, tokenOverlap, cityOk };
