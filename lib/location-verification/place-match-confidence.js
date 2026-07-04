/**
 * Place match confidence scoring for pre-import location verification.
 */

/** @type {string[][]} */
const BILINGUAL_TOKEN_GROUPS = [
  ["convention", "convenciones", "convencion"],
  ["center", "centro", "centres"],
  ["university", "universidad"],
  ["college", "colegio"],
  ["hospital", "clinica", "clinic"],
  ["stadium", "estadio"],
  ["airport", "aeropuerto"],
  ["international", "internacional"],
  ["national", "nacional"],
  ["metropolitan", "metropolitana"],
  ["sanctuary", "santuario"],
  ["events", "eventos", "event"],
  ["waterfront", "malecon", "costanera", "costera"],
  ["beach", "playa"],
  ["delfines", "dolphins"],
  ["arqueologica", "archaeological", "archeological"],
  ["fundadores", "founders"],
  ["garrafon", "garrafón"],
  ["kukulcan", "kukulkan"],
  ["campus", "sedes"],
  ["industrial", "industria"],
  ["zone", "zona"],
  ["park", "parque"],
  ["port", "puerto"],
  ["island", "isla"],
  ["valley", "valle"],
  ["river", "rio"],
  ["boulevard", "bulevar", "bulevard"],
  ["palace", "palacio"],
  ["museum", "museo"],
  ["theater", "teatro"],
  ["plaza", "square"],
  ["attraction", "atraccion", "turistico"],
  ["district", "distrito", "corredor"],
  ["government", "gobierno", "alcaldia"],
  ["medical", "salud"],
  ["sports", "deportivo", "deportiva"],
  ["entertainment", "entretenimiento"],
  ["metropolitan", "metropolitana", "metropolitano"],
  ["melendez", "meléndez"],
  ["roberto", "roberto"],
  ["stadium", "estadio"],
  ["administrative", "administrativo", "administrativa"],
  ["municipal", "municipal"],
  ["international", "internacional"],
  ["aeropuerto", "airport"],
  ["malecon", "malecón"],
];

/** @type {Map<string, string>} */
const TOKEN_CANON = new Map();
for (const group of BILINGUAL_TOKEN_GROUPS) {
  const canon = group[0];
  for (const token of group) TOKEN_CANON.set(token, canon);
}

const STOP_TOKENS = new Set([
  "the",
  "de",
  "del",
  "la",
  "el",
  "los",
  "las",
  "and",
  "of",
  "y",
  "en",
  "san",
  "santa",
  "santo",
]);

function normalizeText(value) {
  return String(value || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function stripParentheticals(value) {
  return String(value || "")
    .replace(/\([^)]*\)/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function canonicalToken(token) {
  return TOKEN_CANON.get(token) || token;
}

function tokenSet(value) {
  const tokens = normalizeText(stripParentheticals(value))
    .split(" ")
    .filter((t) => t.length > 2 && !STOP_TOKENS.has(t))
    .map(canonicalToken);
  return new Set(tokens);
}

function jaccardSimilarity(setA, setB) {
  if (!setA.size || !setB.size) return 0;
  let intersection = 0;
  for (const t of setA) if (setB.has(t)) intersection += 1;
  return intersection / (setA.size + setB.size - intersection);
}

function containmentBoost(a, b) {
  const na = normalizeText(stripParentheticals(a));
  const nb = normalizeText(stripParentheticals(b));
  if (!na || !nb) return 0;
  if (na === nb) return 0.35;
  if (na.includes(nb) || nb.includes(na)) return 0.2;

  const shorter = na.length <= nb.length ? na : nb;
  const longer = na.length <= nb.length ? nb : na;
  const shorterTokens = shorter.split(" ").filter((t) => t.length > 3 && !STOP_TOKENS.has(t));
  if (!shorterTokens.length) return 0;
  const matched = shorterTokens.filter((t) => longer.includes(t)).length;
  if (matched === shorterTokens.length) return 0.15;
  return 0;
}

export function nameSimilarity(a, b) {
  const A = tokenSet(a);
  const B = tokenSet(b);
  const base = jaccardSimilarity(A, B);
  const boost = containmentBoost(a, b);
  return Math.min(1, base + boost);
}

export function haversineMeters(lat1, lng1, lat2, lng2) {
  const R = 6371000;
  const dLat = ((lat2 - lat1) * Math.PI) / 180;
  const dLng = ((lng2 - lng1) * Math.PI) / 180;
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos((lat1 * Math.PI) / 180) *
      Math.cos((lat2 * Math.PI) / 180) *
      Math.sin(dLng / 2) ** 2;
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

function inferPointTypeCompatibility(candidatePointType, googleTypes = []) {
  const p = normalizeText(candidatePointType);
  const types = (googleTypes || []).map(normalizeText);
  if (!p) return true;
  if (p.includes("medical") && types.some((t) => t.includes("hospital") || t.includes("health"))) return true;
  if (p.includes("university") && types.some((t) => t.includes("university") || t.includes("school"))) return true;
  if (p.includes("convention") && types.some((t) => t.includes("convention") || t.includes("event"))) return true;
  if (p.includes("sports") && types.some((t) => t.includes("stadium") || t.includes("sports"))) return true;
  if (p.includes("beach") && types.some((t) => t.includes("beach") || t.includes("tourist_attraction"))) return true;
  if (p.includes("government") && types.some((t) => t.includes("government") || t.includes("city_hall"))) return true;
  if (p.includes("industrial") && types.some((t) => t.includes("industrial") || t.includes("storage"))) return true;
  if (p.includes("entertainment") && types.some((t) => t.includes("night_club") || t.includes("tourist_attraction"))) return true;
  if (p.includes("tourist") && types.some((t) => t.includes("tourist_attraction") || t.includes("museum"))) return true;
  if (p.includes("business") && types.some((t) => t.includes("locality") || t.includes("neighborhood"))) return true;
  return true;
}

function cityCountryMatch(candidateCity, candidateCountry, formattedAddress) {
  const addrNorm = normalizeText(formattedAddress);
  const cityNorm = normalizeText(candidateCity);
  const countryNorm = normalizeText(candidateCountry);

  const cityOk =
    !cityNorm ||
    addrNorm.includes(cityNorm) ||
    cityNorm.split(" ").some((part) => part.length > 3 && addrNorm.includes(part));

  const countryOk = !countryNorm || addrNorm.includes(countryNorm);
  return cityOk && countryOk;
}

export function evaluatePlaceMatchConfidence(input) {
  const {
    candidateName,
    candidateCity,
    candidateCountry,
    candidateLatitude,
    candidateLongitude,
    candidatePointType,
    result,
    competingResults = [],
  } = input || {};

  if (!result) {
    return {
      verificationStatus: "No Match",
      matchConfidence: "Low",
      verificationNotes: "No usable Google result returned.",
      distanceMeters: null,
      nameScore: 0,
    };
  }

  const googleName = result.googleName || result.displayName || "";
  const nameScore = nameSimilarity(candidateName, googleName);
  const cityCountryOk = cityCountryMatch(
    candidateCity,
    candidateCountry,
    result.googleFormattedAddress || ""
  );
  const countryNorm = normalizeText(candidateCountry);
  const addrNorm = normalizeText(result.googleFormattedAddress || "");

  let distanceMeters = null;
  if (
    Number.isFinite(candidateLatitude) &&
    Number.isFinite(candidateLongitude) &&
    Number.isFinite(result.googleLatitude) &&
    Number.isFinite(result.googleLongitude)
  ) {
    distanceMeters = haversineMeters(
      Number(candidateLatitude),
      Number(candidateLongitude),
      Number(result.googleLatitude),
      Number(result.googleLongitude)
    );
  }

  const typeCompatible = inferPointTypeCompatibility(candidatePointType, result.googleTypes || []);

  const countryOnlyOk =
    Boolean(countryNorm && addrNorm.includes(countryNorm)) &&
    nameScore >= 0.85 &&
    (distanceMeters == null || distanceMeters <= 500);
  const locationOk = cityCountryOk || countryOnlyOk;

  const secondBest = (competingResults || [])
    .filter((r) => r && r !== result)
    .map((r) => nameSimilarity(candidateName, r.googleName || r.displayName || ""))
    .sort((a, b) => b - a)[0];
  const ambiguous =
    nameScore < 0.95 &&
    secondBest != null &&
    Math.abs((secondBest || 0) - nameScore) <= 0.08 &&
    secondBest >= 0.65;
  if (ambiguous) {
    return {
      verificationStatus: "Ambiguous Match",
      matchConfidence: "Low",
      verificationNotes: "Multiple plausible Google results with similar confidence.",
      distanceMeters,
      nameScore,
    };
  }

  const hasCandidateCoords = Number.isFinite(candidateLatitude) && Number.isFinite(candidateLongitude);
  const close750 = distanceMeters == null || distanceMeters <= 750;
  const close3500 = distanceMeters == null || distanceMeters <= 3500;
  const farSeedCoords = distanceMeters != null && distanceMeters > 750;
  const veryFarSeedCoords = distanceMeters != null && distanceMeters > 3500;

  let matchConfidence = "Low";
  let verificationStatus = "Needs Review";

  if (!locationOk || !typeCompatible) {
    matchConfidence = "Low";
    verificationStatus = "Needs Review";
  } else if (nameScore >= 0.85 && close3500) {
    matchConfidence = "High";
    verificationStatus = "Verified";
  } else if (nameScore >= 0.85 && veryFarSeedCoords) {
    matchConfidence = "Medium";
    verificationStatus = "Needs Review";
  } else if (nameScore >= 0.7 && close750) {
    matchConfidence = "Medium";
    verificationStatus = "Needs Review";
  } else if (nameScore >= 0.7 && farSeedCoords && close3500) {
    matchConfidence = "Medium";
    verificationStatus = "Needs Review";
  } else {
    matchConfidence = "Low";
    verificationStatus = "Needs Review";
  }

  const notes = [];
  notes.push(`nameScore=${nameScore.toFixed(2)}`);
  if (distanceMeters != null) notes.push(`distance=${Math.round(distanceMeters)}m`);
  notes.push(`cityCountryMatch=${cityCountryOk ? "yes" : countryOnlyOk ? "country-only" : "no"}`);
  notes.push(`typeCompatible=${typeCompatible ? "yes" : "no"}`);
  if (farSeedCoords && nameScore >= 0.7) notes.push("coordCorrection=yes");

  return {
    verificationStatus,
    matchConfidence,
    verificationNotes: notes.join("; "),
    distanceMeters,
    nameScore,
  };
}
