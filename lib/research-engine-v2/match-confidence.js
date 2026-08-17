/**
 * Research Engine V2.1 — entity match confidence gate.
 * Material corrections require Exact or High unless explicit official ID binds the record.
 */

import { tokenSimilarity, tokenize } from "./adapters/adapter-utils.js";
import { countriesAlign, citiesAlign, normalizeGeoLabel } from "./geo-normalize.js";
import { canonicalizeObservedBrand } from "./brand-family.js";

export const MATCH_LEVELS = Object.freeze(["Exact", "High", "Medium", "Low", "Reject"]);

/** Frozen V1.1 thresholds (do not tune after unseen benchmark starts). */
export const MATCH_GATE_CONFIG_V1_1 = Object.freeze({
  version: "contradiction-first-v1.1",
  materialRequires: ["Exact", "High"],
  reviewAllowed: ["Medium"],
  blockMaterial: ["Low", "Reject"],
  minNameSimilarityForMedium: 0.55,
  minNameSimilarityForHigh: 0.72,
  requireCountryAlign: true,
  requireCityAlignForMaterial: true,
  requireBrandFamilyAlignForBrandChange: true,
  stopTokens: Object.freeze([
    "the",
    "and",
    "hotel",
    "hotels",
    "resort",
    "resorts",
    "spa",
    "by",
    "member",
    "collection",
    "portfolio",
    "autograph",
    "tribute",
    "indigo",
    "kimpton",
    "avani",
    "radisson",
    "individuals",
    "faranda",
    "boutique",
    "grand",
    "express",
    "ascend",
    "holiday",
    "inn",
    "intercontinental",
  ]),
});

/**
 * @param {object} dealalityHotel
 * @param {object} directoryCandidate
 * @param {object} [opts]
 * @returns {{
 *   level: "Exact"|"High"|"Medium"|"Low"|"Reject",
 *   score: number,
 *   signals: object,
 *   reasons: string[],
 *   allowMaterialCorrection: boolean,
 *   allowReviewOnly: boolean,
 * }}
 */
export function assessEntityMatch(dealalityHotel, directoryCandidate, opts = {}) {
  const cfg = { ...MATCH_GATE_CONFIG_V1_1, ...(opts.config || {}) };
  const reasons = [];
  /** @type {Record<string, boolean|number|string|null>} */
  const signals = {};

  if (!directoryCandidate) {
    return result("Reject", 0, signals, ["No directory candidate"], cfg);
  }

  const dName = String(dealalityHotel.name || dealalityHotel.hotelName || "");
  const cName = String(
    directoryCandidate.officialHotelName ||
      directoryCandidate.name ||
      directoryCandidate.inferredHotelName ||
      directoryCandidate.propertyName ||
      directoryCandidate.title ||
      ""
  );

  const nameSim = distinctiveNameSimilarity(dName, cName, cfg.stopTokens);
  signals.nameSimilarity = Number(nameSim.toFixed(3));
  signals.dealalityName = dName;
  signals.candidateName = cName;

  // Explicit property ID / MARSHA / mnemonic / official URL bind
  const idHit = identityIdAlign(dealalityHotel, directoryCandidate);
  signals.propertyIdAlign = idHit.align;
  signals.propertyIdType = idHit.type;
  if (idHit.align) reasons.push(`Explicit ${idHit.type} identity align`);

  const urlHit = officialUrlAlign(dealalityHotel, directoryCandidate);
  signals.officialUrlAlign = urlHit;
  if (urlHit) reasons.push("Official URL aligns");

  const countryOk = countriesAlign(dealalityHotel.country, directoryCandidate.country || directoryCandidate.countryRegion);
  signals.countryAlign = countryOk;
  if (!countryOk && cfg.requireCountryAlign) {
    reasons.push("Country mismatch → Reject");
    return result("Reject", nameSim * 0.2, signals, reasons, cfg);
  }

  const cityOk = citiesAlign(
    dealalityHotel.city || "",
    directoryCandidate.city || directoryCandidate.citySlug || "",
    { hotelName: dName, candidateName: cName }
  );
  signals.cityAlign = cityOk.status;
  signals.cityNote = cityOk.note || null;
  if (cityOk.status === "reject") {
    reasons.push(`City/geo reject: ${cityOk.note}`);
    return result("Reject", nameSim * 0.25, signals, reasons, cfg);
  }

  const brandAlign = brandLabelsAlign(
    dealalityHotel.currentBrand || dealalityHotel.affiliation,
    directoryCandidate.brand || directoryCandidate.observedBrand
  );
  signals.brandAlign = brandAlign;
  if (brandAlign === "conflict" && opts.requireSameBrand !== false) {
    // Sibling under same parent but different brand → strong reject for identity
    reasons.push("Property-level brand conflict (parent-family contamination risk)");
    return result("Reject", nameSim * 0.3, signals, reasons, cfg);
  }

  // Distinctive token collision without city/brand → often sibling (Casa Francia vs Casa Nizuc)
  const distinctive = distinctiveTokens(dName, cfg.stopTokens);
  const candDistinctive = distinctiveTokens(cName, cfg.stopTokens);
  const sharedDistinctive = distinctive.filter((t) => candDistinctive.includes(t));
  signals.sharedDistinctiveTokens = sharedDistinctive;
  if (
    sharedDistinctive.length === 0 &&
    nameSim < cfg.minNameSimilarityForMedium &&
    !idHit.align &&
    !urlHit
  ) {
    reasons.push("No shared distinctive name tokens");
    return result("Reject", nameSim, signals, reasons, cfg);
  }

  // Sibling detector: same city + same brand family tokens but different distinctive place tokens
  if (
    cityOk.status === "align" &&
    brandAlign === "align" &&
    distinctive.length >= 1 &&
    candDistinctive.length >= 1 &&
    sharedDistinctive.length === 0 &&
    nameSim < 0.65 &&
    !idHit.align
  ) {
    reasons.push("Likely same-brand sibling (city align, no shared place token)");
    return result("Reject", nameSim, signals, reasons, cfg);
  }

  let level = "Low";
  if (idHit.align && (countryOk || urlHit) && (brandAlign !== "conflict")) {
    level = cityOk.status === "align" || urlHit ? "Exact" : "High";
    reasons.push("Exact/High via explicit property identity");
  } else if (urlHit && brandAlign !== "conflict" && countryOk) {
    level = nameSim >= 0.4 || cityOk.status === "align" ? "Exact" : "High";
    reasons.push("Official website bind");
  } else if (
    nameSim >= cfg.minNameSimilarityForHigh &&
    countryOk &&
    (cityOk.status === "align" || cityOk.status === "alias") &&
    brandAlign !== "conflict"
  ) {
    level = "High";
    reasons.push("High name + geo + brand-compatible");
  } else if (
    nameSim >= cfg.minNameSimilarityForMedium &&
    countryOk &&
    cityOk.status !== "reject" &&
    brandAlign !== "conflict"
  ) {
    level = cityOk.status === "unknown" ? "Medium" : "Medium";
    reasons.push("Medium: likely but ambiguous geo/name");
  } else if (nameSim >= 0.35 && countryOk) {
    level = "Low";
    reasons.push("Low: partial name overlap only");
  } else {
    level = "Reject";
    reasons.push("Insufficient identity overlap");
  }

  // Material geo hard gate
  if (
    (level === "Exact" || level === "High") &&
    cfg.requireCityAlignForMaterial &&
    cityOk.status === "unknown" &&
    !idHit.align &&
    !urlHit
  ) {
    level = "Medium";
    reasons.push("Downgraded to Medium: city not confirmed");
  }

  return result(level, nameSim, signals, reasons, cfg);
}

function result(level, score, signals, reasons, cfg) {
  return {
    level,
    score: Number(score.toFixed(3)),
    signals,
    reasons,
    allowMaterialCorrection: cfg.materialRequires.includes(level),
    allowReviewOnly: cfg.reviewAllowed.includes(level) || cfg.materialRequires.includes(level),
  };
}

function distinctiveNameSimilarity(a, b, stopTokens) {
  const stop = new Set(stopTokens || MATCH_GATE_CONFIG_V1_1.stopTokens);
  const ta = new Set(tokenize(a).filter((t) => !stop.has(t)));
  const tb = new Set(tokenize(b).filter((t) => !stop.has(t)));
  // fall back to full token similarity if stripping removes everything
  if (!ta.size || !tb.size) return tokenSimilarity(a, b);
  let inter = 0;
  for (const t of ta) if (tb.has(t)) inter++;
  return inter / (ta.size + tb.size - inter);
}

function distinctiveTokens(name, stopTokens) {
  const stop = new Set(stopTokens || MATCH_GATE_CONFIG_V1_1.stopTokens);
  return tokenize(name).filter((t) => !stop.has(t));
}

function identityIdAlign(hotel, candidate) {
  const hotelIds = [
    hotel.propertyId,
    hotel.brandPropertyCode,
    hotel.marsha,
    hotel.ctyhocn,
    hotel.mnemonic,
  ]
    .map((x) => String(x || "").trim().toUpperCase())
    .filter(Boolean);
  const candIds = [
    candidate.propertyId,
    candidate.brandPropertyCode,
    candidate.marsha,
    candidate.marshaCode,
    candidate.ctyhocn,
    candidate.mnemonic,
    candidate.rawSignals?.marsha,
  ]
    .map((x) => String(x || "").trim().toUpperCase())
    .filter(Boolean);

  for (const id of hotelIds) {
    if (candIds.includes(id)) return { align: true, type: "property_code" };
  }

  // Extract mnemonic from IHG/Marriott URL if present
  const candUrl = String(candidate.officialUrl || candidate.propertyUrl || candidate.website || "");
  const hotelUrl = String(hotel.website || hotel.officialUrl || "");
  if (hotelUrl && candUrl && normalizeUrlKey(hotelUrl) === normalizeUrlKey(candUrl)) {
    return { align: true, type: "url" };
  }
  return { align: false, type: null };
}

function officialUrlAlign(hotel, candidate) {
  const hotelUrl = String(hotel.website || hotel.officialUrl || "").trim();
  const candUrl = String(candidate.officialUrl || candidate.propertyUrl || candidate.website || "").trim();
  if (!hotelUrl || !candUrl) return false;
  return normalizeUrlKey(hotelUrl) === normalizeUrlKey(candUrl);
}

function normalizeUrlKey(url) {
  return String(url || "")
    .toLowerCase()
    .replace(/^https?:\/\//, "")
    .replace(/\/+$/, "")
    .replace(/\/(overview|photos|hoteldetail)\/?$/, "/$1");
}

/**
 * @param {string} a
 * @param {string} b
 * @returns {"align"|"compatible"|"conflict"|"unknown"}
 */
export function brandLabelsAlign(a, b) {
  const ca = canonicalizeObservedBrand(a);
  const cb = canonicalizeObservedBrand(b);
  if (!ca || !cb) return "unknown";
  const na = ca.toLowerCase();
  const nb = cb.toLowerCase();
  if (na === nb) return "align";
  if (na.includes(nb) || nb.includes(na)) return "align";
  if (/indigo/.test(na) && /indigo/.test(nb)) return "align";
  if (/kimpton/.test(na) && /kimpton/.test(nb)) return "align";
  if (/tribute/.test(na) && /tribute/.test(nb)) return "align";
  if (/autograph/.test(na) && /autograph/.test(nb)) return "align";
  if (/design hotels/.test(na) && /design hotels/.test(nb)) return "align";
  if (/radisson individual/.test(na) && /radisson individual/.test(nb)) return "align";
  if (/avani/.test(na) && /avani/.test(nb)) return "align";

  // Same parent family but different brand → conflict for identity matching
  const ihg = /indigo|kimpton|holiday|intercontinental|crowne|voco|staybridge/;
  const marriott = /tribute|autograph|design hotels|marriott|westin|sheraton/;
  const choice = /radisson|ascend|cambria|choice|quality|comfort/;
  if (ihg.test(na) && ihg.test(nb) && na !== nb) return "conflict";
  if (marriott.test(na) && marriott.test(nb) && na !== nb) return "conflict";
  if (choice.test(na) && choice.test(nb) && na !== nb) return "conflict";
  return "conflict";
}

export { normalizeGeoLabel };
