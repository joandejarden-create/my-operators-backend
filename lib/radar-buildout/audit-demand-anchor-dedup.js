/**
 * Audit Demand Anchor records for duplicate / near-duplicate clusters.
 */

import {
  normalizeAnchorName,
  nameSimilarity,
  coordsWithinTolerance,
  isDuplicateCandidate,
} from "../demand-anchors/import-validation.js";

const CANCUN_MARKET_KEYWORDS = [
  "cancun",
  "cancún",
  "cozumel",
  "tulum",
  "playa del carmen",
  "riviera maya",
  "isla mujeres",
  "puerto aventuras",
  "akumal",
  "mayakoba",
  "playa mujeres",
  "costa mujeres",
  "quintana roo",
  "puerto morelos",
];

function norm(value) {
  return String(value || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .trim();
}

function haystack(record) {
  return [record.name, record.city, record.submarket, record.notes, record.market]
    .map(norm)
    .join(" | ");
}

function matchesMarket(record, market, keywords) {
  const m = norm(market);
  const h = haystack(record);
  if (m && h.includes(m)) return true;
  return (keywords || []).some((kw) => h.includes(norm(kw)));
}

function classifyPair(a, b, ctx) {
  const dup = isDuplicateCandidate(a, b, ctx);
  if (dup.duplicate) {
    if (
      dup.reason === "same_normalized_name_city_country" ||
      dup.reason === "same_coordinates" ||
      dup.reason === "same_source_reference"
    ) {
      return { level: "definite", reason: dup.reason, similarity: dup.similarity };
    }
    return { level: "possible", reason: dup.reason, similarity: dup.similarity };
  }

  const sim = nameSimilarity(a.name, b.name);
  if (sim >= 0.85 && a.pointType === b.pointType) {
    const sameCity = norm(a.city) === norm(b.city);
    const near =
      coordsWithinTolerance(a.latitude, a.longitude, b.latitude, b.longitude) ||
      (sameCity && sim >= 0.92);
    if (near) return { level: "possible", reason: "similar_name_type_proximity", similarity: sim };
  }

  return null;
}

function recommendAction(level, reason, a, b) {
  if (level === "definite") {
    if (reason === "same_source_reference") return { action: "merge/suppress one", keep: pickStronger(a, b) };
    if (reason === "same_coordinates") return { action: "merge/suppress one", keep: pickStronger(a, b) };
    if (reason === "same_normalized_name_city_country") {
      return { action: "merge/suppress one", keep: pickStronger(a, b) };
    }
    return { action: "manual review", keep: "review both" };
  }
  if (reason === "similar_name_same_type_market") {
    return { action: "manual review", keep: pickStronger(a, b) };
  }
  if (reason === "similar_name_type_proximity") {
    return { action: "update name", keep: pickStronger(a, b) };
  }
  return { action: "leave unchanged", keep: "both" };
}

function pickStronger(a, b) {
  const score = (r) => {
    let s = 0;
    if (String(r.sourceReference || "").trim()) s += 2;
    if (String(r.dataConfidence || "").toLowerCase() === "high") s += 2;
    if (String(r.submarket || "").trim()) s += 1;
    if (String(r.notes || "").length > 80) s += 1;
    return s;
  };
  return score(a) >= score(b) ? a.name : b.name;
}

/**
 * @param {object[]} records — normalized demand anchor points
 * @param {object} options
 */
export function auditDemandAnchorDedup(records, options = {}) {
  const country = options.country || "";
  const market = options.market || "";
  const keywords = options.keywords || CANCUN_MARKET_KEYWORDS;

  const scoped = (records || []).filter((r) => {
    if (country && norm(r.country) !== norm(country)) return false;
    if (market) return matchesMarket(r, market, keywords);
    return true;
  });

  const definiteDuplicates = [];
  const possibleDuplicates = [];
  const involvedIds = new Set();

  for (let i = 0; i < scoped.length; i += 1) {
    for (let j = i + 1; j < scoped.length; j += 1) {
      const a = scoped[i];
      const b = scoped[j];
      const hit = classifyPair(a, b, { market, country, region: options.region });
      if (!hit) continue;

      const rec = {
        recordA: { id: a.id, name: a.name, city: a.city, submarket: a.submarket, pointType: a.pointType, latitude: a.latitude, longitude: a.longitude, sourceReference: a.sourceReference },
        recordB: { id: b.id, name: b.name, city: b.city, submarket: b.submarket, pointType: b.pointType, latitude: b.latitude, longitude: b.longitude, sourceReference: b.sourceReference },
        reason: hit.reason,
        similarity: hit.similarity,
        recommendation: recommendAction(hit.level, hit.reason, a, b),
      };

      involvedIds.add(a.id);
      involvedIds.add(b.id);

      if (hit.level === "definite") definiteDuplicates.push(rec);
      else possibleDuplicates.push(rec);
    }
  }

  const safeToKeep = scoped.filter((r) => !involvedIds.has(r.id)).map((r) => ({
    id: r.id,
    name: r.name,
    city: r.city,
    submarket: r.submarket,
    pointType: r.pointType,
  }));

  const manualReview = [...definiteDuplicates, ...possibleDuplicates].map((d) => ({
    names: [d.recordA.name, d.recordB.name],
    reason: d.reason,
    recommendedAction: d.recommendation.action,
    keep: d.recommendation.keep,
  }));

  const importedNames = new Set(scoped.map((r) => normalizeAnchorName(r.name)));

  return {
    auditedAt: new Date().toISOString(),
    country,
    market,
    summary: {
      countryDemandAnchorsTotal: (records || []).filter((r) => norm(r.country) === norm(country)).length,
      marketScopedRecords: scoped.length,
      definiteDuplicatePairs: definiteDuplicates.length,
      possibleDuplicatePairs: possibleDuplicates.length,
      safeToKeepCount: safeToKeep.length,
      manualReviewCount: manualReview.length,
      importedNormalizedNames: importedNames.size,
    },
    definiteDuplicates,
    possibleDuplicates,
    safeToKeep,
    manualReview,
    importedNameKeys: [...importedNames],
    notes: [
      "Read-only audit — no Airtable modifications performed.",
      "Use importedNameKeys to filter delta fixture candidates.",
    ],
  };
}
