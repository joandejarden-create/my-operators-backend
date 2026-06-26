/**
 * Mexico Cancún — Demand Anchor duplicate suppression (safe pairs only).
 */

import {
  normalizeAnchorName,
  nameSimilarity,
  coordsWithinTolerance,
} from "../demand-anchors/import-validation.js";
import { DEMAND_ANCHORS_FIELDS as F } from "../demand-anchors/airtable-demand-anchors-fields.js";

const SUPPRESS_NOTE =
  "Suppressed during Cancún / Riviera Maya duplicate cleanup; stronger duplicate retained.";

const SAFE_DEFINITE_REASONS = new Set([
  "same_normalized_name_city_country",
  "same_coordinates",
]);

const GOVERNANCE_FIELD_CANDIDATES = {
  scopeLevel: ["Scope Level"],
  relevanceTier: ["Relevance Tier"],
  defaultMapVisibility: ["Default Map Visibility"],
  externalVisibilityLevel: ["External Visibility Level"],
};

const GOVERNANCE_SUPPRESS_VALUES = {
  scopeLevel: "Reference Only",
  relevanceTier: "Tier 4 — Reference Only",
  defaultMapVisibility: "Hide By Default",
  externalVisibilityLevel: "Do Not Share",
};

function norm(value) {
  return String(value || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .trim();
}

function isAlreadySuppressed(record) {
  if (record.includeOnRadarMap === false) return true;
  const tier = norm(record.relevanceTier || record["Relevance Tier"]);
  if (tier.includes("tier 4") || tier.includes("reference only")) return true;
  const vis = norm(record.defaultMapVisibility || record["Default Map Visibility"]);
  if (vis.includes("hide")) return true;
  return false;
}

function scoreRecord(record) {
  let score = 0;
  if (String(record.sourceReference || "").trim()) score += 2;
  const conf = norm(record.dataConfidence);
  if (conf === "high") score += 3;
  else if (conf === "medium") score += 1;
  const sm = String(record.submarket || "").trim();
  if (sm) score += 2;
  if (sm && norm(sm) !== "other") score += 1;
  const notes = String(record.notes || "");
  if (notes.length > 80) score += 1;
  if (notes.length > 200) score += 1;
  if (String(record.hotelDemandRationale || "").trim()) score += 1;
  if (record.includeOnRadarMap !== false) score += 1;
  const tier = norm(record.relevanceTier || record["Relevance Tier"]);
  if (tier.includes("tier 1")) score += 2;
  else if (tier.includes("tier 2")) score += 1;
  if (String(record.lastVerified || "").trim()) score += 1;
  return score;
}

function classifyPairReason(a, b) {
  const aName = normalizeAnchorName(a.name);
  const bName = normalizeAnchorName(b.name);
  if (
    aName &&
    bName &&
    aName === bName &&
    norm(a.city) === norm(b.city) &&
    norm(a.country) === norm(b.country)
  ) {
    return "same_normalized_name_city_country";
  }
  if (
    coordsWithinTolerance(a.latitude, a.longitude, b.latitude, b.longitude) &&
    a.latitude != null &&
    b.latitude != null
  ) {
    return "same_coordinates";
  }
  return null;
}

function isSafeDefinitePair(reason, a, b) {
  if (!SAFE_DEFINITE_REASONS.has(reason)) return false;
  if (reason === "same_coordinates") {
    if (a.pointType && b.pointType && a.pointType === b.pointType) return true;
    return nameSimilarity(a.name, b.name) >= 0.85;
  }
  return true;
}

function pickStronger(a, b) {
  const sa = scoreRecord(a);
  const sb = scoreRecord(b);
  if (sa !== sb) return sa > sb ? a : b;
  return String(a.id) < String(b.id) ? a : b;
}

class UnionFind {
  constructor(ids) {
    this.parent = new Map(ids.map((id) => [id, id]));
  }
  find(id) {
    let p = this.parent.get(id);
    while (p !== this.parent.get(p)) {
      this.parent.set(id, this.parent.get(p));
      p = this.parent.get(p);
    }
    return p;
  }
  union(a, b) {
    const ra = this.find(a);
    const rb = this.find(b);
    if (ra !== rb) this.parent.set(rb, ra);
  }
}

/**
 * Resolve governance field names present in schema.
 * @param {Set<string>} schema
 */
export function resolveGovernanceFieldMap(schema) {
  const out = {};
  if (!schema) return out;
  for (const [key, candidates] of Object.entries(GOVERNANCE_FIELD_CANDIDATES)) {
    const hit = candidates.find((name) => schema.has(name));
    if (hit) out[key] = hit;
  }
  return out;
}

/**
 * @param {object} record
 * @param {object} governanceMap
 * @param {Set<string>} schema
 */
export function buildSuppressionPatch(record, governanceMap, schema) {
  const notes = String(record.notes || "").trim();
  const nextNotes = notes.includes(SUPPRESS_NOTE) ? notes : `${notes} ${SUPPRESS_NOTE}`.trim();

  const patch = {
    [F.includeOnRadarMap]: false,
    [F.notes]: nextNotes,
  };

  if (governanceMap.scopeLevel) patch[governanceMap.scopeLevel] = GOVERNANCE_SUPPRESS_VALUES.scopeLevel;
  if (governanceMap.relevanceTier) patch[governanceMap.relevanceTier] = GOVERNANCE_SUPPRESS_VALUES.relevanceTier;
  if (governanceMap.defaultMapVisibility) {
    patch[governanceMap.defaultMapVisibility] = GOVERNANCE_SUPPRESS_VALUES.defaultMapVisibility;
  }
  if (governanceMap.externalVisibilityLevel) {
    patch[governanceMap.externalVisibilityLevel] = GOVERNANCE_SUPPRESS_VALUES.externalVisibilityLevel;
  }

  if (!schema) return patch;
  const filtered = {};
  for (const [k, v] of Object.entries(patch)) {
    if (schema.has(k)) filtered[k] = v;
  }
  return filtered;
}

/**
 * @param {object[]} records — normalized demand anchor points with id
 * @param {object} [options]
 */
export function planMexicoCancunDuplicateCleanup(records, options = {}) {
  const country = options.country || "Mexico";
  const scoped = (records || []).filter((r) => norm(r.country) === norm(country));

  const safePairs = [];
  const manualReviewPairs = [];

  for (let i = 0; i < scoped.length; i += 1) {
    for (let j = i + 1; j < scoped.length; j += 1) {
      const a = scoped[i];
      const b = scoped[j];
      const reason = classifyPairReason(a, b);
      if (!reason) continue;
      const pair = { recordA: a, recordB: b, reason };
      if (isSafeDefinitePair(reason, a, b)) safePairs.push(pair);
      else manualReviewPairs.push({ ...pair, level: "possible" });
    }
  }

  const uf = new UnionFind(scoped.map((r) => r.id));
  for (const pair of safePairs) {
    uf.union(pair.recordA.id, pair.recordB.id);
  }

  const clusters = new Map();
  for (const r of scoped) {
    const root = uf.find(r.id);
    if (!clusters.has(root)) clusters.set(root, []);
    clusters.get(root).push(r);
  }

  const toSuppress = [];
  const toKeep = [];
  const clusterPlans = [];

  for (const members of clusters.values()) {
    if (members.length < 2) {
      if (!isAlreadySuppressed(members[0])) toKeep.push(members[0]);
      continue;
    }
    const active = members.filter((m) => !isAlreadySuppressed(m));
    const alreadySuppressed = members.filter((m) => isAlreadySuppressed(m));
    if (!active.length) {
      toKeep.push(members[0]);
      continue;
    }
    const keeper = active.reduce((best, cur) => pickStronger(best, cur));
    toKeep.push(keeper);
    for (const m of active) {
      if (m.id === keeper.id) continue;
      toSuppress.push({
        record: m,
        keepId: keeper.id,
        keepName: keeper.name,
        reason: safePairs.find(
          (p) =>
            (p.recordA.id === m.id || p.recordB.id === m.id) &&
            (p.recordA.id === keeper.id || p.recordB.id === keeper.id)
        )?.reason || "cluster_duplicate",
        scoreWeak: scoreRecord(m),
        scoreKeep: scoreRecord(keeper),
      });
    }
    clusterPlans.push({
      keeper: { id: keeper.id, name: keeper.name, score: scoreRecord(keeper) },
      suppressed: active.filter((m) => m.id !== keeper.id).map((m) => ({ id: m.id, name: m.name })),
      alreadySuppressed: alreadySuppressed.map((m) => ({ id: m.id, name: m.name })),
    });
  }

  const keepIds = new Set(toKeep.map((r) => r.id));
  for (const r of scoped) {
    if (!keepIds.has(r.id) && !toSuppress.some((s) => s.record.id === r.id) && !isAlreadySuppressed(r)) {
      toKeep.push(r);
    }
  }

  return {
    scanned: scoped.length,
    safePairs: safePairs.length,
    manualReviewPairs,
    clusters: clusterPlans.length,
    proposedSuppressions: toSuppress,
    proposedKeep: [...new Map(toKeep.map((r) => [r.id, r])).values()],
    samples: toSuppress.slice(0, 8).map((s) => ({
      suppressId: s.record.id,
      suppressName: s.record.name,
      keepId: s.keepId,
      keepName: s.keepName,
      reason: s.reason,
      scoreWeak: s.scoreWeak,
      scoreKeep: s.scoreKeep,
    })),
  };
}

export { SUPPRESS_NOTE, SAFE_DEFINITE_REASONS };
