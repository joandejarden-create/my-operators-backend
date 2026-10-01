/**
 * Packet 2.5 — Evaluation metrics for claim extraction / entity resolution / relationships.
 * Gold fixtures must be loaded only by the evaluation harness — never by the pipeline.
 */

import { CRITICAL_CLAIM_TYPES } from "./claim-types.js";

export const CLAIM_METRICS_VERSION = "claim-metrics-v1";

/**
 * Soft match key for a claim (type + normalized object + temporal).
 * @param {object} claim
 */
export function claimMatchKey(claim) {
  return [
    claim.claim_type,
    normalize(claim.object_raw || claim.object_canonical_id || claim.canonical_object_id),
    normalize(claim.subject_raw || claim.canonical_subject_id),
    claim.temporal_status || "",
    claim.ownership_percentage != null ? String(claim.ownership_percentage) : "",
  ].join("|");
}

/**
 * @param {object[]} predicted
 * @param {object[]} gold
 * @param {{ criticalOnly?: boolean }} [opts]
 */
export function scoreClaimExtraction(predicted, gold, opts = {}) {
  const goldList = opts.criticalOnly
    ? gold.filter((g) => g.importance === "CRITICAL" || CRITICAL_CLAIM_TYPES.includes(g.claim_type))
    : gold;
  const predList = opts.criticalOnly
    ? predicted.filter(
        (p) =>
          p.importance === "CRITICAL" || CRITICAL_CLAIM_TYPES.includes(p.claim_type)
      )
    : predicted;

  const goldKeys = goldList.map((g) => ({ g, key: goldKey(g) }));
  const predKeys = predList.map((p) => ({ p, key: predKey(p) }));

  let tp = 0;
  const matchedGold = new Set();
  const matchedPred = new Set();
  const errors = [];

  for (let i = 0; i < predKeys.length; i += 1) {
    const { p, key } = predKeys[i];
    const hit = goldKeys.find((gk, j) => !matchedGold.has(j) && keysMatch(key, gk.key));
    if (hit) {
      tp += 1;
      matchedGold.add(goldKeys.indexOf(hit));
      matchedPred.add(i);
      const err = classifyClaimError(p, hit.g);
      if (err) errors.push(err);
    } else {
      errors.push({
        code: isFalseConfident(p) ? "OVERINFERENCE" : "WRONG_RELATION",
        predicted: summarize(p),
        gold: null,
      });
    }
  }

  for (let j = 0; j < goldKeys.length; j += 1) {
    if (matchedGold.has(j)) continue;
    errors.push({
      code: "MISSED_RELATION",
      predicted: null,
      gold: summarize(goldKeys[j].g),
    });
  }

  const precision = predList.length ? tp / predList.length : 1;
  const recall = goldList.length ? tp / goldList.length : 1;

  let falseConfidentCount = 0;
  for (let i = 0; i < predKeys.length; i += 1) {
    if (matchedPred.has(i)) continue;
    if (isFalseConfident(predKeys[i].p)) falseConfidentCount += 1;
  }

  return {
    predicted: predList.length,
    gold: goldList.length,
    true_positives: tp,
    precision,
    recall,
    false_confident_claims: falseConfidentCount,
    errors,
  };
}

/**
 * @param {object[]} predictedCandidates
 * @param {object[]} goldRelationships
 */
export function scoreRelationships(predictedCandidates, goldRelationships) {
  let tp = 0;
  const errors = [];
  const matched = new Set();

  for (const p of predictedCandidates || []) {
    const hitIdx = (goldRelationships || []).findIndex((g, i) => {
      if (matched.has(i)) return false;
      return (
        p.relationship_type === g.relationship_type &&
        temporalCompat(p.temporal_status, g.temporal_status) &&
        entityCompat(p.object_canonical_id, g.object_id || g.object_canonical_id) &&
        percentCompat(p.ownership_percentage, g.ownership_percentage)
      );
    });
    if (hitIdx >= 0) {
      tp += 1;
      matched.add(hitIdx);
      const g = goldRelationships[hitIdx];
      if (p.temporal_status !== g.temporal_status) {
        errors.push({ code: "WRONG_TIME_PERIOD", predicted: p, gold: g });
      }
    } else if (
      p.relationship_type === "OWNED_BY" &&
      (p.temporal_status === "CURRENT" || p.promotion_status === "promoted")
    ) {
      errors.push({ code: "WRONG_EDGE_TYPE", predicted: summarizeCand(p), gold: null });
    }
  }

  for (let i = 0; i < (goldRelationships || []).length; i += 1) {
    if (matched.has(i)) continue;
    errors.push({
      code: "MISSED_RELATION",
      predicted: null,
      gold: goldRelationships[i],
    });
  }

  const nPred = (predictedCandidates || []).length;
  const nGold = (goldRelationships || []).length;
  return {
    precision: nPred ? tp / nPred : 1,
    recall: nGold ? tp / nGold : 1,
    true_positives: tp,
    errors,
  };
}

/**
 * Entity-link accuracy for resolved claims vs gold entity ids.
 */
export function scoreEntityResolution(predictedClaims, goldClaims) {
  let hotelOk = 0;
  let hotelN = 0;
  let orgOk = 0;
  let orgN = 0;
  let falseMerges = 0;
  const errors = [];

  for (const g of goldClaims || []) {
    const p = (predictedClaims || []).find((x) => softClaimMatch(x, g));
    if (g.hotel_context_id) {
      hotelN += 1;
      if (p && p.hotel_context_id === g.hotel_context_id) hotelOk += 1;
      else if (p && p.hotel_context_id && p.hotel_context_id !== g.hotel_context_id) {
        errors.push({ code: "WRONG_HOTEL", predicted: p.hotel_context_id, gold: g.hotel_context_id });
      }
    }
    if (g.canonical_object_id) {
      orgN += 1;
      if (p && p.canonical_object_id === g.canonical_object_id) orgOk += 1;
      else if (p?.canonical_object_id && p.canonical_object_id !== g.canonical_object_id) {
        errors.push({
          code: "WRONG_ENTITY",
          predicted: p.canonical_object_id,
          gold: g.canonical_object_id,
        });
      }
    }
  }

  // Detect critical false merges in predictions
  for (const p of predictedClaims || []) {
    const sid = p.canonical_subject_id;
    const oid = p.canonical_object_id;
    if (!sid || !oid) continue;
    // Same id on both ends of a relationship is not automatically a merge,
    // but Grand↔Resort or IHVSF↔GSF identity collapse is.
    if (
      (sid === "dhl_kgpv" && oid === "dhl_krystal_resort_pv") ||
      (oid === "dhl_kgpv" && sid === "dhl_krystal_resort_pv")
    ) {
      if (p.claim_type === "ENTITY_IS_OWNER_OF_PROPERTY") {
        falseMerges += 1;
        errors.push({ code: "FALSE_MERGE", predicted: summarize(p), detail: "grand_resort" });
      }
    }
    if (
      (sid === "dle_ihvsf" && oid === "dle_gsf") ||
      (oid === "dle_ihvsf" && sid === "dle_gsf")
    ) {
      if (p.claim_type !== "ENTITY_IS_PARENT_OF_ENTITY" && p.claim_type !== "ENTITY_IS_SUBSIDIARY_OF_ENTITY" && p.claim_type !== "ENTITY_CONTROLS_ENTITY") {
        // Control/parent edges are allowed; identity merge is not — only flag if ids equal
      }
    }
    if (sid === oid && (sid === "dle_ihvsf" || sid === "dle_gsf")) {
      falseMerges += 1;
      errors.push({ code: "FALSE_MERGE", predicted: summarize(p), detail: "ihvsf_gsf_collapse" });
    }
  }

  return {
    hotel_entity_link_accuracy: hotelN ? hotelOk / hotelN : 1,
    organization_entity_link_accuracy: orgN ? orgOk / orgN : 1,
    false_merges: falseMerges,
    errors,
  };
}

function goldKey(g) {
  return {
    type: g.claim_type,
    object: normalize(g.object_match || g.object_raw || g.canonical_object_id),
    subject: normalize(g.subject_match || g.subject_raw || g.canonical_subject_id),
    temporal: g.temporal_status || "",
    pct: g.ownership_percentage != null ? String(g.ownership_percentage) : "",
  };
}

function predKey(p) {
  return {
    type: p.claim_type,
    object: normalize(p.object_raw || p.canonical_object_id),
    subject: normalize(p.subject_raw || p.canonical_subject_id),
    temporal: p.temporal_status || "",
    pct: p.ownership_percentage != null ? String(p.ownership_percentage) : "",
  };
}

function keysMatch(a, b) {
  if (a.type !== b.type) {
    // Allow close variants
    if (
      !(
        (a.type === "ENTITY_IS_OWNER_OF_PROPERTY" &&
          b.type === "ENTITY_HOLDS_TITLE") ||
        (b.type === "ENTITY_IS_OWNER_OF_PROPERTY" &&
          a.type === "ENTITY_HOLDS_TITLE") ||
        (a.type === "ENTITY_OPERATES_HOTEL" && b.type === "ENTITY_MANAGES_HOTEL") ||
        (b.type === "ENTITY_OPERATES_HOTEL" && a.type === "ENTITY_MANAGES_HOTEL")
      )
    ) {
      return false;
    }
  }
  if (a.pct && b.pct && a.pct !== b.pct) return false;
  if (a.temporal && b.temporal && a.temporal !== b.temporal) {
    if (
      !(
        (a.temporal === "CURRENT" && b.temporal === "CURRENT_UNVERIFIED") ||
        (b.temporal === "CURRENT" && a.temporal === "CURRENT_UNVERIFIED")
      )
    ) {
      return false;
    }
  }
  return (
    includesNorm(a.object, b.object) &&
    (includesNorm(a.subject, b.subject) || !a.subject || !b.subject)
  );
}

function softClaimMatch(p, g) {
  return keysMatch(predKey(p), goldKey(g));
}

function includesNorm(a, b) {
  if (!a || !b) return !a && !b ? true : Boolean(a || b);
  return a.includes(b) || b.includes(a);
}

function normalize(s) {
  return String(s || "")
    .toLowerCase()
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function isFalseConfident(p) {
  return (
    (p.overall_claim_confidence || 0) >= 0.85 &&
    ["ENTITY_IS_OWNER_OF_PROPERTY", "ENTITY_HOLDS_TITLE"].includes(p.claim_type) &&
    (p.temporal_status === "CURRENT" || p.temporal_status === "CURRENT_UNVERIFIED")
  );
}

function classifyClaimError(p, g) {
  if (p.ownership_percentage != null && g.ownership_percentage != null) {
    if (Number(p.ownership_percentage) !== Number(g.ownership_percentage)) {
      return { code: "WRONG_PERCENTAGE", predicted: summarize(p), gold: summarize(g) };
    }
  }
  if (p.temporal_status && g.temporal_status && p.temporal_status !== g.temporal_status) {
    return { code: "WRONG_TEMPORAL_STATUS", predicted: summarize(p), gold: summarize(g) };
  }
  return null;
}

function temporalCompat(a, b) {
  if (!a || !b) return true;
  if (a === b) return true;
  return (
    (a === "CURRENT" && b === "CURRENT_UNVERIFIED") ||
    (b === "CURRENT" && a === "CURRENT_UNVERIFIED")
  );
}

function entityCompat(a, b) {
  if (!a || !b) return true;
  return normalize(a) === normalize(b) || normalize(a).includes(normalize(b)) || normalize(b).includes(normalize(a));
}

function percentCompat(a, b) {
  if (a == null || b == null) return true;
  return Number(a) === Number(b);
}

function summarize(c) {
  if (!c) return null;
  return {
    claim_type: c.claim_type,
    object_raw: c.object_raw,
    temporal_status: c.temporal_status,
    ownership_percentage: c.ownership_percentage,
  };
}

function summarizeCand(c) {
  return {
    relationship_type: c.relationship_type,
    object_canonical_id: c.object_canonical_id,
    temporal_status: c.temporal_status,
  };
}
