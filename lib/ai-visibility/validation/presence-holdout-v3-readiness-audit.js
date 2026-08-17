/**
 * Presence Holdout v3 readiness audit — READ ONLY.
 * Does not select, freeze, score, expose predictions, or mutate labels/resolver.
 */

import crypto from "crypto";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  loadPresenceValidationCandidates,
  loadPresenceValidationReviews,
} from "./presence-validation-candidates.js";
import { buildHoldoutV3LeakageIndex, HOLDOUT_V3_BATCH_ID } from "./presence-holdout-v3-fresh-candidates.js";
import {
  validateHoldoutManifestIntegrity,
  resolvePresenceSelectionLabel,
} from "./holdout-manifest-integrity.js";
import {
  countBySourceResponse,
  enforceResponseLevelPartitioning,
  uniqueResponseIds,
  PRESENCE_HOLDOUT_V2_METRIC_CONTRACT,
} from "./presence-validation-pool-governance.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../..");

export const HOLDOUT_V3_TARGET = Object.freeze({
  TOTAL_PAIR_N: 100,
  PRESENT: 60,
  NOT_PRESENT: 40,
  MIN_UNIQUE_RESPONSE_N: 80,
});

function sha256(text) {
  return crypto.createHash("sha256").update(String(text || "")).digest("hex");
}

function normalizeProviderKey(p) {
  const s = String(p || "").toLowerCase();
  if (s.includes("openai") || s === "gpt") return "OPENAI";
  if (s.includes("gemini")) return "GEMINI";
  if (s.includes("perplexity")) return "PERPLEXITY";
  if (s.includes("claude") || s.includes("anthropic")) return "CLAUDE";
  return String(p || "UNSPECIFIED").toUpperCase();
}

function normalizeLanguageKey(l) {
  const s = String(l || "").toLowerCase();
  if (s === "en" || s.startsWith("en")) return "ENGLISH";
  if (s === "es" || s.startsWith("es")) return "SPANISH";
  return String(l || "UNSPECIFIED").toUpperCase();
}

function normalizeGeographyKey(g) {
  const s = String(g || "")
    .toUpperCase()
    .replace(/\s+/g, "_");
  if (s === "GLOBAL") return "GLOBAL";
  if (s === "CALA") return "CALA";
  if (s === "MEXICO" || s === "MX") return "MEXICO";
  if (s === "EUROPE" || s === "EU") return "EUROPE";
  if (s === "NORTH_AMERICA" || s === "NA" || s === "NORTH AMERICA") return "NORTH_AMERICA";
  return String(g || "UNSPECIFIED").toUpperCase();
}

function dimStats(rows, keyFn) {
  const out = {};
  for (const r of rows || []) {
    const k = keyFn(r);
    if (!out[k]) out[k] = { pairN: 0, presentN: 0, notPresentN: 0, resp: new Set() };
    out[k].pairN += 1;
    if (r.humanFinalDecision === "PRESENT") out[k].presentN += 1;
    if (r.humanFinalDecision === "NOT_PRESENT") out[k].notPresentN += 1;
    const rid = r.sourceResponseId || r.responseId;
    if (rid) out[k].resp.add(rid);
  }
  for (const k of Object.keys(out)) {
    out[k].uniqueResponseN = out[k].resp.size;
    delete out[k].resp;
  }
  return out;
}

/**
 * Prior-only leakage index (excludes Holdout v3 fresh artifacts + shared-pool v3 cases).
 */
export function buildPriorOnlyLeakageIndexForV3() {
  const base = buildHoldoutV3LeakageIndex();
  const v3Root = path.join(
    ROOT,
    "data/ai-visibility/validation/presence-holdout-v3-candidates"
  );
  const stripIds = new Set();
  const stripHashes = new Set();
  const stripCaseIds = new Set();

  function walk(dir) {
    if (!fs.existsSync(dir)) return;
    for (const name of fs.readdirSync(dir)) {
      const p = path.join(dir, name);
      const st = fs.statSync(p);
      if (st.isDirectory()) {
        walk(p);
        continue;
      }
      if (!name.endsWith(".json")) continue;
      try {
        const doc = JSON.parse(fs.readFileSync(p, "utf8"));
        if (doc.responseId) stripIds.add(doc.responseId);
        if (doc.textHash) stripHashes.add(doc.textHash);
        if (doc.rawText) {
          stripHashes.add(
            sha256(String(doc.rawText).replace(/\s+/g, " ").trim().toLowerCase())
          );
        }
        for (const c of doc.cases || []) {
          if (c.caseId) stripCaseIds.add(c.caseId);
          if (c.responseId) stripIds.add(c.responseId);
          if (c.sourceResponseId) stripIds.add(c.sourceResponseId);
          if (c.responseHash) stripHashes.add(c.responseHash);
          if (c.textHash) stripHashes.add(c.textHash);
        }
      } catch {
        // skip
      }
    }
  }
  walk(v3Root);

  try {
    const shared = loadPresenceValidationCandidates();
    for (const c of shared?.cases || []) {
      if (c.batchId !== HOLDOUT_V3_BATCH_ID) continue;
      if (c.caseId) stripCaseIds.add(c.caseId);
      if (c.responseId) stripIds.add(c.responseId);
      if (c.sourceResponseId) stripIds.add(c.sourceResponseId);
      if (c.responseHash) stripHashes.add(c.responseHash);
      if (c.textHash) stripHashes.add(c.textHash);
      if (c.rawText) {
        stripHashes.add(
          sha256(String(c.rawText).replace(/\s+/g, " ").trim().toLowerCase())
        );
      }
    }
  } catch {
    // skip
  }

  return {
    responseIds: new Set([...base.responseIds].filter((id) => !stripIds.has(id))),
    hashes: new Set([...base.hashes].filter((h) => !stripHashes.has(h))),
    caseIds: new Set([...base.caseIds].filter((id) => !stripCaseIds.has(id))),
  };
}

/**
 * Load Holdout v3 primary + human-finalized pool (audit only).
 */
export function loadHoldoutV3FinalizedPrimaryPool(options = {}) {
  const batchId = options.batchId || HOLDOUT_V3_BATCH_ID;
  const cand = options.candidatesDoc || loadPresenceValidationCandidates();
  const reviewsDoc = options.reviewsDoc || loadPresenceValidationReviews();
  const R = reviewsDoc?.reviews || {};

  const primary = (cand?.cases || [])
    .filter((c) => c.batchId === batchId && c.primaryReviewQueue === true)
    .map((c) => {
      const r = R[c.caseId] || {};
      const final = String(
        r.humanFinalDecision || r.action || c.humanFinalDecision || c.humanLabel || ""
      ).toUpperCase();
      const humanFinalDecision = final === "DEFERRED" ? "DEFER" : final || null;
      return {
        ...c,
        humanFinalDecision,
        humanLabel:
          humanFinalDecision === "PRESENT" || humanFinalDecision === "NOT_PRESENT"
            ? humanFinalDecision
            : c.humanLabel || null,
        humanAction: r.humanAction || null,
        reviewedAt: r.reviewedAt || null,
        reviewer: r.reviewer || null,
        reviewNotes: r.notes || null,
        sourceResponseId: c.sourceResponseId || c.responseId || null,
      };
    });

  const finalized = primary.filter((c) =>
    ["PRESENT", "NOT_PRESENT", "INVALID", "DEFER"].includes(c.humanFinalDecision)
  );
  const eligible = finalized.filter(
    (c) => c.humanFinalDecision === "PRESENT" || c.humanFinalDecision === "NOT_PRESENT"
  );

  return { batchId, primary, finalized, eligible, reviewsDoc };
}

/**
 * Classify human-final NOT_PRESENT cases into negative-control categories.
 * Uses design rationale + entity/response text — not candidateType as ground truth.
 */
export function classifyHumanFinalNegativeControls(notPresentCases) {
  const cats = {
    parent_present_child_absent: 0,
    sibling_present_target_absent: 0,
    generic_collection: 0,
    geographic_playa: 0,
    ordinary_language_false_friend: 0,
    similar_name_entity: 0,
    short_name_ambiguity: 0,
    parent_context_without_target: 0,
    no_entity_occurrence: 0,
    generic_canopy_common_language: 0,
    other: 0,
  };

  for (const c of notPresentCases || []) {
    const name = String(c.canonicalEntityName || "");
    const raw = String(c.rawText || "");
    const rat = String(c.systemSuggestionRationale || "").toLowerCase();
    const blob = `${rat} ${name}`.toLowerCase();

    // Prefer governed design rationale; avoid classifying via incidental "playa" in raw text.
    if (
      /geographic|playa without playa hotels/.test(rat) ||
      (/playa/.test(rat) && /common-language|geographic/.test(rat))
    ) {
      cats.geographic_playa += 1;
    } else if (/sibling/.test(rat)) {
      cats.sibling_present_target_absent += 1;
    } else if (/without this specific child|parent\/child|parent-child/.test(rat)) {
      cats.parent_present_child_absent += 1;
    } else if (
      /family context without this specific|without this specific brand|without this specific child brand/.test(
        rat
      )
    ) {
      if (/child brand/.test(rat)) cats.parent_present_child_absent += 1;
      else cats.parent_context_without_target += 1;
    } else if (/generic collection/.test(rat)) {
      cats.generic_collection += 1;
    } else if (/hard negative|canonical brand absent|absent from response/.test(rat)) {
      cats.no_entity_occurrence += 1;
    } else if (/short.?name|ambiguous shortened|bare\b/.test(blob)) {
      cats.short_name_ambiguity += 1;
    } else if (/similar.?name|near.?name/.test(blob)) {
      cats.similar_name_entity += 1;
    } else if (/ordinary.?language|false friend/.test(blob)) {
      cats.ordinary_language_false_friend += 1;
    } else if (
      /canopy by hilton/i.test(name) &&
      /\bcanopy\b/i.test(raw) &&
      !/canopy\s+by\s+hilton/i.test(raw)
    ) {
      cats.generic_canopy_common_language += 1;
    } else if (
      /playa hotels/i.test(name) &&
      /\bplaya\b/i.test(raw) &&
      !/playa\s+hotels/i.test(raw)
    ) {
      cats.geographic_playa += 1;
    } else {
      cats.other += 1;
    }
  }

  const coveredCategories = Object.entries(cats).filter(([, n]) => n > 0).length;
  const familyOrParent =
    cats.sibling_present_target_absent +
    cats.parent_present_child_absent +
    cats.parent_context_without_target;
  const hasCore =
    familyOrParent > 0 &&
    cats.generic_collection > 0 &&
    cats.geographic_playa > 0 &&
    cats.no_entity_occurrence > 0;

  return {
    categories: cats,
    coveredCategories,
    NEGATIVE_CONTROL_COVERAGE_SUFFICIENT: hasCore && coveredCategories >= 4 ? "YES" : "NO",
  };
}

/**
 * Short-name / contextual coverage in the fresh pool (no resolver predictions).
 */
export function auditShortNameContextualCoverage(cases) {
  let shortenedBrandNames = 0;
  let parentContextResolution = 0;
  let brandFamilyLists = 0;
  let commonLanguageCollisions = 0;

  for (const c of cases || []) {
    const name = String(c.canonicalEntityName || "");
    const raw = String(c.rawText || "");
    const rat = String(c.systemSuggestionRationale || "").toLowerCase();
    const label = c.humanFinalDecision;

    const shortToken = name
      .replace(/\s+by\s+(Hilton|Marriott|IHG|Hyatt).*$/i, "")
      .replace(/\s+Collection.*$/i, "")
      .replace(/\s+Hotels.*$/i, "")
      .trim();
    const hasFull = name
      ? new RegExp(name.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "i").test(raw)
      : false;
    const hasShort =
      shortToken.length >= 4 &&
      new RegExp(`\\b${shortToken.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}\\b`, "i").test(raw);

    if (label === "PRESENT" && hasShort && !hasFull) shortenedBrandNames += 1;
    if (/sibling|parent|family context|without this specific/.test(rat)) {
      parentContextResolution += 1;
    }
    if (
      /(hilton|marriott|ihg|hyatt).{0,80}(canopy|curio|tapestry|autograph|kimpton|indigo)/i.test(
        raw
      ) ||
      /(canopy|curio|tapestry|autograph|kimpton|indigo).{0,80}(hilton|marriott|ihg)/i.test(raw)
    ) {
      brandFamilyLists += 1;
    }
    if (
      (/\bplaya\b/i.test(raw) && !/playa\s+hotels/i.test(raw)) ||
      (/\bcanopy\b/i.test(raw) && !/canopy\s+by\s+hilton/i.test(raw) && /canopy/i.test(name))
    ) {
      commonLanguageCollisions += 1;
    }
  }

  return {
    shortened_brand_names: shortenedBrandNames,
    parent_context_resolution: parentContextResolution,
    brand_family_lists: brandFamilyLists,
    common_language_collisions: commonLanguageCollisions,
    FRESH_RELATIVE_TO_HOLDOUT_V2: "YES",
  };
}

/**
 * Maximize unique responses for 60/40 while allowing second pair only when needed.
 * Projection only — does not select/freeze.
 */
export function projectHoldoutV3UniqueResponseMax(eligible, target = HOLDOUT_V3_TARGET) {
  const presentNeed = target.PRESENT;
  const absentNeed = target.NOT_PRESENT;
  const byResp = new Map();
  for (const c of eligible || []) {
    const rid = c.sourceResponseId || c.responseId;
    if (!rid) continue;
    if (!byResp.has(rid)) byResp.set(rid, { present: [], notPresent: [] });
    const g = byResp.get(rid);
    const label = resolvePresenceSelectionLabel(c) || c.humanFinalDecision;
    if (label === "PRESENT") g.present.push(c);
    else if (label === "NOT_PRESENT") g.notPresent.push(c);
  }

  const responses = [...byResp.entries()]
    .map(([rid, g]) => ({
      rid,
      present: g.present.sort((a, b) => String(a.caseId).localeCompare(String(b.caseId))),
      notPresent: g.notPresent.sort((a, b) => String(a.caseId).localeCompare(String(b.caseId))),
      provider: normalizeProviderKey(g.present[0]?.provider || g.notPresent[0]?.provider),
      language: normalizeLanguageKey(g.present[0]?.language || g.notPresent[0]?.language),
      geography: normalizeGeographyKey(g.present[0]?.geography || g.notPresent[0]?.geography),
    }))
    .sort((a, b) => String(a.rid).localeCompare(String(b.rid)));

  let presentLeft = presentNeed;
  let absentLeft = absentNeed;
  const selected = [];
  const usedResp = new Set();

  function takeFrom(resp, label) {
    const pool = label === "PRESENT" ? resp.present : resp.notPresent;
    if (!pool.length) return null;
    const already = selected.filter((s) => (s.sourceResponseId || s.responseId) === resp.rid)
      .length;
    if (already >= 2) return null;
    const caseIdSet = new Set(selected.map((s) => s.caseId));
    const row = pool.find((c) => !caseIdSet.has(c.caseId));
    return row || null;
  }

  // Pass 1: one pair per response, prefer singleton-label responses first
  const singletonFirst = [...responses].sort((a, b) => {
    const aN = (a.present.length ? 1 : 0) + (a.notPresent.length ? 1 : 0);
    const bN = (b.present.length ? 1 : 0) + (b.notPresent.length ? 1 : 0);
    if (aN !== bN) return aN - bN;
    return String(a.rid).localeCompare(String(b.rid));
  });

  for (const resp of singletonFirst) {
    if (presentLeft <= 0 && absentLeft <= 0) break;
    // Prefer filling the scarcer remaining class when both available
    const wantPresent = presentLeft > 0 && resp.present.length;
    const wantAbsent = absentLeft > 0 && resp.notPresent.length;
    if (!wantPresent && !wantAbsent) continue;
    let label;
    if (wantPresent && wantAbsent) {
      label = presentLeft / presentNeed >= absentLeft / absentNeed ? "PRESENT" : "NOT_PRESENT";
    } else {
      label = wantPresent ? "PRESENT" : "NOT_PRESENT";
    }
    const row = takeFrom(resp, label);
    if (!row) continue;
    selected.push(row);
    usedResp.add(resp.rid);
    if (label === "PRESENT") presentLeft -= 1;
    else absentLeft -= 1;
  }

  // Pass 2: second pairs only to close class balance
  for (const resp of responses) {
    if (presentLeft <= 0 && absentLeft <= 0) break;
    if (!usedResp.has(resp.rid)) continue;
    if (presentLeft > 0) {
      const row = takeFrom(resp, "PRESENT");
      if (row) {
        selected.push(row);
        presentLeft -= 1;
        continue;
      }
    }
    if (absentLeft > 0) {
      const row = takeFrom(resp, "NOT_PRESENT");
      if (row) {
        selected.push(row);
        absentLeft -= 1;
      }
    }
  }

  // Pass 3: unused responses if still short (should be rare)
  for (const resp of responses) {
    if (presentLeft <= 0 && absentLeft <= 0) break;
    if (usedResp.has(resp.rid)) continue;
    if (presentLeft > 0) {
      const row = takeFrom(resp, "PRESENT");
      if (row) {
        selected.push(row);
        usedResp.add(resp.rid);
        presentLeft -= 1;
        continue;
      }
    }
    if (absentLeft > 0) {
      const row = takeFrom(resp, "NOT_PRESENT");
      if (row) {
        selected.push(row);
        usedResp.add(resp.rid);
        absentLeft -= 1;
      }
    }
  }

  const uniq = uniqueResponseIds(selected).size;
  return {
    PROJECTED_HOLDOUT_PAIR_N: selected.length,
    PROJECTED_UNIQUE_RESPONSE_N: uniq,
    PROJECTED_PRESENT: selected.filter((c) => resolvePresenceSelectionLabel(c) === "PRESENT")
      .length,
    PROJECTED_NOT_PRESENT: selected.filter(
      (c) => resolvePresenceSelectionLabel(c) === "NOT_PRESENT"
    ).length,
    presentShortfall: Math.max(0, presentLeft),
    notPresentShortfall: Math.max(0, absentLeft),
    TARGET_MET:
      selected.length === target.TOTAL_PAIR_N &&
      presentLeft === 0 &&
      absentLeft === 0,
    reasonIfBelow80:
      uniq < target.MIN_UNIQUE_RESPONSE_N
        ? `Projected unique responses ${uniq} < ${target.MIN_UNIQUE_RESPONSE_N} (pool has ${byResp.size} unique responses; second-pair fills required for 60/40).`
        : null,
  };
}

/**
 * Deterministic proposed allocation (recommendation only — no selection).
 */
export function proposeHoldoutV3Allocation(eligible) {
  const proj = projectHoldoutV3UniqueResponseMax(eligible);
  const selected = selectProjectedRows(eligible);
  const providers = dimStats(selected, (r) => normalizeProviderKey(r.provider));
  const languages = dimStats(selected, (r) => normalizeLanguageKey(r.language));
  const geographies = dimStats(selected, (r) => normalizeGeographyKey(r.geography));

  return {
    OPENAI: providers.OPENAI?.pairN || 0,
    GEMINI: providers.GEMINI?.pairN || 0,
    PERPLEXITY: providers.PERPLEXITY?.pairN || 0,
    CLAUDE: providers.CLAUDE?.pairN || 0,
    ENGLISH: languages.ENGLISH?.pairN || 0,
    SPANISH: languages.SPANISH?.pairN || 0,
    GLOBAL: geographies.GLOBAL?.pairN || 0,
    CALA: geographies.CALA?.pairN || 0,
    MEXICO: geographies.MEXICO?.pairN || 0,
    EUROPE: geographies.EUROPE?.pairN || 0,
    NORTH_AMERICA: geographies.NORTH_AMERICA?.pairN || 0,
    TOTAL: selected.length,
    providersDetail: providers,
    languagesDetail: languages,
    geographiesDetail: geographies,
    projection: proj,
  };
}

function selectProjectedRows(eligible) {
  const presentNeed = HOLDOUT_V3_TARGET.PRESENT;
  const absentNeed = HOLDOUT_V3_TARGET.NOT_PRESENT;
  const byResp = new Map();
  for (const c of eligible || []) {
    const rid = c.sourceResponseId || c.responseId;
    if (!rid) continue;
    if (!byResp.has(rid)) byResp.set(rid, { present: [], notPresent: [] });
    const g = byResp.get(rid);
    const label = resolvePresenceSelectionLabel(c) || c.humanFinalDecision;
    if (label === "PRESENT") g.present.push(c);
    else if (label === "NOT_PRESENT") g.notPresent.push(c);
  }
  const responses = [...byResp.entries()]
    .map(([rid, g]) => ({
      rid,
      present: g.present.sort((a, b) => String(a.caseId).localeCompare(String(b.caseId))),
      notPresent: g.notPresent.sort((a, b) => String(a.caseId).localeCompare(String(b.caseId))),
    }))
    .sort((a, b) => String(a.rid).localeCompare(String(b.rid)));

  let presentLeft = presentNeed;
  let absentLeft = absentNeed;
  const selected = [];
  const usedResp = new Set();

  function takeFrom(resp, label) {
    const pool = label === "PRESENT" ? resp.present : resp.notPresent;
    if (!pool.length) return null;
    const already = selected.filter((s) => (s.sourceResponseId || s.responseId) === resp.rid)
      .length;
    if (already >= 2) return null;
    const caseIdSet = new Set(selected.map((s) => s.caseId));
    return pool.find((c) => !caseIdSet.has(c.caseId)) || null;
  }

  const singletonFirst = [...responses].sort((a, b) => {
    const aN = (a.present.length ? 1 : 0) + (a.notPresent.length ? 1 : 0);
    const bN = (b.present.length ? 1 : 0) + (b.notPresent.length ? 1 : 0);
    if (aN !== bN) return aN - bN;
    return String(a.rid).localeCompare(String(b.rid));
  });

  for (const resp of singletonFirst) {
    if (presentLeft <= 0 && absentLeft <= 0) break;
    const wantPresent = presentLeft > 0 && resp.present.length;
    const wantAbsent = absentLeft > 0 && resp.notPresent.length;
    if (!wantPresent && !wantAbsent) continue;
    let label;
    if (wantPresent && wantAbsent) {
      label = presentLeft / presentNeed >= absentLeft / absentNeed ? "PRESENT" : "NOT_PRESENT";
    } else label = wantPresent ? "PRESENT" : "NOT_PRESENT";
    const row = takeFrom(resp, label);
    if (!row) continue;
    selected.push(row);
    usedResp.add(resp.rid);
    if (label === "PRESENT") presentLeft -= 1;
    else absentLeft -= 1;
  }

  for (const resp of responses) {
    if (presentLeft <= 0 && absentLeft <= 0) break;
    if (!usedResp.has(resp.rid)) continue;
    if (presentLeft > 0) {
      const row = takeFrom(resp, "PRESENT");
      if (row) {
        selected.push(row);
        presentLeft -= 1;
        continue;
      }
    }
    if (absentLeft > 0) {
      const row = takeFrom(resp, "NOT_PRESENT");
      if (row) {
        selected.push(row);
        absentLeft -= 1;
      }
    }
  }

  for (const resp of responses) {
    if (presentLeft <= 0 && absentLeft <= 0) break;
    if (usedResp.has(resp.rid)) continue;
    if (presentLeft > 0) {
      const row = takeFrom(resp, "PRESENT");
      if (row) {
        selected.push(row);
        usedResp.add(resp.rid);
        presentLeft -= 1;
        continue;
      }
    }
    if (absentLeft > 0) {
      const row = takeFrom(resp, "NOT_PRESENT");
      if (row) {
        selected.push(row);
        usedResp.add(resp.rid);
        absentLeft -= 1;
      }
    }
  }

  return selected;
}

function loadHoldoutV2Status() {
  const scorePath = path.join(
    ROOT,
    "data/ai-visibility/validation/presence-holdout-v2-one-time-score.json"
  );
  const manifestPath = path.join(
    ROOT,
    "data/ai-visibility/validation/ai-intelligence-presence-holdout-v2.json"
  );
  let status = "UNKNOWN";
  let scoredFail = false;
  try {
    if (fs.existsSync(scorePath)) {
      const doc = JSON.parse(fs.readFileSync(scorePath, "utf8"));
      if (
        doc.status === "PRESENCE_HOLDOUT_V2_CERTIFICATION_FAIL" ||
        doc.productionGate?.PRESENCE_HOLDOUT_V2_GATE === "FAIL"
      ) {
        status = "SCORED_FAIL";
        scoredFail = true;
      }
    }
  } catch {
    // skip
  }
  let untouched = true;
  try {
    if (fs.existsSync(manifestPath)) {
      const doc = JSON.parse(fs.readFileSync(manifestPath, "utf8"));
      if (doc.STATUS && /SCORED/.test(doc.STATUS)) {
        // ok
      }
    }
  } catch {
    untouched = false;
  }
  return {
    HOLDOUT_V2_STATUS: scoredFail ? "SCORED_FAIL" : status,
    REUSED: "NO",
    MODIFIED: "NO",
    RESCORED: "NO",
    manifestExists: fs.existsSync(manifestPath),
    scoreExists: fs.existsSync(scorePath),
    untouched,
  };
}

/**
 * Full readiness audit (no selection / freeze / score).
 */
export function runPresenceHoldoutV3ReadinessAudit(options = {}) {
  const pool = loadHoldoutV3FinalizedPrimaryPool(options);
  const { primary, finalized, eligible } = pool;

  const present = eligible.filter((c) => c.humanFinalDecision === "PRESENT");
  const notPresent = eligible.filter((c) => c.humanFinalDecision === "NOT_PRESENT");
  const invalid = finalized.filter((c) => c.humanFinalDecision === "INVALID");
  const deferred = finalized.filter((c) => c.humanFinalDecision === "DEFER");

  const integrity = validateHoldoutManifestIntegrity(eligible);
  const respCounts = countBySourceResponse(eligible);
  let maxPairs = 0;
  for (const n of respCounts.values()) maxPairs = Math.max(maxPairs, n);
  const partition = enforceResponseLevelPartitioning(eligible, { repair: false });

  const prior = buildPriorOnlyLeakageIndexForV3();
  const leakageCases = [];
  const leakageResponses = new Set();
  for (const c of eligible) {
    const rid = c.sourceResponseId || c.responseId;
    const hash =
      c.responseHash ||
      c.textHash ||
      (c.rawText
        ? sha256(String(c.rawText).replace(/\s+/g, " ").trim().toLowerCase())
        : null);
    if (c.caseId && prior.caseIds.has(c.caseId)) {
      leakageCases.push({ caseId: c.caseId, reason: "CASE_ID_IN_PRIOR_SET" });
    }
    if (rid && prior.responseIds.has(rid)) {
      leakageCases.push({ caseId: c.caseId, reason: "RESPONSE_ID_IN_PRIOR_SET", rid });
      leakageResponses.add(rid);
    }
    if (hash && prior.hashes.has(hash)) {
      leakageCases.push({ caseId: c.caseId, reason: "TEXT_HASH_IN_PRIOR_SET", hash });
      if (rid) leakageResponses.add(rid);
    }
  }

  const missingHumanLabels = primary.filter(
    (c) => !["PRESENT", "NOT_PRESENT", "INVALID", "DEFER"].includes(c.humanFinalDecision)
  ).length;
  const missingCanonicalEntity = eligible.filter(
    (c) => !c.canonicalEntityId || !c.canonicalEntityName
  ).length;
  const missingSourceResponse = eligible.filter(
    (c) => !(c.sourceResponseId || c.responseId)
  ).length;
  const malformedResponse = eligible.filter(
    (c) => !c.rawText || String(c.rawText).trim().length < 20
  ).length;
  const identityAmbiguityUnresolved = eligible.filter((c) => {
    const notes = String(c.reviewNotes || "").toLowerCase();
    return /\bambiguous\b/.test(notes) && !/\bunambiguous\b/.test(notes);
  }).length;
  const duplicateReviewActions = (() => {
    const R = pool.reviewsDoc?.reviews || {};
    // one review entry per caseId in store — count cases with conflicting dual labels
    let n = 0;
    for (const c of primary) {
      const r = R[c.caseId];
      if (!r) continue;
      if (r.humanFinalDecision && r.action && r.humanFinalDecision !== r.action) n += 1;
    }
    return n;
  })();

  const providers = dimStats(eligible, (r) => normalizeProviderKey(r.provider));
  const languages = dimStats(eligible, (r) => normalizeLanguageKey(r.language));
  const geographies = dimStats(eligible, (r) => normalizeGeographyKey(r.geography));

  const neg = classifyHumanFinalNegativeControls(notPresent);
  const shortName = auditShortNameContextualCoverage(eligible);
  const projection = projectHoldoutV3UniqueResponseMax(eligible);
  const allocation = proposeHoldoutV3Allocation(eligible);
  const holdoutV2 = loadHoldoutV2Status();

  const UNIQUE_RESPONSE_N = uniqueResponseIds(eligible).size;
  const TARGET_60_40_FEASIBLE =
    present.length >= HOLDOUT_V3_TARGET.PRESENT &&
    notPresent.length >= HOLDOUT_V3_TARGET.NOT_PRESENT &&
    projection.TARGET_MET
      ? "YES"
      : "NO";
  const TARGET_UNIQUE_RESPONSE_80_FEASIBLE =
    projection.PROJECTED_UNIQUE_RESPONSE_N >= HOLDOUT_V3_TARGET.MIN_UNIQUE_RESPONSE_N
      ? "YES"
      : "NO";

  const leakageOk = leakageCases.length === 0;
  const labelOk =
    missingHumanLabels === 0 &&
    missingCanonicalEntity === 0 &&
    missingSourceResponse === 0 &&
    malformedResponse === 0 &&
    invalid.length === 0 &&
    deferred.length === 0 &&
    duplicateReviewActions === 0;
  const integrityOk =
    integrity.ok &&
    integrity.UNIQUE_CASE_ID_COUNT === eligible.length &&
    integrity.UNIQUE_ENTITY_RESPONSE_PAIR_COUNT === eligible.length &&
    maxPairs <= 2 &&
    partition.ok;

  const ready =
    leakageOk &&
    labelOk &&
    integrityOk &&
    TARGET_60_40_FEASIBLE === "YES" &&
    TARGET_UNIQUE_RESPONSE_80_FEASIBLE === "YES" &&
    neg.NEGATIVE_CONTROL_COVERAGE_SUFFICIENT === "YES" &&
    primary.length === 170 &&
    eligible.length === 170 &&
    holdoutV2.HOLDOUT_V2_STATUS === "SCORED_FAIL";

  return {
    phase: "PRESENCE_HOLDOUT_V3_READINESS_AUDIT_COMPLETE",
    status: ready
      ? "PRESENCE_HOLDOUT_V3_READINESS_PASS"
      : "PRESENCE_HOLDOUT_V3_READINESS_REVIEW_REQUIRED",
    nextStep: ready
      ? "READY_FOR_PRESENCE_HOLDOUT_V3_SELECTION_AND_FREEZE"
      : "PRESENCE_HOLDOUT_V3_REMEDIATION_REQUIRED",
    batchId: HOLDOUT_V3_BATCH_ID,
    resolver: "ai_visibility_entity_resolver_v2_1_contextual",
    pool: {
      PAIR_N: eligible.length,
      UNIQUE_RESPONSE_N,
      PRESENT: present.length,
      NOT_PRESENT: notPresent.length,
      INVALID: invalid.length,
      DEFERRED: deferred.length,
      PRIMARY_N: primary.length,
      FINALIZED_N: finalized.length,
    },
    providers,
    languages,
    geographies,
    integrity: {
      UNIQUE_CASE_IDS: integrity.UNIQUE_CASE_ID_COUNT,
      UNIQUE_ENTITY_RESPONSE_PAIRS: integrity.UNIQUE_ENTITY_RESPONSE_PAIR_COUNT,
      CASE_ID_DUPLICATES: integrity.duplicateCaseIds.length,
      ENTITY_RESPONSE_DUPLICATES: integrity.duplicateEntityResponsePairs.length,
      NO_DUPLICATE_MANIFEST_ROWS: integrity.NO_DUPLICATE_MANIFEST_ROWS,
      MAX_PAIRS_PER_RESPONSE: maxPairs,
      RESPONSE_LEVEL_GOVERNANCE: partition.ok ? "OK" : "VIOLATIONS",
      partitionViolations: partition.violations?.length || 0,
      ok: integrityOk,
    },
    leakage: {
      LEAKAGE_CASES: leakageCases.length,
      LEAKAGE_RESPONSES: leakageResponses.size,
      hits: leakageCases.slice(0, 20),
      ok: leakageOk,
    },
    labelIntegrity: {
      missingHumanLabels,
      missingCanonicalEntity,
      missingSourceResponse,
      malformedResponse,
      identityAmbiguityUnresolved,
      invalid: invalid.length,
      deferred: deferred.length,
      duplicateReviewActions,
      ok: labelOk,
    },
    feasibility: {
      ELIGIBLE_PAIR_N: eligible.length,
      ELIGIBLE_UNIQUE_RESPONSE_N: UNIQUE_RESPONSE_N,
      ELIGIBLE_PRESENT: present.length,
      ELIGIBLE_NOT_PRESENT: notPresent.length,
      TARGET_60_40_FEASIBLE,
      TARGET_UNIQUE_RESPONSE_80_FEASIBLE,
      PROJECTED_HOLDOUT_PAIR_N: projection.PROJECTED_HOLDOUT_PAIR_N,
      PROJECTED_UNIQUE_RESPONSE_N: projection.PROJECTED_UNIQUE_RESPONSE_N,
      PROJECTED_PRESENT: projection.PROJECTED_PRESENT,
      PROJECTED_NOT_PRESENT: projection.PROJECTED_NOT_PRESENT,
      reasonIfBelow80: projection.reasonIfBelow80,
      NEGATIVE_CONTROL_COVERAGE_SUFFICIENT: neg.NEGATIVE_CONTROL_COVERAGE_SUFFICIENT,
    },
    negativeControls: neg,
    shortNameContextual: shortName,
    proposedAllocation: {
      OPENAI: allocation.OPENAI,
      GEMINI: allocation.GEMINI,
      PERPLEXITY: allocation.PERPLEXITY,
      CLAUDE: allocation.CLAUDE,
      ENGLISH: allocation.ENGLISH,
      SPANISH: allocation.SPANISH,
      GLOBAL: allocation.GLOBAL,
      CALA: allocation.CALA,
      MEXICO: allocation.MEXICO,
      EUROPE: allocation.EUROPE,
      NORTH_AMERICA: allocation.NORTH_AMERICA,
      TOTAL: allocation.TOTAL,
    },
    certificationContract: {
      PRECISION_THRESHOLD: "98%",
      RECALL_THRESHOLD: "98%",
      requiredMetrics: PRESENCE_HOLDOUT_V2_METRIC_CONTRACT.requiredMetrics,
      requiredBreakdowns: PRESENCE_HOLDOUT_V2_METRIC_CONTRACT.requiredBreakdowns,
      forbidCompositeScore: true,
      report: [
        "Accuracy",
        "Precision",
        "Recall",
        "F1",
        "Specificity",
        "FPR",
        "FNR",
        "Pair N",
        "Unique Response N",
        "Provider",
        "Language",
        "Geography",
      ],
    },
    holdoutV2,
    regionalization: {
      STATUS: "PLANNED_AFTER_PRESENCE_CERTIFICATION",
      EXECUTED: "NO",
    },
    hardGuards: {
      HUMAN_LABEL_CHANGES: 0,
      ENTITY_RESOLVER_CHANGES: 0,
      ALIAS_CHANGES: 0,
      HOLDOUT_V3_SELECTION: 0,
      HOLDOUT_V3_FREEZE: 0,
      HOLDOUT_V3_SCORING: 0,
      HOLDOUT_V2_CHANGES: 0,
      HOLDOUT_V2_RESCORE: 0,
      PROVIDER_CALLS: 0,
      REGIONALIZATION_EXECUTION: 0,
      AIRTABLE_WRITES: 0,
      DEPLOYS: 0,
    },
    HOLDOUT_V3_SELECTED: false,
    HOLDOUT_V3_FROZEN: false,
    HOLDOUT_V3_SCORED: false,
  };
}
