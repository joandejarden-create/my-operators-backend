/**
 * Presence Holdout v3 — deterministic selection (NO scoring / NO prediction inspection).
 *
 * Priority after 60/40 class balance: maximize UNIQUE_RESPONSE_N (prefer 1 pair/response).
 * Soft guidance for provider / language / geography; fail-closed integrity before seal.
 */

import crypto from "crypto";
import {
  HOLDOUT_V3_TARGET,
  loadHoldoutV3FinalizedPrimaryPool,
  classifyHumanFinalNegativeControls,
  auditShortNameContextualCoverage,
  buildPriorOnlyLeakageIndexForV3,
} from "./presence-holdout-v3-readiness-audit.js";
import { HOLDOUT_V3_BATCH_ID } from "./presence-holdout-v3-fresh-candidates.js";
import {
  validateHoldoutManifestIntegrity,
  resolvePresenceSelectionLabel,
  dedupeHoldoutSelectionByCaseId,
} from "./holdout-manifest-integrity.js";
import {
  countBySourceResponse,
  uniqueResponseIds,
  enforceResponseLevelPartitioning,
  enrichCandidatesWithResponseGovernance,
  CANDIDATE_CAP_PER_RESPONSE_HOLDOUT,
  PRESENCE_HOLDOUT_V2_METRIC_CONTRACT,
  sha256Hex,
} from "./presence-validation-pool-governance.js";

export const HOLDOUT_V3_VERSION = "ai_intelligence_presence_holdout_v3";
export const HOLDOUT_V3_SELECTION_VERSION =
  "presence_holdout_v3_unique_response_priority_select_v1";
export const HOLDOUT_V3_SELECTION_ALGORITHM =
  "selectHoldoutV3WithUniqueResponsePriority — maximize unique responses after 60/40; soft provider/language/geography guidance; response-atomic; caseId lexicographic tie-break; no resolver prediction inspection";
export const HOLDOUT_V3_SELECTION_SEED =
  "presence_holdout_v3_freeze_20260815_readiness_pass";

/** Holdout v2 Canopy FN — must not be reused. */
export const HOLDOUT_V2_CANOPY_FN_CASE_ID = "presval_260089d8b1bc";

export function normalizeProviderKey(p) {
  const s = String(p || "").toLowerCase();
  if (s.includes("openai") || s === "gpt") return "OPENAI";
  if (s.includes("gemini")) return "GEMINI";
  if (s.includes("perplexity")) return "PERPLEXITY";
  if (s.includes("claude") || s.includes("anthropic")) return "CLAUDE";
  return String(p || "UNSPECIFIED").toUpperCase();
}

export function normalizeLanguageKey(l) {
  const s = String(l || "").toLowerCase();
  if (s === "en" || s.startsWith("en")) return "ENGLISH";
  if (s === "es" || s.startsWith("es")) return "SPANISH";
  return String(l || "UNSPECIFIED").toUpperCase();
}

export function normalizeGeographyKey(g) {
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

export function classifyNegCategory(c) {
  const name = String(c.canonicalEntityName || "");
  const raw = String(c.rawText || "");
  const rat = String(c.systemSuggestionRationale || "").toLowerCase();
  if (
    /geographic|playa without playa hotels/.test(rat) ||
    (/playa/.test(rat) && /common-language|geographic/.test(rat))
  ) {
    return "GEOGRAPHIC_PLAYA";
  }
  if (/sibling/.test(rat)) return "SIBLING_TARGET_ABSENT";
  if (/without this specific child|parent\/child|parent-child/.test(rat)) {
    return "PARENT_CHILD";
  }
  if (
    /family context without this specific|without this specific brand|without this specific child brand/.test(
      rat
    )
  ) {
    return "PARENT_CHILD";
  }
  if (/generic collection/.test(rat)) return "GENERIC_COLLECTION";
  if (/hard negative|canonical brand absent|absent from response/.test(rat)) {
    return "NO_ENTITY";
  }
  if (
    /canopy by hilton/i.test(name) &&
    /\bcanopy\b/i.test(raw) &&
    !/canopy\s+by\s+hilton/i.test(raw)
  ) {
    return "COMMON_LANGUAGE_COLLISION";
  }
  if (/short.?name|ambiguous shortened|bare\b/.test(rat)) return "SHORT_NAME_AMBIGUITY";
  if (
    /playa hotels/i.test(name) &&
    /\bplaya\b/i.test(raw) &&
    !/playa\s+hotels/i.test(raw)
  ) {
    return "GEOGRAPHIC_PLAYA";
  }
  if (/\bplaya\b/i.test(raw) && !/playa\s+hotels/i.test(raw) && /playa/i.test(name)) {
    return "GEOGRAPHIC_PLAYA";
  }
  if (
    /\bcanopy\b/i.test(raw) &&
    !/canopy\s+by\s+hilton/i.test(raw) &&
    /canopy/i.test(name)
  ) {
    return "COMMON_LANGUAGE_COLLISION";
  }
  return "OTHER";
}

function seededRank(seed, caseId) {
  return crypto
    .createHash("sha256")
    .update(`${seed}::${caseId}`)
    .digest("hex");
}

function sortBySeedThenCaseId(rows, seed) {
  return [...(rows || [])].sort((a, b) => {
    const ra = seededRank(seed, a.caseId);
    const rb = seededRank(seed, b.caseId);
    if (ra !== rb) return ra.localeCompare(rb);
    return String(a.caseId).localeCompare(String(b.caseId));
  });
}

function countByNorm(rows, keyFn) {
  const out = {};
  for (const r of rows || []) {
    const k = keyFn(r);
    out[k] = (out[k] || 0) + 1;
  }
  return out;
}

function stableStringify(obj) {
  if (obj === null || typeof obj !== "object") return JSON.stringify(obj);
  if (Array.isArray(obj)) return `[${obj.map(stableStringify).join(",")}]`;
  const keys = Object.keys(obj).sort();
  return `{${keys.map((k) => `${JSON.stringify(k)}:${stableStringify(obj[k])}`).join(",")}}`;
}

/**
 * Deterministic Holdout v3 selection — unique-response priority.
 * Does not inspect resolver predictions.
 */
export function selectHoldoutV3WithUniqueResponsePriority(eligibleCases, options = {}) {
  const TOTAL_N = options.TOTAL_N ?? HOLDOUT_V3_TARGET.TOTAL_PAIR_N;
  const PRESENT_N = options.PRESENT_N ?? HOLDOUT_V3_TARGET.PRESENT;
  const NOT_PRESENT_N = options.NOT_PRESENT_N ?? HOLDOUT_V3_TARGET.NOT_PRESENT;
  const cap = options.CANDIDATE_CAP_PER_RESPONSE ?? CANDIDATE_CAP_PER_RESPONSE_HOLDOUT;
  const seed = options.selectionSeed || HOLDOUT_V3_SELECTION_SEED;

  const enriched = enrichCandidatesWithResponseGovernance(
    (eligibleCases || []).filter((c) => {
      if (c.caseId === HOLDOUT_V2_CANOPY_FN_CASE_ID) return false;
      const label = resolvePresenceSelectionLabel(c) || c.humanFinalDecision;
      return label === "PRESENT" || label === "NOT_PRESENT";
    })
  );

  const byResp = new Map();
  for (const c of enriched) {
    const rid = c.sourceResponseId || c.responseId;
    if (!rid) continue;
    if (!byResp.has(rid)) byResp.set(rid, { present: [], notPresent: [], meta: c });
    const g = byResp.get(rid);
    const label = resolvePresenceSelectionLabel(c) || c.humanFinalDecision;
    if (label === "PRESENT") g.present.push(c);
    else if (label === "NOT_PRESENT") g.notPresent.push(c);
  }

  for (const g of byResp.values()) {
    g.present = sortBySeedThenCaseId(g.present, seed);
    g.notPresent = sortBySeedThenCaseId(g.notPresent, seed);
  }

  const responses = [...byResp.entries()]
    .map(([rid, g]) => ({
      rid,
      present: g.present,
      notPresent: g.notPresent,
      provider: normalizeProviderKey(g.meta.provider),
      language: normalizeLanguageKey(g.meta.language),
      geography: normalizeGeographyKey(g.meta.geography),
    }))
    .sort((a, b) => {
      const aN = (a.present.length ? 1 : 0) + (a.notPresent.length ? 1 : 0);
      const bN = (b.present.length ? 1 : 0) + (b.notPresent.length ? 1 : 0);
      if (aN !== bN) return aN - bN;
      const ra = seededRank(seed, a.rid);
      const rb = seededRank(seed, b.rid);
      if (ra !== rb) return ra.localeCompare(rb);
      return String(a.rid).localeCompare(String(b.rid));
    });

  let presentLeft = PRESENT_N;
  let absentLeft = NOT_PRESENT_N;
  const selected = [];
  const usedResp = new Set();
  const selectedIds = new Set();

  function takeFrom(resp, label) {
    const pool = label === "PRESENT" ? resp.present : resp.notPresent;
    if (!pool.length) return null;
    const already = selected.filter(
      (s) => (s.sourceResponseId || s.responseId) === resp.rid
    ).length;
    if (already >= cap) return null;
    return pool.find((c) => !selectedIds.has(c.caseId)) || null;
  }

  function pushRow(row, label) {
    if (!row || selectedIds.has(row.caseId)) return false;
    selected.push(row);
    selectedIds.add(row.caseId);
    usedResp.add(row.sourceResponseId || row.responseId);
    if (label === "PRESENT") presentLeft -= 1;
    else absentLeft -= 1;
    return true;
  }

  const softProvider = { OPENAI: 25, GEMINI: 20, PERPLEXITY: 26, CLAUDE: 29 };
  const softLang = { ENGLISH: 68, SPANISH: 32 };
  const softGeo = {
    GLOBAL: 16,
    CALA: 31,
    MEXICO: 29,
    EUROPE: 12,
    NORTH_AMERICA: 12,
  };

  function underSoft(resp, label) {
    const provN = selected.filter(
      (s) => normalizeProviderKey(s.provider) === resp.provider
    ).length;
    const langN = selected.filter(
      (s) => normalizeLanguageKey(s.language) === resp.language
    ).length;
    const geoN = selected.filter(
      (s) => normalizeGeographyKey(s.geography) === resp.geography
    ).length;
    const pNeed = Math.max(0, (softProvider[resp.provider] || 0) - provN);
    const lNeed = Math.max(0, (softLang[resp.language] || 0) - langN);
    const gNeed = Math.max(0, (softGeo[resp.geography] || 0) - geoN);
    const missingProv = selected.every(
      (s) => normalizeProviderKey(s.provider) !== resp.provider
    )
      ? 100
      : 0;
    const missingLang = selected.every(
      (s) => normalizeLanguageKey(s.language) !== resp.language
    )
      ? 50
      : 0;
    const missingGeo = selected.every(
      (s) => normalizeGeographyKey(s.geography) !== resp.geography
    )
      ? 40
      : 0;
    let negBoost = 0;
    if (label === "NOT_PRESENT") {
      const row = takeFrom(resp, "NOT_PRESENT");
      if (row) {
        const cat = classifyNegCategory(row);
        const have = selected
          .filter((s) => resolvePresenceSelectionLabel(s) === "NOT_PRESENT")
          .some((s) => classifyNegCategory(s) === cat);
        if (!have && cat !== "OTHER") negBoost = 30;
      }
    }
    return missingProv + missingLang + missingGeo + pNeed + lNeed + gNeed + negBoost;
  }

  while ((presentLeft > 0 || absentLeft > 0) && selected.length < TOTAL_N) {
    let best = null;
    let bestScore = -1;
    let bestLabel = null;
    for (const resp of responses) {
      if (usedResp.has(resp.rid)) continue;
      const candidates = [];
      if (presentLeft > 0 && resp.present.length) candidates.push("PRESENT");
      if (absentLeft > 0 && resp.notPresent.length) candidates.push("NOT_PRESENT");
      if (!candidates.length) continue;
      let label;
      if (candidates.length === 2) {
        const presentRatio = presentLeft / PRESENT_N;
        const absentRatio = absentLeft / NOT_PRESENT_N;
        label = presentRatio >= absentRatio ? "PRESENT" : "NOT_PRESENT";
        const sPresent = underSoft(resp, "PRESENT");
        const sAbsent = underSoft(resp, "NOT_PRESENT");
        if (Math.abs(presentRatio - absentRatio) < 0.15 && sAbsent > sPresent + 20) {
          label = "NOT_PRESENT";
        } else if (
          Math.abs(presentRatio - absentRatio) < 0.15 &&
          sPresent > sAbsent + 20
        ) {
          label = "PRESENT";
        }
      } else {
        label = candidates[0];
      }
      const row = takeFrom(resp, label);
      if (!row) continue;
      const score = underSoft(resp, label);
      const tie = seededRank(seed, resp.rid);
      if (
        score > bestScore ||
        (score === bestScore && (!best || tie < seededRank(seed, best.rid)))
      ) {
        best = resp;
        bestScore = score;
        bestLabel = label;
      }
    }
    if (!best) break;
    const row = takeFrom(best, bestLabel);
    if (!row || !pushRow(row, bestLabel)) break;
  }

  const usedList = responses.filter((r) => usedResp.has(r.rid));
  usedList.sort((a, b) => seededRank(seed, a.rid).localeCompare(seededRank(seed, b.rid)));

  function fillSeconds(preferLabel) {
    for (const resp of usedList) {
      if (selected.length >= TOTAL_N) break;
      if (preferLabel === "PRESENT" && presentLeft <= 0) break;
      if (preferLabel === "NOT_PRESENT" && absentLeft <= 0) break;
      const row = takeFrom(resp, preferLabel);
      if (row) pushRow(row, preferLabel);
    }
  }

  if (presentLeft / PRESENT_N >= absentLeft / NOT_PRESENT_N) {
    fillSeconds("PRESENT");
    fillSeconds("NOT_PRESENT");
  } else {
    fillSeconds("NOT_PRESENT");
    fillSeconds("PRESENT");
  }

  for (const resp of responses) {
    if (selected.length >= TOTAL_N) break;
    if (usedResp.has(resp.rid)) continue;
    if (presentLeft > 0) {
      const row = takeFrom(resp, "PRESENT");
      if (row && pushRow(row, "PRESENT")) continue;
    }
    if (absentLeft > 0) {
      const row = takeFrom(resp, "NOT_PRESENT");
      if (row) pushRow(row, "NOT_PRESENT");
    }
  }

  const requiredNeg = [
    "SIBLING_TARGET_ABSENT",
    "PARENT_CHILD",
    "GENERIC_COLLECTION",
    "GEOGRAPHIC_PLAYA",
    "NO_ENTITY",
  ];
  const negSelected = () =>
    selected.filter((s) => resolvePresenceSelectionLabel(s) === "NOT_PRESENT");

  for (const cat of requiredNeg) {
    const have = negSelected().some((s) => classifyNegCategory(s) === cat);
    if (have) continue;
    const pool = sortBySeedThenCaseId(
      enriched.filter(
        (c) =>
          !selectedIds.has(c.caseId) &&
          resolvePresenceSelectionLabel(c) === "NOT_PRESENT" &&
          classifyNegCategory(c) === cat
      ),
      seed
    );
    for (const candidate of pool) {
      const rid = candidate.sourceResponseId || candidate.responseId;
      const fromResp = selected.filter(
        (s) => (s.sourceResponseId || s.responseId) === rid
      ).length;
      if (fromResp >= cap) continue;
      const swapOut = sortBySeedThenCaseId(
        negSelected().filter((s) => {
          const c = classifyNegCategory(s);
          return c === "OTHER" || c === "SIBLING_TARGET_ABSENT";
        }),
        seed
      ).find((s) => {
        const c = classifyNegCategory(s);
        if (requiredNeg.includes(c)) {
          const n = negSelected().filter((x) => classifyNegCategory(x) === c).length;
          if (n <= 1) return false;
        }
        return true;
      });
      if (!swapOut) continue;
      const swapRid = swapOut.sourceResponseId || swapOut.responseId;
      selectedIds.delete(swapOut.caseId);
      const idx = selected.findIndex((s) => s.caseId === swapOut.caseId);
      if (idx >= 0) selected.splice(idx, 1);
      selected.push(candidate);
      selectedIds.add(candidate.caseId);
      usedResp.add(rid);
      if (
        !selected.some((s) => (s.sourceResponseId || s.responseId) === swapRid)
      ) {
        usedResp.delete(swapRid);
      }
      break;
    }
  }

  const deduped = dedupeHoldoutSelectionByCaseId(selected).slice(0, TOTAL_N);
  const integrity = validateHoldoutManifestIntegrity(deduped);
  const uniq = uniqueResponseIds(deduped);
  const presentFinal = deduped.filter(
    (c) => resolvePresenceSelectionLabel(c) === "PRESENT"
  ).length;
  const absentFinal = deduped.filter(
    (c) => resolvePresenceSelectionLabel(c) === "NOT_PRESENT"
  ).length;

  return {
    selected: deduped,
    CANDIDATE_PAIR_N: deduped.length,
    UNIQUE_RESPONSE_N: uniq.size,
    PRESENT_N: presentFinal,
    NOT_PRESENT_N: absentFinal,
    CANDIDATE_CAP_PER_RESPONSE: cap,
    selectionVersion: HOLDOUT_V3_SELECTION_VERSION,
    selectionAlgorithm: HOLDOUT_V3_SELECTION_ALGORITHM,
    selectionSeed: seed,
    manifestIntegrity: integrity,
    SELECTION_INTEGRITY_OK: integrity.ok,
    COMPOSITION_OK:
      deduped.length === TOTAL_N &&
      presentFinal === PRESENT_N &&
      absentFinal === NOT_PRESENT_N,
  };
}

export function verifyHoldoutV3Leakage(selected) {
  const prior = buildPriorOnlyLeakageIndexForV3();
  const hits = [];
  for (const c of selected || []) {
    const rid = c.sourceResponseId || c.responseId;
    const hash =
      c.responseHash ||
      c.textHash ||
      (c.rawText
        ? crypto
            .createHash("sha256")
            .update(String(c.rawText).replace(/\s+/g, " ").trim().toLowerCase())
            .digest("hex")
        : null);
    if (c.caseId === HOLDOUT_V2_CANOPY_FN_CASE_ID) {
      hits.push({ caseId: c.caseId, reason: "HOLDOUT_V2_CANOPY_FN_REUSE" });
    }
    if (c.caseId && prior.caseIds.has(c.caseId)) {
      hits.push({ caseId: c.caseId, reason: "CASE_ID_IN_PRIOR_SET" });
    }
    if (rid && prior.responseIds.has(rid)) {
      hits.push({ caseId: c.caseId, reason: "RESPONSE_ID_IN_PRIOR_SET", rid });
    }
    if (hash && prior.hashes.has(hash)) {
      hits.push({ caseId: c.caseId, reason: "TEXT_HASH_IN_PRIOR_SET" });
    }
  }
  return { ok: hits.length === 0, hits, LEAKAGE_CASES: hits.length };
}

export function buildHoldoutV3NegativeControlCounts(selected) {
  const neg = (selected || []).filter(
    (c) => resolvePresenceSelectionLabel(c) === "NOT_PRESENT"
  );
  const counts = {
    SIBLING_TARGET_ABSENT: 0,
    PARENT_CHILD: 0,
    GENERIC_COLLECTION: 0,
    GEOGRAPHIC_PLAYA: 0,
    NO_ENTITY: 0,
    COMMON_LANGUAGE_COLLISION: 0,
    SHORT_NAME_AMBIGUITY: 0,
    OTHER: 0,
  };
  for (const c of neg) {
    const cat = classifyNegCategory(c);
    if (counts[cat] != null) counts[cat] += 1;
    else counts.OTHER += 1;
  }
  return counts;
}

export function buildHoldoutV3FreezeArtifacts(options = {}) {
  const pool = loadHoldoutV3FinalizedPrimaryPool(options);
  const { eligible, primary } = pool;

  if (primary.length !== 170) {
    return { ok: false, error: `PRIMARY_COUNT_NE_170:${primary.length}`, DO_NOT_SEAL: true };
  }
  if (eligible.length !== 170) {
    return { ok: false, error: `ELIGIBLE_COUNT_NE_170:${eligible.length}`, DO_NOT_SEAL: true };
  }

  const selection = selectHoldoutV3WithUniqueResponsePriority(eligible, {
    selectionSeed: options.selectionSeed || HOLDOUT_V3_SELECTION_SEED,
  });
  const selected = selection.selected || [];

  if (!selection.COMPOSITION_OK) {
    return { ok: false, error: "COMPOSITION_MISMATCH", DO_NOT_SEAL: true, selection };
  }
  if (!selection.SELECTION_INTEGRITY_OK) {
    return {
      ok: false,
      error: "HOLDOUT_MANIFEST_INTEGRITY_FAIL_DO_NOT_SEAL",
      DO_NOT_SEAL: true,
      selection,
    };
  }
  if (selection.UNIQUE_RESPONSE_N < HOLDOUT_V3_TARGET.MIN_UNIQUE_RESPONSE_N) {
    return {
      ok: false,
      error: `UNIQUE_RESPONSE_BELOW_80:${selection.UNIQUE_RESPONSE_N}`,
      DO_NOT_SEAL: true,
      selection,
    };
  }

  const leakage = verifyHoldoutV3Leakage(selected);
  if (!leakage.ok) {
    return {
      ok: false,
      error: "LEAKAGE_TO_PRIOR_VALIDATION",
      DO_NOT_SEAL: true,
      leakage,
      selection,
    };
  }

  const selectedIds = new Set(selected.map((c) => c.caseId));
  const holdoutResponseIds = new Set(
    selected.map((c) => c.sourceResponseId || c.responseId).filter(Boolean)
  );

  const reserve = eligible.filter((c) => {
    const rid = c.sourceResponseId || c.responseId;
    if (selectedIds.has(c.caseId)) return false;
    if (holdoutResponseIds.has(rid)) return false;
    return true;
  });

  const holdoutSameResponseRemainder = eligible.filter((c) => {
    const rid = c.sourceResponseId || c.responseId;
    return !selectedIds.has(c.caseId) && holdoutResponseIds.has(rid);
  });

  const counts = countBySourceResponse(selected);
  let maxPairs = 0;
  for (const n of counts.values()) maxPairs = Math.max(maxPairs, n);
  const overCap = [...counts.entries()].filter(
    ([, n]) => n > CANDIDATE_CAP_PER_RESPONSE_HOLDOUT
  );
  const partCheck = enforceResponseLevelPartitioning(
    selected.map((c) => ({ ...c, validationPartition: "HOLDOUT_V3" })),
    { repair: false }
  );
  const reserveOverlap = reserve.filter((c) =>
    holdoutResponseIds.has(c.sourceResponseId || c.responseId)
  );

  if (overCap.length || !partCheck.ok || reserveOverlap.length) {
    return {
      ok: false,
      error: "ATOMICITY_FAIL",
      DO_NOT_SEAL: true,
      overCap,
      partCheck,
      reserveOverlap: reserveOverlap.length,
    };
  }

  const negativeControlCounts = buildHoldoutV3NegativeControlCounts(selected);
  const contextualCoverageCounts = auditShortNameContextualCoverage(selected);
  const createdAt = options.createdAt || new Date().toISOString();

  const casePayload = selected
    .map((c) => ({
      caseId: c.caseId,
      sourceResponseId: c.sourceResponseId || c.responseId,
      responseHash: c.responseHash || c.textHash || null,
      canonicalEntityId: c.canonicalEntityId,
      canonicalEntityName: c.canonicalEntityName,
      humanFinalLabel: resolvePresenceSelectionLabel(c) || c.humanFinalDecision,
      humanAction: c.humanAction || null,
      provider: c.provider,
      model: c.model || null,
      language: c.language,
      geography: c.geography,
      promptId: c.promptId || null,
      promptFamily: c.promptFamily || c.intentTerritory || null,
      candidateType: c.candidateType,
      batchId: c.batchId || HOLDOUT_V3_BATCH_ID,
      systemSuggestionRationale: c.systemSuggestionRationale || null,
      negativeControlType:
        resolvePresenceSelectionLabel(c) === "NOT_PRESENT"
          ? classifyNegCategory(c)
          : null,
    }))
    .sort((a, b) => String(a.caseId).localeCompare(String(b.caseId)));

  const eligibilityFingerprint = sha256Hex(
    stableStringify({
      eligibleCaseIds: eligible.map((c) => c.caseId).sort(),
      selectionVersion: HOLDOUT_V3_SELECTION_VERSION,
      selectionSeed: HOLDOUT_V3_SELECTION_SEED,
      TOTAL_N: 100,
      PRESENT_N: 60,
      NOT_PRESENT_N: 40,
      cap: CANDIDATE_CAP_PER_RESPONSE_HOLDOUT,
      batchId: HOLDOUT_V3_BATCH_ID,
    })
  );

  const contentBody = {
    caseIds: casePayload.map((c) => c.caseId),
    sourceResponseIds: [...holdoutResponseIds].sort(),
    humanFinalLabels: casePayload.map((c) => ({
      caseId: c.caseId,
      label: c.humanFinalLabel,
    })),
    presentCount: selection.PRESENT_N,
    notPresentCount: selection.NOT_PRESENT_N,
  };
  const contentHash = sha256Hex(stableStringify(contentBody));

  const providerCounts = countByNorm(selected, (r) => normalizeProviderKey(r.provider));
  const languageCounts = countByNorm(selected, (r) => normalizeLanguageKey(r.language));
  const geographyCounts = countByNorm(selected, (r) =>
    normalizeGeographyKey(r.geography)
  );

  const providersOk =
    providerCounts.OPENAI > 0 &&
    providerCounts.GEMINI > 0 &&
    providerCounts.PERPLEXITY > 0 &&
    providerCounts.CLAUDE > 0;
  const languagesOk = languageCounts.ENGLISH > 0 && languageCounts.SPANISH > 0;
  const geosOk =
    geographyCounts.GLOBAL > 0 &&
    geographyCounts.CALA > 0 &&
    geographyCounts.MEXICO > 0 &&
    geographyCounts.EUROPE > 0 &&
    geographyCounts.NORTH_AMERICA > 0;

  if (!providersOk || !languagesOk || !geosOk) {
    return {
      ok: false,
      error: "COVERAGE_FLOOR_FAIL",
      DO_NOT_SEAL: true,
      providerCounts,
      languageCounts,
      geographyCounts,
    };
  }

  const negOk =
    negativeControlCounts.SIBLING_TARGET_ABSENT > 0 &&
    negativeControlCounts.GENERIC_COLLECTION > 0 &&
    negativeControlCounts.GEOGRAPHIC_PLAYA > 0 &&
    negativeControlCounts.NO_ENTITY > 0;

  if (!negOk) {
    return {
      ok: false,
      error: "NEGATIVE_CONTROL_COVERAGE_FAIL",
      DO_NOT_SEAL: true,
      negativeControlCounts,
    };
  }

  const sealIntegrity = validateHoldoutManifestIntegrity(selected);
  if (
    !sealIntegrity.ok ||
    sealIntegrity.UNIQUE_CASE_ID_COUNT !== 100 ||
    sealIntegrity.UNIQUE_ENTITY_RESPONSE_PAIR_COUNT !== 100
  ) {
    return {
      ok: false,
      error: "HOLDOUT_MANIFEST_INTEGRITY_FAIL_DO_NOT_SEAL",
      DO_NOT_SEAL: true,
      sealIntegrity,
    };
  }

  const manifestCore = {
    version: HOLDOUT_V3_VERSION,
    holdoutVersion: HOLDOUT_V3_VERSION,
    createdAt,
    STATUS: "READY_UNSCORED",
    SCORED: false,
    USED_FOR_TUNING: false,
    PREDICTIONS_EXPOSED: false,
    FRESH_RESPONSES: true,
    NOT_USED_FOR_TUNING: true,
    UNSCORED: true,
    HOLDOUT_V3_SCORING: 0,
    batchId: HOLDOUT_V3_BATCH_ID,
    resolverVersion: "ai_visibility_entity_resolver_v2_1_contextual",
    selectionAlgorithm: HOLDOUT_V3_SELECTION_ALGORITHM,
    selectionVersion: HOLDOUT_V3_SELECTION_VERSION,
    selectionSeed: HOLDOUT_V3_SELECTION_SEED,
    eligibilityRules: [
      "batchId=presence_validation_holdout_v3_candidate_batch_v1",
      "primaryReviewQueue=true",
      "HUMAN_FINALIZED PRESENT|NOT_PRESENT",
      "INVALID=false DEFERRED=false",
      "canonicalEntityId present",
      "sourceResponseId present",
      "rawText present",
      "leakage vs Golden/DEV/Holdout v1/v2/Reserve/classifier-lab/fixtures = 0",
      "caseId unique",
      "entity-response pair unique",
      "response-level atomicity",
      "max 2 pairs per sourceResponseId",
      "exclude Holdout v2 Canopy FN caseId",
    ],
    tieBreakRules: [
      "seeded sha256(seed::caseId|responseId) then lexicographic caseId",
      "prefer 1 pair per response; second pair only for 60/40 or coverage",
      "soft provider/language/geography guidance from readiness audit (not hard quotas)",
      "no resolver prediction inspection",
    ],
    design: {
      TOTAL_N: 100,
      PRESENT_N: 60,
      NOT_PRESENT_N: 40,
      MIN_UNIQUE_RESPONSE_N: 80,
      CANDIDATE_CAP_PER_RESPONSE: CANDIDATE_CAP_PER_RESPONSE_HOLDOUT,
    },
    pairCount: selected.length,
    uniqueResponseCount: selection.UNIQUE_RESPONSE_N,
    presentCount: selection.PRESENT_N,
    notPresentCount: selection.NOT_PRESENT_N,
    providerCounts,
    languageCounts,
    geographyCounts,
    negativeControlCounts,
    contextualCoverageCounts,
    caseIds: casePayload.map((c) => c.caseId),
    sourceResponseIds: [...holdoutResponseIds].sort(),
    responseHashes: casePayload.map((c) => c.responseHash),
    canonicalEntityIds: casePayload.map((c) => c.canonicalEntityId),
    canonicalEntityNames: casePayload.map((c) => c.canonicalEntityName),
    humanFinalLabels: casePayload.map((c) => ({
      caseId: c.caseId,
      label: c.humanFinalLabel,
    })),
    cases: casePayload,
    holdoutSameResponseRemainderCaseIds: holdoutSameResponseRemainder
      .map((c) => c.caseId)
      .sort(),
    RESPONSE_LEVEL_ATOMICITY: "PASS",
    MAX_PAIRS_PER_RESPONSE: maxPairs,
    UNIQUE_CASE_ID_COUNT: sealIntegrity.UNIQUE_CASE_ID_COUNT,
    UNIQUE_ENTITY_RESPONSE_PAIR_COUNT: sealIntegrity.UNIQUE_ENTITY_RESPONSE_PAIR_COUNT,
    NO_DUPLICATE_MANIFEST_ROWS: sealIntegrity.NO_DUPLICATE_MANIFEST_ROWS,
    MANIFEST_INTEGRITY: "PASS",
    metricContract: PRESENCE_HOLDOUT_V2_METRIC_CONTRACT,
    eligibilityFingerprint,
    contentHash,
    scorecard: {
      PRESENCE_DEV: "PASS",
      HOLDOUT_V1: "INSPECTED_DIAGNOSTIC",
      HOLDOUT_V2: "SCORED_FAIL",
      HOLDOUT_V3: "READY_UNSCORED",
      PRESENCE_PRODUCTION_READINESS: "NOT_READY",
      RECOMMENDED: "NOT_READY",
      FIRST_RECOMMENDATION: "NOT_READY",
      NEGATIVE: "NOT_READY",
      COMPARATOR: "NOT_READY",
    },
    regionalization: {
      STATUS: "PLANNED_AFTER_PRESENCE_CERTIFICATION",
      EXECUTED: false,
      PROVIDER_CALLS: 0,
    },
    hardGuards: {
      HOLDOUT_V3_SCORING: 0,
      PREDICTIONS_EXPOSED: 0,
      ENTITY_RESOLVER_CHANGES: 0,
      ALIAS_CHANGES: 0,
      GROUND_TRUTH_CHANGES: 0,
      HUMAN_LABEL_CHANGES: 0,
      HOLDOUT_V2_CHANGES: 0,
      HOLDOUT_V2_RESCORE: 0,
      PROVIDER_CALLS: 0,
      REGIONALIZATION_EXECUTION: 0,
      AIRTABLE_WRITES: 0,
      DEPLOYS: 0,
    },
  };

  const manifestHash = sha256Hex(stableStringify(manifestCore));
  const manifest = {
    ...manifestCore,
    manifestHash,
    HOLDOUT_V3_SEALED: true,
    sealedAt: createdAt,
  };

  const reserveDoc = {
    version: "presence_validation_v3_reserve_v1",
    partition: "PRESENCE_VALIDATION_V3_RESERVE",
    createdAt,
    relatedHoldout: HOLDOUT_V3_VERSION,
    relatedManifestHash: manifestHash,
    PAIR_N: reserve.length,
    UNIQUE_RESPONSE_N: uniqueResponseIds(reserve).size,
    PRESENT_N: reserve.filter(
      (c) => resolvePresenceSelectionLabel(c) === "PRESENT"
    ).length,
    NOT_PRESENT_N: reserve.filter(
      (c) => resolvePresenceSelectionLabel(c) === "NOT_PRESENT"
    ).length,
    caseIds: reserve.map((c) => c.caseId).sort(),
    sourceResponseIds: [...uniqueResponseIds(reserve)].sort(),
    SOURCE_RESPONSE_OVERLAP_WITH_HOLDOUT: 0,
    note: "Eligible human-finalized Holdout v3 primary pairs not in the sealed 100 and not sharing a Holdout v3 sourceResponseId. Never mutate frozen Holdout v3. Holdout v2 remains SCORED_FAIL / untouched.",
  };

  const poolNeg = classifyHumanFinalNegativeControls(
    eligible.filter((c) => resolvePresenceSelectionLabel(c) === "NOT_PRESENT")
  );

  return {
    ok: true,
    DO_NOT_SEAL: false,
    manifest,
    reserveDoc,
    selected,
    eligible,
    reserve,
    holdoutSameResponseRemainder,
    selection,
    leakage,
    poolNeg,
    createdAt,
  };
}
