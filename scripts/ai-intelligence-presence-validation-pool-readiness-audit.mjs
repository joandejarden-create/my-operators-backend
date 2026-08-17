#!/usr/bin/env node
/**
 * Presence validation pool readiness audit — READ ONLY.
 * Does not select, freeze, or score Holdout v2.
 */
import fs from "fs";
import path from "path";
import crypto from "crypto";
import { fileURLToPath } from "url";
import {
  countBySourceResponse,
  enforceResponseLevelPartitioning,
  CANDIDATE_CAP_PER_RESPONSE_HOLDOUT,
  PRESENCE_HOLDOUT_V2_METRIC_CONTRACT,
  selectHoldoutV2WithResponseGovernance,
} from "../lib/ai-visibility/validation/presence-validation-pool-governance.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

function sha256(t) {
  return crypto.createHash("sha256").update(String(t || "")).digest("hex");
}

function loadJson(p) {
  return fs.existsSync(p) ? JSON.parse(fs.readFileSync(p, "utf8")) : null;
}

function uniqResp(rows) {
  return new Set(rows.map((c) => c.sourceResponseId || c.responseId).filter(Boolean)).size;
}

function byDim(rows, key) {
  const out = {};
  for (const r of rows) {
    const k = r[key] || "unspecified";
    if (!out[k]) out[k] = { candidatePairN: 0, presentN: 0, notPresentN: 0, resp: new Set() };
    out[k].candidatePairN += 1;
    if (r.humanLabel === "PRESENT") out[k].presentN += 1;
    if (r.humanLabel === "NOT_PRESENT") out[k].notPresentN += 1;
    const rid = r.sourceResponseId || r.responseId;
    if (rid) out[k].resp.add(rid);
  }
  for (const k of Object.keys(out)) {
    out[k].uniqueResponseN = out[k].resp.size;
    delete out[k].resp;
  }
  return out;
}

function ingestPrior(arr, buckets) {
  for (const c of arr || []) {
    if (c.caseId) buckets.caseIds.add(c.caseId);
    if (c.responseId) buckets.responseIds.add(c.responseId);
    if (c.sourceResponseId) buckets.responseIds.add(c.sourceResponseId);
    const t = c.rawResponseExcerpt || c.text || c.rawText || "";
    if (t) {
      buckets.hashes.add(sha256(String(t).replace(/\s+/g, " ").trim().toLowerCase()));
    }
    if (c.responseHash) buckets.hashes.add(c.responseHash);
    if (c.textHash) buckets.hashes.add(c.textHash);
  }
}

function walkJsonCases(dir, buckets) {
  if (!fs.existsSync(dir)) return;
  for (const f of fs.readdirSync(dir)) {
    const fp = path.join(dir, f);
    const st = fs.statSync(fp);
    if (st.isDirectory()) walkJsonCases(fp, buckets);
    else if (f.endsWith(".json") && !/presence-validation-candidates/i.test(fp)) {
      try {
        const d = JSON.parse(fs.readFileSync(fp, "utf8"));
        ingestPrior(d.cases || d.pairs || d.candidates || d.holdoutCases || [], buckets);
      } catch {
        // skip
      }
    }
  }
}

const cand = loadJson(
  path.join(
    ROOT,
    "data/ai-visibility/validation/presence-validation-candidates/candidates/candidates.json"
  )
);
const reviews = loadJson(
  path.join(
    ROOT,
    "data/ai-visibility/validation/presence-validation-candidates/reviews/reviews.json"
  )
);
const holdoutDesign = loadJson(
  path.join(ROOT, "data/ai-visibility/validation/ai-intelligence-presence-holdout-v2.json")
);

const cases = cand?.cases || [];
const R = reviews?.reviews || {};

const finalized = cases
  .map((c) => {
    const r = R[c.caseId];
    if (!r) return null;
    const humanLabel =
      r.action === "PRESENT"
        ? "PRESENT"
        : r.action === "NOT_PRESENT"
          ? "NOT_PRESENT"
          : r.action === "INVALID"
            ? "INVALID"
            : r.action === "DEFER"
              ? "DEFER"
              : r.action;
    return {
      ...c,
      humanLabel,
      humanFinalDecision: r.humanFinalDecision || r.action,
      humanAction: r.humanAction || null,
      reviewNotes: r.notes || null,
    };
  })
  .filter(Boolean);

const present = finalized.filter((c) => c.humanLabel === "PRESENT");
const notPresent = finalized.filter((c) => c.humanLabel === "NOT_PRESENT");
const invalid = finalized.filter((c) => c.humanLabel === "INVALID");
const deferred = finalized.filter((c) => c.humanLabel === "DEFER");
const eligible = finalized.filter(
  (c) => c.humanLabel === "PRESENT" || c.humanLabel === "NOT_PRESENT"
);

const missingSourceId = finalized.filter((c) => !(c.sourceResponseId || c.responseId));
const counts = countBySourceResponse(finalized);
let maxPairs = 0;
for (const v of counts.values()) maxPairs = Math.max(maxPairs, v);
const overCap = [...counts.entries()].filter(([, n]) => n > CANDIDATE_CAP_PER_RESPONSE_HOLDOUT);
const partCheck = enforceResponseLevelPartitioning(finalized, { repair: false });

// Leakage against prior sets ONLY (not current pool)
const prior = { hashes: new Set(), responseIds: new Set(), caseIds: new Set() };
for (const rel of [
  "fixtures/ai-visibility/ai-intelligence-golden-set-v1.json",
  "fixtures/ai-visibility/ai-intelligence-golden-set-v2.json",
]) {
  const d = loadJson(path.join(ROOT, rel));
  if (d) ingestPrior(d.cases, prior);
}
for (const rel of [
  "data/ai-visibility/validation/ai-intelligence-presence-holdout-v1.json",
  "fixtures/ai-visibility/ai-intelligence-presence-holdout-v1.json",
]) {
  const d = loadJson(path.join(ROOT, rel));
  if (d) ingestPrior(d.cases || d.pairs || d.holdoutCases, prior);
}
walkJsonCases(path.join(ROOT, "data/ai-visibility/validation/classifier-lab"), prior);
walkJsonCases(path.join(ROOT, "data/ai-visibility/validation/human-review"), prior);
// Holdout v2 design file has empty caseIds — still ingest if any
const hv2 = holdoutDesign;
if (hv2) ingestPrior(hv2.cases || hv2.selectedCases || [], prior);

const leakageCases = [];
const dupCaseIds = [];
const seenCase = new Set();
const dupRespEntity = [];
const seenRE = new Set();
const withinPoolDupHashes = [];
const seenHash = new Set();

for (const c of finalized) {
  if (seenCase.has(c.caseId)) dupCaseIds.push(c.caseId);
  seenCase.add(c.caseId);

  if (prior.caseIds.has(c.caseId)) {
    leakageCases.push({ caseId: c.caseId, reason: "CASE_ID_IN_PRIOR_SET" });
  }
  const rid = c.sourceResponseId || c.responseId;
  if (rid && prior.responseIds.has(rid)) {
    leakageCases.push({ caseId: c.caseId, reason: "RESPONSE_ID_IN_PRIOR_SET", rid });
  }
  const h =
    c.responseHash ||
    c.textHash ||
    sha256(String(c.rawText || "").replace(/\s+/g, " ").trim().toLowerCase());
  if (h && prior.hashes.has(h)) {
    leakageCases.push({ caseId: c.caseId, reason: "TEXT_HASH_IN_PRIOR_SET" });
  }

  const reKey = [rid, c.canonicalEntityId || c.canonicalEntityName, c.humanLabel].join("|");
  if (seenRE.has(reKey)) dupRespEntity.push(reKey);
  seenRE.add(reKey);

  // Same response text hash across multiple candidate pairs is expected (cap=2)
  if (h) {
    if (seenHash.has(h) && !withinPoolDupHashes.includes(h)) withinPoolDupHashes.push(h);
    seenHash.add(h);
  }
}

const missingFinal = finalized.filter((c) => !c.humanFinalDecision && !c.humanLabel);
const missingEntityId = finalized.filter((c) => !c.canonicalEntityId);
const missingSource = finalized.filter((c) => !(c.sourceResponseId || c.responseId));
const malformedText = finalized.filter((c) => !(c.rawText && String(c.rawText).trim()));
const ambiguityNotes = finalized.filter((c) =>
  /identity|ambiguous|collision|unclear|invalid subject/i.test(
    String(c.reviewNotes || "") + String(c.systemSuggestionRationale || "")
  )
);

const negTypes = {
  parent_or_sibling_hilton_context: 0,
  marriott_family_context: 0,
  generic_collection_language: 0,
  geographic_or_playa_negative: 0,
  hard_negative_pool_absent: 0,
  other_false_rationale: 0,
};
const negEntities = {};
for (const c of notPresent) {
  const rat = String(c.systemSuggestionRationale || "").toLowerCase();
  const name = c.canonicalEntityName || "unknown";
  negEntities[name] = (negEntities[name] || 0) + 1;
  if (/hilton context without|sibling\/parent hilton/.test(rat)) {
    negTypes.parent_or_sibling_hilton_context += 1;
  } else if (/marriott family|marriott context without/.test(rat)) {
    negTypes.marriott_family_context += 1;
  } else if (/generic collection/.test(rat)) {
    negTypes.generic_collection_language += 1;
  } else if (/playa/.test(rat) || /playa hotels/i.test(name)) {
    negTypes.geographic_or_playa_negative += 1;
  } else if (/hard negative pool|canonical brand absent|absent from response/.test(rat)) {
    negTypes.hard_negative_pool_absent += 1;
  } else {
    negTypes.other_false_rationale += 1;
  }
}

const sim = selectHoldoutV2WithResponseGovernance(eligible, {
  TOTAL_N: 100,
  PRESENCE_TRUE_N: 75,
  PRESENCE_FALSE_N: 25,
});
const simCases = sim.selected || [];
const simPresent = sim.PRESENT_N ?? simCases.filter((c) => c.humanLabel === "PRESENT").length;
const simNot = sim.NOT_PRESENT_N ?? simCases.filter((c) => c.humanLabel === "NOT_PRESENT").length;
const simByProv = byDim(simCases, "provider");
const simByLang = byDim(simCases, "language");
const simByGeo = byDim(simCases, "geography");

const presentEnough = eligible.filter((c) => c.humanLabel === "PRESENT").length >= 75;
const notEnough = eligible.filter((c) => c.humanLabel === "NOT_PRESENT").length >= 25;
const allFour =
  ["openai", "gemini", "perplexity", "claude"].every(
    (p) => (byDim(eligible, "provider")[p]?.candidatePairN || 0) > 0
  );
const bothLang =
  (byDim(eligible, "language").en?.candidatePairN || 0) > 0 &&
  (byDim(eligible, "language").es?.candidatePairN || 0) > 0;

const NEGATIVE_CONTROL_TYPES_PRESENT = Object.entries(negTypes)
  .filter(([, n]) => n > 0)
  .map(([k]) => k);
const expectedNeg = [
  "parent_or_sibling_hilton_context",
  "marriott_family_context",
  "generic_collection_language",
  "geographic_or_playa_negative",
  "hard_negative_pool_absent",
];
const NEGATIVE_CONTROL_TYPES_MISSING = expectedNeg.filter((k) => !negTypes[k]);

const leakageFail = leakageCases.length > 0;
const govPass =
  missingSourceId.length === 0 &&
  partCheck.ok &&
  maxPairs <= CANDIDATE_CAP_PER_RESPONSE_HOLDOUT &&
  overCap.length === 0;
const labelPass =
  missingFinal.length === 0 &&
  missingEntityId.length === 0 &&
  missingSource.length === 0 &&
  malformedText.length === 0 &&
  invalid.length === 0 &&
  deferred.length === 0;

const targetFeasible =
  presentEnough &&
  notEnough &&
  allFour &&
  bothLang &&
  uniqResp(eligible) >= 50 &&
  simCases.length === 100 &&
  simPresent === 75 &&
  simNot === 25;

const report = {
  phase: "PRESENCE_VALIDATION_POOL_READINESS_AUDIT_COMPLETE",
  auditedAt: new Date().toISOString(),
  HOLDOUT_V2_SELECTION: 0,
  HOLDOUT_V2_FREEZE: 0,
  HOLDOUT_V2_SCORING: 0,
  HUMAN_LABEL_CHANGES: 0,
  PROVIDER_CALLS: 0,
  pool: {
    CANDIDATE_PAIRS: finalized.length,
    UNIQUE_RESPONSES: uniqResp(finalized),
    PRESENT: present.length,
    NOT_PRESENT: notPresent.length,
    INVALID: invalid.length,
    DEFERRED: deferred.length,
    BY_PROVIDER: byDim(finalized, "provider"),
    BY_LANGUAGE: byDim(finalized, "language"),
    BY_GEOGRAPHY: byDim(finalized, "geography"),
  },
  governance: {
    RESPONSE_LEVEL_GOVERNANCE: govPass ? "PASS" : "FAIL",
    MAX_PAIRS_PER_RESPONSE: maxPairs,
    CANDIDATE_CAP_PER_RESPONSE: CANDIDATE_CAP_PER_RESPONSE_HOLDOUT,
    missingSourceResponseId: missingSourceId.length,
    partitionAtomicityOk: partCheck.ok,
    overCapResponseCount: overCap.length,
  },
  leakage: {
    LEAKAGE: leakageFail ? "FAIL" : "PASS",
    LEAKAGE_CASES: leakageCases.length,
    LEAKAGE_SAMPLE: leakageCases.slice(0, 15),
    DUPLICATE_CASE_IDS: dupCaseIds.length,
    DUPLICATE_RESPONSE_ENTITY_PAIRS: dupRespEntity.length,
    DUPLICATE_RESPONSE_HASHES_WITHIN_POOL_EXPECTED:
      withinPoolDupHashes.length + " (same response → ≤2 pairs share hash — expected)",
    withinPoolSharedResponseHashes: withinPoolDupHashes.length,
  },
  labelIntegrity: {
    LABEL_INTEGRITY: labelPass ? "PASS" : "FAIL",
    missingFinalDecisions: missingFinal.length,
    missingCanonicalEntityIds: missingEntityId.length,
    missingSourceResponses: missingSource.length,
    malformedResponseText: malformedText.length,
    invalidCases: invalid.length,
    deferredCases: deferred.length,
    identityAmbiguityNotes: ambiguityNotes.length,
  },
  holdoutV2: {
    designStatus: holdoutDesign?.STATUS || null,
    CURRENT_TARGET: "75 / 25",
    TOTAL_N: 100,
    ELIGIBLE_PAIR_N: eligible.length,
    ELIGIBLE_UNIQUE_RESPONSE_N: uniqResp(eligible),
    ELIGIBLE_PRESENT: present.length,
    ELIGIBLE_NOT_PRESENT: notPresent.length,
    TARGET_75_25_FEASIBLE: targetFeasible ? "YES" : "NO",
    TARGET_RECOMMENDATION: targetFeasible ? "KEEP" : "REVIEW",
    dryRunSelection: {
      selectedPairN: simCases.length,
      uniqueResponseN: sim.UNIQUE_RESPONSE_N || uniqResp(simCases),
      presentN: simPresent,
      notPresentN: simNot,
      byProvider: simByProv,
      byLanguage: simByLang,
      byGeography: simByGeo,
    },
  },
  negativeControls: {
    counts: negTypes,
    byEntity: negEntities,
    NEGATIVE_CONTROL_TYPES_PRESENT,
    NEGATIVE_CONTROL_TYPES_MISSING,
    NEGATIVE_CONTROL_COVERAGE_SUFFICIENT:
      NEGATIVE_CONTROL_TYPES_PRESENT.length >= 3 && notPresent.length >= 25 ? "YES" : "NO",
  },
  proposedAllocation: {
    note: "Recommended from dry-run stratified selection — NOT frozen",
    byProvider: simByProv,
    PROJECTED_UNIQUE_RESPONSE_N: sim.UNIQUE_RESPONSE_N || uniqResp(simCases),
    designTargets: holdoutDesign?.selectionRule?.design?.PROVIDER_COUNTS_TARGET || null,
  },
  languageGeoAllocation: {
    recommendedFromPool: {
      languages: byDim(eligible, "language"),
      geographies: byDim(eligible, "geography"),
    },
    dryRunHoldout: { languages: simByLang, geographies: simByGeo },
    rationale:
      "Do not force equal EN/ES or geo balance; stratify from actual eligible pool. Design targets (~50/50 EN/ES) are aspirational — actual Spanish share in pool drives holdout share.",
  },
  finalGate: {
    PRECISION_THRESHOLD: "98%",
    RECALL_THRESHOLD: "98%",
    metricContract: PRESENCE_HOLDOUT_V2_METRIC_CONTRACT,
    forbidCompositeScore: true,
  },
  regionalization: {
    STATUS: "PLANNED_AFTER_PRESENCE_CERTIFICATION",
    PROVIDER_CALLS: 0,
    CURRENT_PRESENCE_CERTIFICATION_EFFECT: 0,
  },
  universe: {
    TOTAL_CANDIDATES: cases.length,
    PRIMARY_REVIEWED: finalized.length,
    UNREVIEWED: cases.length - finalized.length,
    NON_PRIMARY_RESERVE: cases.filter((c) => !c.primaryReviewQueue).length,
    DO_REMAINING_BLOCK_HOLDOUT: false,
  },
};

const pass =
  !leakageFail &&
  govPass &&
  labelPass &&
  targetFeasible &&
  report.negativeControls.NEGATIVE_CONTROL_COVERAGE_SUFFICIENT === "YES";

report.status = pass
  ? "PRESENCE_VALIDATION_POOL_READINESS_PASS"
  : "PRESENCE_VALIDATION_POOL_READINESS_REVIEW_REQUIRED";
report.NEXT_STEP = pass
  ? "READY_FOR_PRESENCE_HOLDOUT_V2_SELECTION_AND_FREEZE"
  : "PRESENCE_VALIDATION_POOL_REMEDIATION_REQUIRED";

const out = path.join(
  ROOT,
  "data/ai-visibility/validation/presence-validation-pool-readiness-audit.json"
);
fs.writeFileSync(out, JSON.stringify(report, null, 2) + "\n");
console.log(JSON.stringify(report, null, 2));
console.log(`\nWrote ${out}`);
