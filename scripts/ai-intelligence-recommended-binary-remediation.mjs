#!/usr/bin/env node
/**
 * Recommended binary classifier remediation (definition-lock aligned).
 *
 *   node scripts/ai-intelligence-recommended-binary-remediation.mjs
 *
 * No GT writes, no Presence changes, no provider calls, no holdout generation.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { loadGoldenSet } from "../lib/ai-visibility/validation/golden-set.js";
import { hydrateGoldenSetCasesForScoring } from "../lib/ai-visibility/validation/hydrate-golden-set-texts.js";
import { extractMentions } from "../lib/ai-visibility/extract-mentions.js";
import { buildGoldenSetScoringEntityIndex } from "../lib/ai-visibility/validation/golden-set-entity-index.js";
import { classifyMentionRoleV4_1 } from "../lib/ai-visibility/recommendation-classifier-v4_1.js";
import { buildTypedSections } from "../lib/ai-visibility/recommendation-evidence-v4_1.js";
import { POSITIVE_RECOMMENDATION_ROLES } from "../lib/ai-visibility/metrics.js";
import {
  proposeLockedRecommendedBinary,
  RECOMMENDED_DEFINITION_LOCK_VERSION,
} from "../lib/ai-visibility/signal-architecture/recommended-signal-definition.js";
import {
  classifyRecommendedBinary,
  RECOMMENDED_BINARY_CLASSIFIER_VERSION,
  RECOMMENDED_BINARY_RULE_VERSION,
  RECOMMENDED_REGRESSION_SUITE_VERSION,
} from "../lib/ai-visibility/recommended-binary-classifier-v1.js";
import { runRecommendedBinaryRegressionSuite } from "../lib/ai-visibility/validation/recommended-binary-regression-suite.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT = path.join(
  ROOT,
  "data/ai-visibility/validation/recommended-binary-remediation-report.json"
);
const OUT_MD = path.join(
  ROOT,
  "data/ai-visibility/validation/recommended-binary-remediation-report.md"
);
const QUEUE_PATH = path.join(
  ROOT,
  "data/ai-visibility/validation/recommended-definition-review-queue.json"
);

const POSITIVE = new Set(POSITIVE_RECOMMENDATION_ROLES);
const ROLE_RANK = [
  "negative_or_qualified",
  "first_recommendation",
  "ranked_recommendation",
  "explicit_recommendation",
  "associated_option",
  "comparator",
  "discussed",
  "passing_mention",
  "source_only",
  "no_mention",
];

const TAXONOMY = [
  "shortlist_wording",
  "scope_inheritance",
  "table_list_inheritance",
  "multi_entity_structure",
  "implicit_recommendation",
  "qualified_recommendation",
  "comparator",
  "descriptive_mention",
  "negative_exclusion",
  "prompt_intent_error",
  "other",
];

function windowAround(text, start, end, pad = 200) {
  const s = Math.max(0, Number(start) - pad);
  const e = Math.min(String(text || "").length, Number(end) + pad);
  return String(text || "").slice(s, e).replace(/\s+/g, " ").trim();
}

function prf(tp, fp, fn, tn) {
  const precision = tp + fp ? tp / (tp + fp) : null;
  const recall = tp + fn ? tp / (tp + fn) : null;
  const f1 =
    precision != null && recall != null && precision + recall
      ? (2 * precision * recall) / (precision + recall)
      : null;
  const specificity = tn + fp ? tn / (tn + fp) : null;
  return { precision, recall, f1, specificity };
}

function pct(x) {
  return x == null ? null : `${(x * 100).toFixed(2)}%`;
}

function categorize(errType, reason, snippet) {
  const s = String(snippet || "");
  const r = String(reason || "");
  if (errType === "FP") {
    if (/comparator/i.test(r) || /\bversus|competes/i.test(s)) return "comparator";
    if (/descriptive/i.test(r)) return "descriptive_mention";
    if (/negative/i.test(r)) return "negative_exclusion";
    return "other";
  }
  // FN
  if (/shortlist|consideration|associated/i.test(r) || /\bshortlist|brands?\s+to\s+consider/i.test(s)) {
    return "shortlist_wording";
  }
  if (/inherited|heading|section/i.test(r)) return "scope_inheritance";
  if (/table|list_inheritance|bullet/i.test(r) || /^\s*[-*•]|\|/m.test(s)) {
    return "table_list_inheritance";
  }
  if (/coordinated|multi_entity/i.test(r) || /,\s+and\s+/i.test(s)) {
    return "multi_entity_structure";
  }
  if (/qualified/i.test(r)) return "qualified_recommendation";
  if (/implicit|affirmative_cue/i.test(r)) return "implicit_recommendation";
  if (/comparator/i.test(r)) return "comparator";
  if (/descriptive/i.test(r)) return "descriptive_mention";
  if (/negative/i.test(r)) return "negative_exclusion";
  if (/prompt|decision_prompt/i.test(r)) return "prompt_intent_error";
  return "other";
}

function scorePairs(pairs) {
  let tp = 0;
  let tn = 0;
  let fp = 0;
  let fn = 0;
  for (const { human, pred } of pairs) {
    if (human && pred) tp += 1;
    else if (!human && pred) fp += 1;
    else if (!human && !pred) tn += 1;
    else fn += 1;
  }
  return { N: pairs.length, TP: tp, TN: tn, FP: fp, FN: fn, ...prf(tp, fp, fn, tn) };
}

async function main() {
  const queue = fs.existsSync(QUEUE_PATH)
    ? JSON.parse(fs.readFileSync(QUEUE_PATH, "utf8"))
    : { CASES: [] };
  const ambiguousIds = new Set((queue.CASES || []).map((c) => c.caseId));

  const golden = loadGoldenSet();
  const index = buildGoldenSetScoringEntityIndex({});
  const { cases } = await hydrateGoldenSetCasesForScoring(
    (golden.cases || []).filter(
      (c) =>
        c.holdoutSplit !== "holdout" &&
        c.expectedRecommendationRole &&
        c.excludeFromClassificationDenominator !== true
    ),
    {}
  );

  const rows = [];
  for (const c of cases) {
    const text = c.text || "";
    const typedSections = buildTypedSections(text);
    const promptFamily = c.promptFamily || c.promptIntentTerritory || null;
    const mentions = extractMentions({
      responseId: "rec_bin",
      text,
      entityIndex: index.aliasIndex,
    });
    const hits = mentions.filter((m) => m.canonicalEntityName === c.entityName);
    const spans =
      hits.length > 0
        ? hits
        : [
            {
              mentionPosition: Math.max(0, text.toLowerCase().indexOf(String(c.entityName || "").toLowerCase())),
              rawMention: c.entityName,
              role: "discussed",
            },
          ];

    let v41Role = "no_mention";
    let v41Rec = false;
    let binaryRec = false;
    let binaryReason = "no_span";
    let start = Number(spans[0].mentionPosition ?? 0);
    let end = start + String(spans[0].rawMention || c.entityName || "").length;
    let rawMention = spans[0].rawMention || c.entityName;

    for (const hit of spans) {
      const s = Number(hit.mentionPosition ?? 0);
      const e = s + String(hit.rawMention || c.entityName || "").length;
      const raw = hit.rawMention || c.entityName;
      const role = classifyMentionRoleV4_1({
        text,
        start: s,
        end: e,
        rawMention: raw,
        canonicalEntityName: c.entityName,
        mentionPosition: s,
        typedSections,
      }).role;
      if (
        v41Role === "no_mention" ||
        ROLE_RANK.indexOf(role) < ROLE_RANK.indexOf(v41Role)
      ) {
        v41Role = role;
      }
      if (POSITIVE.has(role)) v41Rec = true;

      const binary = classifyRecommendedBinary({
        text,
        start: s,
        end: e,
        rawMention: raw,
        canonicalEntityName: c.entityName,
        promptFamily,
        typedSections,
        entityPresent: c.expectedRecommendationRole !== "no_mention",
      });
      if (binary.value === true) {
        binaryRec = true;
        binaryReason = binary.reason;
        start = s;
        end = e;
        rawMention = raw;
      } else if (!binaryRec) {
        binaryReason = binary.reason;
        start = s;
        end = e;
        rawMention = raw;
      }
    }

    const snippet = windowAround(text, start, end);
    const proposal = proposeLockedRecommendedBinary({
      humanRole: c.expectedRecommendationRole,
      promptFamily,
      snippet,
      text,
      entityPresent: c.expectedRecommendationRole !== "no_mention",
    });

    rows.push({
      caseId: c.caseId,
      entity: c.entityName,
      promptFamily,
      humanRole: c.expectedRecommendationRole,
      proposed: proposal.proposed,
      ambiguous:
        proposal.ambiguous ||
        proposal.proposed == null ||
        ambiguousIds.has(c.caseId),
      v41Role,
      v41Rec,
      binaryRec,
      binaryReason,
      snippet,
      rawMention,
    });
  }

  const unambiguous = rows.filter(
    (r) => !r.ambiguous && (r.proposed === true || r.proposed === false)
  );
  const ambiguousRows = rows.filter((r) => r.ambiguous || ambiguousIds.has(r.caseId));

  const before = scorePairs(
    unambiguous.map((r) => ({ human: r.proposed === true, pred: r.v41Rec }))
  );
  const after = scorePairs(
    unambiguous.map((r) => ({ human: r.proposed === true, pred: r.binaryRec }))
  );

  const taxonomyCounts = Object.fromEntries(TAXONOMY.map((k) => [k, 0]));
  const remaining = [];
  for (const r of unambiguous) {
    if (r.proposed === r.binaryRec) continue;
    const errType = r.proposed && !r.binaryRec ? "FN" : "FP";
    const cat = categorize(errType, r.binaryReason, r.snippet);
    taxonomyCounts[cat] += 1;
    remaining.push({
      caseId: r.caseId,
      entity: r.entity,
      errorType: errType,
      category: cat,
      reason: r.binaryReason,
      proposed: r.proposed,
      pred: r.binaryRec,
    });
  }

  const ambObs = {
    N: ambiguousRows.length,
    CLASSIFIER_TRUE: ambiguousRows.filter((r) => r.binaryRec).length,
    CLASSIFIER_FALSE: ambiguousRows.filter((r) => !r.binaryRec).length,
    USED_FOR_GATE: "NO",
    samples: ambiguousRows.slice(0, 12).map((r) => ({
      caseId: r.caseId,
      entity: r.entity,
      classifier: r.binaryRec,
      reason: r.binaryReason,
    })),
  };

  const regression = runRecommendedBinaryRegressionSuite();

  const reserve = {
    AVAILABLE: "NO",
    RECOMMENDED_RESERVE_AVAILABLE: "NO",
    N: null,
    PRECISION: null,
    RECALL: null,
    F1: null,
    note: "No eligible Recommended reserve found; do not manufacture from holdout.",
  };

  const precisionGate = (after.precision ?? 0) >= 0.98;
  const recallGate = (after.recall ?? 0) >= 0.98;
  const devGate = precisionGate && recallGate ? "PASS" : "FAIL";
  const nextStep =
    devGate === "PASS"
      ? "READY_FOR_FRESH_RECOMMENDED_VALIDATION_POOL"
      : "RECOMMENDED_BINARY_CLASSIFIER_REMEDIATION_CONTINUE";
  const finalStatus =
    devGate === "PASS"
      ? "RECOMMENDED_BINARY_CLASSIFIER_REMEDIATION_PASS"
      : "RECOMMENDED_BINARY_CLASSIFIER_REMEDIATION_REVIEW_REQUIRED";

  const report = {
    phase: "RECOMMENDED_BINARY_CLASSIFIER_REMEDIATION_COMPLETE",
    auditedAt: new Date().toISOString(),
    classifier: {
      VERSION: RECOMMENDED_BINARY_CLASSIFIER_VERSION,
      RULE_VERSION: RECOMMENDED_BINARY_RULE_VERSION,
      definitionVersion: RECOMMENDED_DEFINITION_LOCK_VERSION,
      regressionSuiteVersion: RECOMMENDED_REGRESSION_SUITE_VERSION,
    },
    hardGuards: {
      PRESENCE_CHANGES: 0,
      PRESENCE_RESCORE: 0,
      GROUND_TRUTH_WRITES: 0,
      HUMAN_LABEL_WRITES: 0,
      AMBIGUOUS_CASES_FORCED: 0,
      NEW_RECOMMENDED_HOLDOUT: 0,
      PROVIDER_CALLS: 0,
      RECOMMENDATION_SHARE_ENABLE: 0,
      FIRST_RECOMMENDATION_WORK: 0,
      AIRTABLE_WRITES: 0,
      DEPLOYS: 0,
    },
    DEV_BEFORE: {
      N: before.N,
      TP: before.TP,
      TN: before.TN,
      FP: before.FP,
      FN: before.FN,
      PRECISION: before.precision,
      RECALL: before.recall,
      F1: before.f1,
      PRECISION_PCT: pct(before.precision),
      RECALL_PCT: pct(before.recall),
      F1_PCT: pct(before.f1),
      note: "v4.1 positive-role mapping vs locked proposed labels",
    },
    DEV_AFTER: {
      N: after.N,
      TP: after.TP,
      TN: after.TN,
      FP: after.FP,
      FN: after.FN,
      PRECISION: after.precision,
      RECALL: after.recall,
      F1: after.f1,
      SPECIFICITY: after.specificity,
      PRECISION_PCT: pct(after.precision),
      RECALL_PCT: pct(after.recall),
      F1_PCT: pct(after.f1),
      SPECIFICITY_PCT: pct(after.specificity),
      note: "recommended_binary_v1 vs locked proposed labels; not certified",
    },
    remainingErrors: {
      counts: taxonomyCounts,
      sample: remaining.slice(0, 25),
      total: remaining.length,
    },
    regression,
    ambiguousQueue: ambObs,
    reserve,
    productionGate: {
      PRECISION_THRESHOLD: "98%",
      RECALL_THRESHOLD: "98%",
      DEV_GATE: devGate,
    },
    presence: {
      STATUS: "PRODUCTION_VALIDATED",
      CHANGED: "NO",
    },
    recommended: {
      STATUS: "NOT_PRODUCTION_CERTIFIED",
      INTERNAL_VALIDATION_ONLY: true,
      RECOMMENDATION_SHARE: "BLOCKED",
    },
    nextStep,
    finalStatus,
  };

  fs.mkdirSync(path.dirname(OUT), { recursive: true });
  fs.writeFileSync(OUT, JSON.stringify(report, null, 2) + "\n", "utf8");

  const md = [
    `# Recommended Binary Classifier Remediation`,
    ``,
    `**Classifier:** ${RECOMMENDED_BINARY_CLASSIFIER_VERSION}`,
    `**Rules:** ${RECOMMENDED_BINARY_RULE_VERSION}`,
    `**Definition:** ${RECOMMENDED_DEFINITION_LOCK_VERSION}`,
    `**DEV gate:** ${devGate}`,
    ``,
    `## DEV Before (v4.1)`,
    ``,
    `- N ${before.N} · TP ${before.TP} TN ${before.TN} FP ${before.FP} FN ${before.FN}`,
    `- P ${pct(before.precision)} · R ${pct(before.recall)} · F1 ${pct(before.f1)}`,
    ``,
    `## DEV After (binary v1)`,
    ``,
    `- N ${after.N} · TP ${after.TP} TN ${after.TN} FP ${after.FP} FN ${after.FN}`,
    `- P ${pct(after.precision)} · R ${pct(after.recall)} · F1 ${pct(after.f1)} · Spec ${pct(after.specificity)}`,
    ``,
    `## Remaining errors`,
    ``,
    ...TAXONOMY.map((k) => `- **${k}**: ${taxonomyCounts[k]}`),
    ``,
    `## Regression`,
    ``,
    `- Positive ${regression.POSITIVE_CASES} / Negative ${regression.NEGATIVE_CASES} → **${regression.status}**`,
    ``,
    `## Next`,
    ``,
    `**${nextStep}**`,
    ``,
    `Status: **${finalStatus}**`,
    ``,
  ].join("\n");
  fs.writeFileSync(OUT_MD, md, "utf8");

  console.log("RECOMMENDED_BINARY_CLASSIFIER_REMEDIATION_COMPLETE");
  console.log(`VERSION: ${RECOMMENDED_BINARY_CLASSIFIER_VERSION}`);
  console.log(
    `BEFORE: N=${before.N} TP=${before.TP} TN=${before.TN} FP=${before.FP} FN=${before.FN} P=${pct(before.precision)} R=${pct(before.recall)}`
  );
  console.log(
    `AFTER:  N=${after.N} TP=${after.TP} TN=${after.TN} FP=${after.FP} FN=${after.FN} P=${pct(after.precision)} R=${pct(after.recall)} Spec=${pct(after.specificity)}`
  );
  console.log(`REGRESSION: ${regression.status}`);
  console.log(`DEV_GATE: ${devGate}`);
  console.log(`NEXT: ${nextStep}`);
  console.log(`STATUS: ${finalStatus}`);
  console.log(`wrote ${path.relative(ROOT, OUT)}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
