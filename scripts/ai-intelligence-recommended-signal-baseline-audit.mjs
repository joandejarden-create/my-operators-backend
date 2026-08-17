#!/usr/bin/env node
/**
 * Recommended signal baseline + FP/FN error taxonomy (read-only).
 *
 *   node scripts/ai-intelligence-recommended-signal-baseline-audit.mjs
 *
 * No classifier changes, no GT changes, no provider calls, no Presence changes.
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
import {
  deriveCandidateEFromInternalRole,
} from "../lib/ai-visibility/production-taxonomy/simplification-candidates.js";
import { POSITIVE_RECOMMENDATION_ROLES } from "../lib/ai-visibility/metrics.js";
import { DEV_SIGNAL_VALIDATION_SNAPSHOT } from "../lib/ai-visibility/signal-architecture/readiness.js";
import {
  RECOMMENDATION_CLASSIFIER_VERSION,
} from "../lib/ai-visibility/recommendation-classifier-v4_1.js";
import { RECOMMENDATION_EVIDENCE_VERSION } from "../lib/ai-visibility/recommendation-evidence-v4_1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT_JSON = path.join(
  ROOT,
  "data/ai-visibility/validation/recommended-signal-baseline-and-error-taxonomy.json"
);
const OUT_MD = path.join(
  ROOT,
  "data/ai-visibility/validation/recommended-signal-production-validation-start.md"
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

const TAXONOMY_KEYS = [
  "shortlist_wording",
  "implicit_recommendation",
  "recommendation_in_table_list",
  "conditional_recommendation",
  "qualified_recommendation",
  "comparator_mistaken_for_recommendation",
  "descriptive_mention_mistaken_for_recommendation",
  "negative_recommendation",
  "parent_sibling_confusion",
  "recommendation_scope_mismatch",
  "multi_entity_sentence_ambiguity",
  "other",
];

function windowAround(text, start, end, pad = 180) {
  const s = Math.max(0, Number(start) - pad);
  const e = Math.min(String(text || "").length, Number(end) + pad);
  return String(text || "").slice(s, e).replace(/\s+/g, " ").trim();
}

function categorizeDisagreement({ humanRole, predRole, humanRec, predRec, snippet }) {
  const s = String(snippet || "");
  const lower = s.toLowerCase();

  // FP: predicted Recommended, human not
  if (!humanRec && predRec) {
    if (humanRole === "associated_option") {
      return "shortlist_wording";
    }
    if (humanRole === "comparator" || /\b(versus|vs\.?|compared to|alternative to)\b/i.test(s)) {
      return "comparator_mistaken_for_recommendation";
    }
    if (humanRole === "negative_or_qualified" || /\b(not recommend|avoid|poor fit|weaker)\b/i.test(s)) {
      return "negative_recommendation";
    }
    if (
      humanRole === "discussed" ||
      humanRole === "passing_mention" ||
      /\b(operates|portfolio|presence|history|launched)\b/i.test(s)
    ) {
      return "descriptive_mention_mistaken_for_recommendation";
    }
    if (/\band\b.+\band\b/i.test(s) || /,\s*[^,]+\s*,\s*and\s+/i.test(s)) {
      return "multi_entity_sentence_ambiguity";
    }
    return "other";
  }

  // FN: human Recommended, predicted not
  if (humanRec && !predRec) {
    if (predRole === "associated_option") {
      return "shortlist_wording";
    }
    if (predRole === "comparator") {
      return "comparator_mistaken_for_recommendation";
    }
    if (predRole === "negative_or_qualified") {
      return "negative_recommendation";
    }
    if (
      /\b(parent|sibling|collection\s+of|part\s+of\s+(?:hilton|marriott|ihg|hyatt))\b/i.test(lower)
    ) {
      return "parent_sibling_confusion";
    }
    if (/\b(only\s+if|may\s+work|provided\s+that|if\s+the\s+owner|condicional|siempre\s+que)\b/i.test(s)) {
      return "conditional_recommendation";
    }
    if (/\b(with\s+caveats?|qualified|although|however|pero)\b/i.test(s)) {
      return "qualified_recommendation";
    }
    if (
      /^\s*[-*•]\s+/m.test(s) ||
      /\b\d+[\).]\s+\*?[A-Z]/i.test(s) ||
      /\|\s*[A-Za-z]/i.test(s) ||
      /\b(top\s+\d+|ranked|priority\s+\d+|lista)\b/i.test(s)
    ) {
      return "recommendation_in_table_list";
    }
    if (/\band\b.+\band\b/i.test(s) || /,\s*[^,]+\s*,\s*and\s+/i.test(s)) {
      return "multi_entity_sentence_ambiguity";
    }
    // Affirmative option language present but classifier stayed non-recommended
    if (
      /\b(recommend|recomiend|strong\s+(?:fit|candidate|option)|best\s+fit|should\s+consider|opci[oó]n|shortlist|top\s+pick)\b/i.test(
        s
      )
    ) {
      return "implicit_recommendation";
    }
    if (
      humanRole === "first_recommendation" ||
      humanRole === "ranked_recommendation" ||
      humanRole === "explicit_recommendation"
    ) {
      return "recommendation_scope_mismatch";
    }
    return "other";
  }

  return "other";
}

function prf(tp, fp, fn) {
  const precision = tp + fp ? tp / (tp + fp) : null;
  const recall = tp + fn ? tp / (tp + fn) : null;
  const f1 =
    precision != null && recall != null && precision + recall
      ? (2 * precision * recall) / (precision + recall)
      : null;
  return { precision, recall, f1 };
}

async function main() {
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

  let tp = 0;
  let fp = 0;
  let tn = 0;
  let fn = 0;
  const disagreements = [];
  const taxonomyCounts = Object.fromEntries(TAXONOMY_KEYS.map((k) => [k, 0]));

  for (const c of cases) {
    const text = c.text || "";
    const mentions = extractMentions({
      responseId: "rec_audit",
      text,
      entityIndex: index.aliasIndex,
    });
    const hits = mentions.filter((m) => m.canonicalEntityName === c.entityName);
    const best = hits
      .slice()
      .sort(
        (a, b) =>
          ROLE_RANK.indexOf(a.role) - ROLE_RANK.indexOf(b.role) ||
          a.mentionPosition - b.mentionPosition
      )[0];
    const start = best?.mentionPosition ?? 0;
    const end = start + String(best?.rawMention || c.entityName || "").length;
    const rawMention = best?.rawMention || c.entityName;

    const predRole = classifyMentionRoleV4_1({
      text,
      start,
      end,
      rawMention,
      canonicalEntityName: c.entityName,
      mentionPosition: start,
      typedSections: buildTypedSections(text),
    }).role;

    const humanRole = c.expectedRecommendationRole;
    const humanRec = deriveCandidateEFromInternalRole(humanRole).RECOMMENDED === true;
    const predRec = POSITIVE.has(predRole);

    if (humanRec && predRec) tp += 1;
    else if (!humanRec && predRec) fp += 1;
    else if (!humanRec && !predRec) tn += 1;
    else fn += 1;

    if (humanRec !== predRec) {
      const snippet = windowAround(text, start, end);
      const category = categorizeDisagreement({
        humanRole,
        predRole,
        humanRec,
        predRec,
        snippet,
      });
      taxonomyCounts[category] = (taxonomyCounts[category] || 0) + 1;
      disagreements.push({
        caseId: c.caseId,
        entity: c.entityName,
        errorType: humanRec && !predRec ? "FN" : "FP",
        humanRole,
        predRole,
        humanRecommended: humanRec,
        predRecommended: predRec,
        category,
        snippet,
      });
    }
  }

  const metrics = prf(tp, fp, fn);
  const snap = DEV_SIGNAL_VALIDATION_SNAPSHOT.signals.RECOMMENDED;
  const definitionGap = {
    productDefinitionIncludes:
      "shortlist membership; option owner should consider; ranked/unranked affirmative options",
    currentClassifierMapsConsiderationTo:
      "associated_option → AI_SIGNAL_RECOMMENDED = false",
    severity: "MATERIAL_DEFINITION_GAP",
    note:
      "Product contract for AI_SIGNAL_RECOMMENDED treats shortlist / affirmative option membership as TRUE. Current v4.1 production mapping excludes associated_option / consideration-set cues. Resolve definition before recall remediation.",
  };

  const report = {
    phase: "RECOMMENDED_PRODUCTION_VALIDATION_WORKSTREAM_STARTED",
    auditedAt: new Date().toISOString(),
    hardGuards: {
      PRESENCE_CHANGES: 0,
      PRESENCE_RESCORE: 0,
      RECOMMENDED_PRODUCTION_ENABLE: 0,
      RECOMMENDATION_SHARE_ENABLE: 0,
      FIRST_RECOMMENDATION_WORK: 0,
      REGIONALIZATION_PROVIDER_CALLS: 0,
      REGIONALIZATION_STAGE2: 0,
      GROUND_TRUTH_CHANGES: 0,
      NEW_RECOMMENDED_HOLDOUT: 0,
      PROVIDER_CALLS: 0,
      AIRTABLE_WRITES: 0,
      DEPLOYS: 0,
    },
    queryOrigin: {
      STATUS: "RESEARCH_COMPLETE_NO_PRODUCTIZATION",
      PRODUCTION_DIMENSION: "NO",
      STAGE_2_CLAUDE: "NOT_PLANNED",
    },
    presence: {
      STATUS: "PRODUCTION_VALIDATED",
      CHANGED: "NO",
    },
    recommendedBaseline: {
      CURRENT_RESOLVER_VERSION:
        DEV_SIGNAL_VALIDATION_SNAPSHOT.resolverVersion,
      CURRENT_CLASSIFIER_VERSION: RECOMMENDATION_CLASSIFIER_VERSION,
      CURRENT_EVIDENCE_VERSION: RECOMMENDATION_EVIDENCE_VERSION,
      CURRENT_RULES: {
        recommendedTrueInternalRoles: [...POSITIVE_RECOMMENDATION_ROLES],
        recommendedFalseIncludes: [
          "associated_option",
          "comparator",
          "discussed",
          "passing_mention",
          "source_only",
          "negative_or_qualified",
          "no_mention",
        ],
        presenceRequired: true,
        failClosedIfPresenceFalse: true,
        decisionTreeSummary: [
          "negative cue → negative_or_qualified (not Recommended)",
          "lead cue / rank position 1 → first_recommendation (Recommended)",
          "confirmed rank position >1 → ranked_recommendation (Recommended)",
          "directPositiveCue or sectionPositiveCue → explicit_recommendation (Recommended)",
          "considerationSetCue / consideration section membership → associated_option (NOT Recommended)",
          "comparator / source-only / incidental → non-Recommended roles",
          "default → discussed (NOT Recommended)",
        ],
      },
      sourceArtifact:
        "production-signal-taxonomy-study.json#candidateE.benchmarks.v4.1.RECOMMENDED_FLAG",
      DEV_N: cases.length,
      TP: tp,
      TN: tn,
      FP: fp,
      FN: fn,
      PRECISION: metrics.precision,
      RECALL: metrics.recall,
      F1: metrics.f1,
      PRECISION_PCT: metrics.precision != null ? `${(metrics.precision * 100).toFixed(2)}%` : null,
      RECALL_PCT: metrics.recall != null ? `${(metrics.recall * 100).toFixed(2)}%` : null,
      F1_PCT: metrics.f1 != null ? `${(metrics.f1 * 100).toFixed(2)}%` : null,
      snapshotCrossCheck: {
        DEV_N: snap.N,
        precision: snap.precision,
        recall: snap.recall,
        f1: snap.f1,
        matchesLiveAudit:
          cases.length === snap.N &&
          Math.abs((metrics.precision ?? 0) - snap.precision) < 1e-9 &&
          Math.abs((metrics.recall ?? 0) - snap.recall) < 1e-9,
      },
    },
    errorTaxonomy: {
      disagreementN: disagreements.length,
      expectedDisagreementN: fp + fn,
      counts: taxonomyCounts,
      byErrorType: {
        FP: disagreements.filter((d) => d.errorType === "FP").length,
        FN: disagreements.filter((d) => d.errorType === "FN").length,
      },
      fnPredRoleHistogram: disagreements
        .filter((d) => d.errorType === "FN")
        .reduce((acc, d) => {
          acc[d.predRole] = (acc[d.predRole] || 0) + 1;
          return acc;
        }, {}),
      fpHumanRoleHistogram: disagreements
        .filter((d) => d.errorType === "FP")
        .reduce((acc, d) => {
          acc[d.humanRole] = (acc[d.humanRole] || 0) + 1;
          return acc;
        }, {}),
      samples: {
        FP: disagreements.filter((d) => d.errorType === "FP").slice(0, 5),
        FN: disagreements.filter((d) => d.errorType === "FN").slice(0, 15),
      },
      allDisagreements: disagreements,
    },
    recommendedArchitecture: {
      SIGNAL: "AI_SIGNAL_RECOMMENDED",
      BINARY: "YES",
      PRESENCE_REQUIRED: "YES",
      OLD_10_CLASS_PRODUCTION_GATE: "NO",
      simplificationFirst:
        "Prefer observable recommendation language + structural context + canonical entity resolution; avoid recreating full human nuance beyond validated reliability.",
      definitionGap,
    },
    readiness: {
      CURRENT_STATUS: "NOT_READY",
      PRODUCTION_GATE: {
        PRECISION: ">= 98%",
        RECALL: ">= 98%",
      },
      recommendationShare: "BLOCKED until Recommended certification",
      freshHoldout: "REQUIRED after DEV/Reserve gate; NOT generated this phase",
    },
    nextStep: "RECOMMENDED_SIGNAL_DEFINITION_REVIEW_REQUIRED",
    nextStepRationale:
      "Largest FN cluster maps to associated_option / shortlist-consideration wording that current rules treat as NOT Recommended, while the product binary definition treats affirmative shortlist/option membership as Recommended TRUE. Resolve this contract before classifier remediation.",
  };

  fs.mkdirSync(path.dirname(OUT_JSON), { recursive: true });
  fs.writeFileSync(OUT_JSON, JSON.stringify(report, null, 2) + "\n", "utf8");

  const md = [
    `# Recommended Signal — Production Validation Workstream Start`,
    ``,
    `**Status:** NOT_READY`,
    `**Classifier:** ${RECOMMENDATION_CLASSIFIER_VERSION}`,
    `**Evidence:** ${RECOMMENDATION_EVIDENCE_VERSION}`,
    `**Resolver (unchanged):** ${DEV_SIGNAL_VALIDATION_SNAPSHOT.resolverVersion}`,
    ``,
    `## Baseline (Clean DEV, n=${cases.length})`,
    ``,
    `| Metric | Value |`,
    `|---|---|`,
    `| TP | ${tp} |`,
    `| TN | ${tn} |`,
    `| FP | ${fp} |`,
    `| FN | ${fn} |`,
    `| Precision | ${report.recommendedBaseline.PRECISION_PCT} |`,
    `| Recall | ${report.recommendedBaseline.RECALL_PCT} |`,
    `| F1 | ${report.recommendedBaseline.F1_PCT} |`,
    ``,
    `Gate remains Precision ≥ 98% and Recall ≥ 98%.`,
    ``,
    `## Error taxonomy (FP+FN=${fp + fn})`,
    ``,
    ...TAXONOMY_KEYS.map((k) => `- **${k}**: ${taxonomyCounts[k]}`),
    ``,
    `## Definition gap`,
    ``,
    definitionGap.note,
    ``,
    `## Next step`,
    ``,
    `**RECOMMENDED_SIGNAL_DEFINITION_REVIEW_REQUIRED**`,
    ``,
    `- Do not enable Recommendation Share`,
    `- Do not start First Recommendation work`,
    `- Do not generate Recommended holdout this phase`,
    `- Presence remains PRODUCTION_VALIDATED / unchanged`,
    ``,
  ].join("\n");
  fs.writeFileSync(OUT_MD, md, "utf8");

  console.log("RECOMMENDED_PRODUCTION_VALIDATION_WORKSTREAM_STARTED");
  console.log(`DEV_N: ${cases.length}`);
  console.log(`TP: ${tp} TN: ${tn} FP: ${fp} FN: ${fn}`);
  console.log(
    `PRECISION: ${report.recommendedBaseline.PRECISION_PCT} RECALL: ${report.recommendedBaseline.RECALL_PCT} F1: ${report.recommendedBaseline.F1_PCT}`
  );
  console.log("ERROR_TAXONOMY:");
  for (const k of TAXONOMY_KEYS) {
    console.log(`  ${k}: ${taxonomyCounts[k]}`);
  }
  console.log(`NEXT: ${report.nextStep}`);
  console.log(`wrote ${path.relative(ROOT, OUT_JSON)}`);
  console.log(`wrote ${path.relative(ROOT, OUT_MD)}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
