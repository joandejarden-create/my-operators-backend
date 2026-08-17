#!/usr/bin/env node
/**
 * Recommended signal definition lock + DEV semantic relabeling study.
 *
 *   node scripts/ai-intelligence-recommended-signal-definition-study.mjs
 *
 * Study only — no classifier changes, no GT writes, no provider calls.
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
import { deriveCandidateEFromInternalRole } from "../lib/ai-visibility/production-taxonomy/simplification-candidates.js";
import { POSITIVE_RECOMMENDATION_ROLES } from "../lib/ai-visibility/metrics.js";
import {
  RECOMMENDED_DEFINITION_LOCK,
  RECOMMENDED_DEFINITION_LOCK_VERSION,
  RECOMMENDED_PRODUCT_QUESTION,
  proposeLockedRecommendedBinary,
  classifyAssociatedOptionPopulation,
  classifyDiscussedPopulation,
} from "../lib/ai-visibility/signal-architecture/recommended-signal-definition.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT_JSON = path.join(
  ROOT,
  "data/ai-visibility/validation/recommended-signal-definition-study.json"
);
const OUT_MD = path.join(
  ROOT,
  "data/ai-visibility/validation/recommended-signal-definition-study.md"
);
const OUT_QUEUE = path.join(
  ROOT,
  "data/ai-visibility/validation/recommended-definition-review-queue.json"
);
const OUT_LOCK = path.join(
  ROOT,
  "data/ai-visibility/validation/recommended-signal-definition-lock.json"
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

function windowAround(text, start, end, pad = 220) {
  const s = Math.max(0, Number(start) - pad);
  const e = Math.min(String(text || "").length, Number(end) + pad);
  return String(text || "").slice(s, e).replace(/\s+/g, " ").trim();
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

function pct(x) {
  return x == null ? null : `${(x * 100).toFixed(2)}%`;
}

function categorizeLockedError({ humanProposed, predRec, predRole, snippet }) {
  const s = String(snippet || "");
  if (!humanProposed && predRec) {
    if (predRole === "explicit_recommendation" || predRole === "ranked_recommendation") {
      if (/\b(shortlist|consider|options?\s+include)\b/i.test(s)) return "shortlist_wording";
      return "descriptive_mention_mistaken_for_recommendation";
    }
    return "other";
  }
  if (humanProposed && !predRec) {
    if (predRole === "associated_option") return "shortlist_wording";
    if (predRole === "comparator") return "comparator_mistaken_for_recommendation";
    if (predRole === "negative_or_qualified") return "negative_recommendation";
    if (/\b(could\s+be|if\s+|although|however)\b/i.test(s)) return "qualified_recommendation";
    if (/(^\s*[-*•]|\b\d+[\).]|\|)/m.test(s)) return "recommendation_in_table_list";
    if (/,\s+and\s+|,\s+[^,]+,\s+and\s+/i.test(s)) return "multi_entity_sentence_ambiguity";
    if (/\b(recommend|consider|shortlist|opci[oó]n)\b/i.test(s)) return "implicit_recommendation";
    if (predRole === "discussed" || predRole === "passing_mention") {
      return "recommendation_scope_mismatch";
    }
    return "other";
  }
  return "other";
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

  const rows = [];
  for (const c of cases) {
    const text = c.text || "";
    const mentions = extractMentions({
      responseId: "def_study",
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
    const snippet = windowAround(text, start, end);
    const humanRole = c.expectedRecommendationRole;
    const oldBinary = deriveCandidateEFromInternalRole(humanRole).RECOMMENDED === true;
    const predRec = POSITIVE.has(predRole);
    const proposal = proposeLockedRecommendedBinary({
      humanRole,
      promptFamily: c.promptFamily || c.promptIntentTerritory,
      snippet,
      text,
      entityPresent: humanRole !== "no_mention",
    });

    rows.push({
      caseId: c.caseId,
      entity: c.entityName,
      promptFamily: c.promptFamily || c.promptIntentTerritory || null,
      humanRole,
      oldBinaryRecommended: oldBinary,
      predRole,
      predRecommended: predRec,
      proposed: proposal.proposed,
      ambiguous: proposal.ambiguous || proposal.proposed == null,
      reason: proposal.reason,
      associatedSplit: proposal.associatedSplit || null,
      discussedClass: proposal.discussedClass || null,
      snippet,
      prompt: c.promptFamily || c.promptIntentTerritory || null,
    });
  }

  // --- Associated option audit (human role = associated_option) ---
  const associatedRows = rows.filter((r) => r.humanRole === "associated_option");
  let assocAffirm = 0;
  let assocContextual = 0;
  let assocAmb = 0;
  for (const r of associatedRows) {
    const split =
      r.associatedSplit ||
      classifyAssociatedOptionPopulation({
        snippet: r.snippet,
        promptFamily: r.promptFamily,
      });
    if (split === "AFFIRMATIVE_CONSIDERATION_OPTION") assocAffirm += 1;
    else if (split === "CONTEXTUAL_ASSOCIATED_ENTITY") assocContextual += 1;
    else assocAmb += 1;
  }

  // --- Discussed FN audit (old binary TRUE, pred discussed) ---
  const discussedFn = rows.filter(
    (r) => r.oldBinaryRecommended && r.predRole === "discussed"
  );
  let discTrue = 0;
  let discFalse = 0;
  let discAmb = 0;
  for (const r of discussedFn) {
    const cls =
      r.discussedClass ||
      classifyDiscussedPopulation({
        snippet: r.snippet,
        promptFamily: r.promptFamily,
        humanRole: r.humanRole,
      });
    if (cls === "AFFIRMATIVE_DECISION_SET") discTrue += 1;
    else if (cls === "DESCRIPTIVE_DISCUSSION") discFalse += 1;
    else discAmb += 1;
  }

  // --- DEV semantic study ---
  const unambiguous = rows.filter((r) => r.proposed === true || r.proposed === false);
  const ambiguousRows = rows.filter((r) => r.ambiguous || r.proposed == null);
  const proposedTrue = rows.filter((r) => r.proposed === true).length;
  const proposedFalse = rows.filter((r) => r.proposed === false).length;

  // Mapping disagreements: old binary vs proposed (unambiguous only)
  let oldVsProposedDisagree = 0;
  let oldFalseProposedTrue = 0;
  let oldTrueProposedFalse = 0;
  for (const r of unambiguous) {
    if (r.oldBinaryRecommended !== r.proposed) {
      oldVsProposedDisagree += 1;
      if (!r.oldBinaryRecommended && r.proposed) oldFalseProposedTrue += 1;
      if (r.oldBinaryRecommended && !r.proposed) oldTrueProposedFalse += 1;
    }
  }

  // --- v4.1 under locked definition (unambiguous proposed labels only) ---
  let tp = 0;
  let tn = 0;
  let fp = 0;
  let fn = 0;
  const taxonomyCounts = Object.fromEntries(TAXONOMY_KEYS.map((k) => [k, 0]));
  const lockedErrors = [];
  for (const r of unambiguous) {
    const human = r.proposed === true;
    const pred = r.predRecommended;
    if (human && pred) tp += 1;
    else if (!human && pred) fp += 1;
    else if (!human && !pred) tn += 1;
    else fn += 1;
    if (human !== pred) {
      const cat = categorizeLockedError({
        humanProposed: human,
        predRec: pred,
        predRole: r.predRole,
        snippet: r.snippet,
      });
      taxonomyCounts[cat] += 1;
      lockedErrors.push({
        caseId: r.caseId,
        entity: r.entity,
        errorType: human && !pred ? "FN" : "FP",
        category: cat,
        humanRole: r.humanRole,
        predRole: r.predRole,
        proposed: r.proposed,
      });
    }
  }
  const metrics = prf(tp, fp, fn);

  // Definition review queue
  const queue = ambiguousRows.map((r) => ({
    caseId: r.caseId,
    prompt: r.prompt,
    entity: r.entity,
    relevantResponseExcerpt: r.snippet.slice(0, 500),
    existingRole: r.humanRole,
    proposedBinaryLabel:
      r.proposed === true
        ? "RECOMMENDED_TRUE"
        : r.proposed === false
          ? "RECOMMENDED_FALSE"
          : "UNRESOLVED",
    ambiguityReason: r.reason,
  }));

  const lockReady = queue.length <= 25; // small ambiguity residual OK to proceed with human review queue alongside remediation prep
  // Stricter: if >10% ambiguous or associated_option ambiguous dominates → review required
  const ambRate = rows.length ? ambiguousRows.length / rows.length : 1;
  const lockReadyStrict = ambRate <= 0.08 && assocAmb <= 8;

  const finalStatus = lockReadyStrict
    ? "RECOMMENDED_SIGNAL_DEFINITION_LOCK_PASS"
    : "RECOMMENDED_SIGNAL_DEFINITION_REVIEW_REQUIRED";
  const nextStep = lockReadyStrict
    ? "READY_FOR_RECOMMENDED_BINARY_CLASSIFIER_REMEDIATION"
    : "RECOMMENDED_DEFINITION_HUMAN_REVIEW_REQUIRED";

  const lockDoc = {
    ...RECOMMENDED_DEFINITION_LOCK,
    lockedAt: new Date().toISOString(),
    STATUS: finalStatus,
    LOCK_READY: lockReadyStrict,
    associatedOptionSplitRule: {
      AFFIRMATIVE_CONSIDERATION_OPTION:
        "Brand-decision prompt and/or affirmative consideration/shortlist/option language (or list structure) placing entity in actionable set → RECOMMENDED TRUE",
      CONTEXTUAL_ASSOCIATED_ENTITY:
        "Descriptive, parent/sibling, comparator, market-context, or non-decision association without affirmative decision-set placement → RECOMMENDED FALSE",
      doNotMapEntireClassToTrue: true,
    },
  };

  const report = {
    phase: "RECOMMENDED_SIGNAL_DEFINITION_STUDY_COMPLETE",
    version: RECOMMENDED_DEFINITION_LOCK_VERSION,
    question: RECOMMENDED_PRODUCT_QUESTION,
    auditedAt: new Date().toISOString(),
    hardGuards: {
      PRESENCE_CHANGES: 0,
      CLASSIFIER_RULE_CHANGES: 0,
      GROUND_TRUTH_WRITES: 0,
      HUMAN_LABEL_WRITES: 0,
      NEW_HOLDOUT_GENERATION: 0,
      PROVIDER_CALLS: 0,
      RECOMMENDATION_SHARE_ENABLE: 0,
      FIRST_RECOMMENDATION_WORK: 0,
      NEGATIVE_PRODUCTION_WORK: 0,
      COMPARATOR_PRODUCTION_WORK: 0,
      AIRTABLE_WRITES: 0,
      DEPLOYS: 0,
    },
    lockedDefinition: {
      QUESTION: RECOMMENDED_PRODUCT_QUESTION,
      PRESENCE_REQUIRED: "YES",
      SHORTLIST_COUNTS_AS_RECOMMENDED: "YES",
      OPTION_TO_CONSIDER_COUNTS: "YES",
      RECOMMENDATION_LIST_COUNTS: "YES",
      QUALIFIED_AFFIRMATIVE_COUNTS: "YES",
      DESCRIPTIVE_MENTION_COUNTS: "NO",
      COMPARATOR_ONLY_COUNTS: "NO",
      NEGATIVE_EXCLUSION_COUNTS: "NO",
    },
    associatedOptionAudit: {
      TOTAL: associatedRows.length,
      AFFIRMATIVE_CONSIDERATION_OPTION: assocAffirm,
      CONTEXTUAL_ASSOCIATED_ENTITY: assocContextual,
      AMBIGUOUS: assocAmb,
    },
    discussedAudit: {
      TOTAL: discussedFn.length,
      RECOMMENDED_TRUE: discTrue,
      RECOMMENDED_FALSE: discFalse,
      AMBIGUOUS: discAmb,
      note: "Audit of FN cases under prior binary mapping where v4.1 predicted discussed",
    },
    devSemanticStudy: {
      N: rows.length,
      PROPOSED_TRUE: proposedTrue,
      PROPOSED_FALSE: proposedFalse,
      AMBIGUOUS: ambiguousRows.length,
      unambiguousN: unambiguous.length,
      oldVsProposed: {
        disagreeN: oldVsProposedDisagree,
        oldFalse_proposedTrue: oldFalseProposedTrue,
        oldTrue_proposedFalse: oldTrueProposedFalse,
      },
      note: "Proposed labels are study-only; human finals unchanged.",
    },
    currentV41UnderLockedDefinition: {
      N: unambiguous.length,
      excludedAmbiguous: ambiguousRows.length,
      TP: tp,
      TN: tn,
      FP: fp,
      FN: fn,
      PRECISION: metrics.precision,
      RECALL: metrics.recall,
      F1: metrics.f1,
      PRECISION_PCT: pct(metrics.precision),
      RECALL_PCT: pct(metrics.recall),
      F1_PCT: pct(metrics.f1),
      certified: false,
      note: "Semantic study only — not certification.",
    },
    errorTaxonomyUnderLockedDefinition: taxonomyCounts,
    definitionReviewQueue: {
      AMBIGUOUS_N: queue.length,
      CASES: queue,
    },
    recommendedProductionContract: {
      LOCK_READY: lockReadyStrict ? "YES" : "NO",
      presenceRequired: true,
      signal:
        "AI_SIGNAL_RECOMMENDED = TRUE iff PRESENCE=TRUE AND response affirmatively places entity into actionable decision/consideration set for stated question (explicit or structurally inherited).",
      recommendationShareStillBlocked: true,
    },
    nextStep,
    finalStatus,
    lockedErrorsSample: lockedErrors.slice(0, 20),
  };

  fs.mkdirSync(path.dirname(OUT_JSON), { recursive: true });
  fs.writeFileSync(OUT_LOCK, JSON.stringify(lockDoc, null, 2) + "\n", "utf8");
  fs.writeFileSync(OUT_JSON, JSON.stringify(report, null, 2) + "\n", "utf8");
  fs.writeFileSync(
    OUT_QUEUE,
    JSON.stringify(
      {
        queueId: "RECOMMENDED_DEFINITION_REVIEW_QUEUE",
        AMBIGUOUS_N: queue.length,
        CASES: queue,
      },
      null,
      2
    ) + "\n",
    "utf8"
  );

  const md = [
    `# Recommended Signal Definition Study`,
    ``,
    `**Version:** ${RECOMMENDED_DEFINITION_LOCK_VERSION}`,
    `**Status:** ${finalStatus}`,
    `**LOCK_READY:** ${lockReadyStrict ? "YES" : "NO"}`,
    ``,
    `## Locked question`,
    ``,
    `> ${RECOMMENDED_PRODUCT_QUESTION}`,
    ``,
    `## Associated option audit`,
    ``,
    `- TOTAL: ${associatedRows.length}`,
    `- AFFIRMATIVE_CONSIDERATION_OPTION: ${assocAffirm}`,
    `- CONTEXTUAL_ASSOCIATED_ENTITY: ${assocContextual}`,
    `- AMBIGUOUS: ${assocAmb}`,
    ``,
    `## Discussed FN audit (pred=discussed, old Recommended TRUE)`,
    ``,
    `- TOTAL: ${discussedFn.length}`,
    `- RECOMMENDED_TRUE: ${discTrue}`,
    `- RECOMMENDED_FALSE: ${discFalse}`,
    `- AMBIGUOUS: ${discAmb}`,
    ``,
    `## DEV semantic study`,
    ``,
    `- N: ${rows.length}`,
    `- PROPOSED_TRUE: ${proposedTrue}`,
    `- PROPOSED_FALSE: ${proposedFalse}`,
    `- AMBIGUOUS: ${ambiguousRows.length}`,
    `- Old vs proposed disagree (unambiguous): ${oldVsProposedDisagree} (oldF→propT ${oldFalseProposedTrue}; oldT→propF ${oldTrueProposedFalse})`,
    ``,
    `## Current v4.1 under locked definition (unambiguous only)`,
    ``,
    `- N: ${unambiguous.length} (excluded ambiguous ${ambiguousRows.length})`,
    `- TP ${tp} / TN ${tn} / FP ${fp} / FN ${fn}`,
    `- Precision ${pct(metrics.precision)} · Recall ${pct(metrics.recall)} · F1 ${pct(metrics.f1)}`,
    `- **Not certified** — study only`,
    ``,
    `## Error taxonomy under locked definition`,
    ``,
    ...TAXONOMY_KEYS.map((k) => `- **${k}**: ${taxonomyCounts[k]}`),
    ``,
    `## Next step`,
    ``,
    `**${nextStep}**`,
    ``,
  ].join("\n");
  fs.writeFileSync(OUT_MD, md, "utf8");

  console.log("RECOMMENDED_SIGNAL_DEFINITION_STUDY_COMPLETE");
  console.log(`ASSOCIATED: total=${associatedRows.length} affirm=${assocAffirm} contextual=${assocContextual} amb=${assocAmb}`);
  console.log(`DISCUSSED_FN: total=${discussedFn.length} true=${discTrue} false=${discFalse} amb=${discAmb}`);
  console.log(`DEV: N=${rows.length} TRUE=${proposedTrue} FALSE=${proposedFalse} AMB=${ambiguousRows.length}`);
  console.log(`V41_LOCKED: TP=${tp} TN=${tn} FP=${fp} FN=${fn} P=${pct(metrics.precision)} R=${pct(metrics.recall)} F1=${pct(metrics.f1)}`);
  console.log(`LOCK_READY: ${lockReadyStrict ? "YES" : "NO"}`);
  console.log(`NEXT: ${nextStep}`);
  console.log(`STATUS: ${finalStatus}`);
  console.log(`wrote ${path.relative(ROOT, OUT_JSON)}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
