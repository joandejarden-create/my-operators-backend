#!/usr/bin/env node
/**
 * Audit 5 residual FIRST_UNDERPROMOTED_TO_ASSOCIATED cases under Rules 1–5.
 * Holdout blocked. No classifier changes.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { loadGoldenSet } from "../lib/ai-visibility/validation/golden-set.js";
import { hydrateGoldenSetCasesForScoring } from "../lib/ai-visibility/validation/hydrate-golden-set-texts.js";
import { extractMentions } from "../lib/ai-visibility/extract-mentions.js";
import { buildGoldenSetScoringEntityIndex } from "../lib/ai-visibility/validation/golden-set-entity-index.js";
import { extractEntityLocalEvidence } from "../lib/ai-visibility/recommendation-evidence-v4_1.js";
import { buildTypedSections } from "../lib/ai-visibility/recommendation-evidence-v4_1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const IDS = [
  "v2_cand_035ffe13",
  "v2_cand_14651e14",
  "v2_cand_2e0a4ca6",
  "v2_cand_359453c0",
  "v2_cand_3c28453a",
];

const CONSIDERATION_HEAD =
  /\b(brands?\s+to\s+consider|options?\s+to\s+consider|commonly\s+(?:considered|shortlisted)|habitualmente\s+considerad|a\s+considerar|soft\s+brands?|collection(?:s)?\s+(?:&|and)\s+soft|most\s+commonly\s+considered)\b/i;
const LEAD =
  /\b(first\s+(?:choice|call|option|recommendation)|top\s+(?:choice|recommendation)|primary\s+recommendation|#\s*1|rank\s*1|primera\s+opci|recomendaci[oó]n\s+principal|leading\s+recommendation|preferred\s+recommendation)\b/i;
const RANK_SEM =
  /\b(ranked\s+in\s+order|priority\s+order|top\s+\d+|orden\s+de\s+prioridad|first\s*\/\s*second|recommended\s+in\s+(?:the\s+)?(?:following\s+)?order|#\s*[23]|segunda\s+opci|tercera\s+opci)\b/i;
const DIRECT_POS =
  /\b(strong\s+(?:candidate|alternative|option|choice|fit)|recommended\s+option|good\s+fit|particularly\s+(?:strong|suitable)|opci[oó]n\s+s[oó]lida|alternativa\s+fuerte|buen\s+candidato)\b/i;

function decide(ctx) {
  const { inConsideration, hasLead, hasRankSem, hasDirectPositive, numberedOnly, entityLocal } =
    ctx;
  // Rule 4: lead overrides
  if (hasLead) {
    return {
      DECISION: "AMEND",
      FINAL_PROPOSED_ROLE: "first_recommendation",
      REASON: "RULE4: explicit lead evidence → first_recommendation",
      taxonomyRuleApplied: ["RULE_4_LEAD_OVERRIDE"],
    };
  }
  if (hasRankSem && /#\s*[2-9]|segunda|tercera|rank\s*[2-9]|priority\s*[2-9]/i.test(entityLocal)) {
    return {
      DECISION: "AMEND",
      FINAL_PROPOSED_ROLE: "ranked_recommendation",
      REASON: "RULE4: explicit non-first meaningful rank → ranked_recommendation",
      taxonomyRuleApplied: ["RULE_4_LEAD_OVERRIDE", "RULE_3_MEANINGFUL_ORDERING_REQUIRED"],
    };
  }
  // Rule 5: direct positive in consideration → explicit (not first)
  if (hasDirectPositive && inConsideration) {
    return {
      DECISION: "AMEND",
      FINAL_PROPOSED_ROLE: "explicit_recommendation",
      REASON: "RULE5: direct positive in consideration set → explicit_recommendation (not first)",
      taxonomyRuleApplied: ["RULE_5_DIRECT_POSITIVE_OVERRIDES_ASSOCIATED", "RULE_1_POSITIVE_NOT_FIRST"],
    };
  }
  if (hasDirectPositive && !hasLead) {
    return {
      DECISION: "AMEND",
      FINAL_PROPOSED_ROLE: "explicit_recommendation",
      REASON: "RULE1: positive language without lead/rank-1 → explicit_recommendation",
      taxonomyRuleApplied: ["RULE_1_POSITIVE_NOT_FIRST"],
    };
  }
  // Rules 2–3: consideration + numbering only → associated
  if (inConsideration || numberedOnly) {
    return {
      DECISION: "AMEND",
      FINAL_PROPOSED_ROLE: "associated_option",
      REASON:
        "RULE2+RULE3: consideration-set / numbered formatting without meaningful order or lead → associated_option",
      taxonomyRuleApplied: ["RULE_2_CONSIDERATION_NOT_RANK", "RULE_3_MEANINGFUL_ORDERING_REQUIRED"],
    };
  }
  return {
    DECISION: "DEFER",
    FINAL_PROPOSED_ROLE: null,
    REASON: "Evidence does not deterministically resolve under Rules 1–5",
    taxonomyRuleApplied: [],
  };
}

const golden = loadGoldenSet();
const index = buildGoldenSetScoringEntityIndex({});
const subset = golden.cases.filter((c) => IDS.includes(c.caseId));
const { cases } = await hydrateGoldenSetCasesForScoring(subset, {});

const out = [];
for (const c of cases) {
  if (c.holdoutSplit === "holdout") {
    out.push({ CASE_ID: c.caseId, DECISION: "BLOCKED_HOLDOUT" });
    continue;
  }
  const text = c.text || "";
  const mentions = extractMentions({
    responseId: "audit",
    text,
    entityIndex: index.aliasIndex,
  });
  const hits = mentions.filter((m) => m.canonicalEntityName === c.entityName);
  const roleRank = [
    "first_recommendation",
    "ranked_recommendation",
    "explicit_recommendation",
    "associated_option",
    "comparator",
    "discussed",
    "passing_mention",
    "negative_or_qualified",
    "source_only",
  ];
  const predicted = hits.length
    ? hits
        .slice()
        .sort(
          (a, b) =>
            roleRank.indexOf(a.role) - roleRank.indexOf(b.role) ||
            a.mentionPosition - b.mentionPosition
        )[0].role
    : null;

  const name = String(c.entityName || "");
  const idx = text.toLowerCase().indexOf(name.toLowerCase().slice(0, Math.min(24, name.length)));
  const windowStart = Math.max(0, idx - 350);
  const windowEnd = Math.min(text.length, (idx >= 0 ? idx : 0) + Math.max(name.length, 20) + 350);
  const FULL_RELEVANT_CONTEXT = text.slice(windowStart, windowEnd).replace(/\s+/g, " ").trim();

  const typed = buildTypedSections(text);
  const hit = hits[0];
  const start = hit?.mentionPosition ?? (idx >= 0 ? idx : 0);
  const end = start + (hit?.rawMention?.length || name.length);
  const ev = extractEntityLocalEvidence({
    text,
    start,
    end,
    rawMention: hit?.rawMention || name,
    canonicalEntityName: name,
    typedSections: typed,
  });

  const section = typed.find(
    (s) => start >= (s.start ?? 0) && start < (s.end ?? text.length)
  ) || typed[0];
  const entityLocal = text.slice(Math.max(0, start - 120), Math.min(text.length, end + 120));
  const behind = text.slice(Math.max(0, start - 400), start);
  const inConsideration =
    CONSIDERATION_HEAD.test(behind) ||
    CONSIDERATION_HEAD.test(section?.title || "") ||
    Boolean(ev.recommendationEvidence?.considerationSetCue);
  const hasLead = LEAD.test(entityLocal) || Boolean(ev.recommendationEvidence?.leadCue);
  const hasRankSem =
    RANK_SEM.test(behind) ||
    RANK_SEM.test(section?.title || "") ||
    Boolean(ev.confirmedRankStructure);
  const hasDirectPositive =
    DIRECT_POS.test(entityLocal) || Boolean(ev.recommendationEvidence?.directPositiveCue);
  const numberedOnly = /^\s*\d+[.)]/.test(
    text.slice(Math.max(0, start - 40), start + 5)
  ) || /#{1,3}\s*\d+[.)]/.test(section?.title || "");

  const STRUCTURAL_CONTEXT = {
    inConsideration,
    hasLead,
    hasRankSem,
    hasDirectPositive,
    numberedOnly,
    sectionType: ev.sectionType || section?.sectionType || null,
    sectionTitle: (section?.title || "").slice(0, 120),
    considerationSetCue: Boolean(ev.recommendationEvidence?.considerationSetCue),
    leadCue: Boolean(ev.recommendationEvidence?.leadCue),
    directPositiveCue: Boolean(ev.recommendationEvidence?.directPositiveCue),
    confirmedRankStructure: Boolean(ev.confirmedRankStructure),
  };

  const decision = decide({
    inConsideration,
    hasLead,
    hasRankSem,
    hasDirectPositive,
    numberedOnly,
    entityLocal,
  });

  // If human already matches proposed → KEEP
  let DECISION = decision.DECISION;
  let FINAL = decision.FINAL_PROPOSED_ROLE;
  let REASON = decision.REASON;
  if (
    DECISION === "AMEND" &&
    FINAL === c.expectedRecommendationRole &&
    predicted === FINAL
  ) {
    DECISION = "KEEP";
    REASON = "Human label already matches Rules 1–5 outcome and classifier";
  } else if (DECISION === "AMEND" && FINAL === c.expectedRecommendationRole) {
    DECISION = "KEEP";
    REASON = "Human label already matches Rules 1–5 proposed role";
  }

  out.push({
    CASE_ID: c.caseId,
    ENTITY: c.entityName,
    CURRENT_HUMAN_LABEL: c.expectedRecommendationRole,
    CURRENT_CLASSIFIER_LABEL: predicted,
    FULL_RELEVANT_CONTEXT: FULL_RELEVANT_CONTEXT.slice(0, 900),
    STRUCTURAL_CONTEXT,
    TAXONOMY_RULE_APPLICABLE: decision.taxonomyRuleApplied,
    DECISION,
    FINAL_PROPOSED_ROLE: DECISION === "KEEP" ? c.expectedRecommendationRole : FINAL,
    REASON,
    taxonomyRuleApplied: decision.taxonomyRuleApplied,
    holdoutSplit: c.holdoutSplit || "development",
  });
}

const summary = {
  TOTAL: out.length,
  KEEP: out.filter((x) => x.DECISION === "KEEP").length,
  AMEND: out.filter((x) => x.DECISION === "AMEND").length,
  DEFER: out.filter((x) => x.DECISION === "DEFER").length,
  BY_TRANSITION: {},
  cases: out,
};
for (const c of out.filter((x) => x.DECISION === "AMEND")) {
  const k = `${c.CURRENT_HUMAN_LABEL} => ${c.FINAL_PROPOSED_ROLE}`;
  summary.BY_TRANSITION[k] = (summary.BY_TRANSITION[k] || 0) + 1;
}

const outPath = path.join(
  __dirname,
  "../data/ai-visibility/validation/taxonomy-resolution-5-residual-cases.json"
);
fs.writeFileSync(outPath, JSON.stringify(summary, null, 2));
console.log(JSON.stringify({ ...summary, cases: undefined, out: outPath, sample: out }, null, 2));
