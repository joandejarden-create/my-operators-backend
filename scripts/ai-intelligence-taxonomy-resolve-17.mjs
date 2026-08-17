#!/usr/bin/env node
/**
 * Part A — adjudicate 17 HUMAN_GOVERNANCE cases under Joan Rules 1–5.
 * Holdout untouched. No apply in this script.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { loadGoldenSet } from "../lib/ai-visibility/validation/golden-set.js";
import { hydrateGoldenSetCasesForScoring } from "../lib/ai-visibility/validation/hydrate-golden-set-texts.js";
import { extractMentions } from "../lib/ai-visibility/extract-mentions.js";
import { buildGoldenSetScoringEntityIndex } from "../lib/ai-visibility/validation/golden-set-entity-index.js";
import { buildTypedSections } from "../lib/ai-visibility/recommendation-evidence-v4_1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const REPORT = path.join(
  __dirname,
  "../data/ai-visibility/validation/classifier-lab-final-report.json"
);
const OUT = path.join(
  __dirname,
  "../data/ai-visibility/validation/taxonomy-resolution-17-cases.json"
);

const flagged = JSON.parse(fs.readFileSync(REPORT, "utf8")).GROUND_TRUTH_REVIEW_NEEDED || [];
const ROLES = [
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
const roleRank = new Map(ROLES.map((r, i) => [r, i]));

const DIRECT_POSITIVE =
  /\b(strong\s+(?:candidate|alternative|option|options|choice|choices|fit)|recommended\s+option|good\s+fit|particularly\s+(?:suitable|strong)|well\s+suited|opci[oó]n\s+s[oó]lida|alternativa\s+fuerte|buen\s+candidato|particularly\s+suitable)\b/i;

const LEAD =
  /\b(first\s+(?:call|choice|option|recommendation)|top\s+(?:choice|recommendation|pick)|primary\s+recommendation|leading\s+(?:recommendation|candidate|option)|preferred\s+recommendation|primera\s+opci[oó]n|recomendaci[oó]n\s+principal|#\s*1\b|rank\s*1|1st\s+(?:choice|recommendation))\b/i;

const RANK_SEM =
  /\b(ranked\s+in\s+order|recommended\s+in\s+(?:the\s+)?(?:following\s+)?order|priority\s+order|orden\s+de\s+prioridad|top\s+\d+|first\s*\/\s*second|#\s*[23]\b|rank\s*[23]|segunda\s+opci[oó]n|tercera\s+opci[oó]n|2nd\s+choice|3rd\s+choice)\b/i;

const CONSIDERATION_HEAD =
  /\b(brands?\s+to\s+consider|options?\s+to\s+consider|brands?\s+commonly\s+considered|potential\s+options?|alternatives?\s+(?:include|to\s+consider)|marcas?\s+a\s+considerar|opciones?\s+a\s+considerar|alternativas(?:\s+incluyen)?|operators?\s+to\s+consider|##\s*brands?\s+to\s+consider|##\s*operators?\s+to\s+consider)\b/i;

function bestRole(mentions, entity) {
  const hits = mentions.filter((m) => m.canonicalEntityName === entity);
  if (!hits.length) return null;
  return hits
    .slice()
    .sort(
      (a, b) =>
        (roleRank.get(a.role) ?? 99) - (roleRank.get(b.role) ?? 99) ||
        a.mentionPosition - b.mentionPosition
    )[0].role;
}

function localContext(text, entity) {
  const idx = text.indexOf(entity.split(" ")[0]);
  const start = Math.max(0, (idx >= 0 ? idx : 0) - 200);
  const end = Math.min(text.length, (idx >= 0 ? idx : 0) + entity.length + 280);
  return text.slice(start, end);
}

function adjudicate(c, predicted, text) {
  const human = c.expectedRecommendationRole;
  const sections = buildTypedSections(text);
  const ctx = localContext(text, c.entityName);
  const headBlock = text.slice(0, 400);
  const inConsideration =
    CONSIDERATION_HEAD.test(headBlock) ||
    CONSIDERATION_HEAD.test(ctx) ||
    sections.some(
      (s) =>
        s.sectionType === "CONSIDERATION_SET_SECTION" &&
        text.indexOf(c.entityName) >= s.start &&
        text.indexOf(c.entityName) < s.end
    );
  const hasLead = LEAD.test(ctx) || LEAD.test(text.slice(0, Math.min(text.length, 500)));
  const hasRankSem = RANK_SEM.test(ctx) || RANK_SEM.test(headBlock);
  const hasDirectPositive = DIRECT_POSITIVE.test(ctx);
  const numberedOnly =
    /(?:^|\n)\s*\d+[.)]\s*.{0,80}/m.test(ctx) && !hasRankSem && !hasLead;

  const structural = {
    inConsideration,
    hasLead,
    hasRankSem,
    hasDirectPositive,
    numberedOnly,
    sectionTypesNear: sections
      .filter((s) => {
        const i = text.indexOf(c.entityName);
        return i >= s.start - 50 && i < s.end;
      })
      .map((s) => ({ type: s.sectionType, title: s.title })),
  };

  let decision = "DEFER";
  let finalRole = human;
  let reason = "";
  let taxonomyRuleApplied = [];

  // RULE 1: positive ≠ first
  if (
    human === "first_recommendation" &&
    !hasLead &&
    !hasRankSem &&
    (hasDirectPositive || predicted === "explicit_recommendation") &&
    !inConsideration
  ) {
    decision = "AMEND";
    finalRole = "explicit_recommendation";
    reason =
      "RULE1: strong/explicit positive without separate lead/rank-1 evidence → explicit_recommendation";
    taxonomyRuleApplied = ["RULE_1_POSITIVE_NOT_FIRST"];
  }

  // RULE 2+3: consideration list numbering ≠ rank
  if (
    (human === "first_recommendation" || human === "ranked_recommendation") &&
    inConsideration &&
    !hasLead &&
    !hasRankSem
  ) {
    decision = "AMEND";
    // RULE 5: direct positive in consideration → explicit
    if (hasDirectPositive) {
      finalRole = "explicit_recommendation";
      reason =
        "RULE2+RULE5: consideration-set membership with direct positive → explicit_recommendation (numbering alone is not rank)";
      taxonomyRuleApplied = ["RULE_2_CONSIDERATION_NOT_RANK", "RULE_5_DIRECT_POSITIVE_OVERRIDES_ASSOCIATED"];
    } else {
      finalRole = "associated_option";
      reason =
        "RULE2+RULE3: consideration-set list; numbering alone is not meaningful ranking → associated_option";
      taxonomyRuleApplied = ["RULE_2_CONSIDERATION_NOT_RANK", "RULE_3_MEANINGFUL_ORDERING_REQUIRED"];
    }
  }

  // RULE 4: lead in consideration → first
  if (inConsideration && hasLead && human !== "first_recommendation") {
    decision = "AMEND";
    finalRole = "first_recommendation";
    reason = "RULE4: lead semantics inside consideration set → first_recommendation";
    taxonomyRuleApplied = ["RULE_4_LEAD_OVERRIDES_CONSIDERATION"];
  }

  // If classifier already matches rule outcome and human differs — already handled as AMEND
  // If human already equals proposed under rules — KEEP
  if (decision === "AMEND" && finalRole === human) {
    decision = "KEEP";
    reason = "Human label already matches governing taxonomy";
  }

  // If we couldn't establish affirmative evidence for change and human is first with only positive
  // already covered. If ambiguous (long noisy text, unclear lead), DEFER.
  if (decision === "DEFER") {
    // Try RULE1 when predicted explicit and human first without lead — even if hasDirectPositive unclear
    if (
      human === "first_recommendation" &&
      predicted === "explicit_recommendation" &&
      !hasLead &&
      !hasRankSem &&
      !inConsideration
    ) {
      decision = "AMEND";
      finalRole = "explicit_recommendation";
      reason =
        "RULE1: first label with only explicit-positive classifier evidence and no lead/rank → amend to explicit";
      taxonomyRuleApplied = ["RULE_1_POSITIVE_NOT_FIRST"];
    } else if (
      human === "first_recommendation" &&
      predicted === "associated_option" &&
      inConsideration &&
      !hasLead &&
      !hasRankSem
    ) {
      decision = "AMEND";
      finalRole = "associated_option";
      reason = "RULE2: consideration list without ranking semantics → associated_option";
      taxonomyRuleApplied = ["RULE_2_CONSIDERATION_NOT_RANK"];
    } else if (finalRole === human && predicted === human) {
      decision = "KEEP";
      reason = "Human and classifier already agree under governing rules";
    } else {
      reason =
        "Insufficient deterministic evidence under RULES 1–5 to amend or keep confidently";
    }
  }

  // KEEP when human already equals final proposed and no conflict
  if (decision === "AMEND" && finalRole === human) {
    decision = "KEEP";
  }

  return {
    CASE_ID: c.caseId,
    ENTITY: c.entityName,
    CURRENT_HUMAN_LABEL: human,
    CURRENT_CLASSIFIER_LABEL: predicted,
    ROOT_CAUSE: flagged.find((f) => f.caseId === c.caseId)?.rootCause || null,
    FULL_RELEVANT_CONTEXT: ctx.slice(0, 600),
    STRUCTURAL_CONTEXT: structural,
    DECISION: decision,
    FINAL_PROPOSED_ROLE: finalRole,
    REASON: reason,
    taxonomyRuleApplied,
    holdoutSplit: c.holdoutSplit || null,
  };
}

const g = loadGoldenSet();
const index = buildGoldenSetScoringEntityIndex({});
const ids = new Set(flagged.map((f) => f.caseId));
const { cases } = await hydrateGoldenSetCasesForScoring(
  g.cases.filter((c) => ids.has(c.caseId)),
  {}
);

const results = [];
for (const c of cases) {
  if (c.holdoutSplit === "holdout") {
    results.push({
      CASE_ID: c.caseId,
      DECISION: "DEFER",
      REASON: "HOLDOUT_BLOCKED",
      holdoutSplit: "holdout",
    });
    continue;
  }
  const mentions = extractMentions({
    responseId: "tax",
    text: c.text || "",
    entityIndex: index.aliasIndex,
  });
  const predicted = bestRole(mentions, c.entityName);
  results.push(adjudicate(c, predicted, c.text || ""));
}

const summary = {
  TOTAL: results.length,
  KEEP: results.filter((r) => r.DECISION === "KEEP").length,
  AMEND: results.filter((r) => r.DECISION === "AMEND").length,
  DEFER: results.filter((r) => r.DECISION === "DEFER").length,
  BY_TRANSITION: {},
  cases: results,
};
for (const r of results.filter((x) => x.DECISION === "AMEND")) {
  const k = `${r.CURRENT_HUMAN_LABEL} => ${r.FINAL_PROPOSED_ROLE}`;
  summary.BY_TRANSITION[k] = (summary.BY_TRANSITION[k] || 0) + 1;
}

fs.writeFileSync(OUT, JSON.stringify(summary, null, 2));
console.log(
  JSON.stringify(
    {
      TOTAL: summary.TOTAL,
      KEEP: summary.KEEP,
      AMEND: summary.AMEND,
      DEFER: summary.DEFER,
      BY_TRANSITION: summary.BY_TRANSITION,
      out: OUT,
      sample: results.map((r) => ({
        id: r.CASE_ID,
        d: r.DECISION,
        from: r.CURRENT_HUMAN_LABEL,
        to: r.FINAL_PROPOSED_ROLE,
        reason: r.REASON?.slice(0, 100),
      })),
    },
    null,
    2
  )
);
