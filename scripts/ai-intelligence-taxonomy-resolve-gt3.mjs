#!/usr/bin/env node
/**
 * Audit GROUND_TRUTH_REVIEW_REQUIRED cases from hybrid prototype under Rules 1–5.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { loadGoldenSet } from "../lib/ai-visibility/validation/golden-set.js";
import { hydrateGoldenSetCasesForScoring } from "../lib/ai-visibility/validation/hydrate-golden-set-texts.js";
import { extractMentions } from "../lib/ai-visibility/extract-mentions.js";
import { buildGoldenSetScoringEntityIndex } from "../lib/ai-visibility/validation/golden-set-entity-index.js";
import {
  extractEntityLocalEvidence,
  buildTypedSections,
} from "../lib/ai-visibility/recommendation-evidence-v4_1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

// From hybrid report: first→explicit/associated tagged GT review; find all 3
const IDS = ["v1_g034", "v1_g041", "v2_cand_035ffe13"];

const LEAD =
  /\b(first\s+(?:choice|call|option|recommendation)|top\s+(?:choice|recommendation)|primary\s+recommendation|#\s*1\b|rank\s*1\b|1st\s+|primera\s+opci|recomendaci[oó]n\s+principal|leading\s+recommendation|preferred\s+recommendation)\b/i;
const DIRECT_POS =
  /\b(strong\s+(?:candidate|alternative|option|choice|fit)|recommended\s+option|good\s+fit|particularly\s+(?:strong|suitable)|best\s+(?:option|fit|value)|opci[oó]n\s+s[oó]lida|alternativa\s+fuerte)\b/i;
const CONSIDERATION =
  /\b(to\s+consider|commonly\s+(?:considered|shortlisted)|habitualmente\s+considerad|a\s+considerar|options?\s+include|alternatives?)\b/i;

const golden = loadGoldenSet();
const index = buildGoldenSetScoringEntityIndex({});
const { cases } = await hydrateGoldenSetCasesForScoring(
  golden.cases.filter((c) => IDS.includes(c.caseId)),
  {}
);

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

const out = [];
for (const c of cases) {
  if (c.holdoutSplit === "holdout") continue;
  const text = c.text || "";
  const mentions = extractMentions({
    responseId: "gt3",
    text,
    entityIndex: index.aliasIndex,
  });
  const hits = mentions.filter((m) => m.canonicalEntityName === c.entityName);
  const best = hits
    .slice()
    .sort(
      (a, b) =>
        ROLES.indexOf(a.role) - ROLES.indexOf(b.role) || a.mentionPosition - b.mentionPosition
    )[0];
  const name = String(c.entityName || "");
  const idx = text.toLowerCase().indexOf(name.toLowerCase().slice(0, Math.min(24, name.length)));
  const ctx = text
    .slice(Math.max(0, idx - 400), Math.min(text.length, (idx >= 0 ? idx : 0) + 400))
    .replace(/\s+/g, " ")
    .trim();
  const start = best?.mentionPosition ?? (idx >= 0 ? idx : 0);
  const end = start + (best?.rawMention?.length || name.length);
  const ev = extractEntityLocalEvidence({
    text,
    start,
    end,
    rawMention: best?.rawMention || name,
    canonicalEntityName: name,
    typedSections: buildTypedSections(text),
  });
  const local = text.slice(Math.max(0, start - 150), Math.min(text.length, end + 150));
  const hasLead = LEAD.test(local) || Boolean(ev.recommendationEvidence?.leadCue);
  const hasPos =
    DIRECT_POS.test(local) || Boolean(ev.recommendationEvidence?.directPositiveCue);
  const inCons =
    CONSIDERATION.test(text.slice(Math.max(0, start - 400), start)) ||
    Boolean(ev.recommendationEvidence?.considerationSetCue);
  const predFromReport =
    ({
      v1_g034: "explicit_recommendation",
      v1_g041: "explicit_recommendation",
      "v2_cand_035ffe13": "associated_option",
    }[c.caseId] ||
      best?.role ||
      null);

  let DECISION = "DEFER";
  let FINAL = null;
  let REASON = "Ambiguous under Rules 1–5";
  let rules = [];

  if (hasLead) {
    DECISION = "KEEP";
    FINAL = "first_recommendation";
    REASON = "RULE4: lead evidence supports first_recommendation — keep human";
    rules = ["RULE_4_LEAD_OVERRIDE"];
  } else if (hasPos && !hasLead) {
    DECISION = "AMEND";
    FINAL = "explicit_recommendation";
    REASON = "RULE1: positive/endorsement without separate lead/rank-1 → explicit_recommendation";
    rules = ["RULE_1_POSITIVE_NOT_FIRST"];
  } else if (inCons && !hasLead && !hasPos) {
    DECISION = "AMEND";
    FINAL = "associated_option";
    REASON = "RULE2+RULE3: consideration without lead/positive → associated_option";
    rules = ["RULE_2_CONSIDERATION_NOT_RANK", "RULE_3_MEANINGFUL_ORDERING_REQUIRED"];
  }

  if (DECISION === "AMEND" && FINAL === c.expectedRecommendationRole) {
    DECISION = "KEEP";
    REASON = "Human already matches Rules 1–5 outcome";
  }

  out.push({
    CASE_ID: c.caseId,
    ENTITY: c.entityName,
    CURRENT_HUMAN_LABEL: c.expectedRecommendationRole,
    CURRENT_PREDICTION: predFromReport,
    FULL_RELEVANT_CONTEXT: ctx.slice(0, 900),
    TAXONOMY_CONFLICT: `human=${c.expectedRecommendationRole} vs pred=${predFromReport}; lead=${hasLead} pos=${hasPos} cons=${inCons}`,
    STRUCTURAL: {
      hasLead,
      hasPos,
      inCons,
      leadCue: Boolean(ev.recommendationEvidence?.leadCue),
      directPositiveCue: Boolean(ev.recommendationEvidence?.directPositiveCue),
      considerationSetCue: Boolean(ev.recommendationEvidence?.considerationSetCue),
    },
    DECISION,
    FINAL_PROPOSED_ROLE: DECISION === "KEEP" ? c.expectedRecommendationRole : FINAL,
    REASON,
    taxonomyRuleApplied: rules,
    holdoutSplit: c.holdoutSplit || "development",
  });
}

// If only 2 found, also search current DEV for first vs explicit mismatches with strong positive
if (out.length < 3) {
  console.error("WARN: expected 3 cases, found", out.length, "IDS", IDS);
}

const summary = {
  TOTAL: out.length,
  KEEP: out.filter((x) => x.DECISION === "KEEP").length,
  AMEND: out.filter((x) => x.DECISION === "AMEND").length,
  DEFER: out.filter((x) => x.DECISION === "DEFER").length,
  cases: out,
};
const outPath = path.join(
  __dirname,
  "../data/ai-visibility/validation/taxonomy-resolution-gt3-hierarchical.json"
);
fs.writeFileSync(outPath, JSON.stringify(summary, null, 2));
console.log(JSON.stringify({ ...summary, cases: out }, null, 2));
