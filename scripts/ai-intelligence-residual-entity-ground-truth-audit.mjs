#!/usr/bin/env node
/**
 * Residual Entity Ground-Truth Audit (DEVELOPMENT only).
 * No classifier/resolver/alias changes. No holdout. No auto label edits.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { loadGoldenSet, scoreGoldenSetHydrated } from "../lib/ai-visibility/validation/golden-set.js";
import { resolveFullResponseText } from "../lib/ai-visibility/validation/hydrate-golden-set-texts.js";
import { createBrandAiVisibilityReadStore } from "../lib/ai-visibility/storage/index.js";
import { extractMentions } from "../lib/ai-visibility/extract-mentions.js";
import { buildGoldenSetScoringEntityIndex } from "../lib/ai-visibility/validation/golden-set-entity-index.js";
import {
  assessPlayaBrandReference,
  assessIhgManagedReference,
  AMENDMENT_ACTIONS,
} from "../lib/ai-visibility/validation/golden-set-ground-truth-amendment.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT_DIR = path.join(
  ROOT,
  "data/ai-visibility/validation/human-review/residual-entity-ground-truth"
);
const HARDENING = path.join(
  ROOT,
  "data/ai-visibility/validation/classifier-hardening-1-entity-dev-result.json"
);

function questionStatusFromRole(role, entityPresent) {
  if (!entityPresent) return "MISSING";
  if (role === "first_recommendation") return "FIRST_RECOMMENDED";
  if (role === "ranked_recommendation" || role === "explicit_recommendation") return "RECOMMENDED";
  if (role === "negative_or_qualified") return "NEGATIVE_OR_NOT_RECOMMENDED";
  if (
    role === "discussed" ||
    role === "passing_mention" ||
    role === "comparator" ||
    role === "associated_option"
  ) {
    return "DISCUSSION_ONLY";
  }
  return entityPresent ? "PRESENT" : "NOT_APPLICABLE";
}

function classifyCase(c, aliasIndex) {
  const mentions = extractMentions({
    responseId: `resp_${c.caseId}`,
    text: c.text || "",
    entityIndex: aliasIndex,
    promptIntentTerritory: c.promptIntentTerritory,
  });
  const hits = mentions.filter((m) => m.canonicalEntityName === c.entityName);
  const role = hits[0]?.role || null;
  const entityPresent = hits.length > 0;
  return {
    entityPresent,
    recommendationStatus: role,
    firstRecommendation: entityPresent ? role === "first_recommendation" : false,
    questionStatus: questionStatusFromRole(role, entityPresent),
  };
}

function nominationVerdict(entityName, playa, ihg) {
  if (/^Playa Hotels/i.test(entityName)) {
    if (playa.UNAMBIGUOUS_BRAND_REFERENCE === "YES") {
      return {
        verdict: "VALID_ENTITY_SPECIFIC_CANDIDATE",
        rootCause: "legitimate_resolver_miss_or_alias_gap",
      };
    }
    return {
      verdict: "CANDIDATE_SUBJECT_OVERREACH",
      rootCause:
        "generic_spanish_playa_or_place_name_nominated_as_Playa_Hotels_and_Resorts; human entityPresent likely inherited subject overreach",
    };
  }
  if (/IHG Hotels & Resorts \(Managed\)/i.test(entityName)) {
    if (ihg.SPECIFIC_CANONICAL_REFERENCE === "YES") {
      return {
        verdict: "VALID_ENTITY_SPECIFIC_CANDIDATE",
        rootCause: "legitimate_resolver_miss_or_alias_gap",
      };
    }
    return {
      verdict: "CANDIDATE_SUBJECT_OVERREACH",
      rootCause:
        "parent_IHG_mention_nominated_as_specific_IHG_Hotels_and_Resorts_Managed; human entityPresent treats parent as managed subject",
    };
  }
  return { verdict: "UNKNOWN", rootCause: "unclassified" };
}

const hardening = fs.existsSync(HARDENING)
  ? JSON.parse(fs.readFileSync(HARDENING, "utf8"))
  : null;
const residualIds = new Set(
  (hardening?.remainingEntityFailureClusters || []).flatMap((c) => c.CASE_IDS || [])
);

const golden = loadGoldenSet();
const after = await scoreGoldenSetHydrated(golden, { holdoutPolicy: "exclude" });
if (after.HOLDOUT_ACCESSED || after.HOLDOUT_METRICS_RUN) {
  console.error("BLOCKED: holdout accessed");
  process.exit(3);
}

const fnErrors = (after.errors || []).filter((e) => e.ERROR_TYPE === "alias_resolution");
const fnIds = fnErrors.map((e) => e.CASE_ID);
if (residualIds.size && [...residualIds].some((id) => !fnIds.includes(id))) {
  // allow drift note only
}

const store = createBrandAiVisibilityReadStore({});
const index = buildGoldenSetScoringEntityIndex({});
const casesOut = [];

for (const err of fnErrors) {
  const raw = (golden.cases || []).find((c) => c.caseId === err.CASE_ID);
  if (!raw) continue;
  if (raw.holdoutSplit === "holdout") {
    console.error("BLOCKED: residual FN is holdout", err.CASE_ID);
    process.exit(4);
  }
  const full = await resolveFullResponseText(raw, store);
  const text = full.text || raw.text || "";
  const hydratedCase = { ...raw, text };
  const classifier = classifyCase(hydratedCase, index.aliasIndex);
  const playa = assessPlayaBrandReference(text);
  const ihg = assessIhgManagedReference(text);
  const nom = nominationVerdict(err.ENTITY, playa, ihg);

  const surfaceThatLikelyDroveLabel = /^Playa/i.test(err.ENTITY)
    ? playa.EXACT_TEXT
    : ihg.EXACT_TEXT;

  casesOut.push({
    CASE_ID: err.CASE_ID,
    SOURCE_CASE_ID: raw.sourceCaseId || String(err.CASE_ID).replace(/^v2_/, ""),
    PROVIDER: err.PROVIDER,
    LANGUAGE: err.LANGUAGE,
    GEOGRAPHY: err.GEOGRAPHY,
    PROMPT_FAMILY: err.PROMPT_FAMILY,
    CANONICAL_SUBJECT: err.ENTITY,
    CANONICAL_ID: raw.canonicalEntityId,
    FULL_STORED_RESPONSE: text,
    RESPONSE_SOURCE: full.source || null,
    RESPONSE_LENGTH: text.length,
    EXACT_SURFACE_TEXT_THAT_CAUSED_HUMAN_LABEL_PRESENT: surfaceThatLikelyDroveLabel,
    SURFACE_EVIDENCE_WINDOWS: /^Playa/i.test(err.ENTITY)
      ? playa.EXAMPLE_SURFACES
      : [ihg.EXACT_TEXT].filter(Boolean),
    CURRENT_HUMAN: {
      entityPresent: raw.expectedEntityPresent,
      recommendationStatus: raw.expectedRecommendationRole || raw.expectedRecommendationClass,
      firstRecommendation: raw.expectedFirstRecommendation,
      questionStatus: raw.expectedQuestionStatus,
      reviewStatus: raw.reviewStatus,
      reviewer: raw.reviewer,
      reviewedAt: raw.reviewedAt,
    },
    CURRENT_CLASSIFIER: classifier,
    PLAYA_AUDIT: /^Playa/i.test(err.ENTITY) ? playa : null,
    IHG_AUDIT: /IHG/i.test(err.ENTITY) ? ihg : null,
    CANDIDATE_NOMINATION: nom,
    WHY_FLAGGED:
      "Classifier Hardening 1 residual entity false negative on DEVELOPMENT set; suspected ground-truth or nomination overreach — not a safe alias gap",
    HUMAN_ACTIONS: Object.values(AMENDMENT_ACTIONS),
    HUMAN_ACTION_SELECTED: null,
    GROUND_TRUTH_REVIEW_REQUIRED: "YES",
  });
}

const playaCases = casesOut.filter((c) => /^Playa/i.test(c.CANONICAL_SUBJECT));
const ihgCases = casesOut.filter((c) => /IHG/i.test(c.CANONICAL_SUBJECT));

const summary = {
  version: "ai_intelligence_residual_entity_ground_truth_audit_v1",
  generatedAt: new Date().toISOString(),
  queueLabel: "Residual Entity Ground-Truth Review",
  TOTAL_CASES: casesOut.length,
  HOLDOUT_ACCESSED: false,
  HOLDOUT_CASES_INSPECTED: 0,
  HOLDOUT_METRICS_RUN: false,
  AUTO_LABEL_CHANGES: 0,
  CLASSIFIER_CHANGES: 0,
  ALIAS_CHANGES: 0,
  ENTITY_RESOLVER_CHANGES: 0,
  PLAYA_CASES: {
    COUNT: playaCases.length,
    UNAMBIGUOUS: playaCases.filter((c) => c.PLAYA_AUDIT?.UNAMBIGUOUS_BRAND_REFERENCE === "YES")
      .length,
    AMBIGUOUS_GENERIC: playaCases.filter(
      (c) => c.PLAYA_AUDIT?.UNAMBIGUOUS_BRAND_REFERENCE === "NO"
    ).length,
    GROUND_TRUTH_REVIEW_REQUIRED: playaCases.filter(
      (c) => c.PLAYA_AUDIT?.GROUND_TRUTH_REVIEW_REQUIRED === "YES"
    ).length,
  },
  IHG_CASES: {
    COUNT: ihgCases.length,
    SPECIFIC_REFERENCE: ihgCases.filter((c) => c.IHG_AUDIT?.SPECIFIC_CANONICAL_REFERENCE === "YES")
      .length,
    PARENT_ONLY: ihgCases.filter((c) => c.IHG_AUDIT?.PARENT_ONLY_REFERENCE === "YES").length,
    GROUND_TRUTH_REVIEW_REQUIRED: ihgCases.filter(
      (c) => c.IHG_AUDIT?.GROUND_TRUTH_REVIEW_REQUIRED === "YES"
    ).length,
  },
  VALID_RESOLVER_MISSES: casesOut.filter(
    (c) => c.CANDIDATE_NOMINATION.verdict === "VALID_ENTITY_SPECIFIC_CANDIDATE"
  ).length,
  POTENTIAL_GROUND_TRUTH_ERRORS: casesOut.filter(
    (c) =>
      c.PLAYA_AUDIT?.UNAMBIGUOUS_BRAND_REFERENCE === "NO" ||
      c.IHG_AUDIT?.SPECIFIC_CANONICAL_REFERENCE === "NO"
  ).length,
  POTENTIAL_CANDIDATE_NOMINATION_ERRORS: casesOut.filter(
    (c) => c.CANDIDATE_NOMINATION.verdict === "CANDIDATE_SUBJECT_OVERREACH"
  ).length,
  HUMAN_REVIEW_REQUIRED: true,
  GOVERNANCE_GUIDANCE: {
    ENTITY_PRESENT_MEANS:
      "The specific canonical entity is actually represented in the response.",
    ENTITY_PRESENT_DOES_NOT_MEAN:
      "A generic word happens to overlap with the brand name (e.g. Spanish 'playa').",
    PARENT_RULE:
      "A parent-company mention does NOT automatically imply a specific child brand/operator/managed entity.",
    AMENDMENT_RULE:
      "Labels change only via explicit human KEEP/CORRECT/INVALIDATE/DEFER; original review audit preserved.",
  },
  CASE_IDS: casesOut.map((c) => c.CASE_ID),
};

fs.mkdirSync(OUT_DIR, { recursive: true });
fs.writeFileSync(path.join(OUT_DIR, "audit-summary.json"), JSON.stringify(summary, null, 2), "utf8");
fs.writeFileSync(
  path.join(OUT_DIR, "review-queue.json"),
  JSON.stringify(
    {
      ...summary,
      cases: casesOut,
    },
    null,
    2
  ),
  "utf8"
);

// Compact index without full response bodies for quick listing
fs.writeFileSync(
  path.join(OUT_DIR, "review-queue-index.json"),
  JSON.stringify(
    {
      ...summary,
      cases: casesOut.map((c) => {
        const { FULL_STORED_RESPONSE, ...rest } = c;
        return {
          ...rest,
          FULL_STORED_RESPONSE_LENGTH: FULL_STORED_RESPONSE?.length || 0,
          FULL_STORED_RESPONSE_PREVIEW: String(FULL_STORED_RESPONSE || "").slice(0, 280),
        };
      }),
    },
    null,
    2
  ),
  "utf8"
);

console.log(JSON.stringify(summary, null, 2));
console.log(`\nReview queue written: ${path.join(OUT_DIR, "review-queue.json")}`);
