/**
 * Golden Set candidate sampler — real stored responses only.
 * SYSTEM_SUGGESTION may be pre-filled; ground truth requires human review.
 * LIVE_PROVIDER_CALLS: 0. LLM labels are NEVER accepted as ground truth.
 */

import crypto from "crypto";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { createBrandAiVisibilityReadStore } from "../storage/index.js";
import { isPositiveRecommendationRole } from "../metrics.js";
import { extractMentions } from "../extract-mentions.js";
import { buildLiveAiVisibilityEntityIndex } from "../entity-index.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const FIXTURES = path.resolve(__dirname, "../../../fixtures/ai-visibility");
const OUT_CANDIDATES = path.join(FIXTURES, "ai-intelligence-golden-set-v2-candidates.json");
const OUT_V1 = path.join(FIXTURES, "ai-intelligence-golden-set-v1.json");
const PHASE2C = path.join(FIXTURES, "phase2c-classification-golden.json");

export const GOLDEN_SET_V1_VERSION = "ai_intelligence_golden_set_v1";
export const GOLDEN_SET_V2_CANDIDATES_VERSION = "ai_intelligence_golden_set_v2_candidates";

export const QUESTION_STATUS_TAXONOMY = Object.freeze([
  "PRESENT",
  "MISSING",
  "RECOMMENDED",
  "FIRST_RECOMMENDED",
  "DISCUSSION_ONLY",
  "NEGATIVE_OR_NOT_RECOMMENDED",
  "NOT_APPLICABLE",
]);

function hashRank(s) {
  return crypto.createHash("sha256").update(String(s)).digest().readUInt32BE(0);
}

/**
 * Promote Phase 2C human labels into versioned v1 (does not invent labels).
 */
export function materializeGoldenSetV1() {
  const raw = JSON.parse(fs.readFileSync(PHASE2C, "utf8"));
  const cases = (raw.cases || []).map((c) => ({
    caseId: `v1_${c.id}`,
    source: "phase2c_human_labelled",
    humanLabelled: true,
    llmLabelledAsGroundTruth: false,
    provider: null,
    model: null,
    batchId: null,
    responseId: null,
    promptId: null,
    geography: c.geography || null,
    language: c.language || null,
    promptIntentTerritory: c.promptIntentTerritory || null,
    promptText: null,
    rawResponseExcerpt: c.text,
    candidateEntity: c.entityName,
    canonicalEntityId: null,
    expectedRecommendationClass: c.expectedRole,
    expectedFirstRecommendation: c.expectedRole === "first_recommendation",
    expectedQuestionStatus: null,
    expectedCitationAssociation: null,
    caseType: classifyCaseType(c),
    hardCase: isHardCase(c),
    reviewer: "phase2c_human_review",
    reviewedAt: "2026-08-13",
    notes: "Imported from Phase 2C human-labelled golden; provider/geo/lang unspecified in source.",
    goldenSetVersion: GOLDEN_SET_V1_VERSION,
  }));

  const doc = {
    version: GOLDEN_SET_V1_VERSION,
    previousVersion: "ai_visibility_phase2c_classification_golden_v1",
    caseCount: cases.length,
    humanLabelled: cases.length,
    llmLabelledAsGroundTruth: 0,
    reviewDate: "2026-08-13",
    note: "Versioned promotion of Phase 2C human-labelled cases. Coverage gaps: no provider/geo/language stamps.",
    coverageAudit: auditGoldenCoverage(cases),
    cases,
  };
  fs.writeFileSync(OUT_V1, JSON.stringify(doc, null, 2), "utf8");
  return doc;
}

function classifyCaseType(c) {
  const role = c.expectedRole || c.expectedRecommendationClass || "";
  if (role === "negative_or_qualified") return "negative_recommendation";
  if (role === "first_recommendation") return "first_recommendation";
  if (role === "ranked_recommendation") return "ranked_list";
  if (role === "comparator") return "comparison_only";
  if (role === "associated_option") return "association";
  if (role === "discussed" || role === "passing_mention") return "discussion_only";
  return role || "other";
}

function isHardCase(c) {
  const role = c.expectedRole || "";
  const text = String(c.text || "");
  return (
    role === "negative_or_qualified" ||
    role === "comparator" ||
    role === "associated_option" ||
    /alternative|versus|vs\.|not recommend|avoid|instead of/i.test(text) ||
    (text.match(/Collection|Hotels|Hilton|Marriott|IHG|Choice/gi) || []).length >= 3
  );
}

export function auditGoldenCoverage(cases) {
  const counts = {
    PROVIDER: {},
    GEOGRAPHY: {},
    LANGUAGE: {},
    PROMPT_FAMILY: {},
    CASE_TYPE: {},
    RECOMMENDATION_STATUS: {},
    FIRST_RECOMMENDATION: 0,
    NEGATIVE_RECOMMENDATION: 0,
    HARD_CASE_COUNT: 0,
    QUESTION_STATUS_LABEL_AVAILABLE: 0,
    CITATION_ASSOCIATION_LABEL_COUNT: 0,
    HUMAN_LABELLED: 0,
    PENDING_HUMAN_REVIEW: 0,
  };
  for (const c of cases) {
    const bump = (obj, k) => {
      const key = k || "unspecified";
      obj[key] = (obj[key] || 0) + 1;
    };
    bump(counts.PROVIDER, c.provider);
    bump(counts.GEOGRAPHY, c.geography);
    bump(counts.LANGUAGE, c.language);
    bump(counts.PROMPT_FAMILY, c.promptIntentTerritory || c.promptFamily);
    bump(counts.CASE_TYPE, c.caseType || classifyCaseType(c));
    bump(counts.RECOMMENDATION_STATUS, c.expectedRecommendationClass);
    if (c.expectedFirstRecommendation) counts.FIRST_RECOMMENDATION += 1;
    if (c.expectedRecommendationClass === "negative_or_qualified") {
      counts.NEGATIVE_RECOMMENDATION += 1;
    }
    if (c.hardCase || isHardCase(c)) counts.HARD_CASE_COUNT += 1;
    if (c.expectedQuestionStatus) counts.QUESTION_STATUS_LABEL_AVAILABLE += 1;
    if (c.expectedCitationAssociation != null) counts.CITATION_ASSOCIATION_LABEL_COUNT += 1;
    if (c.humanLabelled) counts.HUMAN_LABELLED += 1;
    if (c.reviewStatus === "PENDING_HUMAN_REVIEW") counts.PENDING_HUMAN_REVIEW += 1;
  }
  counts.TOTAL = cases.length;
  counts.GAPS = [];
  if ((counts.PROVIDER.unspecified || 0) === cases.length) {
    counts.GAPS.push("No provider stamps on human-labelled cases");
  }
  if ((counts.LANGUAGE.unspecified || 0) === cases.length) {
    counts.GAPS.push("No language stamps — Spanish coverage missing in v1");
  }
  if ((counts.GEOGRAPHY.unspecified || 0) === cases.length) {
    counts.GAPS.push("No geography stamps on v1 cases");
  }
  if (counts.QUESTION_STATUS_LABEL_AVAILABLE === 0) {
    counts.GAPS.push("No question-status human labels");
  }
  if (!counts.PROVIDER.openai && !counts.PROVIDER.gemini) {
    counts.GAPS.push("No multi-provider representation in labelled set");
  }
  return counts;
}

/**
 * Sample diverse candidates from stored monitoring for human review.
 * Every case MUST be RESPONSE × canonical entity (no null-subject placeholders).
 * @param {{ target?: number, store?: object, entityIndex?: object }} [options]
 */
export async function sampleGoldenSetCandidates(options = {}) {
  const target = Math.max(85, Number(options.target) || 100);
  const store = options.store || createBrandAiVisibilityReadStore({});
  const maxPerResponse = Number(options.maxEntitiesPerResponse) || 2;
  const index =
    options.entityIndex ||
    (await buildLiveAiVisibilityEntityIndex(options.entityIndexOpts || {})).index;

  const summaries = await store.listBatchSummaries({});
  const publishablePrefer = summaries.filter((s) => {
    return s.status === "completed" || s.status === "partial";
  });

  const buckets = {
    "openai|en": [],
    "openai|es": [],
    "gemini|en": [],
    "gemini|es": [],
    "perplexity|en": [],
    "perplexity|es": [],
    "claude|en": [],
    "claude|es": [],
  };

  for (const summary of publishablePrefer) {
    const provider = String(summary.provider?.name || summary.provider || "").toLowerCase();
    if (!["openai", "gemini", "perplexity", "claude"].includes(provider)) continue;
    const runs = (await store.listBatchRuns(summary.batchId)) || [];
    for (const run of runs) {
      if (run.status !== "completed") continue;
      const language = String(run.language || "").toLowerCase();
      if (language !== "en" && language !== "es") continue;
      const key = `${provider}|${language}`;
      if (!buckets[key]) continue;
      buckets[key].push({ summary, run });
    }
  }

  const perBucket = Math.ceil(target / Object.keys(buckets).length);
  const picked = [];
  for (const [key, rows] of Object.entries(buckets)) {
    const ranked = rows
      .map((row, i) => ({
        ...row,
        rank: hashRank(`${key}|${row.run.runId || row.run.evidenceId}|${i}`),
      }))
      .sort((a, b) => a.rank - b.rank)
      .slice(0, perBucket);
    picked.push(...ranked);
  }

  picked.sort((a, b) => a.rank - b.rank);
  const selected = picked.slice(0, target);

  const candidates = [];
  for (const { summary, run } of selected) {
    let evidence = null;
    if (run.evidenceId && store.getEvidence) evidence = await store.getEvidence(run.evidenceId);
    let mentions = evidence?.payload?.mentions || [];
    const citations = evidence?.payload?.citations || [];
    const rawText = run.rawText || evidence?.payload?.rawText || "";
    const excerpt = String(rawText).slice(0, 600);

    // If evidence mentions are empty (common for baseline providers), resolve from text.
    if ((!mentions || !mentions.length) && rawText) {
      mentions = extractMentions({
        responseId: run.responseId || run.runId || "resp",
        text: String(rawText),
        entityIndex: index.aliasIndex,
        promptIntentTerritory: run.intent || evidence?.intentTerritory || null,
      });
    }

    // Pick up to N entity candidates per response — never create null-subject cases
    const entityMentions = [];
    const seen = new Set();
    const sortedMentions = [...(mentions || [])].sort((a, b) => {
      const pri = (r) =>
        r === "first_recommendation"
          ? 0
          : r === "ranked_recommendation"
            ? 1
            : r === "explicit_recommendation"
              ? 2
              : isPositiveRecommendationRole(r)
                ? 3
                : 4;
      return pri(a.role) - pri(b.role) || (a.mentionPosition || 0) - (b.mentionPosition || 0);
    });
    for (const m of sortedMentions) {
      const id = m.canonicalEntityId;
      if (!id || !m.canonicalEntityName || seen.has(id)) continue;
      seen.add(id);
      entityMentions.push(m);
      if (entityMentions.length >= maxPerResponse) break;
    }
    if (!entityMentions.length) {
      // Skip response entirely — do not emit invalid null-subject review cases
      continue;
    }

    for (const m of entityMentions) {
      const systemRole = m.role || null;
      const systemSuggestion = {
        expectedRecommendationClass: systemRole,
        expectedFirstRecommendation: systemRole === "first_recommendation",
        expectedQuestionStatus: suggestQuestionStatus(m, mentions),
        expectedCitationAssociation: citations.length > 0 ? "citations_present_unreviewed" : "none",
        note: "SYSTEM_SUGGESTION only — not ground truth",
      };

      candidates.push({
        caseId: `cand_${hashRank(`${run.runId}|${m.canonicalEntityId}`).toString(16)}`,
        reviewStatus: "PENDING_HUMAN_REVIEW",
        humanLabelled: false,
        llmLabelledAsGroundTruth: false,
        provider: String(run.provider || summary.provider?.name || "").toLowerCase(),
        model: run.model || summary.provider?.model || null,
        batchId: summary.batchId,
        responseId: run.responseId || null,
        promptId: run.promptId || evidence?.promptId || null,
        promptFamily: run.promptFamily || null,
        promptIntentTerritory: run.intent || evidence?.intentTerritory || null,
        geography: run.geographyKey || null,
        language: run.language || null,
        promptText: evidence?.promptText || null,
        rawResponseExcerpt: excerpt,
        candidateEntity: m.canonicalEntityName,
        canonicalEntityId: m.canonicalEntityId,
        canonicalEntityName: m.canonicalEntityName,
        systemSuggestion,
        expectedRecommendationClass: null,
        expectedFirstRecommendation: null,
        expectedQuestionStatus: null,
        expectedCitationAssociation: null,
        caseType: null,
        hardCase: null,
        reviewer: null,
        reviewedAt: null,
        notes: null,
        goldenSetVersion: GOLDEN_SET_V2_CANDIDATES_VERSION,
        citationCount: citations.length,
        mentionCount: mentions.length,
        positiveRecCount: mentions.filter((x) => isPositiveRecommendationRole(x.role)).length,
        sourceResponseId: run.responseId || null,
        sourceBatchId: summary.batchId,
        entityNominationMethod: evidence?.payload?.mentions?.length
          ? "evidence_mentions"
          : "deterministic_extractMentions_v3",
      });
    }
  }

  const doc = {
    version: GOLDEN_SET_V2_CANDIDATES_VERSION,
    previousVersion: GOLDEN_SET_V1_VERSION,
    caseCount: candidates.length,
    entitySpecificCaseCount: candidates.length,
    invalidSubjectCaseCount: 0,
    humanLabelled: 0,
    llmLabelledAsGroundTruth: 0,
    reviewStatus: "PENDING_HUMAN_REVIEW",
    note:
      "Candidates sampled from stored monitoring artifacts. Every case is entity-specific. SYSTEM_SUGGESTION is convenience only. Do not promote to ground truth until human review.",
    coverageAudit: auditGoldenCoverage(candidates),
    cases: candidates,
  };
  fs.writeFileSync(OUT_CANDIDATES, JSON.stringify(doc, null, 2), "utf8");
  return doc;
}

function suggestQuestionStatus(m, mentions) {
  if (!m?.canonicalEntityId) return "MISSING";
  if (m.role === "first_recommendation") return "FIRST_RECOMMENDED";
  if (isPositiveRecommendationRole(m.role)) return "RECOMMENDED";
  if (m.role === "negative_or_qualified") return "NEGATIVE_OR_NOT_RECOMMENDED";
  if (m.role === "discussed" || m.role === "passing_mention" || m.role === "comparator") {
    return "DISCUSSION_ONLY";
  }
  if (mentions.some((x) => x.canonicalEntityId === m.canonicalEntityId)) return "PRESENT";
  return "NOT_APPLICABLE";
}

export { OUT_CANDIDATES, OUT_V1 };
