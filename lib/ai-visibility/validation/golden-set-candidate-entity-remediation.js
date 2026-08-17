/**
 * Golden Set candidate entity-nomination remediation.
 * Every reviewable case = RESPONSE × EVALUATED CANONICAL ENTITY.
 * No LLM. No auto-review. No auto-promotion. No provider calls.
 */

import crypto from "crypto";
import fs from "fs";
import path from "path";
import {
  GOLDEN_SET_V2_CANDIDATES_VERSION,
  OUT_CANDIDATES,
  auditGoldenCoverage,
} from "./golden-set-expansion.js";
import { loadCandidateDocument } from "./golden-set-human-review.js";
import {
  SUPERSEDED_INVALID_SUBJECT,
  CASE_TYPE_NO_ENTITY_DETECTED,
  hasSubjectEntity,
  isActiveReviewCandidate,
  isReviewableCandidate,
  getActiveGoldenSetReviewCandidates,
  summarizeCandidatePopulation,
} from "./golden-set-active-candidates.js";
import { extractMentions } from "../extract-mentions.js";
import { buildLiveAiVisibilityEntityIndex } from "../entity-index.js";
import { isPositiveRecommendationRole } from "../metrics.js";
import { createBrandAiVisibilityReadStore } from "../storage/index.js";

export const REMEDIATION_VERSION = "ai_intelligence_golden_set_entity_nomination_v1";
export {
  SUPERSEDED_INVALID_SUBJECT,
  CASE_TYPE_NO_ENTITY_DETECTED,
  hasSubjectEntity,
  isReviewableCandidate,
  isActiveReviewCandidate,
  getActiveGoldenSetReviewCandidates,
  summarizeCandidatePopulation,
};

/** Max entity-specific children per null-subject source response. */
export const MAX_ENTITIES_PER_RESPONSE = 4;

const ROLE_PRIORITY = Object.freeze({
  first_recommendation: 100,
  ranked_recommendation: 90,
  explicit_recommendation: 85,
  associated_option: 70,
  comparator: 65,
  negative_or_qualified: 60,
  discussed: 40,
  passing_mention: 25,
  source_only: 20,
  no_mention: 5,
});

function hashHex(s) {
  return crypto.createHash("sha256").update(String(s)).digest("hex").slice(0, 8);
}

export function auditCandidateSubjects(doc = null) {
  const source = doc || loadCandidateDocument();
  const pop = summarizeCandidatePopulation(source);
  const entitySpecific = getActiveGoldenSetReviewCandidates(source);
  const superseded = (source.cases || []).filter((c) => c.reviewStatus === SUPERSEDED_INVALID_SUBJECT);
  const nullSubject = (source.cases || []).filter(
    (c) => c.reviewStatus === SUPERSEDED_INVALID_SUBJECT || !hasSubjectEntity(c)
  ).filter((c) => !hasSubjectEntity(c));
  return {
    TOTAL_CANDIDATES: pop.storedCandidateCount,
    VALID_ENTITY_SPECIFIC_CASES: pop.activeReviewCandidateCount,
    NULL_SUBJECT_CASES: pop.nullSubjectActive,
    NULL_SUBJECT_TOTAL: pop.nullSubjectTotal,
    SUPERSEDED_INVALID_CASES: pop.supersededCandidateCount,
    entitySpecific,
    nullSubject,
    superseded,
    population: pop,
    byProvider: summarizeBy(source.cases || [], "provider"),
    nullByProvider: summarizeBy(nullSubject, "provider"),
  };
}

function summarizeBy(rows, field) {
  const out = {};
  for (const r of rows || []) {
    const k = String(r[field] || "unspecified").toLowerCase();
    out[k] = (out[k] || 0) + 1;
  }
  return out;
}

function suggestQuestionStatus(m, mentions) {
  if (!m?.canonicalEntityId) return "MISSING";
  if (m.role === "first_recommendation") return "FIRST_RECOMMENDED";
  if (isPositiveRecommendationRole(m.role)) return "RECOMMENDED";
  if (m.role === "negative_or_qualified") return "NEGATIVE_OR_NOT_RECOMMENDED";
  if (
    m.role === "discussed" ||
    m.role === "passing_mention" ||
    m.role === "comparator" ||
    m.role === "associated_option"
  ) {
    return "DISCUSSION_ONLY";
  }
  if (mentions.some((x) => x.canonicalEntityId === m.canonicalEntityId)) return "PRESENT";
  return "NOT_APPLICABLE";
}

function citationSuggestion(citations) {
  if (!citations || !citations.length) return "NOT_APPLICABLE";
  return "UNKNOWN";
}

/**
 * Deterministic entity sampling for classification diversity (not max volume).
 */
export function selectEntitiesForCandidate(mentions, options = {}) {
  const max = options.maxEntitiesPerResponse ?? MAX_ENTITIES_PER_RESPONSE;
  const byId = new Map();
  for (const m of mentions || []) {
    if (!m?.canonicalEntityId || !m?.canonicalEntityName) continue;
    const prev = byId.get(m.canonicalEntityId);
    if (!prev) {
      byId.set(m.canonicalEntityId, m);
      continue;
    }
    const prevPri = ROLE_PRIORITY[prev.role] || 0;
    const nextPri = ROLE_PRIORITY[m.role] || 0;
    if (nextPri > prevPri) byId.set(m.canonicalEntityId, m);
  }
  const ranked = [...byId.values()].sort((a, b) => {
    const pa = ROLE_PRIORITY[a.role] || 0;
    const pb = ROLE_PRIORITY[b.role] || 0;
    if (pb !== pa) return pb - pa;
    const posA = a.recommendationPosition ?? a.mentionPosition ?? 1e9;
    const posB = b.recommendationPosition ?? b.mentionPosition ?? 1e9;
    if (posA !== posB) return posA - posB;
    return String(a.canonicalEntityId).localeCompare(String(b.canonicalEntityId));
  });

  // Prefer covering distinct role buckets first
  const selected = [];
  const seenRoles = new Set();
  for (const m of ranked) {
    if (selected.length >= max) break;
    const role = m.role || "discussed";
    if (!seenRoles.has(role) || selected.length < Math.min(2, max)) {
      selected.push(m);
      seenRoles.add(role);
    }
  }
  for (const m of ranked) {
    if (selected.length >= max) break;
    if (!selected.some((s) => s.canonicalEntityId === m.canonicalEntityId)) {
      selected.push(m);
    }
  }
  return selected.slice(0, max);
}

export async function resolveStoredResponseText(candidate, options = {}) {
  const store = options.store || createBrandAiVisibilityReadStore({});
  let text = "";
  let fullAvailable = false;
  if (candidate.batchId && typeof store.listBatchRuns === "function") {
    try {
      const runs = (await store.listBatchRuns(candidate.batchId)) || [];
      const run =
        runs.find((r) => r.responseId && r.responseId === candidate.responseId) ||
        runs.find((r) => r.promptId && r.promptId === candidate.promptId) ||
        null;
      if (run?.rawText) {
        text = String(run.rawText);
        fullAvailable = true;
      }
      if ((!text || text.length < 40) && run?.evidenceId && store.getEvidence) {
        const ev = await store.getEvidence(run.evidenceId);
        const t =
          ev?.payload?.rawText || ev?.payload?.text || ev?.payload?.responseText || "";
        if (t) {
          text = String(t);
          fullAvailable = true;
        }
      }
    } catch (err) {
      console.warn(
        "[golden-set-entity-remediation] store text load failed:",
        err?.message || err
      );
    }
  }
  if (!text) text = String(candidate.rawResponseExcerpt || "");
  return { text, fullAvailable };
}

export function buildSystemSuggestionFromMention(m, allMentions, citations = []) {
  const role = m?.role || null;
  return {
    expectedRecommendationClass: role,
    expectedFirstRecommendation: role === "first_recommendation",
    expectedQuestionStatus: suggestQuestionStatus(m, allMentions),
    expectedCitationAssociation:
      citations.length > 0 ? "citations_present_unreviewed" : citationSuggestion(citations),
    note: "SYSTEM_SUGGESTION only — not ground truth",
    classifierVersion: m?.classifierVersion || null,
    resolverVersion: m?.resolverVersion || null,
  };
}

function materializeEntityCase(source, mention, allMentions, options = {}) {
  const entityId = mention.canonicalEntityId;
  const caseId = `cand_${hashHex(`${source.caseId}|${entityId}`)}`;
  const excerpt = String(options.responseText || source.rawResponseExcerpt || "").slice(0, 1200);
  const systemSuggestion = buildSystemSuggestionFromMention(
    mention,
    allMentions,
    options.citations || []
  );
  return {
    caseId,
    reviewStatus: "PENDING_HUMAN_REVIEW",
    humanLabelled: false,
    llmLabelledAsGroundTruth: false,
    autoApproved: false,
    provider: source.provider,
    model: source.model,
    batchId: source.batchId,
    responseId: source.responseId,
    promptId: source.promptId,
    promptFamily: source.promptFamily,
    promptIntentTerritory: source.promptIntentTerritory,
    geography: source.geography,
    language: source.language,
    promptText: source.promptText,
    rawResponseExcerpt: excerpt,
    candidateEntity: mention.canonicalEntityName,
    canonicalEntityId: entityId,
    canonicalEntityName: mention.canonicalEntityName,
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
    citationCount: source.citationCount || 0,
    mentionCount: allMentions.length,
    positiveRecCount: allMentions.filter((x) => isPositiveRecommendationRole(x.role)).length,
    sourceCandidateId: source.caseId,
    sourceResponseId: source.responseId,
    sourceBatchId: source.batchId,
    remediationVersion: REMEDIATION_VERSION,
    entityNominationMethod: "deterministic_extractMentions_v3",
  };
}

/**
 * Audit why subject nomination was absent for null cases.
 */
export async function auditNullSubjectCases(options = {}) {
  const audit = auditCandidateSubjects(options.doc || null);
  const store = options.store || createBrandAiVisibilityReadStore({});
  const index =
    options.entityIndex ||
    (await buildLiveAiVisibilityEntityIndex(options.entityIndexOpts || {})).index;

  const details = [];
  for (const c of audit.nullSubject) {
    const { text, fullAvailable } = await resolveStoredResponseText(c, { store });
    const mentions = text
      ? extractMentions({
          responseId: c.responseId || c.caseId,
          text,
          entityIndex: index.aliasIndex,
          promptIntentTerritory: c.promptIntentTerritory,
        })
      : [];
    const unique = selectEntitiesForCandidate(mentions, { maxEntitiesPerResponse: 99 });
    details.push({
      caseId: c.caseId,
      provider: c.provider,
      language: c.language,
      geography: c.geography,
      promptId: c.promptId,
      responseId: c.responseId,
      textLength: text.length,
      fullAvailable,
      canonicalEntitiesDetected: unique.map((m) => ({
        canonicalEntityId: m.canonicalEntityId,
        canonicalEntityName: m.canonicalEntityName,
        role: m.role,
      })),
      recommendationCandidatesDetected: unique
        .filter((m) => isPositiveRecommendationRole(m.role))
        .map((m) => m.canonicalEntityName),
      whySubjectNominationWasAbsent:
        "Candidate sampler used evidence.payload.mentions; when empty it inserted a null-entity placeholder instead of resolving entities from stored response text via extractMentions.",
    });
  }

  return {
    ROOT_CAUSE:
      "sampleGoldenSetCandidates relied on stored evidence.payload.mentions. OpenAI showcase evidence typically had mentions; Gemini/Perplexity/Claude baseline runs often had no evidence mentions (or no evidenceId), so the sampler created response-level cases with candidateEntity/canonicalEntityId = null.",
    AFFECTED_PROVIDERS: Object.keys(audit.nullByProvider),
    AFFECTED_GENERATOR_PATH:
      "lib/ai-visibility/validation/golden-set-expansion.js → sampleGoldenSetCandidates (entityMentions fallback push of null)",
    TOTAL_CANDIDATES: audit.TOTAL_CANDIDATES,
    VALID_ENTITY_SPECIFIC_CASES: audit.VALID_ENTITY_SPECIFIC_CASES,
    NULL_SUBJECT_CASES: audit.NULL_SUBJECT_CASES,
    details,
  };
}

/**
 * Remediate candidate document: supersede null-subject cases; add entity-specific children.
 */
export async function remediateCandidateEntityNomination(options = {}) {
  const apply = options.apply === true;
  const doc = options.doc || loadCandidateDocument();
  const auditBefore = auditCandidateSubjects(doc);
  const store = options.store || createBrandAiVisibilityReadStore({});
  const live = options.entityIndex
    ? { index: options.entityIndex }
    : await buildLiveAiVisibilityEntityIndex(options.entityIndexOpts || {});
  const index = live.index;

  const kept = [];
  const superseded = [];
  const created = [];
  const nominationFailures = [];

  for (const c of doc.cases || []) {
    if (c.reviewStatus === SUPERSEDED_INVALID_SUBJECT) {
      superseded.push(c);
      continue;
    }
    if (hasSubjectEntity(c)) {
      // Normalize canonicalEntityName alias field
      kept.push({
        ...c,
        canonicalEntityName: c.canonicalEntityName || c.candidateEntity,
        reviewStatus: c.reviewStatus || "PENDING_HUMAN_REVIEW",
      });
      continue;
    }

    const { text, fullAvailable } = await resolveStoredResponseText(c, { store });
    if (!text || text.trim().length < 20) {
      const invalid = {
        ...c,
        reviewStatus: SUPERSEDED_INVALID_SUBJECT,
        supersededReason: "EMPTY_OR_MISSING_STORED_RESPONSE",
        supersededAt: new Date().toISOString(),
        remediationVersion: REMEDIATION_VERSION,
        humanLabelled: false,
        llmLabelledAsGroundTruth: false,
        notReviewable: true,
        notPromotable: true,
        supersededBy: [],
      };
      superseded.push(invalid);
      nominationFailures.push({
        caseId: c.caseId,
        reason: "EMPTY_OR_MISSING_STORED_RESPONSE",
        fullAvailable,
      });
      continue;
    }

    const mentions = extractMentions({
      responseId: c.responseId || c.caseId,
      text,
      entityIndex: index.aliasIndex,
      promptIntentTerritory: c.promptIntentTerritory,
    });
    const selected = selectEntitiesForCandidate(mentions, {
      maxEntitiesPerResponse: options.maxEntitiesPerResponse ?? MAX_ENTITIES_PER_RESPONSE,
    });

    if (!selected.length) {
      const invalid = {
        ...c,
        reviewStatus: SUPERSEDED_INVALID_SUBJECT,
        supersededReason: "NO_CANONICAL_ENTITY_RESOLVED_FROM_RESPONSE",
        supersededAt: new Date().toISOString(),
        remediationVersion: REMEDIATION_VERSION,
        humanLabelled: false,
        llmLabelledAsGroundTruth: false,
        notReviewable: true,
        notPromotable: true,
        supersededBy: [],
        detectedMentionCount: mentions.length,
      };
      superseded.push(invalid);
      nominationFailures.push({
        caseId: c.caseId,
        reason: "NO_CANONICAL_ENTITY_RESOLVED_FROM_RESPONSE",
      });
      continue;
    }

    const children = selected.map((m) =>
      materializeEntityCase(c, m, mentions, { responseText: text })
    );
    created.push(...children);

    superseded.push({
      ...c,
      reviewStatus: SUPERSEDED_INVALID_SUBJECT,
      supersededReason: "NULL_SUBJECT_REPLACED_BY_ENTITY_SPECIFIC_CASES",
      supersededAt: new Date().toISOString(),
      remediationVersion: REMEDIATION_VERSION,
      humanLabelled: false,
      llmLabelledAsGroundTruth: false,
      notReviewable: true,
      notPromotable: true,
      supersededBy: children.map((x) => x.caseId),
      entitiesNominated: selected.map((m) => ({
        canonicalEntityId: m.canonicalEntityId,
        canonicalEntityName: m.canonicalEntityName,
        role: m.role,
      })),
    });
  }

  // Dedupe created caseIds
  const byId = new Map();
  for (const c of [...kept, ...created]) {
    if (!byId.has(c.caseId)) byId.set(c.caseId, c);
  }
  const activeCases = [...byId.values()].filter(isReviewableCandidate);
  const allCases = [...activeCases, ...superseded];

  const coverage = computeActiveCoverage(activeCases);
  const classDist = computeSystemSuggestionDistribution(activeCases);

  const outDoc = {
    version: GOLDEN_SET_V2_CANDIDATES_VERSION,
    previousVersion: doc.version || GOLDEN_SET_V2_CANDIDATES_VERSION,
    remediationVersion: REMEDIATION_VERSION,
    remediatedAt: new Date().toISOString(),
    caseCount: allCases.length,
    activeReviewCaseCount: activeCases.length,
    entitySpecificCaseCount: activeCases.filter(hasSubjectEntity).length,
    invalidSubjectCaseCount: activeCases.filter((c) => !hasSubjectEntity(c)).length,
    supersededInvalidCaseCount: superseded.length,
    humanLabelled: 0,
    llmLabelledAsGroundTruth: 0,
    reviewStatus: "PENDING_HUMAN_REVIEW",
    note:
      "Entity-nomination remediated. Null-subject response-level cases superseded. SYSTEM_SUGGESTION is not ground truth. No auto-review.",
    rootCause:
      "sampleGoldenSetCandidates null-entity placeholder when evidence.mentions empty (baseline providers).",
    coverageAudit: auditGoldenCoverage(activeCases),
    providerCoverage: coverage.providers,
    languageCoverage: coverage.languages,
    geographyCoverage: coverage.geographies,
    systemSuggestionClassificationCoverage: classDist,
    nominationFailures,
    before: {
      TOTAL: auditBefore.TOTAL_CANDIDATES,
      ENTITY_SPECIFIC: auditBefore.VALID_ENTITY_SPECIFIC_CASES,
      NULL_SUBJECT: auditBefore.NULL_SUBJECT_CASES,
    },
    after: {
      ACTIVE_REVIEW_CASES: activeCases.length,
      ENTITY_SPECIFIC_CASES: activeCases.filter(hasSubjectEntity).length,
      NULL_SUBJECT_ACTIVE_CASES: activeCases.filter((c) => !hasSubjectEntity(c)).length,
      SUPERSEDED_INVALID_CASES: superseded.length,
      CREATED_FROM_NULL: created.length,
    },
    cases: allCases,
  };

  if (outDoc.after.NULL_SUBJECT_ACTIVE_CASES !== 0) {
    const err = new Error("NULL_SUBJECT_ACTIVE_CASES_REMAIN");
    err.code = "NULL_SUBJECT_ACTIVE_CASES_REMAIN";
    err.doc = outDoc;
    throw err;
  }

  if (apply) {
    // Archive prior
    const archivePath = OUT_CANDIDATES.replace(
      /\.json$/,
      `.pre-entity-remediation-${new Date().toISOString().replace(/[:.]/g, "-")}.json`
    );
    if (fs.existsSync(OUT_CANDIDATES)) {
      fs.copyFileSync(OUT_CANDIDATES, archivePath);
      outDoc.archivedPriorPath = archivePath;
    }
    fs.writeFileSync(OUT_CANDIDATES, JSON.stringify(outDoc, null, 2), "utf8");
    outDoc.writtenPath = OUT_CANDIDATES;
  }

  return outDoc;
}

export function computeActiveCoverage(cases) {
  const providers = { OPENAI: 0, GEMINI: 0, PERPLEXITY: 0, CLAUDE: 0 };
  const languages = { EN: 0, ES: 0 };
  const geographies = {
    GLOBAL: 0,
    CALA: 0,
    MEXICO: 0,
    EUROPE: 0,
    NORTH_AMERICA: 0,
  };
  for (const c of cases || []) {
    const p = String(c.provider || "").toLowerCase();
    if (p === "openai") providers.OPENAI += 1;
    if (p === "gemini") providers.GEMINI += 1;
    if (p === "perplexity") providers.PERPLEXITY += 1;
    if (p === "claude") providers.CLAUDE += 1;
    const lang = String(c.language || "").toLowerCase();
    if (lang === "en") languages.EN += 1;
    if (lang === "es") languages.ES += 1;
    const geo = String(c.geography || "").toUpperCase();
    if (geographies[geo] != null) geographies[geo] += 1;
  }
  return { providers, languages, geographies };
}

export function computeSystemSuggestionDistribution(cases) {
  const dist = {
    FIRST_RECOMMENDATION: 0,
    RANKED_RECOMMENDATION: 0,
    EXPLICIT_RECOMMENDATION: 0,
    ASSOCIATED_OPTION: 0,
    COMPARATOR: 0,
    DISCUSSION: 0,
    PASSING_MENTION: 0,
    NEGATIVE_OR_QUALIFIED: 0,
    SOURCE_ONLY: 0,
    NO_MENTION: 0,
    NULL_OR_MISSING: 0,
  };
  for (const c of cases || []) {
    const role = c.systemSuggestion?.expectedRecommendationClass || null;
    if (!role) dist.NULL_OR_MISSING += 1;
    else if (role === "first_recommendation") dist.FIRST_RECOMMENDATION += 1;
    else if (role === "ranked_recommendation") dist.RANKED_RECOMMENDATION += 1;
    else if (role === "explicit_recommendation") dist.EXPLICIT_RECOMMENDATION += 1;
    else if (role === "associated_option") dist.ASSOCIATED_OPTION += 1;
    else if (role === "comparator") dist.COMPARATOR += 1;
    else if (role === "discussed") dist.DISCUSSION += 1;
    else if (role === "passing_mention") dist.PASSING_MENTION += 1;
    else if (role === "negative_or_qualified") dist.NEGATIVE_OR_QUALIFIED += 1;
    else if (role === "source_only") dist.SOURCE_ONLY += 1;
    else if (role === "no_mention") dist.NO_MENTION += 1;
    else dist.NULL_OR_MISSING += 1;
  }
  return dist;
}

/**
 * Export validation gate for active review candidates.
 */
export function validateActiveCandidatesForExport(doc = null, options = {}) {
  const source = doc || loadCandidateDocument();
  const active = (source.cases || []).filter(isReviewableCandidate);
  const entityIds = new Set(
    (options.entityIndex?.entities || []).map((e) => e.id || e.canonicalEntityId)
  );
  const failures = [];
  const caseIds = new Set();

  for (const c of active) {
    if (!hasSubjectEntity(c)) {
      failures.push({ code: "NO_NULL_SUBJECTS", caseId: c.caseId });
    }
    if (caseIds.has(c.caseId)) {
      failures.push({ code: "ALL_CASE_IDS_UNIQUE", caseId: c.caseId });
    }
    caseIds.add(c.caseId);
    if (!c.systemSuggestion) {
      failures.push({ code: "ALL_SYSTEM_SUGGESTIONS_PRESENT", caseId: c.caseId });
    }
    if (!c.responseId && !c.sourceResponseId) {
      failures.push({ code: "ALL_SOURCE_RESPONSE_IDS_RESOLVE", caseId: c.caseId });
    }
    if (entityIds.size && c.canonicalEntityId && !entityIds.has(c.canonicalEntityId)) {
      failures.push({
        code: "ALL_CANONICAL_IDS_RESOLVE",
        caseId: c.caseId,
        canonicalEntityId: c.canonicalEntityId,
      });
    }
    if (!c.candidateEntity && !c.canonicalEntityName) {
      failures.push({ code: "ALL_ENTITY_NAMES_RESOLVE", caseId: c.caseId });
    }
  }

  return {
    ok: failures.length === 0,
    activeCount: active.length,
    entitySpecificCaseCount: active.filter(hasSubjectEntity).length,
    invalidSubjectCaseCount: active.filter((c) => !hasSubjectEntity(c)).length,
    failures,
    NO_NULL_SUBJECTS: !failures.some((f) => f.code === "NO_NULL_SUBJECTS"),
    ALL_CASE_IDS_UNIQUE: !failures.some((f) => f.code === "ALL_CASE_IDS_UNIQUE"),
    ALL_SYSTEM_SUGGESTIONS_PRESENT: !failures.some(
      (f) => f.code === "ALL_SYSTEM_SUGGESTIONS_PRESENT"
    ),
  };
}

export { OUT_CANDIDATES };
