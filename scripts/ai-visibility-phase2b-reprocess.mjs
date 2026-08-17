#!/usr/bin/env node
/**
 * Phase 2B — reprocess stored Phase 2A responses (no provider calls).
 *
 * Uses:
 *   - data/ai-visibility/runtime/phase2a/store/responses/*
 *   - cohort-universe.json for entity SSOT snapshot
 *   - runtime alias overlay + resolver/classifier/citation assoc v2/v1
 *
 * Writes:
 *   data/ai-visibility/runtime/phase2b/reprocess-report.json
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  buildAiVisibilityEntityIndex,
  extractMentions,
  extractCitations,
  buildObservationFromExtractions,
  computeAiPresenceRate,
  computeRecommendationShare,
  computeFirstRecommendationRate,
  computeQuestionsWon,
  computeQuestionsMissing,
  computeCompetitivePosition,
  computeCitationRate,
  assembleEvidenceRecord,
  metricEvidenceTrace,
  harvestUnresolvedWithFilterStats,
  classifyMentionRole,
  normalizeMatchKey,
  assessMetricReadiness,
  METRIC_VERSION,
  RESOLVER_VERSION,
  RECOMMENDATION_CLASSIFIER_VERSION,
  CITATION_ASSOC_VERSION,
  UNRESOLVED_FILTER_VERSION,
  loadRuntimeAliasOverlay,
} from "../lib/ai-visibility/index.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const PHASE2A = path.join(ROOT, "data", "ai-visibility", "runtime", "phase2a");
const PHASE2B = path.join(ROOT, "data", "ai-visibility", "runtime", "phase2b");
const COHORT_PATH = path.join(ROOT, "fixtures", "ai-visibility", "phase2a-cohort.json");

function readJson(p) {
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

function writeJson(p, value) {
  fs.mkdirSync(path.dirname(p), { recursive: true });
  fs.writeFileSync(p, JSON.stringify(value, null, 2), "utf8");
}

function knownNameKeys(entities) {
  const keys = new Set();
  for (const e of entities) {
    keys.add(normalizeMatchKey(e.name));
    for (const a of e.aliases || []) keys.add(normalizeMatchKey(a));
  }
  return keys;
}

function roleCounts(mentions) {
  const counts = {
    canonicalMentions: 0,
    first_recommendation: 0,
    explicit_recommendation: 0,
    ranked_recommendation: 0,
    comparator: 0,
    passing_mention: 0,
    negative_or_qualified: 0,
    discussed: 0,
    source_only: 0,
    explicitRecommendations: 0,
  };
  for (const m of mentions) {
    if (!m.canonicalEntityId) continue;
    counts.canonicalMentions += 1;
    const role = m.role || classifyMentionRole(m);
    if (counts[role] != null) counts[role] += 1;
    if (m.explicitRecommendation) counts.explicitRecommendations += 1;
  }
  return counts;
}

function loadBaselineBefore() {
  const reportPath = path.join(PHASE2A, "phase2a-validation-report.json");
  if (!fs.existsSync(reportPath)) {
    throw new Error(`Missing Phase 2A baseline report at ${reportPath}`);
  }
  const report = readJson(reportPath);
  return {
    metricVersion: report.metrics?.metricVersion || "ai_visibility_metrics_v1",
    entityIndexFingerprint: report.universe?.entityIndexFingerprint || null,
    runStats: report.runStats,
    mentionClassification: report.mentionClassification,
    citationReview: report.citationReview,
    entityMatchQuality: report.entityMatchQuality,
    observations: report.observations || [],
  };
}

function providerCitationsFromResponse(response) {
  const fromRaw = [];
  for (const item of response.raw?.output || []) {
    if (item?.type !== "message") continue;
    for (const block of item.content || []) {
      for (const ann of block.annotations || []) {
        if (ann?.type !== "url_citation") continue;
        fromRaw.push({
          url: ann.url,
          title: ann.title || null,
          startIndex: ann.start_index ?? null,
          endIndex: ann.end_index ?? null,
          providerSupplied: true,
        });
      }
    }
  }
  if (fromRaw.length) return fromRaw;
  return (response.citations || []).map((c) => ({
    ...c,
    startIndex: c.startIndex ?? c.start_index ?? null,
    endIndex: c.endIndex ?? c.end_index ?? null,
  }));
}

function processResponse({ response, prompt, index, run }) {
  const mentions = extractMentions({
    responseId: response.responseId,
    text: response.text,
    entityIndex: index.aliasIndex,
  });
  const citations = extractCitations({
    responseId: response.responseId,
    providerCitations: providerCitationsFromResponse(response),
    entities: index.entities,
    mentions,
    responseText: response.text,
  });
  const observation = buildObservationFromExtractions({
    observationId: `obs_${response.responseId}_p2b`,
    promptId: prompt.promptId,
    provider: response.provider,
    periodKey: "phase2b_reprocess",
    success: true,
    mentions,
    citations,
  });
  const unresolved = harvestUnresolvedWithFilterStats(
    response.text,
    knownNameKeys(index.entities)
  );
  const evidence = assembleEvidenceRecord({
    prompt,
    run: run || {
      runId: response.runId || `run_re_${response.responseId}`,
      promptId: prompt.promptId,
      promptVersion: prompt.version,
      provider: response.provider,
      model: response.model,
    },
    response,
    mentions,
    citations,
    metrics: { observation },
  });
  return { mentions, citations, observation, unresolved, evidence };
}

function main() {
  const baseline = loadBaselineBefore();
  const universeSnap = readJson(path.join(PHASE2A, "cohort-universe.json"));
  const cohort = readJson(COHORT_PATH);
  const meta = readJson(path.join(PHASE2A, "cohort-rows-meta.json"));
  const overlay = loadRuntimeAliasOverlay();

  const index = buildAiVisibilityEntityIndex({
    brands: universeSnap.brands,
    operators: universeSnap.operators,
    applyOverlay: true,
    runtimeOverlay: overlay,
  });

  const promptById = new Map(cohort.prompts.map((p) => [p.promptId, p]));
  const rows = [];
  let rawUnresolved = 0;
  let filteredUnresolved = 0;
  let citationsTotal = 0;
  let citationsAssociated = 0;
  let firstPartyAssociated = 0;
  let thirdPartyAssociated = 0;

  for (const responseId of meta.responseIds) {
    const responsePath = path.join(PHASE2A, "store", "responses", `${responseId}.json`);
    if (!fs.existsSync(responsePath)) {
      throw new Error(`Missing stored response ${responseId}`);
    }
    const response = readJson(responsePath);
    const runId = response.runId;
    let run = null;
    if (runId) {
      const runPath = path.join(PHASE2A, "store", "runs", `${runId}.json`);
      if (fs.existsSync(runPath)) run = readJson(runPath);
    }
    const promptId = response.promptId || run?.promptId;
    const prompt = promptById.get(promptId);
    if (!prompt) {
      throw new Error(`No cohort prompt for ${promptId} (${responseId})`);
    }
    const processed = processResponse({ response, prompt, index, run });
    rawUnresolved += processed.unresolved.rawUnresolvedCount;
    filteredUnresolved += processed.unresolved.filteredUnresolvedCount;
    citationsTotal += processed.citations.length;
    for (const c of processed.citations) {
      if (c.entityAssociation) {
        citationsAssociated += 1;
        if (c.firstParty) firstPartyAssociated += 1;
        else thirdPartyAssociated += 1;
      }
    }
    rows.push({
      prompt,
      response,
      run,
      ...processed,
      summary: {
        promptId: prompt.promptId,
        brandsDetected: [
          ...new Set(
            processed.mentions
              .filter((m) => m.entityType === "brand")
              .map((m) => m.canonicalEntityName)
          ),
        ],
        operatorsDetected: [
          ...new Set(
            processed.mentions
              .filter((m) => m.entityType === "operator")
              .map((m) => m.canonicalEntityName)
          ),
        ],
        recommendationOrder: [
          ...new Set(
            processed.mentions
              .filter((m) => m.explicitRecommendation && m.canonicalEntityId)
              .sort(
                (a, b) =>
                  (a.recommendationPosition || 99) - (b.recommendationPosition || 99)
              )
              .map((m) => m.canonicalEntityName)
          ),
        ],
        roles: roleCounts(processed.mentions),
        citationsAssociated: processed.citations.filter((c) => c.entityAssociation).length,
        unresolvedRaw: processed.unresolved.rawUnresolvedCount,
        unresolvedFiltered: processed.unresolved.filteredUnresolvedCount,
        evidenceId: processed.evidence.evidenceId,
      },
    });
  }

  const afterRoles = rows.reduce(
    (acc, r) => {
      const c = r.summary.roles;
      for (const k of Object.keys(c)) acc[k] = (acc[k] || 0) + c[k];
      return acc;
    },
    {}
  );

  const peerIds = index.entities.map((e) => e.id);
  const observations = rows.map((r) => r.observation);
  const promptIds = [...new Set(rows.map((r) => r.prompt.promptId))];
  const byEntity = {};
  for (const id of peerIds) {
    const ent = index.entities.find((e) => e.id === id);
    byEntity[ent.name] = {
      id,
      entityType: ent.entityType,
      presence: computeAiPresenceRate(observations, id).value,
      recommendationShare: computeRecommendationShare(observations, id).value,
      firstRecommendationRate: computeFirstRecommendationRate(observations, id).value,
      questionsWon: computeQuestionsWon(observations, id, promptIds).value,
      questionsMissing: computeQuestionsMissing(observations, id, promptIds).value,
      citationRate: computeCitationRate(observations, id).value,
    };
  }

  const competitivePosition = computeCompetitivePosition(observations, peerIds);
  const competitiveNamed = competitivePosition.peers.map((p) => {
    const ent = index.entities.find((e) => e.id === p.entityId);
    return { name: ent?.name, ...p };
  });

  const noiseReductionPercent =
    rawUnresolved === 0
      ? 0
      : Math.round(((rawUnresolved - filteredUnresolved) / rawUnresolved) * 1000) / 10;

  const evidenceTraces = [];
  for (const row of rows) {
    if (evidenceTraces.length >= 5) break;
    const entityId = row.observation.presentEntityIds[0];
    if (!entityId) continue;
    const presence = computeAiPresenceRate([row.observation], entityId);
    evidenceTraces.push({
      ...metricEvidenceTrace({
        metricResult: presence,
        evidenceId: row.evidence.evidenceId,
        observationIds: [row.observation.observationId],
      }),
      chain: {
        metric: presence.metric,
        observationId: row.observation.observationId,
        evidenceId: row.evidence.evidenceId,
        promptId: row.prompt.promptId,
        promptVersion: row.prompt.version,
        runId: row.run?.runId || row.response.runId,
        responseId: row.response.responseId,
        rawExcerpt: String(row.response.text || "").slice(0, 180),
        mentionIds: row.mentions.map((m) => m.mentionId),
        citationIds: row.citations.map((c) => c.citationId),
        originalStoredResponse: true,
      },
    });
  }

  const report = {
    label: "CONTROLLED VALIDATION SAMPLE — REPROCESSED, NOT PRODUCTION BENCHMARK",
    generatedAt: new Date().toISOString(),
    baseline: {
      BASELINE_METRIC_VERSION: baseline.metricVersion,
      BASELINE_ENTITY_INDEX_FINGERPRINT: baseline.entityIndexFingerprint,
      mentionClassification: baseline.mentionClassification,
      citationReview: baseline.citationReview,
      entityMatchQuality: baseline.entityMatchQuality,
    },
    versions: {
      ENTITY_RESOLVER_VERSION: RESOLVER_VERSION,
      RECOMMENDATION_CLASSIFIER_VERSION,
      CITATION_ASSOC_VERSION,
      UNRESOLVED_FILTER_VERSION,
      METRIC_VERSION,
    },
    overlay: {
      overlayId: overlay.overlayId,
      applied: index.overlayMeta?.applied || [],
      founderReviewOnly: index.overlayMeta?.founderReviewOnly || [],
      AIRTABLE_ALIAS_WRITES: 0,
    },
    entityIndex: {
      fingerprintBefore: baseline.entityIndexFingerprint,
      fingerprintAfter: index.fingerprint,
      entityCount: index.meta.entityCount,
    },
    unresolved: {
      RAW_UNRESOLVED_BEFORE: baseline.entityMatchQuality?.brands?.unresolvedCandidates ?? null,
      RAW_UNRESOLVED_AFTER_HARVEST: rawUnresolved,
      FILTERED_MEANINGFUL_UNRESOLVED: filteredUnresolved,
      NOISE_REDUCTION_PERCENT: noiseReductionPercent,
    },
    mentions: {
      BEFORE: baseline.mentionClassification,
      AFTER: afterRoles,
    },
    citations: {
      TOTAL_CITATIONS: citationsTotal,
      ASSOCIATED_BEFORE: Math.max(
        0,
        (baseline.citationReview?.providerSupplied || 0) -
          (baseline.citationReview?.unresolvedEntityAssociation || 0)
      ),
      ASSOCIATED_AFTER: citationsAssociated,
      FIRST_PARTY_ASSOCIATED: firstPartyAssociated,
      THIRD_PARTY_ASSOCIATED: thirdPartyAssociated,
      UNRESOLVED: citationsTotal - citationsAssociated,
    },
    metrics: {
      label: "CONTROLLED VALIDATION SAMPLE — REPROCESSED, NOT PRODUCTION BENCHMARK",
      metricVersion: METRIC_VERSION,
      competitivePosition: competitiveNamed,
      byEntity,
    },
    evidenceTraces,
    responseSummaries: rows.map((r) => r.summary),
    metricReadiness: assessMetricReadiness({
      classificationIntegrity: true,
      citationAssociationCompleteness: "partial",
      testCoverage: true,
      parentBrandCollisions: 0,
      manualClassificationAccuracy: null,
    }),
  };

  writeJson(path.join(PHASE2B, "reprocess-report.json"), report);
  writeJson(path.join(PHASE2B, "reprocess-rows-lite.json"), {
    responseIds: rows.map((r) => r.response.responseId),
    evidenceIds: rows.map((r) => r.evidence.evidenceId),
  });

  console.log(
    JSON.stringify(
      {
        responsesReprocessed: rows.length,
        providerCalls: 0,
        unresolved: report.unresolved,
        mentionsAfter: afterRoles,
        citations: report.citations,
        overlayApplied: report.overlay.applied.map((a) => a.alias),
        reportPath: path.join(PHASE2B, "reprocess-report.json"),
      },
      null,
      2
    )
  );
}

main();
