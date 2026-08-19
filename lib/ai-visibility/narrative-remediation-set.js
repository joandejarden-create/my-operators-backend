/**
 * Build sealed narrative remediation set from stored evidence.
 */

import { createHash } from "crypto";
import { loadPeerSetConfig, resolvePeerSetMembership, PEER_SET_ID_V2 } from "./peer-sets.js";
import { enrichEvidenceWithPromptMetadata } from "./associations/prompt-metadata-lookup.js";
import {
  classifyAssociationsFromEvidence,
  extractAssociationCandidatesFromEvidence,
} from "./associations/deterministic-extractor.js";
import { indexMentionsByEntityId } from "./associations/entity-binding.js";
import { mapAttributeToNarrativeFamily } from "./narrative-taxonomy.js";
import { PORTFOLIO_BRANDS } from "./narrative-intelligence.js";
import {
  caseId,
  FAMILY_ATTRIBUTE_IDS,
  oracleLabelRemediationCase,
  splitRemediationCases,
  PRIORITY_FAMILIES,
} from "./narrative-remediation-rules.js";

export const NARRATIVE_REMEDIATION_SET_VERSION = "ai_visibility_narrative_remediation_set_v1";

const TARGET_CASES = 80;
const HOLDOUT_RATIO = 0.28;

function peerNamesForEvidence(evidence, config) {
  const membership = resolvePeerSetMembership(
    { peerSetId: PEER_SET_ID_V2, commercialRegion: "CALA" },
    config
  );
  const mentions = evidence?.payload?.mentions || [];
  const names = mentions.map((m) => m.canonicalEntityName || m.rawMention).filter(Boolean);
  return names;
}

/**
 * Build remediation cases from existing evidence — not reusing 140-case oracle unchanged.
 */
export function buildNarrativeRemediationSet(evidence = [], options = {}) {
  const config = options.peerConfig || loadPeerSetConfig();
  const portfolioIds = new Set(Object.values(PORTFOLIO_BRANDS));
  const targetFamilies = options.families || PRIORITY_FAMILIES;
  const cases = [];
  const seen = new Set();

  for (const raw of evidence) {
    const ev = enrichEvidenceWithPromptMetadata(raw);
    const text = String(ev?.payload?.rawResponseText || "");
    if (!text.trim()) continue;

    const peerNames = peerNamesForEvidence(ev, config);
    const { candidates } = extractAssociationCandidatesFromEvidence(ev, {
      peerNames,
      includeDeferred: false,
    });
    const { publishable, researchOnly } = classifyAssociationsFromEvidence(ev, { peerNames });
    const allCandidates = [...publishable, ...researchOnly];

    for (const family of targetFamilies) {
      const attrIds = FAMILY_ATTRIBUTE_IDS[family] || [];
      for (const attrId of attrIds) {
        const hits = allCandidates.filter(
          (c) => c.attributeId === attrId && portfolioIds.has(c.entityId)
        );
        for (const hit of hits) {
          const sentenceText = hit.supportingSpan?.text || text.slice(0, 200);
          const seed = `${ev.evidenceId}|${hit.entityId}|${attrId}|${sentenceText.slice(0, 60)}`;
          if (seen.has(seed)) continue;
          seen.add(seed);

          const entityMap = indexMentionsByEntityId(ev.payload?.mentions || []);
          const entity = entityMap.get(hit.entityId);
          const oracle = oracleLabelRemediationCase({
            family,
            sentenceText,
            entityName: hit.entityName,
            entityBinding: hit.entityBinding,
            promptText: ev.promptText || "",
            peerNames,
            entity,
            attributeId: attrId,
          });

          cases.push({
            caseId: caseId(seed),
            evidenceId: ev.evidenceId,
            responseId: ev.responseId,
            promptId: ev.promptId,
            provider: ev.provider,
            language: ev.language,
            entityId: hit.entityId,
            entityName: hit.entityName,
            attributeId: attrId,
            narrativeFamily: family,
            sentenceText,
            entityBinding: hit.entityBinding,
            promptText: ev.promptText || "",
            peerNames,
            oracleLabel: oracle.label,
            oracleConfidence: oracle.confidence,
            caseType: "candidate_hit",
          });
        }

        // Hard negatives: attribute language in response but wrong binding
        if (allCandidates.length === 0 && attrIds.includes(attrId)) continue;
      }
    }

    // Prompt echo negatives
    for (const family of targetFamilies) {
      const promptText = String(ev.promptText || "");
      if (!promptText.trim()) continue;
      const fakeSentence = promptText.slice(0, 180);
      const portfolioMention = (ev.payload?.mentions || []).find((m) =>
        portfolioIds.has(m.canonicalEntityId)
      );
      if (!portfolioMention) continue;
      const seed = `prompt_echo|${ev.evidenceId}|${family}`;
      if (seen.has(seed)) continue;
      if (!text.toLowerCase().includes(fakeSentence.slice(0, 40).toLowerCase())) {
        const oracle = oracleLabelRemediationCase({
          family,
          sentenceText: fakeSentence,
          entityName: portfolioMention.canonicalEntityName,
          entityBinding: "span_not_in_response",
          promptText,
          peerNames,
          attributeId: FAMILY_ATTRIBUTE_IDS[family]?.[0],
        });
        if (oracle.label === "NEGATIVE_PROMPT_ECHO") {
          seen.add(seed);
          cases.push({
            caseId: caseId(seed),
            evidenceId: ev.evidenceId,
            responseId: ev.responseId,
            promptId: ev.promptId,
            narrativeFamily: family,
            sentenceText: fakeSentence,
            entityBinding: "span_not_in_response",
            entityId: portfolioMention.canonicalEntityId,
            entityName: portfolioMention.canonicalEntityName,
            promptText,
            oracleLabel: oracle.label,
            oracleConfidence: oracle.confidence,
            caseType: "prompt_echo_negative",
          });
        }
      }
    }
  }

  // Balance toward target — prioritize diverse labels and families
  const byFamily = new Map();
  for (const c of cases) {
    const k = `${c.narrativeFamily}|${c.oracleLabel}`;
    if (!byFamily.has(k)) byFamily.set(k, []);
    byFamily.get(k).push(c);
  }

  const selected = [];
  const perBucket = Math.ceil(TARGET_CASES / (targetFamilies.length * 3));
  for (const [, bucket] of byFamily.entries()) {
    selected.push(...bucket.slice(0, perBucket));
  }
  const finalCases = selected.slice(0, Math.max(60, Math.min(100, selected.length)));
  if (finalCases.length < cases.length) {
    for (const c of cases) {
      if (finalCases.length >= 80) break;
      if (!finalCases.find((x) => x.caseId === c.caseId)) finalCases.push(c);
    }
  }

  /** Stratified sealed split — each family gets its own holdout slice. */
  const devCases = [];
  const holdoutCases = [];
  for (const family of targetFamilies) {
    const familyRows = finalCases
      .filter((c) => c.narrativeFamily === family)
      .sort((a, b) => String(a.caseId).localeCompare(String(b.caseId)));
    const holdoutN = Math.max(1, Math.round(familyRows.length * HOLDOUT_RATIO));
    holdoutCases.push(...familyRows.slice(0, holdoutN));
    devCases.push(...familyRows.slice(holdoutN));
  }
  // Include any non-priority-family rows entirely in DEV
  for (const c of finalCases) {
    if (!targetFamilies.includes(c.narrativeFamily)) devCases.push(c);
  }

  const split = {
    dev: devCases,
    holdout: holdoutCases,
    holdoutRatio: HOLDOUT_RATIO,
    sealedAt: new Date().toISOString(),
  };

  const labelCounts = {};
  for (const c of finalCases) {
    labelCounts[c.oracleLabel] = (labelCounts[c.oracleLabel] || 0) + 1;
  }

  return {
    version: NARRATIVE_REMEDIATION_SET_VERSION,
    TOTAL: finalCases.length,
    DEV: split.dev.length,
    HOLDOUT: split.holdout.length,
    HOLDOUT_RATIO,
    labelCounts,
    devCases: split.dev,
    holdoutCases: split.holdout,
    sealedAt: split.sealedAt,
  };
}

/**
 * Family collision audit on remediation cases.
 */
export function buildFamilyCollisionMatrix(cases = []) {
  const pairs = [
    ["SOFT_BRAND_INDIVIDUALITY", "OWNER_FLEXIBILITY_CONTROL"],
    ["SOFT_BRAND_INDIVIDUALITY", "DESIGN_LOCAL_CHARACTER"],
    ["DESIGN_LOCAL_CHARACTER", "BRAND_POSITIONING"],
    ["OWNER_FLEXIBILITY_CONTROL", "BRAND_POSITIONING"],
  ];

  const matrix = {};
  for (const [a, b] of pairs) {
    const key = `${a}_VS_${b}`.replace(/BRAND_POSITIONING/, "POSITIONING");
    const aCases = cases.filter((c) => c.narrativeFamily === a && c.oracleLabel?.startsWith("POSITIVE"));
    const bCases = cases.filter((c) => c.narrativeFamily === b && c.oracleLabel?.startsWith("POSITIVE"));
    const overlapSeeds = new Set(
      aCases.map((c) => `${c.evidenceId}|${c.sentenceText?.slice(0, 50)}`)
    );
    const overlap = bCases.filter((c) =>
      overlapSeeds.has(`${c.evidenceId}|${c.sentenceText?.slice(0, 50)}`)
    ).length;

    const distinct = overlap === 0 || overlap / Math.max(aCases.length, 1) < 0.15;
    matrix[key] = {
      DISTINCT: distinct ? "YES" : "NO",
      COMMON_FALSE_POSITIVE_PATTERN: distinct
        ? "minimal overlap on positive oracle cases"
        : "shared soft-brand / design / flexibility language in same spans",
      RULE_REQUIRED: distinct
        ? "maintain separate semantic gates"
        : "prefer single family for executive; demote weaker family to DETAIL_ONLY",
      overlapCount: overlap,
    };
  }
  return matrix;
}

export { splitRemediationCases, PRIORITY_FAMILIES };
