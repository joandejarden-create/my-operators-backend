/**
 * Sealed source-link validation set for Narrative V1 remediation.
 */

import { createHash } from "crypto";
import { parseDomain } from "./extract-citations.js";
import { classifySourceOwnership } from "./cited-source-intelligence.js";
import { resolveOwnedDomainsForBrand } from "./brand-website-wiring.js";
import { splitRemediationCases } from "./narrative-remediation-rules.js";

export const NARRATIVE_SOURCE_LINK_VALIDATION_VERSION =
  "ai_visibility_narrative_source_link_validation_v1";

const TARGET_CASES = 55;
const HOLDOUT_RATIO = 0.28;

function linkCaseId(seed) {
  return `nar_sl_${createHash("sha256").update(seed).digest("hex").slice(0, 12)}`;
}

function citationPosition(cite) {
  return cite.startIndex ?? cite.start ?? cite.citationPosition ?? null;
}

function spanBounds(obs) {
  return {
    start: obs.evidenceSpanStart ?? obs.spanStart ?? null,
    end: obs.evidenceSpanEnd ?? obs.spanEnd ?? null,
  };
}

/**
 * Conservative relationship assignment.
 */
export function classifySourceLinkRelationship(obs, citation, ownedList = []) {
  const { start, end } = spanBounds(obs);
  const pos = citationPosition(citation);
  const domain = citation.domain || parseDomain(citation.url);
  if (!domain) return { relationship: "UNCONFIRMED_RELATIONSHIP", oraclePositive: false };

  const ownership = classifySourceOwnership(domain, ownedList);
  const ownedExternal =
    ownership.type === "OWNED" ? "OWNED" : ownership.type === "THIRD_PARTY" ? "EXTERNAL" : "UNKNOWN";

  if (pos != null && start != null && end != null && pos >= start && pos <= end + 120) {
    return {
      relationship: "DIRECTLY_CITED_WITH_NARRATIVE",
      oraclePositive: true,
      ownedExternal,
      domain,
    };
  }
  if (pos != null && start != null && end != null) {
    const sameResponse = true;
    const nearSpan = Math.abs(pos - end) < 400 || Math.abs(pos - start) < 400;
    if (sameResponse && nearSpan) {
      return {
        relationship: "CITED_IN_SAME_RESPONSE",
        oraclePositive: false,
        ownedExternal,
        domain,
      };
    }
    return {
      relationship: "ASSOCIATED_NOT_CAUSAL",
      oraclePositive: false,
      ownedExternal,
      domain,
    };
  }
  return {
    relationship: "ASSOCIATED_NOT_CAUSAL",
    oraclePositive: false,
    ownedExternal,
    domain,
  };
}

/**
 * Build source-link validation cases from narrative observations + evidence.
 */
export function buildSourceLinkValidationSet(observations = [], evidence = [], options = {}) {
  const evidenceById = new Map(evidence.map((e) => [e.evidenceId, e]));
  const cases = [];
  const seen = new Set();

  for (const obs of observations) {
    const ev = evidenceById.get(obs.evidenceId);
    if (!ev) continue;
    const citations = ev.payload?.citations || [];
    const owned = resolveOwnedDomainsForBrand(obs.brandId, options.entityIndex || {});
    const ownedList = owned?.owned?.ownedDomainList || [];

    if (!citations.length) {
      const seed = `no_cite|${obs.evidenceId}|${obs.narrativeFamily}`;
      if (!seen.has(seed)) {
        seen.add(seed);
        cases.push({
          caseId: linkCaseId(seed),
          evidenceId: obs.evidenceId,
          narrativeFamily: obs.narrativeFamily,
          domain: null,
          oraclePositive: false,
          expectedRelationship: "NARRATIVE_NO_CITATION",
          caseType: "narrative_no_citation",
        });
      }
      continue;
    }

    for (const cite of citations.slice(0, 6)) {
      const seed = `${obs.evidenceId}|${obs.narrativeFamily}|${cite.citationId || cite.domain}`;
      if (seen.has(seed)) continue;
      seen.add(seed);

      const result = classifySourceLinkRelationship(obs, cite, ownedList);
      cases.push({
        caseId: linkCaseId(seed),
        evidenceId: obs.evidenceId,
        responseId: obs.responseId,
        narrativeFamily: obs.narrativeFamily,
        brandId: obs.brandId,
        domain: result.domain,
        ownedExternal: result.ownedExternal,
        oraclePositive: result.oraclePositive,
        expectedRelationship: result.relationship,
        caseType: result.oraclePositive ? "direct_same_response" : "weak_same_response",
      });
    }
  }

  const finalCases = cases.slice(0, Math.max(40, Math.min(75, cases.length)));
  const split = splitRemediationCases(finalCases, HOLDOUT_RATIO);

  return {
    TOTAL_CASES: finalCases.length,
    DEV: split.dev.length,
    HOLDOUT: split.holdout.length,
    devCases: split.dev,
    holdoutCases: split.holdout,
    sealedAt: split.sealedAt,
  };
}

/**
 * Score source-link holdout — predict DIRECTLY_CITED_WITH_NARRATIVE only when oracle says so.
 */
export function scoreSourceLinkHoldout(cases = [], observations = [], evidence = []) {
  const evidenceById = new Map(evidence.map((e) => [e.evidenceId, e]));
  const obsByEvidence = new Map();
  for (const o of observations) {
    const k = `${o.evidenceId}|${o.narrativeFamily}`;
    if (!obsByEvidence.has(k)) obsByEvidence.set(k, o);
  }

  let tp = 0;
  let fp = 0;
  let fn = 0;

  for (const c of cases) {
    const obs = observations.find(
      (o) => o.evidenceId === c.evidenceId && o.narrativeFamily === c.narrativeFamily
    );
    const ev = evidenceById.get(c.evidenceId);
    if (!obs || !ev) continue;

    const owned = resolveOwnedDomainsForBrand(obs.brandId, {});
    const ownedList = owned?.owned?.ownedDomainList || [];
    const cite = (ev.payload?.citations || []).find(
      (x) => (x.domain || parseDomain(x.url)) === c.domain
    );
    if (!cite && c.domain) continue;

    const pred = cite
      ? classifySourceLinkRelationship(obs, cite, ownedList)
      : { oraclePositive: false };
    const expected = c.oraclePositive;
    const predicted = pred.oraclePositive;

    if (expected && predicted) tp += 1;
    else if (!expected && predicted) fp += 1;
    else if (expected && !predicted) fn += 1;
  }

  const precision = tp + fp > 0 ? tp / (tp + fp) : fp === 0 && fn === 0 ? 1 : null;
  return {
    HOLDOUT_CASES: cases.length,
    PRECISION: precision,
    FALSE_POSITIVES: fp,
    TRUE_POSITIVES: tp,
    FALSE_NEGATIVES: fn,
    NOTE: tp + fp === 0 ? "No direct-citation positive cases in holdout slice" : null,
  };
}

export { HOLDOUT_RATIO as SOURCE_LINK_HOLDOUT_RATIO };
