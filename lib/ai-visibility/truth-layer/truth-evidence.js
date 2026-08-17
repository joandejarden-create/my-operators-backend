/**
 * Truth evidence pipeline — read-only corpus + comparison batch (P0D-A).
 */

import { auditAssociationEvidenceCorpus } from "../associations/evidence-corpus-audit.js";
import { validateEntityBinding } from "../associations/entity-binding.js";
import { validateSupportingSpan } from "../associations/span-validation.js";
import { extractTruthClaimsFromEvidence } from "./truth-claim-extractor.js";
import { compareTruthClaim } from "./truth-comparison.js";
import { loadBrandBasicsTruthIndex } from "./brand-basics-truth.js";

/**
 * Build entity object for claim extraction from basics row.
 */
export function entityFromBasicsRow(row) {
  if (!row?.brandId) return null;
  return {
    id: row.brandId,
    name: row.brandName,
    aliases: [],
  };
}

/**
 * Filter evidence mentioning a brand (by entity id in mentions).
 */
export function evidenceForBrand(evidence = [], brandId) {
  return evidence.filter((ev) =>
    (ev.payload?.mentions || []).some((m) => m.entityId === brandId || m.canonicalEntityId === brandId)
  );
}

/**
 * Extract + compare truth claims for one brand across evidence.
 */
export function processBrandTruthClaims(brandId, evidence = [], basicsIndex, options = {}) {
  const row = basicsIndex.byId.get(brandId);
  const entity = entityFromBasicsRow(row);
  if (!entity) return { claims: [], comparisons: [] };

  const peerParentKeys = (options.peerParentKeys || []).map(String);
  const brandEvidence = evidenceForBrand(evidence, brandId);
  const claims = [];
  const comparisons = [];

  for (const ev of brandEvidence) {
    const extracted = extractTruthClaimsFromEvidence(ev, entity, options);
    for (const claim of extracted) {
      claims.push(claim);
      comparisons.push(compareTruthClaim(claim, basicsIndex.byId, { peerParentKeys }));
    }
  }

  return { claims, comparisons, entity, brandEvidenceCount: brandEvidence.length };
}

/**
 * Full truth pipeline from store/corpus.
 * @param {object} [options]
 */
export async function runTruthEvidencePipeline(options = {}) {
  const corpus = options.corpus || (await auditAssociationEvidenceCorpus(options.corpusOptions || {}));
  const basicsIndex = options.basicsIndex || (await loadBrandBasicsTruthIndex({
    fixtureOnly: options.fixtureOnly !== false,
    loadLive: options.loadLive === true,
  }));

  const brandIds = options.brandIds || [...basicsIndex.byId.keys()].filter((id) => {
    const row = basicsIndex.byId.get(id);
    return row?.activeLive;
  });

  const allClaims = [];
  const allComparisons = [];
  const byBrand = {};

  for (const brandId of brandIds) {
    const result = processBrandTruthClaims(brandId, corpus.evidence, basicsIndex, options);
    byBrand[brandId] = result;
    allClaims.push(...result.claims);
    allComparisons.push(...result.comparisons);
  }

  return {
    corpus,
    basicsIndex,
    brandIds,
    claims: allClaims,
    comparisons: allComparisons,
    byBrand,
    NEW_PROVIDER_CALLS: 0,
    CENSUS_READS_FOR_TRUTH: 0,
    AIRTABLE_WRITES: 0,
  };
}

/**
 * Validate explicit claim requirements.
 */
export function validateExplicitClaim(claim, evidenceText = "") {
  const span = claim?.supportingSpan;
  if (!span?.exactText && !span?.text) {
    return { valid: false, reason: "missing_span" };
  }
  const spanResult = validateSupportingSpan(evidenceText, {
    start: span.start,
    end: span.end,
    exactText: span.exactText || span.text,
  });
  if (!spanResult.valid) {
    return { valid: false, reason: spanResult.failureMode || "invalid_span" };
  }
  return { valid: true };
}

/**
 * Validate entity binding for a claim.
 */
export function validateClaimEntityBinding(claim, entity, evidence) {
  const text = String(evidence?.payload?.rawResponseText || "");
  const span = claim.supportingSpan;
  if (!span) return { ok: false, reason: "no_span" };
  return validateEntityBinding({
    text,
    spanStart: span.start,
    spanEnd: span.end,
    entity,
    mentions: evidence?.payload?.mentions || [],
  });
}
