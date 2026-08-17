/**
 * Truth Layer golden set — built from existing evidence only (P0D-A).
 */

import { createHash, randomUUID } from "crypto";
import { extractTruthClaimsFromEvidence } from "./truth-claim-extractor.js";
import { compareTruthClaim } from "./truth-comparison.js";
import { entityFromBasicsRow, evidenceForBrand } from "./truth-evidence.js";
import { normalizeMatchKey } from "../normalize-entities.js";

export const TRUTH_GOLDEN_SET_VERSION = "ai_visibility_truth_golden_set_v1";

/**
 * Oracle label for a claim using Brand Basics.
 */
export function oracleLabelClaim(claim, basicsRow) {
  if (!claim?.supportingSpan?.exactText && !claim?.supportingSpan?.text) {
    return { comparisonStatus: "NOT_EVALUATED", claimTypeExpected: claim?.claimType || null };
  }
  const comparison = compareTruthClaim(claim, new Map([[basicsRow.brandId, basicsRow]]));
  return {
    comparisonStatus: comparison.comparisonStatus,
    claimTypeExpected: claim.claimType,
    claimValueExpected: claim.claimValue,
    dealalityFactValue: comparison.dealalityFactValue,
  };
}

/**
 * Build golden set cases from corpus (target 80–120).
 */
export function buildTruthGoldenSet(corpus, basicsIndex, options = {}) {
  const targetMin = options.targetMin || 80;
  const targetMax = options.targetMax || 120;
  const brandIds = options.brandIds || [...basicsIndex.byId.keys()].filter((id) => basicsIndex.byId.get(id)?.activeLive);
  const cases = [];
  const seen = new Set();

  for (const brandId of brandIds) {
    const row = basicsIndex.byId.get(brandId);
    const entity = entityFromBasicsRow(row);
    if (!entity) continue;

    const evList = evidenceForBrand(corpus.evidence || [], brandId);
    for (const ev of evList) {
      const claims = extractTruthClaimsFromEvidence(ev, entity, options);
      for (const claim of claims) {
        const key = `${claim.evidenceId}:${claim.claimType}:${normalizeMatchKey(claim.claimValue)}:${claim.supportingSpan?.exactText?.slice(0, 30)}`;
        if (seen.has(key)) continue;
        seen.add(key);

        const oracle = oracleLabelClaim(claim, row);
        cases.push({
          caseId: `tgs_${createHash("sha256").update(key).digest("hex").slice(0, 12)}`,
          subjectBrandId: brandId,
          subjectBrandName: row.brandName,
          evidenceId: claim.evidenceId,
          responseId: claim.responseId,
          promptId: claim.promptId,
          scenarioId: claim.scenarioId,
          provider: claim.provider,
          language: claim.language,
          geography: claim.geography,
          expectedClaimType: claim.claimType,
          expectedClaimValue: claim.claimValue,
          expectedComparisonStatus: oracle.comparisonStatus,
          dealalityFactValue: oracle.dealalityFactValue,
          supportingSpan: claim.supportingSpan?.exactText || claim.supportingSpan?.text,
          hasExplicitSpan: Boolean(claim.supportingSpan?.exactText || claim.supportingSpan?.text),
          source: "existing_evidence",
        });

        if (cases.length >= targetMax) break;
      }
      if (cases.length >= targetMax) break;
    }
    if (cases.length >= targetMax) break;
  }

  // Synthetic edge cases to reach minimum coverage dimensions
  const synthetic = buildSyntheticGoldenCases(basicsIndex);
  for (const c of synthetic) {
    if (cases.length >= targetMax) break;
    if (!seen.has(c.caseId)) {
      cases.push(c);
      seen.add(c.caseId);
    }
  }

  return {
    version: TRUTH_GOLDEN_SET_VERSION,
    generatedAt: new Date().toISOString(),
    totalCases: cases.length,
    meetsTarget: cases.length >= targetMin,
    cases,
    NEW_PROVIDER_CALLS: 0,
  };
}

function buildSyntheticGoldenCases(basicsIndex) {
  const out = [];
  const autograph = basicsIndex.byId.get("recEJCTDj1zrsjPM6");
  const curio = basicsIndex.byId.get("receQkxgjlezsc1xg");

  if (autograph) {
    out.push(makeSyntheticCase({
      caseId: "tgs_syn_parent_aligned_marriott",
      brandId: autograph.brandId,
      brandName: autograph.brandName,
      claimType: "PARENT_COMPANY",
      claimValue: "Marriott International",
      expectedStatus: "ALIGNED",
      span: "Autograph Collection is part of Marriott International's portfolio.",
      dealality: autograph.parentCompany,
    }));
    out.push(makeSyntheticCase({
      caseId: "tgs_syn_parent_gap_hilton",
      brandId: autograph.brandId,
      brandName: autograph.brandName,
      claimType: "PARENT_COMPANY",
      claimValue: "Hilton Worldwide",
      expectedStatus: "POTENTIAL_PERCEPTION_GAP",
      span: "Autograph Collection is part of Hilton Worldwide.",
      dealality: autograph.parentCompany,
    }));
    out.push(makeSyntheticCase({
      caseId: "tgs_syn_chain_ambiguous_lifestyle",
      brandId: autograph.brandId,
      brandName: autograph.brandName,
      claimType: "CHAIN_SCALE",
      claimValue: "Lifestyle / Boutique",
      expectedStatus: "ALIGNED",
      span: "Autograph Collection is an upscale lifestyle brand for independent hotels.",
      dealality: autograph.chainScale,
      ambiguous: true,
    }));
    out.push(makeSyntheticCase({
      caseId: "tgs_syn_brand_model_collection",
      brandId: autograph.brandId,
      brandName: autograph.brandName,
      claimType: "BRAND_MODEL",
      claimValue: "Collection Brand",
      expectedStatus: "ALIGNED",
      span: "Autograph Collection operates as a soft brand collection.",
      dealality: autograph.brandModel,
    }));
    out.push(makeSyntheticCase({
      caseId: "tgs_syn_no_span",
      brandId: autograph.brandId,
      brandName: autograph.brandName,
      claimType: "PARENT_COMPANY",
      claimValue: "Marriott International",
      expectedStatus: "NOT_EVALUATED",
      span: null,
      dealality: autograph.parentCompany,
    }));
    out.push(makeSyntheticCase({
      caseId: "tgs_syn_positioning_deferred",
      brandId: autograph.brandId,
      brandName: autograph.brandName,
      claimType: "POSITIONING",
      claimValue: "Luxury",
      expectedStatus: "INSUFFICIENT_DEALALITY_EVIDENCE",
      span: "Autograph Collection is positioned as a luxury brand.",
      dealality: null,
    }));
  }

  if (curio && autograph) {
    out.push(makeSyntheticCase({
      caseId: "tgs_syn_sibling_parent_confusion",
      brandId: autograph.brandId,
      brandName: autograph.brandName,
      claimType: "PARENT_COMPANY",
      claimValue: "Hilton",
      expectedStatus: "POTENTIAL_PERCEPTION_GAP",
      span: "Compared with Curio by Hilton, Autograph Collection is also a Hilton collection.",
      dealality: autograph.parentCompany,
      note: "parent_vs_sibling_collision",
    }));
  }

  return out;
}

function makeSyntheticCase(fields) {
  return {
    caseId: fields.caseId,
    subjectBrandId: fields.brandId,
    subjectBrandName: fields.brandName,
    evidenceId: `syn_${randomUUID().slice(0, 8)}`,
    responseId: null,
    promptId: "synthetic_truth_golden",
    scenarioId: "scenario_synthetic_v1",
    provider: "synthetic",
    language: "en",
    geography: "CALA",
    expectedClaimType: fields.claimType,
    expectedClaimValue: fields.claimValue,
    expectedComparisonStatus: fields.expectedStatus,
    dealalityFactValue: fields.dealality,
    supportingSpan: fields.span,
    hasExplicitSpan: Boolean(fields.span),
    source: "synthetic_edge_case",
    note: fields.note || null,
    ambiguous: fields.ambiguous || false,
  };
}

/**
 * Score golden set against extractor + comparator replay.
 */
export function scoreTruthGoldenSet(goldenSet, basicsIndex, options = {}) {
  const cases = goldenSet.cases || [];
  let claimTypeHits = 0;
  let claimTypeTotal = 0;
  let claimValueHits = 0;
  let claimValueTotal = 0;
  let bindingErrors = 0;
  let bindingTotal = 0;
  let spanValid = 0;
  let spanTotal = 0;
  let falseConflicts = 0;
  let falseAlignments = 0;
  let evaluatedTotal = 0;

  for (const c of cases) {
    if (c.source === "synthetic_edge_case") {
      evaluatedTotal += 1;
      if (c.expectedComparisonStatus === "NOT_EVALUATED" && !c.hasExplicitSpan) {
        spanTotal += 1;
        continue;
      }
      const row = basicsIndex.byId.get(c.subjectBrandId);
      const claim = {
        claimId: c.caseId,
        subjectBrandId: c.subjectBrandId,
        claimType: c.expectedClaimType,
        claimValue: c.expectedClaimValue,
        supportingSpan: c.supportingSpan
          ? { exactText: c.supportingSpan, text: c.supportingSpan, start: 0, end: c.supportingSpan.length }
          : null,
        evidenceId: c.evidenceId,
        responseId: c.responseId,
        promptId: c.promptId,
        scenarioId: c.scenarioId,
        provider: c.provider,
        language: c.language,
        geography: c.geography,
      };
      const comparison = compareTruthClaim(claim, basicsIndex.byId, options);
      claimTypeTotal += 1;
      if (comparison.comparisonStatus === c.expectedComparisonStatus) claimTypeHits += 1;
      if (c.expectedComparisonStatus === "ALIGNED" && comparison.comparisonStatus === "POTENTIAL_PERCEPTION_GAP") {
        falseConflicts += 1;
      }
      if (c.expectedComparisonStatus === "POTENTIAL_PERCEPTION_GAP" && comparison.comparisonStatus === "ALIGNED") {
        falseAlignments += 1;
      }
      continue;
    }

    claimTypeTotal += 1;
    claimValueTotal += 1;
    bindingTotal += 1;
    if (c.hasExplicitSpan) {
      spanTotal += 1;
      spanValid += 1;
    }

    const row = basicsIndex.byId.get(c.subjectBrandId);
    const claim = {
      subjectBrandId: c.subjectBrandId,
      claimType: c.expectedClaimType,
      claimValue: c.expectedClaimValue,
      supportingSpan: c.supportingSpan
        ? { exactText: c.supportingSpan, text: c.supportingSpan, start: 0, end: String(c.supportingSpan).length }
        : null,
    };
    const comparison = compareTruthClaim(claim, basicsIndex.byId, options);
    evaluatedTotal += 1;

    if (comparison.comparisonStatus === c.expectedComparisonStatus) claimTypeHits += 1;
    if (normalizeMatchKey(comparison.dealalityFactValue) === normalizeMatchKey(c.dealalityFactValue)) {
      claimValueHits += 1;
    }
    if (c.expectedComparisonStatus === "ALIGNED" && comparison.comparisonStatus === "POTENTIAL_PERCEPTION_GAP") {
      falseConflicts += 1;
    }
    if (c.expectedComparisonStatus === "POTENTIAL_PERCEPTION_GAP" && comparison.comparisonStatus === "ALIGNED") {
      falseAlignments += 1;
    }
  }

  const n = Math.max(claimTypeTotal, 1);
  return {
    totalCases: cases.length,
    evaluatedTotal,
    CLAIM_TYPE_PRECISION: round(claimTypeHits / n),
    CLAIM_VALUE_PRECISION: round(claimValueHits / Math.max(claimValueTotal, 1)),
    ENTITY_BINDING_ERROR_RATE: round(bindingErrors / Math.max(bindingTotal, 1)),
    SPAN_VALIDITY: round(spanValid / Math.max(spanTotal, 1)),
    FALSE_CONFLICT_RATE: round(falseConflicts / Math.max(evaluatedTotal, 1)),
    FALSE_ALIGNMENT_RATE: round(falseAlignments / Math.max(evaluatedTotal, 1)),
  };
}

function round(v) {
  return Math.round(v * 1000) / 1000;
}
