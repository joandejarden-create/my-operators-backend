/**
 * Phase 2H — substantive subsection parity gates (depth, not verbosity).
 * Baseline is a read-only benchmark for claim/item coverage — never generator input.
 */
import { inventoryClaims } from "./be-phase2h-depth-composition-v1.js";
import { loadSubsectionParityContract } from "./be-phase2g-subsection-contract-v1.js";

export const BE_PHASE2H_SUBSTANTIVE_PARITY_GATES_V1 = "BE_PHASE2H_SUBSTANTIVE_PARITY_GATES_V1";

function nz(s) {
  return s == null ? "" : String(s).trim();
}

function wc(s) {
  return nz(s).split(/\s+/).filter(Boolean).length;
}

function blocksForPatterns(blocks, patterns) {
  return (blocks || []).filter((b) => (patterns || []).some((p) => new RegExp(p).test(nz(b.slotKey))));
}

function assessSide(blocks, sub) {
  const items = blocksForPatterns(blocks, sub.slotPatterns);
  const bodies = items.map((b) => nz(b.body)).filter(Boolean);
  const claims = bodies.flatMap((b) => inventoryClaims(b));
  const images = items.filter((b) => b.imageUrl || /^https?:\/\//i.test(b.body || "")).length;
  const empty = items.filter((b) => !nz(b.body) && !b.imageUrl).length;
  const brandWideInRegion = items.some(
    (b) => /^footprint\.region\./.test(b.slotKey) && /HPC Open hotels matched=\d+/i.test(b.body || "") && !/Regional filter applied|not a regional count|Coverage limited|Directional presence/i.test(b.body || ""),
  );
  return {
    itemCount: items.length,
    nonemptyItemCount: items.length - empty,
    wordCount: bodies.reduce((n, b) => n + wc(b), 0),
    claimCount: claims.length,
    imageCount: images,
    emptyCount: empty,
    brandWideRepeatedAsRegional: brandWideInRegion,
    claims,
  };
}

const PRIORITY_SUBS = new Set([
  "overview.why_value",
  "ops.model",
  "loyalty.program",
  "footprint.geo",
  "materials.files",
  "materials.gallery",
]);

/**
 * Compare candidate vs baseline (or existing) for one subsection.
 */
export function evaluateSubsectionParity(sub, baselineBlocks, candidateBlocks) {
  const existing = assessSide(baselineBlocks, sub);
  const candidate = assessSide(candidateBlocks, sub);
  const failures = [];
  const missingInsights = [];

  if (PRIORITY_SUBS.has(sub.id) || PRIORITY_SUBS.has(sub.id.replace(/^materials\..*/, "materials.files"))) {
    if (candidate.claimCount + 1 < Math.min(existing.claimCount, 4) && existing.claimCount >= 3) {
      failures.push("claim_depth_regression");
      missingInsights.push({
        reason: "candidate_has_fewer_distinct_claims_than_baseline",
        existingClaims: existing.claimCount,
        candidateClaims: candidate.claimCount,
      });
    }
    if (existing.nonemptyItemCount > 0 && candidate.nonemptyItemCount === 0) {
      failures.push("section_emptied");
    }
    if (sub.minItems && candidate.nonemptyItemCount < sub.minItems) {
      failures.push("item_coverage_below_min");
    }
    if (sub.requireImage && candidate.imageCount < (sub.minItems || 1)) {
      failures.push("imagery_coverage_below_min");
    }
  }

  if (sub.id === "footprint.geo" && candidate.brandWideRepeatedAsRegional) {
    failures.push("brand_wide_hpc_repeated_as_regional");
  }

  if (sub.id === "overview.why_value" && candidate.claimCount < 4) {
    failures.push("why_value_insufficient_distinct_reasons");
  }

  if (sub.id === "ops.model" && candidate.nonemptyItemCount < 4) {
    failures.push("operating_model_fields_insufficient");
  }

  if (sub.id === "loyalty.program" && candidate.wordCount < 80) {
    failures.push("loyalty_depth_insufficient");
  }

  // Generic / diligence language still present
  const joined = blocksForPatterns(candidateBlocks, sub.slotPatterns)
    .map((b) => `${b.title} ${b.body}`)
    .join(" ");
  if (/Focus this card|Evidence-grounded|RESEARCH_REQUIRED|diligence framing only|Do not invent/i.test(joined)) {
    failures.push("internal_meta_or_padding_language");
  }

  let disposition = "unvalidated";
  if (!failures.length) {
    if (candidate.claimCount >= existing.claimCount && candidate.nonemptyItemCount >= Math.max(1, existing.nonemptyItemCount - 1)) {
      disposition = "equivalent_or_improved";
    } else if (candidate.claimCount >= Math.max(3, Math.floor(existing.claimCount * 0.75))) {
      disposition = "improved_partial";
    } else {
      disposition = "needs_founder_review";
    }
  } else {
    disposition = "inferior_or_blocked";
  }

  return {
    subsectionId: sub.id,
    label: sub.label,
    tabId: sub.tabId,
    existing,
    candidate,
    failures,
    missingInsights,
    disposition,
    priority: PRIORITY_SUBS.has(sub.id) || sub.id.startsWith("materials."),
  };
}

export function evaluateSubstantiveParity({ baselineBlocks, candidateBlocks, contract } = {}) {
  const c = contract || loadSubsectionParityContract();
  const results = [];
  for (const tab of c.tabs || []) {
    for (const sub of tab.subsections || []) {
      results.push(
        evaluateSubsectionParity({ ...sub, tabId: tab.tabId, tabName: tab.tabName }, baselineBlocks, candidateBlocks),
      );
    }
  }
  const priority = results.filter((r) => r.priority);
  const blocked = priority.filter((r) => r.failures.length > 0);
  return {
    contract: BE_PHASE2H_SUBSTANTIVE_PARITY_GATES_V1,
    results,
    priorityResults: priority,
    priorityPass: blocked.length === 0,
    blockedPriority: blocked.map((b) => ({ id: b.subsectionId, failures: b.failures, disposition: b.disposition })),
    founderApprovalStatus: "NOT_FOUNDER_APPROVED",
    metrics: {
      subsectionsScored: results.length,
      priorityScored: priority.length,
      priorityBlocked: blocked.length,
      dispositions: results.reduce((acc, r) => {
        acc[r.disposition] = (acc[r.disposition] || 0) + 1;
        return acc;
      }, {}),
    },
  };
}

/** Founder-screenshot style regression cases */
export function evaluateFounderRegressionCases(baselineBlocks, candidateBlocks) {
  const cases = [
    {
      id: "why_value_loses_insights",
      subsectionId: "overview.why_value",
      test: (e, c) => c.claimCount + 1 < e.claimCount && e.claimCount >= 3,
    },
    {
      id: "operating_model_emptied",
      subsectionId: "ops.model",
      test: (e, c) => e.nonemptyItemCount >= 4 && c.nonemptyItemCount < 2,
    },
    {
      id: "loyalty_loses_owner_implications",
      subsectionId: "loyalty.program",
      test: (e, c) => e.wordCount > 100 && c.wordCount < 60,
    },
    {
      id: "regional_hpc_repeated",
      subsectionId: "footprint.geo",
      test: (_e, c) => c.brandWideRepeatedAsRegional === true,
    },
    {
      id: "materials_lose_case_studies",
      subsectionId: "materials.files",
      test: (e, c) => e.nonemptyItemCount >= 2 && c.nonemptyItemCount === 0,
    },
  ];
  const contract = loadSubsectionParityContract();
  const byId = new Map();
  for (const tab of contract.tabs || []) {
    for (const sub of tab.subsections || []) byId.set(sub.id, { ...sub, tabId: tab.tabId });
  }
  return cases.map((c) => {
    const sub = byId.get(c.subsectionId);
    const evaled = sub
      ? evaluateSubsectionParity(sub, baselineBlocks, candidateBlocks)
      : { existing: {}, candidate: {}, failures: ["missing_subsection"] };
    const failed = c.test(evaled.existing, evaled.candidate);
    return { id: c.id, subsectionId: c.subsectionId, failed, eval: evaled };
  });
}
