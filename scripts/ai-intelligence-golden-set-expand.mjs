#!/usr/bin/env node
/**
 * Materialize Golden Set v1 + sample v2 candidates from stored monitoring.
 * LIVE_PROVIDER_CALLS: 0. Does not promote candidates to ground truth.
 */
import {
  materializeGoldenSetV1,
  sampleGoldenSetCandidates,
  auditGoldenCoverage,
  OUT_V1,
  OUT_CANDIDATES,
} from "../lib/ai-visibility/validation/golden-set-expansion.js";

const apply = process.argv.includes("--apply") || !process.argv.includes("--dry-run");

console.log("AI Intelligence Golden Set expansion workflow\n");

const v1 = materializeGoldenSetV1();
console.log(`v1 written: ${OUT_V1}`);
console.log(`  cases=${v1.caseCount} humanLabelled=${v1.humanLabelled} llmGt=${v1.llmLabelledAsGroundTruth}`);
console.log("  coverage:", JSON.stringify(v1.coverageAudit, null, 2));

if (!apply) {
  console.log("\nDry-run: skip candidate sampling write (pass --apply to sample).");
  process.exit(0);
}

const candidates = await sampleGoldenSetCandidates({ target: 100 });
console.log(`\nv2 candidates written: ${OUT_CANDIDATES}`);
console.log(`  cases=${candidates.caseCount} PENDING_HUMAN_REVIEW`);
console.log(`  humanLabelled=${candidates.humanLabelled} llmGt=${candidates.llmLabelledAsGroundTruth}`);
console.log("  coverage:", JSON.stringify(candidates.coverageAudit, null, 2));
console.log("\nBLOCKER: No candidate becomes ground truth until human review.");
console.log(`Combined labelled if all reviewed: ${v1.caseCount + candidates.caseCount} (target ≥150)`);
