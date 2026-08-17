#!/usr/bin/env node
/**
 * P0D-A.1 — Truth Layer semantic integrity + false-conflict audit runner.
 */
import { runSemanticIntegrityAudit, saveSemanticAuditReport } from "../lib/ai-visibility/truth-layer/semantic-integrity-audit.js";

async function main() {
  const audit = await runSemanticIntegrityAudit();
  const paths = saveSemanticAuditReport(audit);
  const r = audit.report;

  console.log("\nHOTEL_BRAND_AI_INTELLIGENCE_P0DA1_TRUTH_SEMANTIC_INTEGRITY_COMPLETE\n");

  console.log("## Original Truth Results");
  console.log(`ALIGNED:\n${r.originalTruthResults.ALIGNED}`);
  console.log(`POTENTIAL_PERCEPTION_GAP:\n${r.originalTruthResults.POTENTIAL_PERCEPTION_GAP}`);

  console.log("\n## Semantic Audit (original 32 gaps reclassified)");
  console.log(JSON.stringify(r.originalGapSemanticAudit, null, 2));

  console.log("\n## Semantic Audit (remaining gaps after remediation)");
  for (const [k, v] of Object.entries(r.semanticAudit)) {
    console.log(`${k}:\n${v}`);
  }

  console.log("\n## Parent Company Conflicts");
  console.log(`TOTAL_REPORTED:\n${r.parentCompanyConflicts.totalReported}`);
  console.log(`NORMALIZATION_VARIATION:\n${r.parentCompanyConflicts.normalizationVariation}`);
  console.log(`GENUINE_CONFLICT:\n${r.parentCompanyConflicts.genuineConflict}`);
  console.log(`UNRESOLVED:\n${r.parentCompanyConflicts.unresolved}`);

  console.log("\n## Brand Model Audit");
  console.log(`totalEvaluated: ${r.brandModelAudit.totalEvaluated}`);
  console.log(`gaps: ${r.brandModelAudit.gaps}`);
  console.log(`notEvaluated: ${r.brandModelAudit.notEvaluated}`);
  for (const u of r.brandModelAudit.uniqueAiValues) {
    console.log(`  ${u.aiValue}: gaps=${u.gapCount} notEval=${u.notEvalCount} ${JSON.stringify(u.classifications)}`);
  }

  console.log("\n## Chain Scale Audit");
  console.log(JSON.stringify(r.chainScaleAudit, null, 2));

  console.log("\n## Soft Brand Audit (Westin)");
  console.log(JSON.stringify(r.softBrandAudit, null, 2));

  console.log("\n## After Remediation");
  const a = r.afterRemediation;
  console.log(`ALIGNED:\n${a.AFTER_ALIGNED}`);
  console.log(`POTENTIAL_PERCEPTION_GAP:\n${a.AFTER_GAPS}`);
  console.log(`NOT_EVALUATED:\n${a.AFTER_NOT_EVALUATED}`);
  console.log(`FALSE_GAPS_REMOVED:\n${a.FALSE_GAPS_REMOVED}`);

  console.log("\n## P0C Class D");
  console.log(`CURRENT_D:\n${r.p0cClassD.CURRENT_D}`);
  console.log(`SURVIVING_D:\n${r.p0cClassD.SURVIVING_D}`);
  console.log(`EXECUTIVE_ELIGIBLE:\n${r.p0cClassD.EXECUTIVE_ELIGIBLE}`);
  console.log(`DETAIL_ONLY:\n${r.p0cClassD.DETAIL_ONLY}`);
  console.log(`REMOVED:\n${r.p0cClassD.REMOVED_FALSE_CONFLICT}`);

  console.log("\n## Sample Executive-Safe Findings");
  if (!r.executiveFindings.length) {
    console.log("NO_EXECUTIVE_ELIGIBLE_GAPS_FOUND");
  } else {
    r.executiveFindings.forEach((f, i) => {
      console.log(`\n${i + 1}. ${f.BRAND} | ${f.DIMENSION}`);
      console.log(`   AI: ${f.AI_PERCEPTION} | Dealality: ${f.DEALALITY_FACT}`);
      console.log(`   EXECUTIVE_ELIGIBLE: ${f.EXECUTIVE_ELIGIBLE}`);
      console.log(`   EVIDENCE: ${f.EVIDENCE?.slice(0, 120)}...`);
    });
  }

  console.log("\n## Certified Layer");
  for (const [k, v] of Object.entries(r.certifiedLayer)) console.log(`${k}:\n${v}`);

  console.log("\n## Census");
  console.log(`STATUS:\n${r.census.STATUS}`);
  console.log(`READS_FOR_TRUTH:\n${r.census.READS_FOR_TRUTH}`);

  console.log("\n## Readiness");
  console.log(r.readiness);
  console.log(`\n## Next\n${r.next}`);
  console.log(`\nReport: ${paths.reportPath}`);
  console.log(`\n${r.finalToken}\n`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
