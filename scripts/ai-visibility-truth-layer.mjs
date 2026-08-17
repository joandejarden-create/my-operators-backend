#!/usr/bin/env node
/**
 * P0D-A — Non-Census Dealality Truth Layer runner (existing evidence only).
 */
import { runTruthLayer, saveTruthLayerReport } from "../lib/ai-visibility/truth-layer/truth-layer-index.js";

async function main() {
  const result = await runTruthLayer();
  const paths = saveTruthLayerReport(result);
  const r = result.report;
  const cv = r.claimValidation;
  const pr = r.productionReadiness;

  console.log("\nHOTEL_BRAND_AI_INTELLIGENCE_P0DA_NON_CENSUS_TRUTH_LAYER_COMPLETE\n");
  console.log("## Truth Sources");
  console.log(`BRAND_BASICS:\n${r.truthSources.BRAND_BASICS}`);
  console.log(`BRAND_EXPLORER:\n${r.truthSources.BRAND_EXPLORER}`);
  console.log("HOTEL_CENSUS:\nDEFERRED_INCOMPLETE_CENSUS");

  console.log("\n## Brand Basics Audit");
  console.log("| Field | Present | Missing | Conflicts | Governance | Truth Eligible |");
  console.log("|-------|---------|---------|-----------|------------|----------------|");
  for (const row of r.brandBasicsAudit) {
    console.log(
      `| ${row.field} | ${row.presentCount} | ${row.missingCount} | ${row.conflictCount || 0} | ${row.governanceState} | ${row.safeForTruthLayer} |`
    );
  }

  console.log("\n## Brand Explorer Audit");
  console.log("| Field | Structured | Governance | Completeness | Truth Eligible |");
  console.log("|-------|------------|------------|--------------|----------------|");
  for (const row of r.brandExplorerAudit) {
    console.log(
      `| ${row.field} | ${row.structured} | ${row.governanceState} | ${row.completeness} | ${row.safeForTruthLayer} |`
    );
  }

  console.log("\n## Truth Dimensions");
  for (const [dim, status] of Object.entries(r.truthDimensions)) {
    if (["COUNTRY_PRESENCE", "GEOGRAPHIC_FOOTPRINT", "OPEN_HOTEL_COUNT", "PIPELINE"].includes(dim)) {
      console.log(`${dim}:\nDEFERRED_CENSUS`);
    } else {
      console.log(`${dim}:\n${status}`);
    }
  }

  console.log("\n## Claim Validation");
  console.log(`TOTAL_CASES:\n${cv.totalCases}`);
  console.log(`CLAIM_TYPE_PRECISION:\n${cv.CLAIM_TYPE_PRECISION}`);
  console.log(`CLAIM_VALUE_PRECISION:\n${cv.CLAIM_VALUE_PRECISION}`);
  console.log(`ENTITY_BINDING_ERROR_RATE:\n${cv.ENTITY_BINDING_ERROR_RATE}`);
  console.log(`SPAN_VALIDITY:\n${cv.SPAN_VALIDITY}`);
  console.log(`FALSE_CONFLICT_RATE:\n${cv.FALSE_CONFLICT_RATE}`);

  console.log("\n## Marriott Sample");
  for (const brand of r.marriottSample) {
    console.log(`\n${brand.brandName} (${brand.brandId})`);
    for (const [dim, counts] of Object.entries(brand.dimensions)) {
      const total = counts.CLAIMS_EVALUATED;
      if (!total) continue;
      console.log(
        `  ${dim}: evaluated=${total} aligned=${counts.ALIGNED} gap=${counts.POTENTIAL_PERCEPTION_GAP} insufficient=${counts.INSUFFICIENT} not_eval=${counts.NOT_EVALUATED}`
      );
    }
  }

  console.log("\n## Comparisons");
  console.log(`ALIGNED:\n${r.comparisons.ALIGNED}`);
  console.log(`POTENTIAL_PERCEPTION_GAP:\n${r.comparisons.POTENTIAL_PERCEPTION_GAP}`);
  console.log(`INSUFFICIENT_DEALALITY_EVIDENCE:\n${r.comparisons.INSUFFICIENT_DEALALITY_EVIDENCE}`);
  console.log(`NOT_EVALUATED:\n${r.comparisons.NOT_EVALUATED}`);

  console.log("\n## P0C Class D");
  console.log(`PRODUCTION_D_GAPS:\n${r.p0cClassD.PRODUCTION_D_GAPS}`);
  console.log(`BLOCKED_D_GAPS:\n${r.p0cClassD.BLOCKED_D_GAPS}`);

  console.log("\n## Census Status");
  console.log("CENSUS_TRUTH_LAYER_STATUS:\nDEFERRED_INCOMPLETE_CENSUS");
  console.log("CENSUS_TRUTH_COMPARISONS:\n0");

  console.log("\n## Safety");
  console.log(`CENSUS_READS_FOR_TRUTH:\n${r.safety.CENSUS_READS_FOR_TRUTH}`);
  console.log(`FUZZY_MATCHES:\n${r.safety.FUZZY_MATCHES}`);
  console.log(`CANONICAL_MUTATIONS:\n${r.safety.CANONICAL_MUTATIONS}`);
  console.log(`AIRTABLE_WRITES:\n${r.safety.AIRTABLE_WRITES}`);

  console.log("\n## Certified Layer");
  for (const [k, v] of Object.entries(r.certifiedLayer)) {
    console.log(`${k}:\n${v}`);
  }

  console.log("\n## Sample Findings");
  if (Array.isArray(r.sampleFindings) && r.sampleFindings.length === 1 && r.sampleFindings[0] === "NO_VALIDATED_PERCEPTION_GAPS_FOUND") {
    console.log("NO_VALIDATED_PERCEPTION_GAPS_FOUND");
  } else {
    for (const f of r.sampleFindings.slice(0, 5)) {
      console.log(`- ${f.subjectBrandName} | ${f.aiClaimType} | AI: ${f.aiClaimValue} | Dealality: ${f.dealalityFactValue} | ${f.comparisonStatus}`);
    }
  }

  console.log("\n## Production Readiness");
  console.log(pr.status);
  console.log(`\n## Next\n${pr.next}`);
  console.log("\nP0DB_CENSUS_TRUTH_LAYER_DEFERRED");
  console.log(`\nReport: ${paths.reportPath}`);
  console.log(`Comparisons: ${paths.comparisonsPath}`);
  console.log(`\n${pr.finalToken}\n`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
