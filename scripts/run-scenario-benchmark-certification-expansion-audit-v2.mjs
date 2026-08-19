#!/usr/bin/env node
import { runScenarioBenchmarkCertificationExpansionAudit } from "../lib/ai-visibility/competitive-moat/scenario-benchmark-certification-expansion-audit.js";

const report = runScenarioBenchmarkCertificationExpansionAudit({ writeReport: true });
console.log("BRAND_AI_SCENARIO_CERTIFICATION_EXPANSION_AUDIT_COMPLETE");
console.log("final:", report.final);
console.log("next:", report.recommendedNextStep);
console.log("audited:", report.audit);
console.log("paths:", report.primaryPathBreakdown);
console.log("target6:", report.expansionRoadmap?.target6?.ROWS);
console.log("providerCalls:", report.providerCalls);
console.log("wrote reports/ai-visibility/scenario-benchmark-certification-expansion-audit-v2.json");
