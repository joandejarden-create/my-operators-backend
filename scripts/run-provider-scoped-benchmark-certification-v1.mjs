#!/usr/bin/env node
/**
 * Offline provider-scoped benchmark certification audit.
 * PROVIDER_CALLS = 0
 */

import { runProviderScopedCertificationAudit } from "../lib/ai-visibility/competitive-moat/provider-scoped-benchmark-certification.js";

const report = runProviderScopedCertificationAudit();
console.log("\nProvider-scoped benchmark certification audit\n");
console.log(JSON.stringify(report.counts, null, 2));
console.log("\nOPENAI certified:", report.certifiedByScope.OPENAI?.length || 0);
console.log("GEMINI certified:", report.certifiedByScope.GEMINI?.length || 0);
console.log("PERPLEXITY certified:", report.certifiedByScope.PERPLEXITY?.length || 0);
console.log("CLAUDE certified:", report.certifiedByScope.CLAUDE?.length || 0);
console.log("\nPROVIDER_SPECIFIC_CERTIFICATION_READY");
