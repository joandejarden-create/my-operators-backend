#!/usr/bin/env node
/**
 * Full GDI customer consistency audit — all hotels, report-only.
 * Does not publish, merge, or rewrite evidence.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  runFullCustomerConsistencyAudit,
  writeAuditArtifacts,
  GDI_FULL_CUSTOMER_CONSISTENCY_AUDIT_V1,
} from "../lib/group-demand-intelligence/customer-consistency/gdi-full-customer-consistency-audit-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(ROOT, "reports/gdi-full-customer-consistency-audit-v1");

const report = await runFullCustomerConsistencyAudit();
writeAuditArtifacts(report, OUT);

console.log(
  JSON.stringify(
    {
      schema: GDI_FULL_CUSTOMER_CONSISTENCY_AUDIT_V1,
      outDir: OUT,
      hotels: report.scope.hotelsScanned,
      opportunities: report.scope.totalOpportunities,
      score: report.score.overall,
      totals: report.totals,
      topHotelsByIssues: [...report.hotelSummaries]
        .sort((a, b) => b.issueCount - a.issueCount)
        .slice(0, 8),
    },
    null,
    2
  )
);

if (report.totals.bySeverity.P0 > 0) process.exitCode = 2;
else if (report.totals.bySeverity.P1 > 0) process.exitCode = 1;
