#!/usr/bin/env node
import {
  buildBethesdaMaturityAlignmentApprovalPackage,
  writeBethesdaMaturityAlignmentApprovalReports,
} from "../lib/group-demand-intelligence/research/bethesda-maturity-alignment-approval-v1.js";

const pkg = buildBethesdaMaturityAlignmentApprovalPackage();
const { reportDir } = writeBethesdaMaturityAlignmentApprovalReports(pkg);
console.log(
  JSON.stringify(
    {
      ok: true,
      reportDir,
      engineAfterCorrection: pkg.engineAfterCorrection,
      proposedPromotions: pkg.proposedPromotions.length,
      rejectedPromotions: pkg.rejectedPromotions.length,
      actionableNow: pkg.actionableNow.length,
      writeApplied: false,
    },
    null,
    2,
  ),
);
if (pkg.engineAfterCorrection.salesReadyEligible !== 0) process.exit(2);
if (pkg.engineAfterCorrection.accountDevelopmentEligible !== 14) process.exit(3);
