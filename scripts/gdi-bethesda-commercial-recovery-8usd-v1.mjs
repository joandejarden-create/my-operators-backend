#!/usr/bin/env node
/**
 * Bethesda commercial recovery — 16 forensic candidates + $8 Exa hard cap.
 *
 * Usage:
 *   node scripts/gdi-bethesda-commercial-recovery-8usd-v1.mjs --dry-run
 *   node scripts/gdi-bethesda-commercial-recovery-8usd-v1.mjs --zero-paid-only
 *   node scripts/gdi-bethesda-commercial-recovery-8usd-v1.mjs --authorize-paid
 */

import {
  executeBethesdaCommercialRecovery8Usd,
  rematerializeAndRefinalizeRecoveryBag,
  AUTHORIZATION_ID,
  HARD_CEILING_USD,
} from "../lib/group-demand-intelligence/research/bethesda-commercial-recovery-8usd-v1.js";

const args = process.argv.slice(2);
const dryRun = args.includes("--dry-run");
const zeroPaidOnly = args.includes("--zero-paid-only");
const rematerializeOnly = args.includes("--rematerialize-only");
const authorizePaid =
  args.includes("--authorize-paid") ||
  args.includes("--authorize-paid-research");

if (!dryRun && !zeroPaidOnly && !authorizePaid && !rematerializeOnly) {
  console.error(
    JSON.stringify(
      {
        error: "PASS_ONE_OF",
        options: [
          "--dry-run",
          "--zero-paid-only",
          "--authorize-paid",
          "--rematerialize-only",
        ],
        authorizationId: AUTHORIZATION_ID,
        hardCeilingUsd: HARD_CEILING_USD,
      },
      null,
      2,
    ),
  );
  process.exit(2);
}

const summary = rematerializeOnly
  ? rematerializeAndRefinalizeRecoveryBag()
  : await executeBethesdaCommercialRecovery8Usd({
      dryRun,
      authorizePaidResearch: authorizePaid && !dryRun && !zeroPaidOnly,
    });

console.log(
  JSON.stringify(
    {
      ok: true,
      authorizationId: summary.authorizationId,
      spentUsd: summary.spentUsd,
      remainingUsd: summary.remainingUsd,
      metrics: summary.metrics,
      stagedForFounderReviewCount: summary.stagedForFounderReviewCount,
      customerFacingParity: summary.customerFacingParity,
      arlingtonPackage: summary.arlingtonPackage || null,
      paidResults: summary.paidResults || null,
      stagedForFounderReview: summary.stagedForFounderReview || null,
    },
    null,
    2,
  ),
);
