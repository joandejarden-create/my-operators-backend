#!/usr/bin/env node
/**
 * Phase 2B: deepen official-domain public research for CMAA + DDAA only.
 * $0 paid providers. No production writes.
 *
 *   set GDI_20OPP_LIVE_PUBLIC_FOUNDER_AUTH=1
 *   npm run gdi:20opp-phase2b-official-domain-deepen-v1
 */

import {
  runPhase2bDeepen,
  PHASE2B_CASES,
} from "../lib/group-demand-intelligence/experiments/gdi-20opp-phase2b-official-domain-deepen-v1.js";

const auth = String(process.env.GDI_20OPP_LIVE_PUBLIC_FOUNDER_AUTH || "");
if (auth !== "1") {
  console.error(
    JSON.stringify(
      {
        ok: false,
        refused: true,
        reason: "Set GDI_20OPP_LIVE_PUBLIC_FOUNDER_AUTH=1 to run Phase 2B live public deepen",
        cases: PHASE2B_CASES.map((c) => c.opportunityId),
      },
      null,
      2,
    ),
  );
  process.exit(2);
}

const { report, outDir } = await runPhase2bDeepen({});
console.log(
  JSON.stringify(
    {
      ok: true,
      decision: report.recommendation.decision,
      bottleneck: report.recommendation.bottleneck,
      actualPaidUsdTotal: report.totals.actualPaidUsdTotal,
      totals: report.totals,
      outDir,
      cases: report.caseResults.map((c) => ({
        id: c.opportunityId,
        usefulPages: c.comparison.newUsefulPages,
        namedContacts: c.comparison.newNamedContacts,
        publicEmails: c.comparison.newPublicEmails,
        demandImproved: c.comparison.commercialDemandImproved,
        contactImproved: c.comparison.actionableContactImproved,
        eligibility: `${c.comparison.eligibilityBefore}→${c.comparison.eligibilityAfter}`,
        paid: c.comparison.actualPaidUsd,
      })),
    },
    null,
    2,
  ),
);
