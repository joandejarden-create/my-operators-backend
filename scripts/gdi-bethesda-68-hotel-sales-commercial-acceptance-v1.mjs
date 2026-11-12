#!/usr/bin/env node
/**
 * Bethesda 68 — Hotel Sales Commercial Acceptance runner.
 * Loads the real commercial list, scores every opportunity DOS-style,
 * writes reports under reports/gdi-bethesda-68-hotel-sales-commercial-acceptance-v1/.
 */
import {
  buildBethesda68HotelSalesCommercialAcceptance,
  writeBethesda68HotelSalesCommercialAcceptanceReports,
} from "../lib/group-demand-intelligence/research/bethesda-68-hotel-sales-commercial-acceptance-v1.js";

const acceptance = buildBethesda68HotelSalesCommercialAcceptance();
const { reportDir } = writeBethesda68HotelSalesCommercialAcceptanceReports(acceptance);

const c = acceptance.counts;
console.log(
  JSON.stringify(
    {
      ok: true,
      reportDir,
      commercialCount: c.commercial,
      usability: c.usability,
      revenue: c.revenue,
      contactCoverage: c.contactCoverage,
      engineMaturity: c.engineMaturity,
      lock: acceptance.lock,
      topProspectCount: acceptance.topProspects.length,
      maturityPromotionCandidates: acceptance.maturityPromotionCandidates.length,
      remediationCandidates: acceptance.remediationCandidates.length,
    },
    null,
    2,
  ),
);

if (c.commercial < 1) process.exit(2);
if ((acceptance.lock.forbiddenLeaks || []).length) process.exit(3);
if (!acceptance.lock.rawIdsPreserved) process.exit(4);
