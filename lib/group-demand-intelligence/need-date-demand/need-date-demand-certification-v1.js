/**
 * Permanent Need-Date Demand Intelligence certification gates.
 */

import {
  NEED_DATE_DEMAND_INTELLIGENCE_V1,
  NEED_DATE_CERT_GATE,
  CERT_OUTCOME,
  NEED_TIMING_OVERLAP,
  NEED_OUTPUT_CATEGORY,
} from "./need-date-demand-contract-v1.js";

function gate(name, outcome, detail = null) {
  return { gate: name, outcome, detail };
}

export function certifyNeedDateCampaign(report = {}) {
  const gates = [];
  const periods = report.needPeriods || [];
  const hotelId = report.hotelId;
  const inventory = report.inventoryMatch || {};
  const strategies = report.strategyPlan || {};
  const ledger = report.campaignLedger || {};
  const cert = report.certificationContext || {};

  // NEED_PERIOD_IDENTITY_PASS
  const identityOk =
    periods.length > 0 &&
    periods.every((p) => p.valid && p.hotelId === hotelId);
  gates.push(
    gate(
      NEED_DATE_CERT_GATE.NEED_PERIOD_IDENTITY_PASS,
      identityOk ? CERT_OUTCOME.PASS : CERT_OUTCOME.FAIL,
      { periodCount: periods.length, hotelId },
    ),
  );

  // NEED_PERIOD_DATE_VALIDATION_PASS
  const datesOk = periods.every(
    (p) => p.valid && p.startDate && p.endDate && !p.errors?.length,
  );
  gates.push(
    gate(
      NEED_DATE_CERT_GATE.NEED_PERIOD_DATE_VALIDATION_PASS,
      datesOk ? CERT_OUTCOME.PASS : CERT_OUTCOME.FAIL,
    ),
  );

  // TIMING_MATCH_EVIDENCE_PASS
  const matches = inventory.matches || [];
  const timingOk =
    matches.length === 0 ||
    matches.every((m) => m.timing?.overlapStatus && m.timing?.reason);
  const hasConfirmed = matches.some(
    (m) =>
      m.timing?.overlapStatus === NEED_TIMING_OVERLAP.CONFIRMED_PERIOD_OVERLAP,
  );
  const sourcingMisclassified = matches.some(
    (m) =>
      m.timing?.demandDateKind === "SOURCING_PLANNING" &&
      m.timing?.overlapStatus === NEED_TIMING_OVERLAP.CONFIRMED_PERIOD_OVERLAP,
  );
  gates.push(
    gate(
      NEED_DATE_CERT_GATE.TIMING_MATCH_EVIDENCE_PASS,
      !timingOk || sourcingMisclassified
        ? CERT_OUTCOME.FAIL
        : CERT_OUTCOME.PASS,
      { hasConfirmed, sourcingMisclassified },
    ),
  );

  // EXISTING_INVENTORY_MATCH_PASS
  gates.push(
    gate(
      NEED_DATE_CERT_GATE.EXISTING_INVENTORY_MATCH_PASS,
      inventory.inventoryCount >= 0 && Array.isArray(inventory.matches)
        ? CERT_OUTCOME.PASS
        : CERT_OUTCOME.FAIL,
      {
        inventoryCount: inventory.inventoryCount,
        existingMatchCount: inventory.existingMatchCount,
      },
    ),
  );

  // CROSS_WORKFLOW_DEDUP_PASS
  // Same canonical opportunity may legitimately link to multiple need months.
  // Fail only when the same opportunityId+needPeriodId pair is duplicated.
  const ids = matches.map((m) => m.opportunityId);
  const unique = new Set(ids);
  const pairKeys = matches.map(
    (m) => `${m.opportunityId || ""}::${m.needPeriodId || ""}`,
  );
  const uniquePairs = new Set(pairKeys);
  const multiLinksOk = (inventory.multiPeriodLinks || []).every(
    (l) => Array.isArray(l.needPeriodIds) && l.needPeriodIds.length >= 2,
  );
  gates.push(
    gate(
      NEED_DATE_CERT_GATE.CROSS_WORKFLOW_DEDUP_PASS,
      pairKeys.length === uniquePairs.size && multiLinksOk !== false
        ? CERT_OUTCOME.PASS
        : CERT_OUTCOME.FAIL,
      {
        matchRows: ids.length,
        uniqueIds: unique.size,
        uniquePairs: uniquePairs.size,
        multiPeriodLinks: (inventory.multiPeriodLinks || []).length,
        note: "Multi-period associations of one canonical ID are allowed; pair duplicates are not.",
      },
    ),
  );

  // REVENUE_TYPE_MATCH_PASS
  const revenueOk = matches.every((m) => m.revenue?.fit === true);
  gates.push(
    gate(
      NEED_DATE_CERT_GATE.REVENUE_TYPE_MATCH_PASS,
      matches.length === 0 || revenueOk ? CERT_OUTCOME.PASS : CERT_OUTCOME.FAIL,
    ),
  );

  // BUYER_ELIGIBILITY_PASS — Preview may be NOT_VERIFIED for enrichment gaps
  const buyerGaps = matches.filter((m) =>
    (m.researchGapHints || []).includes("NAMED_BUYER_OR_REFERRAL"),
  );
  gates.push(
    gate(
      NEED_DATE_CERT_GATE.BUYER_ELIGIBILITY_PASS,
      cert.requireBuyerPass
        ? buyerGaps.length === 0
          ? CERT_OUTCOME.PASS
          : CERT_OUTCOME.FAIL
        : CERT_OUTCOME.NOT_VERIFIED,
      { buyerGapCount: buyerGaps.length },
    ),
  );

  // RESEARCH_PRESERVATION_PASS — no paid research in preview
  gates.push(
    gate(
      NEED_DATE_CERT_GATE.RESEARCH_PRESERVATION_PASS,
      report.mode === "preview" || report.mode === "certify-only"
        ? CERT_OUTCOME.NOT_VERIFIED
        : cert.researchPreserved
          ? CERT_OUTCOME.PASS
          : CERT_OUTCOME.FAIL,
      { note: "Paid research not executed in this run" },
    ),
  );

  // GOLD_DRAWER_PASS
  const drawerSamples = report.drawerComponents || [];
  const drawerOk =
    drawerSamples.length === 0 ||
    drawerSamples.every(
      (d) => d.replacesTenSections === false && d.componentId === "hotel_need_date_match",
    );
  gates.push(
    gate(
      NEED_DATE_CERT_GATE.GOLD_DRAWER_PASS,
      drawerOk ? CERT_OUTCOME.PASS : CERT_OUTCOME.FAIL,
      { componentCount: drawerSamples.length },
    ),
  );

  // BROWSER_PASS
  gates.push(
    gate(
      NEED_DATE_CERT_GATE.BROWSER_PASS,
      cert.browserPass === true
        ? CERT_OUTCOME.PASS
        : CERT_OUTCOME.NOT_VERIFIED,
      { note: cert.browserNote || "Browser probe optional in Preview" },
    ),
  );

  // BUDGET_PASS
  const spend = Number(ledger.actualSpendUsd || 0);
  const paidBlocked = !ledger.authorizePaidResearch;
  gates.push(
    gate(
      NEED_DATE_CERT_GATE.BUDGET_PASS,
      paidBlocked && spend === 0 ? CERT_OUTCOME.PASS : spend <= Number(ledger.budgetCeilingUsd || 0) ? CERT_OUTCOME.PASS : CERT_OUTCOME.FAIL,
      { actualSpendUsd: spend, authorizePaidResearch: Boolean(ledger.authorizePaidResearch) },
    ),
  );

  // PUBLICATION_GATE_PASS
  gates.push(
    gate(
      NEED_DATE_CERT_GATE.PUBLICATION_GATE_PASS,
      report.authorizePublication
        ? CERT_OUTCOME.NOT_VERIFIED
        : CERT_OUTCOME.PASS,
      {
        publicationStatus: ledger.publicationStatus || "NOT_AUTHORIZED",
        authorizePublication: Boolean(report.authorizePublication),
      },
    ),
  );

  const failed = gates.filter((g) => g.outcome === CERT_OUTCOME.FAIL);
  return {
    version: NEED_DATE_DEMAND_INTELLIGENCE_V1,
    ok: failed.length === 0,
    gates,
    failed: failed.map((g) => g.gate),
    summary: {
      pass: gates.filter((g) => g.outcome === CERT_OUTCOME.PASS).length,
      fail: failed.length,
      notVerified: gates.filter((g) => g.outcome === CERT_OUTCOME.NOT_VERIFIED)
        .length,
    },
    categoriesPresent: {
      existingMatch: matches.some(
        (m) => m.category === NEED_OUTPUT_CATEGORY.EXISTING_MATCH,
      ),
      newUniqueForbiddenInPreview: true,
    },
  };
}
