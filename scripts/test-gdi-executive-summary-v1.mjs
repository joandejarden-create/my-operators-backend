#!/usr/bin/env node
/**
 * $0 regression for GDI Executive Summary Phase 1.
 * No paid APIs. No Surfe. No Findymail.
 */
import assert from "assert";
import {
  buildGdiExecutiveSummaryV1,
  prioritizeCustomerOpportunities,
  buildInvestmentRecoveryPlaceholder,
  EXEC_SUMMARY_DATA_STATE,
  GDI_EXECUTIVE_SUMMARY_V1,
  GDI_EXECUTIVE_SUMMARY_AUTH,
} from "../lib/group-demand-intelligence/executive-summary/gdi-executive-summary-v1.js";
import {
  GDI_RESEARCH_RECEIPT_CONTRACT_ID,
  RESEARCH_RECEIPT_AVAILABILITY,
  buildCompactResearchReceiptForPdf,
  emptyResearchReceiptShell,
} from "../lib/group-demand-intelligence/executive-summary/gdi-research-receipt-contract-v1.js";
import { CONTACT_ACTIONABILITY_TIER } from "../lib/group-demand-intelligence/presentation/gdi-contact-actionability-v1.js";
import { WEEKLY_DELTA_STATE } from "../lib/group-demand-intelligence/weekly-delta.js";

let passed = 0;
function pass(name) {
  passed += 1;
  console.log(`PASS ${name}`);
}

{
  assert.equal(GDI_EXECUTIVE_SUMMARY_V1, "gdi-executive-summary-v1");
  assert.equal(
    GDI_EXECUTIVE_SUMMARY_AUTH,
    "FOUNDER_GO_GDI_EXECUTIVE_SUMMARY_PHASE_1_20261127",
  );
  assert.equal(GDI_RESEARCH_RECEIPT_CONTRACT_ID, "gdi-research-receipt-contract-v1");
  pass("contract_ids");
}

{
  const empty = emptyResearchReceiptShell({ hotelId: "recTEST" });
  assert.equal(empty.hotelId, "recTEST");
  assert.ok(empty.metrics.length >= 10);
  assert.ok(
    empty.metrics.every(
      (m) =>
        m.value == null ||
        m.availability === RESEARCH_RECEIPT_AVAILABILITY.AVAILABLE_DERIVED,
    ),
  );
  pass("empty_receipt_no_synthetic_zeros_for_unavailable");
}

{
  const opps = [
    {
      id: "a",
      title: "Alpha Association",
      segmentId: "associations",
      weeklyDeltaState: WEEKLY_DELTA_STATE.NEW,
      contactActionabilityTier: CONTACT_ACTIONABILITY_TIER.SALES_READY,
      score: 10,
      needDateMatch: true,
      whyThisHotel: "Downtown fit",
      recommendedSalesAction: "Call buyer",
      evidenceUrls: ["https://example.com/a"],
      primaryContact: { name: "Pat Buyer" },
      whoHow: {
        whoHowStatus: "VERIFIED_DIRECT_BUYER",
        directBuyer: { fullName: "Pat Buyer" },
      },
    },
    {
      id: "b",
      title: "Beta Corp",
      segmentId: "corporate",
      weeklyDeltaState: WEEKLY_DELTA_STATE.UPDATED,
      contactActionabilityTier: CONTACT_ACTIONABILITY_TIER.REACHABLE,
      score: 50,
      evidence: [{ sourceUrl: "https://example.com/b" }],
    },
    {
      id: "c",
      title: "Old Opp",
      segmentId: "corporate",
      weeklyDeltaState: WEEKLY_DELTA_STATE.UNCHANGED,
      contactActionabilityTier:
        CONTACT_ACTIONABILITY_TIER.CONTACT_IDENTIFICATION_REQUIRED,
      score: 90,
    },
  ];

  const payload = buildGdiExecutiveSummaryV1({
    hotelId: "recCURRENT",
    hotelName: "Test Hotel",
    opportunities: opps,
    customerAuthorizedInventory: true,
    dataState: EXEC_SUMMARY_DATA_STATE.CURRENT_AVAILABLE,
    pursuits: [{ status: "ACTIVE" }],
    feedback: { entries: [{ useful: true }, { salesOutcome: "WON" }] },
  });

  assert.equal(payload.ok, true);
  assert.equal(
    payload.sections.executiveCommercialSnapshot.totalQualifiedOpportunities,
    3,
  );
  assert.equal(
    payload.sections.executiveCommercialSnapshot.newOpportunitiesSincePriorPeriod,
    1,
  );
  assert.equal(
    payload.sections.executiveCommercialSnapshot.actionableOpportunities,
    2,
  );
  assert.ok(payload.sections.priorityCommercialOpportunities.items.length <= 5);
  // Sales-ready + need-period + NEW outweighs raw score alone
  assert.equal(
    payload.sections.priorityCommercialOpportunities.items[0].opportunityId,
    "a",
  );
  assert.ok(payload.sections.priorityCommercialOpportunities.items[0].deepLinkQuery);

  const receipt = payload.sections.researchReceipt;
  const byKey = Object.fromEntries(receipt.metrics.map((m) => [m.key, m]));
  assert.equal(byKey.uniqueOpportunitiesDiscovered.value, 3);
  assert.equal(byKey.newlyDiscoveredIntelligence.value, 1);
  assert.equal(byKey.demandSignalsEvaluated.value, null);
  assert.equal(
    byKey.demandSignalsEvaluated.availability,
    RESEARCH_RECEIPT_AVAILABILITY.TELEMETRY_REQUIRED,
  );

  const eng = payload.sections.commercialEngagementAndOutcomes;
  assert.equal(eng.opportunitiesPursued.value, 1);
  assert.equal(eng.opportunitiesWon.value, 1);

  const inv = payload.sections.investmentRecovery;
  assert.equal(inv.implemented, false);
  assert.ok(inv.hardcodedAdrValuesForbidden.includes(165));
  assert.ok(inv.metrics.every((m) => m.value == null));

  assert.equal(payload.pdfPreparation.pagesRequired, 2);
  assert.equal(payload.pdfPreparation.thirdPageProhibited, true);
  assert.equal(payload.governance.noStagedAsCustomerFallback, true);
  pass("current_inventory_aggregation");
}

{
  const staged = buildGdiExecutiveSummaryV1({
    hotelId: "recSTAGED",
    customerAuthorizedInventory: false,
    dataState: EXEC_SUMMARY_DATA_STATE.STAGED_ONLY_NOT_CUSTOMER,
    opportunities: [{ id: "should-not-count", title: "Hidden" }],
  });
  assert.equal(
    staged.sections.executiveCommercialSnapshot.totalQualifiedOpportunities,
    null,
  );
  assert.equal(staged.sections.priorityCommercialOpportunities.count, 0);
  assert.ok(
    String(
      staged.sections.executiveCommercialSnapshot.unavailableReason || "",
    ).includes("STAGED"),
  );
  pass("staged_not_customer_fallback");
}

{
  const none = buildGdiExecutiveSummaryV1({
    hotelId: "recNONE",
    customerAuthorizedInventory: false,
    dataState: EXEC_SUMMARY_DATA_STATE.NO_ELIGIBLE_PUBLICATION,
  });
  assert.equal(none.sections.priorityCommercialOpportunities.items.length, 0);
  pass("no_publication_empty_state");
}

{
  const ranked = prioritizeCustomerOpportunities(
    [
      { id: "low", score: 1 },
      { id: "high", score: 100, contactActionabilityTier: CONTACT_ACTIONABILITY_TIER.SALES_READY },
    ],
    5,
  );
  assert.equal(ranked[0].opportunityId, "high");
  pass("priority_reuses_existing_signals");
}

{
  const compact = buildCompactResearchReceiptForPdf(
    buildGdiExecutiveSummaryV1({
      hotelId: "recX",
      customerAuthorizedInventory: true,
      dataState: EXEC_SUMMARY_DATA_STATE.CURRENT_AVAILABLE,
      opportunities: [
        {
          id: "1",
          contactActionabilityTier: CONTACT_ACTIONABILITY_TIER.REACHABLE,
          weeklyDeltaState: WEEKLY_DELTA_STATE.UPDATED,
        },
      ],
    }).sections.researchReceipt.metrics,
  );
  assert.ok(compact.commercialContactPathsValidated);
  pass("pdf_compact_receipt_shape");
}

{
  const placeholder = buildInvestmentRecoveryPlaceholder();
  assert.equal(placeholder.syntheticEconomicsProhibited, true);
  pass("investment_recovery_placeholder");
}

{
  // Hotel isolation: two builds must not share opportunity IDs across hotels
  const a = buildGdiExecutiveSummaryV1({
    hotelId: "hotelA",
    customerAuthorizedInventory: true,
    dataState: EXEC_SUMMARY_DATA_STATE.CURRENT_AVAILABLE,
    opportunities: [{ id: "onlyA", score: 10 }],
  });
  const b = buildGdiExecutiveSummaryV1({
    hotelId: "hotelB",
    customerAuthorizedInventory: true,
    dataState: EXEC_SUMMARY_DATA_STATE.CURRENT_AVAILABLE,
    opportunities: [{ id: "onlyB", score: 10 }],
  });
  assert.equal(
    a.sections.priorityCommercialOpportunities.items[0].opportunityId,
    "onlyA",
  );
  assert.equal(
    b.sections.priorityCommercialOpportunities.items[0].opportunityId,
    "onlyB",
  );
  pass("hotel_isolation");
}

console.log(`\n${passed} PASS — gdi-executive-summary-v1 ($0)`);
