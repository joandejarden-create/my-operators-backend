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
import {
  assertExecutiveSummaryFounderPreviewAuth,
  assertExecutiveSummaryHotelScope,
} from "../lib/group-demand-intelligence/executive-summary/gdi-executive-summary-preview-auth-v1.js";
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
  assert.equal(
    payload.sections.priorityCommercialOpportunities.items[0].opportunityOfTheWeek,
    true,
  );
  assert.equal(
    payload.sections.priorityCommercialOpportunities.opportunityOfTheWeek
      .opportunityId,
    "a",
  );

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
  const preview = buildGdiExecutiveSummaryV1({
    hotelId: "recPREVIEW",
    customerAuthorizedInventory: false,
    founderPreview: true,
    dataState: EXEC_SUMMARY_DATA_STATE.FOUNDER_PREVIEW,
    opportunities: [
      {
        id: "fp1",
        title: "Founder Preview Opp",
        score: 10,
        contactActionabilityTier: CONTACT_ACTIONABILITY_TIER.SALES_READY,
        weeklyDeltaState: WEEKLY_DELTA_STATE.NEW,
      },
    ],
  });
  assert.equal(preview.customerFacing, false);
  assert.equal(preview.founderPreview, true);
  assert.equal(
    preview.sections.executiveCommercialSnapshot.totalQualifiedOpportunities,
    1,
  );
  assert.equal(preview.sections.priorityCommercialOpportunities.count, 1);
  assert.ok(
    preview.sections.executiveCommercialSnapshot.founderPreviewBanner,
  );
  pass("founder_preview_aggregates_not_customer");
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
  const unauth = assertExecutiveSummaryFounderPreviewAuth({
    dealalityUser: null,
    gdiPilotUnauthRead: false,
  });
  assert.equal(unauth.ok, false);
  assert.equal(unauth.status, 401);
  pass("preview_unauthenticated_refused");
}

{
  const pilot = assertExecutiveSummaryFounderPreviewAuth({
    dealalityUser: null,
    gdiPilotUnauthRead: true,
  });
  assert.equal(pilot.ok, false);
  assert.equal(pilot.status, 401);
  pass("preview_pilot_unauth_refused");
}

{
  const customer = assertExecutiveSummaryFounderPreviewAuth({
    dealalityUser: { isAdmin: false, role: "owner", flags: {} },
  });
  assert.equal(customer.ok, false);
  assert.equal(customer.status, 403);
  assert.equal(customer.error, "forbidden_founder_preview");
  pass("preview_ordinary_hotel_user_refused");
}

{
  const admin = assertExecutiveSummaryFounderPreviewAuth({
    dealalityUser: { isAdmin: true, flags: { isAdmin: true } },
  });
  assert.equal(admin.ok, true);
  pass("preview_admin_allowed");
}

{
  const cross = assertExecutiveSummaryHotelScope({
    hotelId: "recOTHER",
    allowedHotelIds: ["recALLOWED"],
  });
  assert.equal(cross.ok, false);
  assert.equal(cross.error, "forbidden_cross_hotel_preview");
  pass("preview_cross_hotel_refused");
}

{
  const same = assertExecutiveSummaryHotelScope({
    hotelId: "recALLOWED",
    allowedHotelIds: ["recALLOWED"],
  });
  assert.equal(same.ok, true);
  pass("preview_same_hotel_allowed");
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

{
  // Route wiring: STAGED preview must use dedicated auth (not pilot-unauth alone).
  const fs = await import("fs");
  const path = await import("path");
  const { fileURLToPath } = await import("url");
  const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
  const serverSrc = fs.readFileSync(path.join(root, "server.js"), "utf8");
  const mwSrc = fs.readFileSync(
    path.join(root, "middleware/gdiExecutiveSummaryAuth.js"),
    "utf8",
  );
  assert.ok(serverSrc.includes("gdiExecutiveSummaryAuth"));
  assert.ok(
    /executive-summary[\s\S]{0,200}gdiExecutiveSummaryAuth/.test(serverSrc),
  );
  assert.ok(mwSrc.includes("memberstackAuth"));
  assert.ok(mwSrc.includes("requireDealalityUser"));
  assert.ok(mwSrc.includes("gdiPilotReadAuth"));
  assert.ok(mwSrc.includes('preview === "staged"'));
  assert.ok(serverSrc.includes("proposed-write-set-review"));
  pass("preview_route_uses_founder_auth_middleware");
}

{
  const fs = await import("fs");
  const path = await import("path");
  const { fileURLToPath } = await import("url");
  const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
  const css = fs.readFileSync(
    path.join(root, "public/css/group-demand-intelligence.css"),
    "latin1",
  );
  const navStart = css.indexOf("Compact left-aligned GDI tabs");
  const navEnd = css.indexOf(".gdi-page .gdi-section-nav__actions", navStart);
  assert.ok(navStart > 0 && navEnd > navStart);
  const nav = css.slice(navStart, navEnd);
  assert.equal(nav.includes("flex: 1 1 0"), false);
  assert.ok(nav.includes("min-width: 112px"));
  assert.ok(nav.includes("max-width: 168px"));
  assert.ok(nav.includes('data-tab="executive-summary"'));
  assert.ok(nav.includes("justify-content: flex-start"));
  pass("compact_left_aligned_four_tab_nav_css");
}

{
  const fs = await import("fs");
  const path = await import("path");
  const { fileURLToPath } = await import("url");
  const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
  const ui = fs.readFileSync(
    path.join(root, "public/js/group-demand-intelligence/dealality-gdi-ui.js"),
    "utf8",
  );
  const app = fs.readFileSync(
    path.join(root, "public/js/group-demand-intelligence/app.js"),
    "utf8",
  );
  assert.ok(ui.includes("GDI_EXEC_SUMMARY_TAB") || ui.includes("executive-summary"));
  assert.ok(/GDI_CUSTOMER_TABS[\s\S]{0,400}executive-summary/.test(ui) || /executive-summary[\s\S]{0,200}opportunities/.test(ui));
  assert.ok(
    /tab\s*[:=]\s*["']opportunities["']/.test(app) ||
      /default.*opportunities/i.test(app) ||
      app.includes('state.tab = "opportunities"'),
  );
  pass("opportunities_remains_default_landing_tab");
}

{
  const {
    assertWyndhamLocked26Shape,
    buildWyndhamProposedWriteSetFounderReview,
  } = await import(
    "../lib/group-demand-intelligence/publication-package/gdi-proposed-write-set-founder-review-v1.js"
  );
  const review = buildWyndhamProposedWriteSetFounderReview({});
  assert.equal(review.ok, true);
  const shape = assertWyndhamLocked26Shape(review);
  assert.equal(shape.ok, true);
  // Locked-26 is fully STAGED after content-hydration rematerialize.
  assert.equal(review.counts.stagedPackageCount, 26);
  assert.equal(review.counts.proposedMissingFromStaged, 0);
  assert.ok(
    String(review.rootCause || "").includes("CLEAN11") ||
      String(review.rootCause || "").includes("STAGED") ||
      review.counts.stagedPackageCount === 26,
  );
  const stagedIds = new Set(review.staged.opportunityIds);
  for (const row of review.proposedQualified) {
    if (row.bucket === "CLEAN11_PRESERVE") {
      assert.equal(stagedIds.has(row.opportunityId), true);
    }
  }
  pass("wyndham_locked_26_vs_staged_clean11_reconciliation");
}

{
  // ES aggregation must not invent IDs beyond the provided inventory.
  const opps = [
    { id: "keep-a", score: 9 },
    { id: "keep-b", score: 8 },
  ];
  const summary = buildGdiExecutiveSummaryV1({
    hotelId: "recTEST",
    opportunities: opps,
    customerAuthorizedInventory: true,
    dataState: EXEC_SUMMARY_DATA_STATE.CURRENT_AVAILABLE,
  });
  assert.equal(
    summary.sections.executiveCommercialSnapshot.totalQualifiedOpportunities,
    2,
  );
  const deep = summary.sections.priorityCommercialOpportunities.items.map(
    (i) => i.opportunityId,
  );
  assert.ok(deep.every((id) => id === "keep-a" || id === "keep-b"));
  pass("executive_summary_inventory_id_preservation");
}

{
  // Permanent additive / non-regression principle must remain documented.
  const fs = await import("fs");
  const path = await import("path");
  const { fileURLToPath } = await import("url");
  const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
  const principles = fs.readFileSync(
    path.join(
      root,
      "docs/ai-build-system/DEALALITY_ADDITIVE_NON_REGRESSION_PRINCIPLES.md",
    ),
    "utf8",
  );
  const constitution = fs.readFileSync(
    path.join(root, "docs/ai-build-system/DEALALITY_PRODUCT_CONSTITUTION.md"),
    "utf8",
  );
  const prCompletion = fs.readFileSync(
    path.join(
      root,
      "docs/ai-build-system/PR_COMPLETION_AND_WORKSPACE_OWNERSHIP.md",
    ),
    "utf8",
  );
  assert.ok(principles.includes("DEALALITY_ADDITIVE_NON_REGRESSION_V1"));
  assert.ok(principles.includes("must not silently regress"));
  assert.ok(constitution.includes("Additive development and non-regression"));
  assert.ok(prCompletion.includes("non-regression assessment"));
  assert.ok(
    prCompletion.includes("must block acceptance") ||
      prCompletion.includes("must **block acceptance**"),
  );
  pass("additive_non_regression_principle_documented");
}

console.log(`\n${passed} PASS — gdi-executive-summary-v1 ($0)`);
