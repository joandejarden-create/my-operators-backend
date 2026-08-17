#!/usr/bin/env node
/**
 * Phase 2F tests — company-scoped authorization / subject access.
 * No provider calls. No Airtable writes.
 */
import assert from "node:assert/strict";
import os from "os";
import path from "path";
import {
  normalizeAiVisibilityViewerContext,
  buildFixtureViewerContext,
  normalizeAiVisibilitySubject,
  SUBJECT_TYPES,
  ACCESS_DEPTH,
  ACCESS_REASON,
  resolveEntitledBrands,
  resolveEntitledOperators,
  resolveEntitledDeals,
  resolveBrandPortfolio,
  buildFixtureEntitlementGraph,
  resolveAiIntelligenceAccess,
  buildAiIntelligenceQueryContext,
  getAuthorizedVisibilityOverview,
  getAuthorizedEvidence,
  toBenchmarkSafeEntityView,
  filterEvidenceByAccessDepth,
  auditOwnerAiRecommendationContext,
  createAiVisibilityStore,
} from "../lib/ai-visibility/index.js";
import { dealRecordAllowedForUser as dealAccess } from "../lib/dealality/deal-record-access.js";

let passed = 0;
let failed = 0;

function test(name, fn) {
  try {
    const ret = fn();
    if (ret && typeof ret.then === "function") {
      return ret
        .then(() => {
          passed += 1;
          console.log(`  PASS ${name}`);
        })
        .catch((err) => {
          failed += 1;
          console.error(`  FAIL ${name}: ${err.message}`);
        });
    }
    passed += 1;
    console.log(`  PASS ${name}`);
  } catch (err) {
    failed += 1;
    console.error(`  FAIL ${name}: ${err.message}`);
  }
}

const BRAND_A1 = "recBrandA10000001";
const BRAND_A2 = "recBrandA20000002";
const BRAND_B1 = "recBrandB10000003";
const BRAND_C = "recBrandC00000004";
const OP_A = "recOperA000000001";
const OP_B = "recOperB000000002";
const OP_C = "recOperC000000003";
const COMPANY_A = "recCompanyA000001";
const COMPANY_B = "recCompanyB000002";
const DEAL_A = "recDealA000000001";
const DEAL_B = "recDealB000000002";
const HOTEL_A = "recHotelA00000001";

function brandViewerA() {
  return buildFixtureViewerContext({
    viewerUserId: "recUserBrandA",
    viewerCompanyId: COMPANY_A,
    viewerCompanyName: "Company A Brands",
    viewerCompanyType: "Hotel Brands (Franchise)",
    isBrand: true,
    workspaceAccess: ["Brand"],
  });
}

function operatorViewerA() {
  return buildFixtureViewerContext({
    viewerUserId: "recUserOpA",
    viewerCompanyId: COMPANY_A,
    viewerCompanyName: "Operator A",
    isOperator: true,
    workspaceAccess: ["Operator"],
  });
}

function ownerViewerA() {
  return buildFixtureViewerContext({
    viewerUserId: "recUserOwnerA",
    viewerCompanyId: COMPANY_A,
    viewerCompanyName: "Owner A",
    isOwner: true,
    workspaceAccess: ["Owner"],
  });
}

function brandGraphA() {
  return buildFixtureEntitlementGraph({
    entitledBrandIds: [BRAND_A1, BRAND_A2],
    peerBrandIds: [BRAND_A1, BRAND_A2, BRAND_B1],
    source: "fixture_brand_a",
  });
}

function operatorGraphA() {
  return buildFixtureEntitlementGraph({
    entitledOperatorIds: [OP_A],
    peerOperatorIds: [OP_A, OP_B],
    source: "fixture_operator_a",
  });
}

function ownerGraphA() {
  return buildFixtureEntitlementGraph({
    entitledDealIds: [DEAL_A],
    hotelToDealIds: { [HOTEL_A]: [DEAL_A] },
    source: "fixture_owner_a",
  });
}

async function run() {
  console.log("AI Visibility Phase 2F tests\n");

  console.log("Viewer / subject normalization");
  await test("viewer normalization from dealality-like user", () => {
    const v = normalizeAiVisibilityViewerContext({
      found: true,
      userRecordId: "recU1",
      memberstackId: "ms1",
      companyId: COMPANY_A,
      companyIds: [COMPANY_A],
      companyName: "Acme",
      companyType: "Brand",
      workspaceAccess: ["Brand"],
      isBrand: true,
      canAccessBrandWorkspace: true,
      isAdmin: false,
      flags: {},
    });
    assert.equal(v.viewerUserId, "recU1");
    assert.equal(v.viewerCompanyId, COMPANY_A);
    assert.equal(v.isBrand, true);
    assert.ok(v.roles.includes("brand"));
  });

  await test("subject types normalize; invalid rejected", () => {
    const ok = normalizeAiVisibilitySubject({
      subjectType: "brand",
      subjectEntityId: BRAND_A1,
    });
    assert.equal(ok.ok, true);
    assert.equal(ok.subject.subjectType, SUBJECT_TYPES.BRAND);
    const bad = normalizeAiVisibilitySubject({ subjectType: "parent_company" });
    assert.equal(bad.ok, false);
  });

  console.log("\nBrand entitlement + depth");
  await test("entitled brands A1/A2 deep; peer B1 comparative; C none", () => {
    const viewer = brandViewerA();
    const graph = brandGraphA();
    const a1 = resolveAiIntelligenceAccess({
      viewerContext: viewer,
      subject: { subjectType: "brand", subjectEntityId: BRAND_A1 },
      entitlementGraph: graph,
    });
    assert.equal(a1.accessDepth, ACCESS_DEPTH.DEEP);
    assert.equal(a1.reasonCode, ACCESS_REASON.ENTITLED_BRAND);

    const a2 = resolveAiIntelligenceAccess({
      viewerContext: viewer,
      subject: { subjectType: "brand", subjectEntityId: BRAND_A2 },
      entitlementGraph: graph,
    });
    assert.equal(a2.accessDepth, ACCESS_DEPTH.DEEP);

    const b1 = resolveAiIntelligenceAccess({
      viewerContext: viewer,
      subject: { subjectType: "brand", subjectEntityId: BRAND_B1 },
      entitlementGraph: graph,
    });
    assert.equal(b1.accessDepth, ACCESS_DEPTH.COMPARATIVE);
    assert.equal(b1.reasonCode, ACCESS_REASON.PEER_BRAND_COMPARATIVE);

    const c = resolveAiIntelligenceAccess({
      viewerContext: viewer,
      subject: { subjectType: "brand", subjectEntityId: BRAND_C },
      entitlementGraph: graph,
    });
    assert.equal(c.allowed, false);
    assert.equal(c.accessDepth, ACCESS_DEPTH.NONE);
  });

  await test("brand portfolio deep for own company only; no composite score", () => {
    const viewer = brandViewerA();
    const graph = brandGraphA();
    const own = resolveAiIntelligenceAccess({
      viewerContext: viewer,
      subject: { subjectType: "brand_portfolio", subjectCompanyId: COMPANY_A },
      entitlementGraph: graph,
    });
    assert.equal(own.allowed, true);
    assert.equal(own.reasonCode, ACCESS_REASON.ENTITLED_BRAND_PORTFOLIO);
    assert.equal(own.portfolio.portfolioCompositeScore, null);

    const other = resolveAiIntelligenceAccess({
      viewerContext: viewer,
      subject: { subjectType: "brand_portfolio", subjectCompanyId: COMPANY_B },
      entitlementGraph: graph,
    });
    assert.equal(other.allowed, false);
    assert.equal(other.reasonCode, ACCESS_REASON.WORKSPACE_MISMATCH);
  });

  await test("resolveEntitledBrands uses link IDs only (no text inference)", () => {
    const r = resolveEntitledBrands({
      viewerContext: brandViewerA(),
      companyFields: {
        "Brands You Operate / Support": [BRAND_A1, BRAND_A2],
      },
    });
    assert.deepEqual(r.brandIds.sort(), [BRAND_A1, BRAND_A2].sort());
    assert.equal(r.textInferenceUsed, false);
  });

  console.log("\nOperator entitlement");
  await test("operator A deep; peer B comparative; C none", () => {
    const viewer = operatorViewerA();
    const graph = operatorGraphA();
    assert.equal(
      resolveAiIntelligenceAccess({
        viewerContext: viewer,
        subject: { subjectType: "operator", subjectEntityId: OP_A },
        entitlementGraph: graph,
      }).accessDepth,
      ACCESS_DEPTH.DEEP
    );
    assert.equal(
      resolveAiIntelligenceAccess({
        viewerContext: viewer,
        subject: { subjectType: "operator", subjectEntityId: OP_B },
        entitlementGraph: graph,
      }).accessDepth,
      ACCESS_DEPTH.COMPARATIVE
    );
    assert.equal(
      resolveAiIntelligenceAccess({
        viewerContext: viewer,
        subject: { subjectType: "operator", subjectEntityId: OP_C },
        entitlementGraph: graph,
      }).accessDepth,
      ACCESS_DEPTH.NONE
    );
  });

  console.log("\nOwner deal entitlement");
  await test("owner deal A deep; deal B none; hotel via deal", () => {
    const viewer = ownerViewerA();
    const graph = ownerGraphA();
    const dA = resolveAiIntelligenceAccess({
      viewerContext: viewer,
      subject: { subjectType: "deal", subjectDealId: DEAL_A },
      entitlementGraph: graph,
    });
    assert.equal(dA.accessDepth, ACCESS_DEPTH.DEEP);
    assert.equal(dA.reasonCode, ACCESS_REASON.ENTITLED_DEAL);

    const dB = resolveAiIntelligenceAccess({
      viewerContext: viewer,
      subject: { subjectType: "deal", subjectDealId: DEAL_B },
      entitlementGraph: graph,
    });
    assert.equal(dB.allowed, false);

    const hotel = resolveAiIntelligenceAccess({
      viewerContext: viewer,
      subject: { subjectType: "hotel_asset", subjectHotelId: HOTEL_A },
      entitlementGraph: graph,
    });
    assert.equal(hotel.accessDepth, ACCESS_DEPTH.DEEP);
  });

  await test("deal-record-access still gates live deal fields", () => {
    const user = {
      isAdmin: false,
      isOwner: true,
      companyId: COMPANY_A,
      companyIds: [COMPANY_A],
      userRecordId: "recUserOwnerA",
    };
    assert.equal(dealAccess({ "Company Profile": [COMPANY_A] }, user), true);
    assert.equal(dealAccess({ "Company Profile": [COMPANY_B] }, user), false);
  });

  console.log("\nAdmin + cross-tenant");
  await test("admin override deep on foreign brand", () => {
    const admin = buildFixtureViewerContext({
      viewerUserId: "recAdmin",
      viewerCompanyId: COMPANY_A,
      isAdmin: true,
      workspaceAccess: ["Admin"],
    });
    const r = resolveAiIntelligenceAccess({
      viewerContext: admin,
      subject: { subjectType: "brand", subjectEntityId: BRAND_C },
      entitlementGraph: brandGraphA(),
    });
    assert.equal(r.accessDepth, ACCESS_DEPTH.DEEP);
    assert.equal(r.reasonCode, ACCESS_REASON.ADMIN_OVERRIDE);
  });

  await test("Brand A cannot deep-view Brand B (peer only comparative)", () => {
    const r = resolveAiIntelligenceAccess({
      viewerContext: brandViewerA(),
      subject: { subjectType: "brand", subjectEntityId: BRAND_B1 },
      entitlementGraph: brandGraphA(),
    });
    assert.notEqual(r.accessDepth, ACCESS_DEPTH.DEEP);
    assert.equal(r.accessDepth, ACCESS_DEPTH.COMPARATIVE);
  });

  await test("Operator cannot get Brand deep access", () => {
    const r = resolveAiIntelligenceAccess({
      viewerContext: operatorViewerA(),
      subject: { subjectType: "brand", subjectEntityId: BRAND_A1 },
      entitlementGraph: brandGraphA(),
    });
    assert.equal(r.allowed, false);
    assert.equal(r.reasonCode, ACCESS_REASON.WORKSPACE_MISMATCH);
  });

  await test("Brand cannot access owner deal", () => {
    const r = resolveAiIntelligenceAccess({
      viewerContext: brandViewerA(),
      subject: { subjectType: "deal", subjectDealId: DEAL_A },
      entitlementGraph: ownerGraphA(),
    });
    assert.equal(r.allowed, false);
    assert.equal(r.reasonCode, ACCESS_REASON.WORKSPACE_MISMATCH);
  });

  await test("Owner A cannot access Owner B deal", () => {
    const r = resolveAiIntelligenceAccess({
      viewerContext: ownerViewerA(),
      subject: { subjectType: "deal", subjectDealId: DEAL_B },
      entitlementGraph: ownerGraphA(),
    });
    assert.equal(r.allowed, false);
  });

  await test("direct subject-ID swap does not escalate comparative→deep", () => {
    const viewer = brandViewerA();
    const graph = brandGraphA();
    const peer = resolveAiIntelligenceAccess({
      viewerContext: viewer,
      subject: { subjectType: "brand", subjectEntityId: BRAND_B1 },
      entitlementGraph: graph,
    });
    assert.equal(peer.accessDepth, ACCESS_DEPTH.COMPARATIVE);
    // Attempt to "swap" by requesting deep-looking portfolio of competitor company
    const swap = resolveAiIntelligenceAccess({
      viewerContext: viewer,
      subject: { subjectType: "brand_portfolio", subjectCompanyId: COMPANY_B },
      entitlementGraph: graph,
    });
    assert.equal(swap.allowed, false);
  });

  console.log("\nGeography / provider independence");
  await test("same entitlement across geographies and providers", () => {
    const viewer = brandViewerA();
    const graph = brandGraphA();
    for (const geo of [
      { geographyScope: "Global" },
      { geographyScope: "Region", region: "CALA" },
      { geographyScope: "Region", region: "Europe" },
      { geographyScope: "Region", region: "North America" },
      { geographyScope: "Country", country: "Mexico" },
    ]) {
      const q = buildAiIntelligenceQueryContext({
        viewerContext: viewer,
        subject: { subjectType: "brand", subjectEntityId: BRAND_A1 },
        entitlementGraph: graph,
        provider: "openai",
        ...geo,
      });
      assert.equal(q.allowed, true);
      assert.equal(q.accessDepth, ACCESS_DEPTH.DEEP);
      assert.equal(q.provider, "openai");
    }
  });

  console.log("\nEvidence depth filtering");
  await test("comparative evidence limited; deep full; unauthorized empty", async () => {
    const records = [
      {
        evidenceId: "ev1",
        entityId: BRAND_B1,
        entityName: "Brand B1",
        promptId: "p1",
        opportunityQueue: ["secret"],
        diagnosticReason: "hidden",
        presenceObserved: true,
        citationCount: 2,
      },
      {
        evidenceId: "ev2",
        entityId: BRAND_B1,
        opportunityQueue: ["x"],
      },
      {
        evidenceId: "ev3",
        entityId: BRAND_B1,
      },
      {
        evidenceId: "ev4",
        entityId: BRAND_B1,
      },
    ];
    const limited = filterEvidenceByAccessDepth(records, {
      accessDepth: ACCESS_DEPTH.COMPARATIVE,
      subjectEntityId: BRAND_B1,
    });
    assert.equal(limited.reason, "COMPETITOR_LIMITED_EVIDENCE");
    assert.equal(limited.evidence.length, 3);
    assert.equal(limited.evidence[0].opportunityQueue, undefined);

    const deep = filterEvidenceByAccessDepth(records, {
      accessDepth: ACCESS_DEPTH.DEEP,
      subjectEntityId: BRAND_B1,
    });
    assert.equal(deep.evidence.length, 4);

    const none = filterEvidenceByAccessDepth(records, { accessDepth: ACCESS_DEPTH.NONE });
    assert.equal(none.evidence.length, 0);

    const authEv = await getAuthorizedEvidence({
      viewerContext: brandViewerA(),
      subject: { subjectType: "brand", subjectEntityId: BRAND_B1 },
      entitlementGraph: brandGraphA(),
      evidenceRecords: records,
    });
    assert.equal(authEv.accessDepth, ACCESS_DEPTH.COMPARATIVE);
    assert.ok(authEv.evidence.length <= 3);
  });

  await test("benchmark-safe view strips diagnostic fields", () => {
    const safe = toBenchmarkSafeEntityView({
      entityId: BRAND_B1,
      entityName: "B1",
      aiPresenceRate: 0.5,
      competitivePosition: 2,
      recommendationShare: 0.1,
      opportunityQueue: ["nope"],
      privateWorkflow: true,
    });
    assert.equal(safe.aiPresenceRate, 0.5);
    assert.equal(safe.opportunityQueue, undefined);
    assert.equal(safe.privateWorkflow, undefined);
  });

  console.log("\nAuthorized overview + portfolio");
  await test("portfolio overview returns member brands without composite score", async () => {
    const root = path.join(os.tmpdir(), `aiv-2f-${Date.now()}`);
    const store = createAiVisibilityStore({ rootDir: root });
    await store.saveMetricSnapshot({
      entityId: BRAND_A1,
      entityName: "Brand A1",
      metric: "aiPresenceRate",
      value: 0.8,
      geographyScope: "Region",
      commercialRegion: "CALA",
      provider: "openai",
      batchDate: "2026-08-13",
    });
    await store.saveMetricSnapshot({
      entityId: BRAND_A2,
      entityName: "Brand A2",
      metric: "aiPresenceRate",
      value: 0.4,
      geographyScope: "Region",
      commercialRegion: "CALA",
      provider: "openai",
      batchDate: "2026-08-13",
    });

    const overview = await getAuthorizedVisibilityOverview({
      viewerContext: brandViewerA(),
      subject: { subjectType: "brand_portfolio", subjectCompanyId: COMPANY_A },
      entitlementGraph: brandGraphA(),
      geographyScope: "Region",
      region: "CALA",
      provider: "openai",
      store,
      brandNamesById: { [BRAND_A1]: "Brand A1", [BRAND_A2]: "Brand A2" },
    });
    assert.equal(overview.ok, true);
    assert.equal(overview.overview.portfolioCompositeScore, null);
    assert.equal(overview.overview.brands.length, 2);
  });

  await test("owner overview uses AI Recommendation Intelligence surface", async () => {
    const root = path.join(os.tmpdir(), `aiv-2f-owner-${Date.now()}`);
    const store = createAiVisibilityStore({ rootDir: root });
    const overview = await getAuthorizedVisibilityOverview({
      viewerContext: ownerViewerA(),
      subject: { subjectType: "deal", subjectDealId: DEAL_A },
      entitlementGraph: ownerGraphA(),
      store,
    });
    assert.equal(overview.productSurface, "AI Recommendation Intelligence");
    assert.ok(overview.overview.layers.aiRecommendationPattern === null);
    assert.ok(overview.overview.layers.dealalityAnalysis === null);
    assert.ok(overview.overview.layers.ownerProcess === null);
  });

  console.log("\nOwner context audit + entitlements helpers");
  await test("owner AI recommendation context audit (no schema change)", () => {
    const audit = auditOwnerAiRecommendationContext();
    assert.equal(audit.schemaChangesProposed, 0);
    assert.ok(["PARTIAL", "YES", "NO"].includes(audit.OWNER_AI_RECOMMENDATION_CONTEXT_READY));
    assert.ok(audit.AVAILABLE_FIELDS.includes("country"));
    assert.ok(audit.AVAILABLE_FIELDS.includes("key_count"));
    assert.equal(audit.ownerProductName, "AI Recommendation Intelligence");
  });

  await test("resolveEntitledOperators from Users link field only", () => {
    const r = resolveEntitledOperators({
      viewerContext: operatorViewerA(),
      userFields: { "Operator Setup - Master": [OP_A] },
    });
    assert.deepEqual(r.operatorIds, [OP_A]);
    assert.equal(r.textInferenceUsed, false);
  });

  await test("resolveEntitledDeals from injected graph", () => {
    const r = resolveEntitledDeals({
      viewerContext: ownerViewerA(),
      entitlementGraph: ownerGraphA(),
    });
    assert.deepEqual(r.dealIds, [DEAL_A]);
  });

  await test("brand portfolio helper has no composite score", () => {
    const p = resolveBrandPortfolio({
      viewerContext: brandViewerA(),
      entitlementGraph: brandGraphA(),
    });
    assert.equal(p.portfolioCompositeScore, null);
    assert.equal(p.brands.length, 2);
  });

  console.log(`\nPhase 2F: ${passed} passed, ${failed} failed`);
  if (failed) process.exit(1);
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
