#!/usr/bin/env node
/**
 * Demo Brand Portfolio context — unit + auth regression checks.
 * No Airtable writes. No provider calls.
 */
import assert from "node:assert/strict";
import {
  applyDemoBrandPortfolioContext,
  canUseDemoBrandPortfolioSwitch,
  demoBrandPortfolioEntitlementOverride,
  isDemoBrandPortfolioContextActive,
  listDemoBrandPortfolioOptions,
  normalizeDemoBrandPortfolioKey,
  readDemoBrandPortfolioHeader,
  resolveDemoBrandPortfolio,
  DEMO_BRAND_PORTFOLIO_HEADER,
  DEFAULT_DEMO_BRAND_PORTFOLIO_KEY,
} from "../lib/dealality/demo-brand-portfolio-context.js";
import {
  MAP_DEMO_STAKEHOLDER_COMPANIES,
  applyDemoStakeholderActiveWorkspace,
} from "../lib/dealality/demo-stakeholder-workspace.js";
import { resolveAiIntelligenceAccess } from "../lib/ai-visibility/authorization.js";
import { buildFixtureEntitlementGraph } from "../lib/ai-visibility/entitlements.js";
import { normalizeAiVisibilityViewerContext } from "../lib/ai-visibility/viewer-context.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ownerId = MAP_DEMO_STAKEHOLDER_COMPANIES.Owner.companyId;
const brandId = MAP_DEMO_STAKEHOLDER_COMPANIES.Brand.companyId;

let passed = 0;
async function test(name, fn) {
  await fn();
  passed += 1;
  console.log(`  ok — ${name}`);
}

function baseDemoUser() {
  return {
    email: "dealalitydemo@dealality.com",
    companyId: ownerId,
    companyIds: [ownerId, brandId],
    companyName: "Dealality Owner Demo",
    workspaceAccess: ["Owner", "Operator"],
    canAccessOwnerWorkspace: true,
    canAccessOperatorWorkspace: true,
    canAccessBrandWorkspace: false,
    canAccessDemoWorkspace: true,
    isOwner: true,
    isOperator: true,
    isBrand: false,
    isDemo: true,
    isAdmin: false,
    flags: { isOwner: true, isOperator: true, isBrand: false, isDemo: true },
  };
}

function productionBrandClient() {
  return {
    email: "marriott-client@example.com",
    companyId: "recSomeMarriottClient",
    companyIds: ["recSomeMarriottClient"],
    workspaceAccess: ["Brand"],
    canAccessBrandWorkspace: true,
    isBrand: true,
    isDemo: false,
    isAdmin: false,
    flags: { isBrand: true, isDemo: false },
    activeWorkspace: "Brand",
  };
}

console.log("test:demo-brand-portfolio-context\n");

await test("DEMO_BRAND_PORTFOLIO_SELECTOR_VISIBLE_IN_BRAND_SIDE", () => {
  const u = applyDemoStakeholderActiveWorkspace(baseDemoUser(), "Brand");
  applyDemoBrandPortfolioContext(u, "marriott");
  assert.equal(canUseDemoBrandPortfolioSwitch(u), true);
  assert.equal(isDemoBrandPortfolioContextActive(u), true);
  assert.equal(u.demoBrandPortfolioKey, "marriott");
});

await test("DEMO_BRAND_PORTFOLIO_SELECTOR_HIDDEN_IN_OWNER_SIDE", () => {
  const u = applyDemoStakeholderActiveWorkspace(baseDemoUser(), "Owner");
  applyDemoBrandPortfolioContext(u, "marriott");
  assert.equal(isDemoBrandPortfolioContextActive(u), false);
  assert.equal(u.demoBrandPortfolioKey, null);
  assert.equal(demoBrandPortfolioEntitlementOverride(u), null);
});

await test("DEMO_BRAND_PORTFOLIO_SELECTOR_HIDDEN_IN_OPERATOR_SIDE", () => {
  const u = applyDemoStakeholderActiveWorkspace(baseDemoUser(), "Operator");
  applyDemoBrandPortfolioContext(u, "hilton");
  assert.equal(isDemoBrandPortfolioContextActive(u), false);
  assert.equal(demoBrandPortfolioEntitlementOverride(u), null);
});

await test("MARRIOTT_PORTFOLIO_RESOLVES_GOVERNED_BRANDS", () => {
  const r = resolveDemoBrandPortfolio("marriott");
  assert.equal(r.ok, true);
  assert.equal(r.brandIds.length, 5);
  assert.ok(r.brandIds.every((id) => id.startsWith("rec")));
});

await test("HILTON_PORTFOLIO_RESOLVES_GOVERNED_BRANDS", () => {
  const r = resolveDemoBrandPortfolio("hilton");
  assert.equal(r.ok, true);
  assert.equal(r.brandIds.length, 4);
});

await test("IHG_PORTFOLIO_RESOLVES_GOVERNED_BRANDS", () => {
  const r = resolveDemoBrandPortfolio("ihg");
  assert.equal(r.ok, true);
  assert.equal(r.brandIds.length, 5);
});

await test("CHOICE_PORTFOLIO_RESOLVES_GOVERNED_BRANDS", () => {
  const r = resolveDemoBrandPortfolio("choice");
  assert.equal(r.ok, true);
  assert.equal(r.brandIds.length, 4);
});

await test("EXECUTIVE_SUMMARY_USES_ACTIVE_PORTFOLIO", () => {
  const u = applyDemoStakeholderActiveWorkspace(baseDemoUser(), "Brand");
  applyDemoBrandPortfolioContext(u, "hilton");
  const override = demoBrandPortfolioEntitlementOverride(u);
  assert.ok(override);
  assert.equal(override.demoBrandPortfolioKey, "hilton");
  assert.equal(override.entitledBrandIds.length, 4);
  assert.equal(override.AIRTABLE_WRITES, 0);
});

await test("BRAND_SIDE_OMITTED_HEADER_DEFAULTS_TO_MARRIOTT", () => {
  const u = applyDemoStakeholderActiveWorkspace(baseDemoUser(), "Brand");
  applyDemoBrandPortfolioContext(u, null);
  assert.equal(u.demoBrandPortfolioKey, DEFAULT_DEMO_BRAND_PORTFOLIO_KEY);
  assert.equal(u.demoBrandPortfolioDefaulted, DEFAULT_DEMO_BRAND_PORTFOLIO_KEY);
  const override = demoBrandPortfolioEntitlementOverride(u);
  assert.ok(override);
  assert.equal(override.demoBrandPortfolioKey, "marriott");
  assert.equal(override.entitledBrandIds.length, 5);
});

await test("PRODUCTION_BRAND_OMITTED_HEADER_DOES_NOT_DEFAULT", () => {
  const u = productionBrandClient();
  applyDemoBrandPortfolioContext(u, null);
  assert.equal(u.demoBrandPortfolioKey, null);
  assert.equal(demoBrandPortfolioEntitlementOverride(u), null);
});

await test("DETAILED_VIEW_BRAND_LIST_USES_ACTIVE_PORTFOLIO", () => {
  const u = applyDemoStakeholderActiveWorkspace(baseDemoUser(), "Brand");
  applyDemoBrandPortfolioContext(u, "choice");
  const override = demoBrandPortfolioEntitlementOverride(u);
  assert.deepEqual(override.entitledBrandIds, resolveDemoBrandPortfolio("choice").brandIds);
});

await test("STALE_BRAND_SELECTION_RESETS_ON_PORTFOLIO_SWITCH", () => {
  const marriott = resolveDemoBrandPortfolio("marriott");
  const hilton = resolveDemoBrandPortfolio("hilton");
  const stale = marriott.brandIds[0];
  assert.ok(!hilton.brandIds.includes(stale));
  // UI clears sessionStorage; server denies cross-portfolio via entitlement graph.
  const graph = buildFixtureEntitlementGraph({
    entitledBrandIds: hilton.brandIds,
    source: "demo_showcase_portfolio",
  });
  const viewer = normalizeAiVisibilityViewerContext({
    email: "demo@dealality.com",
    companyId: brandId,
    isBrand: true,
    canAccessBrandWorkspace: true,
  });
  const access = resolveAiIntelligenceAccess({
    viewerContext: viewer,
    subject: { subjectType: "brand", subjectEntityId: stale },
    entitlementGraph: graph,
  });
  assert.equal(access.allowed, false);
});

await test("ADMIN_CANNOT_BYPASS_DEMO_SHOWCASE_PORTFOLIO", () => {
  const choice = resolveDemoBrandPortfolio("choice");
  const hilton = resolveDemoBrandPortfolio("hilton");
  const staleHilton = hilton.brandIds[0];
  assert.ok(!choice.brandIds.includes(staleHilton));
  const graph = buildFixtureEntitlementGraph({
    entitledBrandIds: choice.brandIds,
    source: "demo_showcase_portfolio",
  });
  const adminViewer = normalizeAiVisibilityViewerContext({
    email: "founder@dealality.com",
    companyId: brandId,
    isBrand: true,
    isAdmin: true,
    canAccessBrandWorkspace: true,
  });
  const denied = resolveAiIntelligenceAccess({
    viewerContext: adminViewer,
    subject: { subjectType: "brand", subjectEntityId: staleHilton },
    entitlementGraph: graph,
  });
  assert.equal(denied.allowed, false);
  const allowed = resolveAiIntelligenceAccess({
    viewerContext: adminViewer,
    subject: { subjectType: "brand", subjectEntityId: choice.brandIds[0] },
    entitlementGraph: graph,
  });
  assert.equal(allowed.allowed, true);
  assert.equal(allowed.accessDepth, "deep");
});

await test("NORMAL_CLIENT_CANNOT_USE_DEMO_PORTFOLIO_OVERRIDE", () => {
  const u = productionBrandClient();
  assert.equal(canUseDemoBrandPortfolioSwitch(u), false);
  applyDemoBrandPortfolioContext(u, "marriott");
  assert.equal(u.demoBrandPortfolioKey, null);
  assert.equal(demoBrandPortfolioEntitlementOverride(u), null);
});

await test("ARBITRARY_PORTFOLIO_REJECTED", () => {
  assert.equal(normalizeDemoBrandPortfolioKey("not-a-portfolio"), null);
  assert.equal(normalizeDemoBrandPortfolioKey("recFakeId"), null);
  const u = applyDemoStakeholderActiveWorkspace(baseDemoUser(), "Brand");
  applyDemoBrandPortfolioContext(u, "acme-hotels");
  assert.equal(u.demoBrandPortfolioKey, null);
  assert.equal(u.demoBrandPortfolioRejected, "UNKNOWN_DEMO_BRAND_PORTFOLIO");
});

await test("CROSS_PORTFOLIO_BRAND_ACCESS_DENIED", () => {
  const u = applyDemoStakeholderActiveWorkspace(baseDemoUser(), "Brand");
  applyDemoBrandPortfolioContext(u, "marriott");
  const override = demoBrandPortfolioEntitlementOverride(u);
  const ihg = resolveDemoBrandPortfolio("ihg");
  const graph = buildFixtureEntitlementGraph({
    entitledBrandIds: override.entitledBrandIds,
    source: override.source,
  });
  const viewer = normalizeAiVisibilityViewerContext({
    email: "demo@dealality.com",
    companyId: brandId,
    isBrand: true,
    canAccessBrandWorkspace: true,
  });
  const access = resolveAiIntelligenceAccess({
    viewerContext: viewer,
    subject: { subjectType: "brand", subjectEntityId: ihg.brandIds[0] },
    entitlementGraph: graph,
  });
  assert.equal(access.allowed, false);
});

await test("NO_AIRTABLE_WRITE_REQUIRED", () => {
  const options = listDemoBrandPortfolioOptions();
  assert.ok(options.length >= 4);
  const u = applyDemoStakeholderActiveWorkspace(baseDemoUser(), "Brand");
  applyDemoBrandPortfolioContext(u, DEFAULT_DEMO_BRAND_PORTFOLIO_KEY);
  const override = demoBrandPortfolioEntitlementOverride(u);
  assert.equal(override.AIRTABLE_WRITES, 0);
  assert.equal(override.AUTHORIZATION_BYPASS, false);
});

await test("header read uses governed key only", () => {
  assert.equal(
    readDemoBrandPortfolioHeader({
      headers: { [DEMO_BRAND_PORTFOLIO_HEADER]: "Hilton" },
    }),
    "hilton"
  );
  assert.equal(
    readDemoBrandPortfolioHeader({
      headers: { "X-Dealality-Demo-Brand-Portfolio": "evil" },
    }),
    null
  );
});

await test("UI assets include Brand Portfolio control outside AIV filter bar", () => {
  const root = path.join(__dirname, "..");
  const html = fs.readFileSync(path.join(root, "public/app.html"), "utf8");
  const appJs = fs.readFileSync(path.join(root, "public/app.js"), "utf8");
  const aivHtml = fs.readFileSync(path.join(root, "public/ai-visibility-brand.html"), "utf8");
  assert.match(html, /devBrandPortfolioSelect/);
  assert.match(html, /Brand Portfolio/);
  assert.match(appJs, /DEMO_BRAND_PORTFOLIO_STORAGE_KEY/);
  assert.match(appJs, /canShowDemoBrandPortfolioSelector/);
  assert.doesNotMatch(aivHtml, /Brand Portfolio/);
  assert.doesNotMatch(aivHtml, /devBrandPortfolioSelect/);
});

console.log(`\n${passed} passed`);
