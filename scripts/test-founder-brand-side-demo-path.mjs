#!/usr/bin/env node
/**
 * Simulate founder/admin Brand-Side request path (middleware + entitlements).
 * No live HTTP. No auth weakening for production clients.
 */
import assert from "node:assert/strict";
import {
  applyDemoStakeholderActiveWorkspace,
  canUseDemoFounderNavOverrides,
  getDemoStakeholderWorkspaces,
} from "../lib/dealality/demo-stakeholder-workspace.js";
import {
  applyDemoBrandPortfolioContext,
  demoBrandPortfolioEntitlementOverride,
} from "../lib/dealality/demo-brand-portfolio-context.js";

function adminFounder() {
  return {
    email: "founder@dealality.com",
    companyId: "recAdminOnly",
    companyIds: ["recAdminOnly"],
    isAdmin: true,
    flags: { isAdmin: true },
    canAccessBrandWorkspace: false,
    isBrand: false,
    isDemo: false,
  };
}

function productionBrand() {
  return {
    email: "client@marriott.example",
    companyId: "recClient",
    companyIds: ["recClient"],
    isAdmin: false,
    isBrand: true,
    canAccessBrandWorkspace: true,
    isDemo: false,
    activeWorkspace: "Brand",
  };
}

console.log("test:founder-brand-side-demo-path\n");

const admin = adminFounder();
assert.equal(canUseDemoFounderNavOverrides(admin), true);
assert.ok(getDemoStakeholderWorkspaces(admin).includes("Brand"));
applyDemoStakeholderActiveWorkspace(admin, "Brand");
assert.equal(admin.activeWorkspace, "Brand");
assert.equal(admin.isBrand, true);
assert.equal(admin.canAccessBrandWorkspace, true);
applyDemoBrandPortfolioContext(admin, null); // omitted → marriott default
assert.equal(admin.demoBrandPortfolioKey, "marriott");
const override = demoBrandPortfolioEntitlementOverride(admin);
assert.ok(override);
assert.equal(override.entitledBrandIds.length, 5);
console.log("  ok — admin Brand-Side omitted header → marriott showcase (5 brands)");

const prod = productionBrand();
assert.equal(canUseDemoFounderNavOverrides(prod), false);
assert.deepEqual(getDemoStakeholderWorkspaces(prod), []);
applyDemoBrandPortfolioContext(prod, "marriott");
assert.equal(demoBrandPortfolioEntitlementOverride(prod), null);
console.log("  ok — production Brand client cannot use demo portfolio override");

console.log("\n2 passed");
