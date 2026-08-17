#!/usr/bin/env node
/**
 * Demo stakeholder workspace switching — unit + auth regression checks.
 */
import assert from "node:assert/strict";
import {
  applyDemoStakeholderActiveWorkspace,
  canUseDemoFounderNavOverrides,
  DEMO_ACTIVE_WORKSPACE_HEADER,
  DEMO_BRAND_PORTFOLIO,
  getDemoStakeholderWorkspaces,
  isDemoStakeholderConstellation,
  MAP_DEMO_STAKEHOLDER_COMPANIES,
  readActiveWorkspaceHeader,
} from "../lib/dealality/demo-stakeholder-workspace.js";
import {
  userCanAccessBrandWorkspace,
  userCanAccessOwnerWorkspace,
  userCanAccessOperatorWorkspace,
} from "../lib/dealality/user-workspace-gates.js";
import { requireBrandAiVisibilityAccess } from "../middleware/requireBrandAiVisibilityAccess.js";
import { requireMyDealsAccess } from "../middleware/requireMyDealsAccess.js";

const ownerId = MAP_DEMO_STAKEHOLDER_COMPANIES.Owner.companyId;
const brandId = MAP_DEMO_STAKEHOLDER_COMPANIES.Brand.companyId;

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
    canAccessDemoWorkspace: false,
    isOwner: true,
    isOperator: true,
    isBrand: false,
    isDemo: false,
    isAdmin: false,
    flags: { isOwner: true, isOperator: true, isBrand: false, isDemo: false },
  };
}

await test("constellation detection for Dealality demo companies", () => {
  assert.equal(isDemoStakeholderConstellation(baseDemoUser()), true);
  assert.equal(
    isDemoStakeholderConstellation({
      companyIds: [ownerId],
      workspaceAccess: ["Owner"],
    }),
    false
  );
  assert.deepEqual(getDemoStakeholderWorkspaces(baseDemoUser()), [
    "Owner",
    "Operator",
    "Brand",
  ]);
});

await test("demo isDemo without Brand company still gets full preview including Brand", () => {
  const demoOnlyOwnerLinked = {
    companyIds: [ownerId],
    companyId: ownerId,
    workspaceAccess: ["Owner", "Operator", "Demo"],
    isDemo: true,
    canAccessDemoWorkspace: true,
    flags: { isDemo: true },
  };
  assert.equal(isDemoStakeholderConstellation(demoOnlyOwnerLinked), true);
  assert.deepEqual(getDemoStakeholderWorkspaces(demoOnlyOwnerLinked), [
    "Owner",
    "Operator",
    "Brand",
  ]);
});

await test("DEMO workspace availability flags for founder constellation", () => {
  const ws = getDemoStakeholderWorkspaces(baseDemoUser());
  assert.equal(ws.includes("Owner"), true); // DEMO_OWNER_SIDE_AVAILABLE
  assert.equal(ws.includes("Brand"), true); // DEMO_BRAND_SIDE_AVAILABLE
  assert.equal(ws.includes("Operator"), true); // DEMO_OPERATOR_SIDE_AVAILABLE
});

await test("production owner-operator does NOT receive demo constellation switcher", () => {
  const prod = {
    companyId: "recProdOwnerOp",
    companyIds: ["recProdOwnerOp"],
    workspaceAccess: ["Owner", "Operator"],
    isOwnerOperator: true,
    isOwner: true,
    isOperator: true,
    isDemo: false,
    canAccessDemoWorkspace: false,
    flags: { isDemo: false },
  };
  assert.equal(isDemoStakeholderConstellation(prod), false);
  assert.deepEqual(getDemoStakeholderWorkspaces(prod), []);
});

await test("Brand-Side elevates Brand feature gate only", () => {
  const u = applyDemoStakeholderActiveWorkspace(baseDemoUser(), "Brand");
  assert.equal(u.activeWorkspace, "Brand");
  assert.equal(u.companyId, brandId);
  assert.equal(u.companyName, "Dealality Brand Demo");
  assert.equal(userCanAccessBrandWorkspace(u), true);
  assert.equal(userCanAccessOwnerWorkspace(u), true); // Airtable Owner truth retained
  assert.equal(u.canAccessBrandWorkspace, true);
  assert.equal(u.isBrand, true);
});

await test("Owner-Side forces Brand feature gate off", () => {
  const u = applyDemoStakeholderActiveWorkspace(baseDemoUser(), "Owner");
  assert.equal(u.activeWorkspace, "Owner");
  assert.equal(u.companyId, ownerId);
  assert.equal(userCanAccessBrandWorkspace(u), false);
  assert.equal(u.canAccessBrandWorkspace, false);
});

await test("Operator-Side does not grant Brand AI Visibility", () => {
  const u = applyDemoStakeholderActiveWorkspace(baseDemoUser(), "Operator");
  assert.equal(u.activeWorkspace, "Operator");
  assert.equal(userCanAccessBrandWorkspace(u), false);
  assert.equal(userCanAccessOperatorWorkspace(u), true);
});

await test("header reader accepts only Owner|Operator|Brand", () => {
  assert.equal(
    readActiveWorkspaceHeader({ headers: { [DEMO_ACTIVE_WORKSPACE_HEADER]: "Brand" } }),
    "Brand"
  );
  assert.equal(
    readActiveWorkspaceHeader({ headers: { [DEMO_ACTIVE_WORKSPACE_HEADER]: "Admin" } }),
    ""
  );
  assert.equal(
    readActiveWorkspaceHeader({ headers: { [DEMO_ACTIVE_WORKSPACE_HEADER]: "recHacker" } }),
    ""
  );
});

await test("production owner cannot become Brand via apply", () => {
  const prod = {
    companyId: "recProdOwner",
    companyIds: ["recProdOwner"],
    workspaceAccess: ["Owner"],
    canAccessOwnerWorkspace: true,
    canAccessBrandWorkspace: false,
    isOwner: true,
    isBrand: false,
    isDemo: false,
  };
  const before = { ...prod };
  const after = applyDemoStakeholderActiveWorkspace(prod, "Brand");
  assert.equal(after.canAccessBrandWorkspace, before.canAccessBrandWorkspace);
  assert.equal(after.companyId, before.companyId);
  assert.equal(isDemoStakeholderConstellation(prod), false);
});

await test("Brand AI Visibility middleware respects demo Brand-Side", async () => {
  const u = applyDemoStakeholderActiveWorkspace(baseDemoUser(), "Brand");
  let nextCalled = false;
  await new Promise((resolve) => {
    requireBrandAiVisibilityAccess({ dealalityUser: u }, mockRes(resolve), () => {
      nextCalled = true;
      resolve();
    });
  });
  assert.equal(nextCalled, true);

  const ownerSide = applyDemoStakeholderActiveWorkspace(baseDemoUser(), "Owner");
  const denied = await new Promise((resolve) => {
    requireBrandAiVisibilityAccess({ dealalityUser: ownerSide }, mockRes(resolve), () => {
      resolve({ next: true });
    });
  });
  assert.equal(denied.status, 403);
  assert.equal(denied.body.error, "forbidden_workspace");
});

await test("My Deals still requires Airtable Owner — Demo-only does not elevate", async () => {
  const demoOnly = {
    companyIds: [ownerId, brandId],
    companyId: ownerId,
    workspaceAccess: ["Demo"],
    canAccessOwnerWorkspace: false,
    canAccessOperatorWorkspace: false,
    canAccessBrandWorkspace: false,
    canAccessDemoWorkspace: true,
    isDemo: true,
    isOwner: false,
    isBrand: false,
    isOperator: false,
    flags: { isDemo: true },
  };
  // Force constellation path via company ids; Owner gate stays false from snap.
  const u = applyDemoStakeholderActiveWorkspace(demoOnly, "Owner");
  assert.equal(u.canAccessOwnerWorkspace, false);
  const denied = await new Promise((resolve) => {
    requireMyDealsAccess({ dealalityUser: u }, mockRes(resolve), () => resolve({ next: true }));
  });
  assert.notEqual(denied.next, true);
  assert.equal(denied.status, 403);
});

await test("demo brand portfolio is explicit Active Brand Basics set", () => {
  assert.equal(DEMO_BRAND_PORTFOLIO.length, 7);
  const ids = new Set();
  for (const b of DEMO_BRAND_PORTFOLIO) {
    assert.match(b.brandId, /^rec/);
    assert.ok(b.brandName);
    assert.match(b.note, /Dealality Brand Demo/);
    assert.equal(ids.has(b.brandId), false);
    ids.add(b.brandId);
  }
  assert.ok(ids.has("reclkgOzvAcBheUSo")); // Ascend retained
  assert.equal(ids.has("recOzH5iAE1xEjyD0"), false); // Comfort removed from demo (Phase 3A.2)
  assert.equal(ids.has("recmKqo7M7mLZgRqQ"), false); // Radisson RED removed from demo
});

await test("stakeholder nav helper hides Brand AI Visibility on Owner/Operator", async () => {
  const fs = await import("node:fs");
  const path = await import("node:path");
  const { fileURLToPath } = await import("node:url");
  const fixed = path.join(
    path.dirname(fileURLToPath(import.meta.url)),
    "..",
    "public",
    "js",
    "dealality-stakeholder-nav.js"
  );
  const src = fs.readFileSync(fixed, "utf8");
  assert.match(src, /brand_ai_visibility/);
  assert.match(src, /Operator AI Visibility/);
  assert.match(src, /AI Recommendation Intelligence/);
  const sandbox = {};
  sandbox.window = sandbox;
  sandbox.globalThis = sandbox;
  const fn = new Function("window", "globalThis", src + "; return globalThis.DealalityStakeholderNav;");
  const nav = fn(sandbox, sandbox);
  assert.equal(nav.stakeholderProductVisible("brand_ai_visibility", "brand"), true);
  assert.equal(nav.stakeholderProductVisible("brand_ai_visibility", "owner"), false);
  assert.equal(nav.stakeholderProductVisible("brand_ai_visibility", "operator"), false);
  assert.equal(nav.stakeholderProductVisible("brand_ai_visibility", "admin"), true);
  assert.equal(nav.stakeholderProductVisible("operator_ai_visibility", "operator"), false);
});

await test("founder nav overrides available for demo constellation / admin only", () => {
  assert.equal(canUseDemoFounderNavOverrides(baseDemoUser()), true); // DEMO_ADMIN + ALL via founder tools
  assert.equal(canUseDemoFounderNavOverrides({ isAdmin: true }), true);
  assert.equal(
    canUseDemoFounderNavOverrides({
      companyIds: ["recProd"],
      isDemo: false,
      isAdmin: false,
    }),
    false
  );
  // Demo flag alone without Owner+Brand company links must not unlock Admin/All
  assert.equal(
    canUseDemoFounderNavOverrides({
      companyIds: [ownerId],
      isDemo: true,
      canAccessDemoWorkspace: true,
      flags: { isDemo: true },
      isAdmin: false,
    }),
    false
  );
});

await test("app shell renders canonicalWorkspaceOptions (no competing rebuild)", async () => {
  const fs = await import("node:fs");
  const path = await import("node:path");
  const { fileURLToPath } = await import("node:url");
  const appJs = fs.readFileSync(
    path.join(path.dirname(fileURLToPath(import.meta.url)), "..", "public", "app.js"),
    "utf8"
  );
  assert.match(appJs, /canonicalWorkspaceOptions/);
  assert.match(appJs, /DEMO_WORKSPACE_CONSTELLATION_INVALID/);
  assert.match(appJs, /normalizeMeResultPayload/);
  assert.match(appJs, /canShowFounderNavOverrides/);
  // Must not rebuild demo list from workspaceAccess when canonical exists.
  assert.match(appJs, /Render-only: use server canonicalWorkspaceOptions/);
});

console.log("test-demo-stakeholder-workspace: ok");

function mockRes(resolve) {
  return {
    status(code) {
      this.statusCode = code;
      return this;
    },
    json(body) {
      resolve({ status: this.statusCode || 200, body });
      return this;
    },
  };
}

async function test(name, fn) {
  try {
    await fn();
    console.log("  ok —", name);
  } catch (err) {
    console.error("  FAIL —", name);
    throw err;
  }
}
