#!/usr/bin/env node
/**
 * Durable regression: founder/demo workspace constellation must survive
 * auth resolve → portfolio → storage → nav rebuild → /api/me refresh →
 * workspace/portfolio switches → shell reload.
 *
 * Proves shell render boundary uses canonicalWorkspaceOptions (not a
 * competing workspaceAccess rebuild).
 */
import assert from "node:assert/strict";
import {
  applyDemoStakeholderActiveWorkspace,
  enrichDealalityMeForDemoStakeholder,
  MAP_DEMO_STAKEHOLDER_COMPANIES,
} from "../lib/dealality/demo-stakeholder-workspace.js";
import {
  applyDemoBrandPortfolioContext,
  listDemoBrandPortfolioOptions,
} from "../lib/dealality/demo-brand-portfolio-context.js";
import {
  assertDemoWorkspaceConstellation,
  pickActiveWorkspaceFromStorage,
  resolveProductionWorkspaceOptions,
  resolveWorkspaceOptions,
  shellRenderWorkspaceOptions,
} from "../lib/dealality/resolve-workspace-options.js";

const ownerId = MAP_DEMO_STAKEHOLDER_COMPANIES.Owner.companyId;
const brandId = MAP_DEMO_STAKEHOLDER_COMPANIES.Brand.companyId;

const EXPECTED = Object.freeze(["Owner", "Operator", "Brand"]);

let passed = 0;
let failed = 0;

async function test(name, fn) {
  try {
    await fn();
    passed += 1;
    console.log(`ok - ${name}`);
  } catch (err) {
    failed += 1;
    console.error(`FAIL - ${name}`);
    console.error(err);
  }
}

function baseDemoDealality() {
  return {
    email: "dealalitydemo@dealality.com",
    companyProfileId: ownerId,
    companyId: ownerId,
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
    role: "owner",
    legacyRole: "owner",
  };
}

function assertConstellationFlags(options, label) {
  assert.equal(options.DEMO_OWNER_SIDE_AVAILABLE, true, `${label}: OWNER`);
  assert.equal(options.DEMO_BRAND_SIDE_AVAILABLE, true, `${label}: BRAND`);
  assert.equal(options.DEMO_OPERATOR_SIDE_AVAILABLE, true, `${label}: OPERATOR`);
  assert.equal(options.DEMO_ADMIN_AVAILABLE, true, `${label}: ADMIN`);
  assert.equal(options.DEMO_ALL_WORKSPACES_AVAILABLE, true, `${label}: ALL`);
  assert.deepEqual(options.workspaces, EXPECTED, `${label}: workspaces`);
  const assertion = assertDemoWorkspaceConstellation(options);
  assert.equal(assertion.ok, true, `${label}: assert ok`);
}

function assertShellOptions(dealality, label) {
  const rendered = shellRenderWorkspaceOptions(dealality);
  assert.deepEqual(rendered, EXPECTED, `${label}: shell render`);
  assert.equal(rendered.includes("Brand"), true, `${label}: DEMO_BRAND_SIDE_AVAILABLE`);
  assert.equal(rendered.includes("Owner"), true, `${label}: DEMO_OWNER_SIDE_AVAILABLE`);
  assert.equal(rendered.includes("Operator"), true, `${label}: DEMO_OPERATOR_SIDE_AVAILABLE`);
}

/**
 * Simulate public/app.js auth-ready detail vs whenReady().me shapes.
 */
function normalizeMeResultPayload(raw) {
  if (!raw || typeof raw !== "object") return null;
  if (raw.data && raw.data.dealality) return raw;
  if (raw.me && raw.me.data && raw.me.data.dealality) return raw.me;
  if (raw.dealality) return { ok: true, data: { dealality: raw.dealality } };
  return null;
}

await test("lifecycle: founder demo constellation survives every shell step", () => {
  const lifecycle = {};

  // 1. initialize founder/demo user + resolve
  let dealality = enrichDealalityMeForDemoStakeholder(baseDemoDealality(), {
    companyIds: [ownerId, brandId],
    requestedWorkspace: "Owner",
  });
  lifecycle.INITIAL = [...dealality.canonicalWorkspaceOptions.workspaces];
  assertConstellationFlags(dealality.canonicalWorkspaceOptions, "INITIAL");
  assertShellOptions(dealality, "INITIAL");

  // 2. after auth resolution (same as /api/me enrich)
  lifecycle.AFTER_AUTH = [...shellRenderWorkspaceOptions(dealality)];
  assertShellOptions(dealality, "AFTER_AUTH");

  // 3. Brand Portfolio context attach (must not shrink)
  applyDemoBrandPortfolioContext(dealality, "marriott");
  dealality.demoBrandPortfolioOptions = listDemoBrandPortfolioOptions();
  dealality.canonicalWorkspaceOptions = resolveWorkspaceOptions(dealality);
  lifecycle.AFTER_DEMO_PORTFOLIO = [...shellRenderWorkspaceOptions(dealality)];
  assertShellOptions(dealality, "AFTER_DEMO_PORTFOLIO");

  // 4. storage restore of active workspace only (not option list)
  const storedActive = "Owner";
  const active = pickActiveWorkspaceFromStorage(
    shellRenderWorkspaceOptions(dealality),
    storedActive,
    dealality.activeWorkspace
  );
  assert.equal(active, "Owner");
  lifecycle.AFTER_STORAGE = [...shellRenderWorkspaceOptions(dealality)];
  assertShellOptions(dealality, "AFTER_STORAGE");

  // 5. nav rebuild simulation — options unchanged (renderNav must not rebuild list)
  const afterNav = shellRenderWorkspaceOptions(dealality);
  lifecycle.AFTER_NAV = [...afterNav];
  assertShellOptions(dealality, "AFTER_NAV");

  // 6. simulate /api/me refresh (re-enrich + portfolio)
  dealality = enrichDealalityMeForDemoStakeholder(
    {
      ...baseDemoDealality(),
      workspaceAccess: ["Owner"], // partial WA must not shrink constellation
    },
    {
      companyIds: [ownerId, brandId],
      requestedWorkspace: "Owner",
    }
  );
  applyDemoBrandPortfolioContext(dealality, "hilton");
  dealality.canonicalWorkspaceOptions = resolveWorkspaceOptions(dealality);
  lifecycle.AFTER_ME_REFRESH = [...shellRenderWorkspaceOptions(dealality)];
  assertShellOptions(dealality, "AFTER_ME_REFRESH");

  // 7. switch Brand-Side
  dealality = applyDemoStakeholderActiveWorkspace({ ...dealality }, "Brand");
  dealality.canonicalWorkspaceOptions = resolveWorkspaceOptions(dealality);
  assert.equal(dealality.activeWorkspace, "Brand");
  assertShellOptions(dealality, "AFTER_BRAND_SIDE");

  // 8. switch Marriott → Hilton portfolio
  applyDemoBrandPortfolioContext(dealality, "hilton");
  dealality.canonicalWorkspaceOptions = resolveWorkspaceOptions(dealality);
  assertShellOptions(dealality, "AFTER_PORTFOLIO_HILTON");

  // 9. switch back Owner-Side
  dealality = applyDemoStakeholderActiveWorkspace({ ...dealality }, "Owner");
  dealality.canonicalWorkspaceOptions = resolveWorkspaceOptions(dealality);
  assert.equal(dealality.activeWorkspace, "Owner");
  assertShellOptions(dealality, "AFTER_OWNER_SIDE");

  // 10. reload shell — re-apply me payload (auth-ready wrapper shape)
  const meResult = { ok: true, data: { dealality } };
  const authReadyDetail = { ok: true, jwt: "test", me: meResult, authorized: true };
  const normalizedFromEvent = normalizeMeResultPayload(authReadyDetail);
  assert.ok(normalizedFromEvent);
  assertShellOptions(normalizedFromEvent.data.dealality, "AFTER_RELOAD_EVENT");
  const normalizedFromWhenReady = normalizeMeResultPayload(meResult);
  assertShellOptions(normalizedFromWhenReady.data.dealality, "AFTER_RELOAD_WHEN_READY");
  lifecycle.AFTER_RELOAD = [...shellRenderWorkspaceOptions(dealality)];

  // Prove no step dropped Brand
  for (const [step, list] of Object.entries(lifecycle)) {
    assert.deepEqual(list, EXPECTED, `lifecycle ${step}`);
  }

  console.log(
    JSON.stringify(
      {
        INITIAL_WORKSPACE_OPTIONS: lifecycle.INITIAL,
        OPTIONS_AFTER_AUTH_RESOLUTION: lifecycle.AFTER_AUTH,
        OPTIONS_AFTER_NAV_BUILD: lifecycle.AFTER_NAV,
        OPTIONS_AFTER_DEMO_CONTEXT: lifecycle.AFTER_DEMO_PORTFOLIO,
        OPTIONS_AFTER_STORAGE_RESTORE: lifecycle.AFTER_STORAGE,
        OPTIONS_AFTER_ME_REFRESH: lifecycle.AFTER_ME_REFRESH,
        FINAL_RENDERED_OPTIONS: lifecycle.AFTER_RELOAD,
      },
      null,
      2
    )
  );
});

await test("shell prefers canonical over partial workspaceAccess", () => {
  const dealality = {
    workspaceAccess: ["Owner"],
    isDemo: true,
    demoStakeholderMode: true,
    companyIds: [ownerId, brandId],
    canonicalWorkspaceOptions: resolveWorkspaceOptions({
      isDemo: true,
      companyIds: [ownerId, brandId],
      workspaceAccess: ["Owner"],
    }),
  };
  // Competing rebuild from workspaceAccess alone would yield ["Owner"] only.
  assert.deepEqual(resolveProductionWorkspaceOptions(dealality), ["Owner"]);
  assert.deepEqual(shellRenderWorkspaceOptions(dealality), EXPECTED);
});

await test("auth-ready event shape no longer drops me.dealality", () => {
  const dealality = enrichDealalityMeForDemoStakeholder(baseDemoDealality(), {
    companyIds: [ownerId, brandId],
  });
  const detail = {
    ok: true,
    jwt: "x",
    me: { ok: true, data: { dealality } },
    authorized: true,
  };
  // Legacy bug: applyRoleFromMe(detail) looked for detail.data.dealality
  assert.equal(!!detail.data?.dealality, false);
  const fixed = normalizeMeResultPayload(detail);
  assert.ok(fixed?.data?.dealality);
  assertShellOptions(fixed.data.dealality, "auth-ready normalize");
});

await test("production owner does not gain Brand-Side", () => {
  const prod = {
    companyIds: ["recProdOwner"],
    workspaceAccess: ["Owner"],
    canAccessOwnerWorkspace: true,
    isOwner: true,
    isDemo: false,
    flags: { isDemo: false },
  };
  const opts = resolveWorkspaceOptions(prod);
  assert.equal(opts.DEMO_WORKSPACE_CONSTELLATION_EXPECTED, false);
  assert.deepEqual(opts.workspaces, ["Owner"]);
  assert.equal(shellRenderWorkspaceOptions(prod).includes("Brand"), false);
});

await test("production operator does not gain Brand-Side", () => {
  const prod = {
    companyIds: ["recProdOp"],
    workspaceAccess: ["Operator"],
    canAccessOperatorWorkspace: true,
    isOperator: true,
    isDemo: false,
  };
  assert.deepEqual(resolveWorkspaceOptions(prod).workspaces, ["Operator"]);
});

await test("production brand does not gain Admin via resolver", () => {
  const prod = {
    companyIds: ["recProdBrand"],
    workspaceAccess: ["Brand"],
    canAccessBrandWorkspace: true,
    isBrand: true,
    isDemo: false,
    isAdmin: false,
  };
  const opts = resolveWorkspaceOptions(prod);
  assert.deepEqual(opts.workspaces, ["Brand"]);
  assert.equal(opts.DEMO_ADMIN_AVAILABLE, false);
  assert.equal(opts.DEMO_ALL_WORKSPACES_AVAILABLE, false);
});

await test("header/localStorage cannot invent demo constellation for production", () => {
  const prod = {
    companyIds: ["recProdOwner"],
    workspaceAccess: ["Owner"],
    isDemo: false,
    // Spoofed client fields must not expand production without server enrich flags
    demoStakeholderWorkspaces: ["Owner", "Operator", "Brand"],
  };
  // Without isDemo / constellation companies, resolver stays production.
  assert.deepEqual(resolveWorkspaceOptions(prod).workspaces, ["Owner"]);
  // Spoofed canonical from attacker is ignored by shell only if server overwrites;
  // shell trusts server-attached canonical — production path must not set demo source.
  const serverOpts = resolveWorkspaceOptions(prod);
  assert.equal(serverOpts.source, "production_workspace_access");
});

await test("stale stored Brand does not shrink allowed list when on Owner", () => {
  const dealality = enrichDealalityMeForDemoStakeholder(baseDemoDealality(), {
    companyIds: [ownerId, brandId],
    requestedWorkspace: "Owner",
  });
  const allowed = shellRenderWorkspaceOptions(dealality);
  assert.deepEqual(allowed, EXPECTED);
  const active = pickActiveWorkspaceFromStorage(allowed, "Brand", "Owner");
  assert.equal(active, "Brand"); // still allowed
  assert.deepEqual(shellRenderWorkspaceOptions(dealality), EXPECTED);
});

await test("partial demoStakeholderWorkspaces cannot shrink canonical", () => {
  const probe = {
    ...baseDemoDealality(),
    isDemo: true,
    demoStakeholderMode: true,
    demoStakeholderWorkspaces: ["Owner", "Operator"], // prior bug preference
    companyIds: [ownerId, brandId],
  };
  const opts = resolveWorkspaceOptions(probe);
  assert.deepEqual(opts.workspaces, EXPECTED);
  probe.canonicalWorkspaceOptions = opts;
  assert.deepEqual(shellRenderWorkspaceOptions(probe), EXPECTED);
});

console.log(`\n${passed} passed, ${failed} failed`);
if (failed) process.exit(1);
console.log("DEMO_WORKSPACE_CONSTELLATION_DURABILITY_PASS");
