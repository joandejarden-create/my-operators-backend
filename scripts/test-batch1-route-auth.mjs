/**
 * Batch 1 route auth — static checks + middleware unit tests (no live server required).
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { memberstackAuth } from "../middleware/memberstackAuth.js";
import { requireAdminAccess } from "../middleware/requireAdminAccess.js";
import { mapBodyDealIdToRecordId } from "../middleware/mapBodyDealIdToRecordId.js";
import { mapParamDealIdToRecordId } from "../middleware/mapParamDealIdToRecordId.js";
import { dealRecordAllowedForUser } from "../lib/dealality/deal-record-access.js";
import { getOwnerPilotProvisioningRunbookHandler } from "../api/support-owner-pilot-provisioning-runbook.js";
import {
  getOwnerPilotProvisioningRunbook,
  getOwnerPilotRunbookExpectedSectionTitles,
} from "../lib/support/owner-pilot-provisioning-runbook.js";
import { isInternalRunbookAdmin } from "../lib/dealality/internal-runbook-admin.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.join(__dirname, "..");

let passed = 0;
let failed = 0;

function ok(cond, msg) {
  if (cond) {
    passed += 1;
    console.log("ok:", msg);
  } else {
    failed += 1;
    console.error("FAIL:", msg);
  }
}

function mockRes() {
  const res = { statusCode: 200, body: null };
  res.status = (code) => {
    res.statusCode = code;
    return res;
  };
  res.json = (body) => {
    res.body = body;
    return res;
  };
  return res;
}

async function run() {
  const serverJs = fs.readFileSync(path.join(root, "server.js"), "utf8");
  const pkg = JSON.parse(fs.readFileSync(path.join(root, "package.json"), "utf8"));

  ok(pkg.scripts?.start && pkg.scripts.start.includes("server.js"), "package.json start uses server.js");

  const hardenedPatterns = [
    ['app.get("/api/my-deals/outreach-default", ...myDealsAuth', "outreach-default GET"],
    ['app.patch("/api/my-deals/outreach-default", ...myDealsAuth', "outreach-default PATCH"],
    ['app.get("/api/my-deals/:recordId/outreach-setup", ...myDealsDealAuth', "outreach-setup GET"],
    ['app.get("/api/my-deals/:recordId/match-score-breakdown", ...myDealsDealAuth', "match-score-breakdown"],
    ['app.get("/api/my-deals/:recordId/operator-match-score-breakdown", ...myDealsDealAuth', "operator-match-score-breakdown"],
    ['app.get("/api/my-deals/:recordId/alternative-brands", ...myDealsDealAuth', "alternative-brands"],
    ['app.post("/api/my-deals/:recordId/add-recommended-brand", ...myDealsDealAuth', "add-recommended-brand"],
    ['app.post("/api/my-deals/:recordId/refresh-brand-cache", ...myDealsDealAuth', "refresh-brand-cache"],
    ['app.get("/api/franchise-application/:dealId", mapParamDealIdToRecordId, ...myDealsDealAuth', "franchise GET"],
    ['app.patch("/api/franchise-application/:dealId", mapParamDealIdToRecordId, ...myDealsDealAuth', "franchise PATCH"],
    ['app.post("/api/ai/deal-readiness-review", mapBodyDealIdToRecordId, ...myDealsDealAuth', "DRS POST"],
    ['app.post("/api/ai/deal-readiness-review/save", mapBodyDealIdToRecordId, ...myDealsDealAuth', "DRS save"],
    ['app.post("/api/ai/brand-alignment-snapshot", mapBodyDealIdToRecordId, ...myDealsDealAuth', "BAS POST"],
    ['postOperatorCapabilitySnapshot', "OCS POST handler mounted"],
    ['app.get("/api/user-management", ...adminAuth', "user-management GET"],
    [
      '"/api/support/owner-pilot-provisioning-runbook"',
      "owner-pilot runbook GET",
    ],
  ];

  for (const [needle, label] of hardenedPatterns) {
    ok(serverJs.includes(needle), `server.js mounts auth: ${label}`);
  }
  ok(
    serverJs.includes('"/api/support/owner-pilot-provisioning-runbook"') &&
      serverJs.includes("internalRunbookAuth") &&
      serverJs.includes("requireInternalRunbookAdmin"),
    "server.js owner-pilot runbook uses internalRunbookAuth chain"
  );
  ok(
    serverJs.includes('"/api/ai/operator-capability-snapshot"') &&
      serverJs.includes("mapBodyDealIdToRecordId") &&
      serverJs.includes("postOperatorCapabilitySnapshot"),
    "server.js OCS POST uses mapBodyDealIdToRecordId + myDealsDealAuth chain"
  );

  // memberstackAuth — no token → 401
  {
    const req = { headers: {} };
    const res = mockRes();
    let nextCalled = false;
    await memberstackAuth(req, res, () => {
      nextCalled = true;
    });
    ok(!nextCalled && res.statusCode === 401, "memberstackAuth rejects missing Bearer");
    ok(res.body?.error === "authentication_required", "memberstackAuth error code");
  }

  // requireAdminAccess — non-admin → 403
  {
    const req = { dealalityUser: { isAdmin: false, workspaceAccess: ["Owner"] } };
    const res = mockRes();
    let nextCalled = false;
    requireAdminAccess(req, res, () => {
      nextCalled = true;
    });
    ok(!nextCalled && res.statusCode === 403, "requireAdminAccess rejects owner-only user");
  }

  // requireAdminAccess — admin passes
  {
    const req = { dealalityUser: { isAdmin: true, workspaceAccess: [] } };
    const res = mockRes();
    let nextCalled = false;
    requireAdminAccess(req, res, () => {
      nextCalled = true;
    });
    ok(nextCalled, "requireAdminAccess allows isAdmin");
  }

  // mapBodyDealIdToRecordId
  {
    const req = { body: { dealId: "recTEST123456789" }, params: {} };
    const res = mockRes();
    let nextCalled = false;
    mapBodyDealIdToRecordId(req, res, () => {
      nextCalled = true;
    });
    ok(nextCalled && req.params.recordId === "recTEST123456789", "mapBodyDealIdToRecordId sets recordId");
  }

  // mapParamDealIdToRecordId
  {
    const req = { params: { dealId: "recDEAL999" } };
    mapParamDealIdToRecordId(req, mockRes(), () => {});
    ok(req.params.recordId === "recDEAL999", "mapParamDealIdToRecordId copies dealId");
  }

  // dealRecordAllowedForUser — owner company match
  {
    const allowed = dealRecordAllowedForUser(
      { "Company Profile": ["recCompanyA"] },
      { isOwner: true, companyId: "recCompanyA", companyIds: ["recCompanyA"], userRecordId: "recUser1" }
    );
    ok(allowed === true, "dealRecordAllowedForUser allows matching company");
  }

  {
    const denied = dealRecordAllowedForUser(
      { "Company Profile": ["recCompanyB"] },
      { isOwner: true, companyId: "recCompanyA", companyIds: ["recCompanyA"], userRecordId: "recUser1" }
    );
    ok(denied === false, "dealRecordAllowedForUser denies other company");
  }

  // Owner Pilot runbook — public shell must not contain sensitive content
  {
    const runbookHtml = fs.readFileSync(
      path.join(root, "public/app/support/owner-pilot-provisioning.html"),
      "utf8"
    );
    ok(runbookHtml.includes('id="runbookRoot"'), "runbook shell has dynamic mount point");
    ok(!runbookHtml.includes("tbl6shiyz2wdUqE5F"), "runbook shell excludes Airtable table ids");
    ok(!runbookHtml.includes("mem_cmqdv53pi00bf0suj25u42l46"), "runbook shell excludes live member ids");
    ok(!runbookHtml.includes("link-airtable-user-memberstack"), "runbook shell excludes provisioning scripts");
    ok(runbookHtml.includes("owner-pilot-runbook.js"), "runbook shell loads client renderer");
  }

  // Owner Pilot runbook API handler — admin response shape
  {
    const req = {};
    const res = mockRes();
    getOwnerPilotProvisioningRunbookHandler(req, res);
    ok(res.statusCode === 200, "runbook handler returns 200");
    ok(typeof res.body?.title === "string" && res.body.title.length > 0, "runbook response includes title");
    ok(Array.isArray(res.body?.sections) && res.body.sections.length >= 10, "runbook response includes sections array");
    const titles = (res.body?.sections || []).map((s) => s.title).join(" | ");
    ok(titles.includes("Source of Truth Model"), "runbook includes Source of Truth Model section");
    ok(titles.includes("Pilot Readiness Standards"), "runbook includes Pilot Readiness Standards section");
    ok(titles.includes("Memberstack Setup Steps"), "runbook includes Memberstack Setup Steps section");
    ok(titles.includes("Pilot Owner Provisioning Checklist"), "runbook includes Pilot Owner Provisioning Checklist");
    ok(titles.includes("Common Failure Modes"), "runbook includes Common Failure Modes section");
    ok(titles.includes("Future Access Model"), "runbook includes Future Access Model section");
    ok(titles.includes("Live vs Test Memberstack") || titles.includes("Memberstack Setup Steps"), "runbook covers Memberstack live/test");
    const memberstackSection = (res.body?.sections || []).find((s) => s.id === "memberstack-setup");
    const blocks = JSON.stringify(memberstackSection?.contentBlocks || []);
    ok(blocks.includes("backfill-memberstack-signup-fields"), "runbook includes MS backfill script");
    ok(blocks.includes("mem_sb_"), "runbook includes Test Mode id guidance");
  }

  // Owner Pilot runbook — unauthenticated API → 401 (memberstackAuth)
  {
    const req = { headers: {} };
    const res = mockRes();
    let nextCalled = false;
    await memberstackAuth(req, res, () => {
      nextCalled = true;
    });
    ok(!nextCalled && res.statusCode === 401, "runbook API path: unauthenticated → 401");
  }

  // Owner Pilot runbook — authenticated non-admin → 403 (requireAdminAccess)
  {
    const req = { dealalityUser: { isAdmin: false, workspaceAccess: ["Owner"] } };
    const res = mockRes();
    let nextCalled = false;
    requireAdminAccess(req, res, () => {
      nextCalled = true;
    });
    ok(!nextCalled && res.statusCode === 403, "runbook API path: non-admin → 403");
  }

  // Owner Pilot runbook — authenticated admin → passes requireAdminAccess
  {
    const req = { dealalityUser: { isAdmin: true, workspaceAccess: [] } };
    const res = mockRes();
    let nextCalled = false;
    requireAdminAccess(req, res, () => {
      nextCalled = true;
    });
    ok(nextCalled, "runbook API path: admin passes requireAdminAccess");
  }

  // Content module exports expected section titles helper
  {
    const expected = getOwnerPilotRunbookExpectedSectionTitles();
    ok(expected.length >= 10, "runbook content module exports section titles");
    ok(
      getOwnerPilotProvisioningRunbook().sections.every((s) => s.id && s.title && Array.isArray(s.contentBlocks)),
      "runbook sections have id, title, contentBlocks"
    );
  }

  // App shell — admin-only Support nav uses /api/me isAdmin only
  {
    const appJs = fs.readFileSync(path.join(root, "public/app.js"), "utf8");
    ok(appJs.includes("meContextLoaded"), "app.js tracks /api/me context for admin nav");
    ok(
      appJs.includes("meDealality.isAdmin === true") || appJs.includes("meDealality.isAdmin === true"),
      "app.js hasAdminNavAccess checks dealality.isAdmin"
    );
    ok(
      !/function hasAdminNavAccess\(\)[\s\S]*?resolveCurrentNavRole\(\) === 'admin'/.test(appJs),
      "hasAdminNavAccess does not use dev nav override as admin"
    );
    ok(
      !/function hasAdminNavAccess\(\)[\s\S]*?authenticatedRole === 'admin'/.test(appJs),
      "hasAdminNavAccess does not use authenticatedRole legacy admin"
    );
    ok(appJs.includes("isAdminExclusiveRoles"), "app.js defines admin-exclusive nav role helper");
    ok(
      appJs.includes("'/support/owner-pilot-provisioning'") &&
        appJs.includes("return '/support'"),
      "blocked runbook route redirects to Help Center"
    );
    ok(
      appJs.includes("internalRunbookOnly") && appJs.includes("hasInternalRunbookNavAccess"),
      "app.js gates Owner Pilot Runbook with internal runbook access"
    );
    ok(
      appJs.includes("hasInternalRunbookNavAccess") && appJs.includes("hasAdminNavAccess()"),
      "runbook nav uses same isAdmin signal as admin nav"
    );
    ok(
      appJs.includes("if (internalRunbookOnly) return hasInternalRunbookNavAccess()"),
      "canSee gates internal runbook before dev-all bypass"
    );
  }

  // Internal runbook admin — platform isAdmin from /api/me
  {
    ok(
      !isInternalRunbookAdmin({
        email: "dealalitydemo@dealality.com",
        dealality: { isAdmin: false },
        companyName: "Dealality Owner Demo",
      }),
      "demo owner (no workspace admin) denied runbook"
    );
    ok(
      isInternalRunbookAdmin({
        email: "joan@aohospitalityadvisors.com",
        dealality: { isAdmin: true },
        companyName: "AO Hospitality Advisors",
      }),
      "workspace admin user allowed runbook"
    );
    ok(
      !isInternalRunbookAdmin({
        email: "dealalitydemo@dealality.com",
        dealality: { isAdmin: false },
        companyName: "Dealality Owner Demo",
      }),
      "owner-only demo user denied runbook"
    );
  }

  // Pilot provisioning validators
  {
    const { validateMemberstackIdPair } = await import("../lib/pilot-provisioning/pilot-validators.js");
    const blocked = validateMemberstackIdPair({ primary: "mem_sb_x", mirror: "mem_sb_x" });
    ok(!blocked.ok && blocked.problems.length > 0, "validators reject mem_sb_ for production");
  }

  console.log(`\ntest-batch1-route-auth: ${passed} passed, ${failed} failed`);
  process.exit(failed > 0 ? 1 : 0);
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
