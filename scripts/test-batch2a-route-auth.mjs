/**
 * Batch 2A route auth — static server.js mounts + middleware unit checks.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { requireOwnerBdrActivityAccess } from "../middleware/requireOwnerBdrActivityAccess.js";

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

  const hardened = [
    ['app.get("/api/target-list/:dealId", mapTargetListDealParam, ...myDealsDealAuth', "target-list GET by dealId"],
    ['app.post("/api/target-list", mapBodyDealIdToRecordId, ...myDealsDealAuth', "target-list POST"],
    ['app.post("/api/target-list/batch-delete", ...myDealsAuth', "target-list batch-delete"],
    ['app.patch("/api/target-list/:targetId", ...myDealsAuth, requireTargetListRecordAccess', "target-list PATCH"],
    ['app.post("/api/brand-deal-requests", mapBodyDealIdToRecordId, ...myDealsDealAuth', "BDR create"],
    ['app.post("/api/brand-deal-requests/by-deals", ...myDealsAuth', "BDR by-deals POST"],
    ['app.get("/api/brand-deal-requests/activity", gateOwnerBdrActivity', "BDR activity gate"],
    ['app.get("/api/brand-deal-requests/deal-meta", gateOwnerBdrDealMeta', "BDR deal-meta gate"],
    ['app.post("/api/brand-deal-requests/bulk-update", ...myDealsAuth', "BDR bulk-update"],
    ['requireOwnerBdrRecordAccess', "BDR PATCH owner record access"],
    ['app.post("/api/my-deals/:recordId/attachments", ...myDealsDealAuth', "attachments POST"],
    ['app.get("/api/my-deals/:recordId/attachments/:filename", ...myDealsDealAuth', "attachments GET"],
    ['gateOwnerBdrListAll', "BDR listAll admin gate"],
  ];

  for (const [needle, label] of hardened) {
    ok(serverJs.includes(needle), `server.js: ${label}`);
  }

  ok(
    fs.readFileSync(path.join(root, "public/deal-room-owner.html"), "utf8").includes("In Development"),
    "deal-room-owner caveat banner"
  );
  ok(
    fs.readFileSync(path.join(root, "public/outreach-plans.html"), "utf8").includes("oh-pilot-caveat"),
    "outreach-plans caveat banner"
  );
  ok(
    fs.readFileSync(path.join(root, "public/my-deals.html"), "utf8").includes("function myDealsAuthFetch"),
    "my-deals myDealsAuthFetch helper"
  );
  ok(
    fs.existsSync(path.join(root, "public/js/deal-setup-attachments-ui.js")),
    "deal-setup-attachments-ui.js exists"
  );
  const nds = fs.readFileSync(path.join(root, "public/new-deal-setup.html"), "utf8");
  ok(nds.includes("deal-setup-attachments-ui.js"), "new-deal-setup loads attachment UI helper");
  ok(nds.includes("DealSetupAttachmentsUi.renderCurrentAttachments"), "new-deal-setup uses authenticated attachment render");

  // requireOwnerBdrActivityAccess — dealId without token → 401
  {
    const req = { headers: {}, query: { dealId: "recTEST123456789" } };
    const res = mockRes();
    let nextCalled = false;
    await requireOwnerBdrActivityAccess(req, res, () => {
      nextCalled = true;
    });
    ok(!nextCalled && res.statusCode === 401, "activity access rejects dealId without token");
  }

  // gateOwnerBdrListAll — non-admin → 403
  {
    const req = { query: { all: "1" }, dealalityUser: { isAdmin: false, workspaceAccess: ["Owner"] } };
    const res = mockRes();
    let nextCalled = false;
    requireAdminForListAllInline(req, res, () => {
      nextCalled = true;
    });
    ok(!nextCalled && res.statusCode === 403, "listAll gate rejects non-admin");
  }

  console.log(`\nBatch 2A static: ${passed} passed, ${failed} failed`);
  process.exit(failed > 0 ? 1 : 0);
}

function requireAdminForListAllInline(req, res, next) {
  const u = req.dealalityUser;
  if (!u?.isAdmin) {
    return res.status(403).json({
      ok: false,
      success: false,
      error: "forbidden",
      message: "Admin access required for listAll.",
    });
  }
  return next();
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
