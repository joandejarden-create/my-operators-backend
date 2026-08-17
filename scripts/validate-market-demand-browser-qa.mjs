#!/usr/bin/env node
/**
 * Browser QA checklist (automated + manual sign-in items).
 *   node scripts/validate-market-demand-browser-qa.mjs [dealId]
 */
import "../load-env.js";
import { readFileSync } from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  getDealDemandCenters,
  getDealMarketDemandSnapshot,
  postPreviewDemandCenterImport,
} from "../api/market-demand.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DEAL_ID = process.argv[2] || "rec6JMTqtSUn1ygtd";
const PORT = process.env.PORT || 8080;
const BASE = `http://localhost:${PORT}`;

const report = {
  dealId: DEAL_ID,
  automated: [],
  manualSignInRequired: [],
  pass: 0,
  fail: 0,
};

function record(name, ok, detail) {
  report.automated.push({ name, ok, detail });
  if (ok) report.pass += 1;
  else report.fail += 1;
}

function mockRes() {
  const out = { statusCode: 200, body: null };
  return {
    status(c) {
      out.statusCode = c;
      return this;
    },
    json(b) {
      out.body = b;
      return this;
    },
    out,
  };
}

async function fetchStatus(url) {
  try {
    const res = await fetch(url);
    return { status: res.status, ok: res.ok };
  } catch (err) {
    return { status: 0, error: err.message };
  }
}

async function main() {
  const page = await fetchStatus(`${BASE}/market-demand.html`);
  record("Static page /market-demand.html", page.status === 200, `HTTP ${page.status}`);

  const js = await fetchStatus(`${BASE}/js/market-demand.js`);
  record("market-demand.js asset", js.status === 200, `HTTP ${js.status}`);

  const css = await fetchStatus(`${BASE}/css/market-demand.css`);
  record("market-demand.css asset", css.status === 200, `HTTP ${css.status}`);

  const apiUnauth = await fetch(`${BASE}/api/deals/${DEAL_ID}/demand-centers`);
  const apiBody = await apiUnauth.json();
  record(
    "API route registered (auth required)",
    apiBody.error === "authentication_required",
    apiBody.error || "unexpected"
  );

  const previewRoute = await fetch(`${BASE}/api/deals/${DEAL_ID}/preview-demand-center-import`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ demandCenters: [{ name: "X", category: "Leisure" }] }),
  });
  const previewBody = await previewRoute.json();
  record(
    "Preview route registered",
    previewBody.error === "authentication_required" || previewBody.preview === true,
    previewBody.error || "preview ok"
  );

  const myDeals = readFileSync(path.join(__dirname, "../public/my-deals.html"), "utf8");
  record(
    "My Deals More menu → Market Demand link",
    myDeals.includes("Market Demand") && myDeals.includes("/market-demand.html?dealId="),
    "link present in my-deals.html"
  );

  const resDc = mockRes();
  await getDealDemandCenters({ params: { dealId: DEAL_ID } }, resDc);
  const dc = resDc.out.body;
  record(
    "API demand centers count (seeded deal)",
    dc.ok && dc.demandCenters?.length >= 3,
    `count=${dc.demandCenters?.length}`
  );
  record(
    "API summary.categories present",
    dc.ok && dc.summary?.categories && Object.keys(dc.summary.categories).length >= 1,
    JSON.stringify(dc.summary?.categories || {})
  );

  const resSnap = mockRes();
  await getDealMarketDemandSnapshot({ params: { dealId: DEAL_ID } }, resSnap);
  const snap = resSnap.out.body;
  record(
    "Snapshot narrative available",
    snap.ok && snap.hasSnapshot && snap.snapshot?.demandSummary,
    snap.snapshot?.overallDemandStrength || "no snapshot"
  );

  const resPreview = mockRes();
  await postPreviewDemandCenterImport(
    {
      params: { dealId: DEAL_ID },
      body: { demandCenters: [{ name: "QA Preview Row", category: "Corporate" }] },
    },
    resPreview
  );
  record(
    "Import preview handler",
    resPreview.out.body?.preview === true && resPreview.out.body?.previewRows?.length === 1,
    resPreview.out.body?.summary
  );

  const mdJs = readFileSync(path.join(__dirname, "../public/js/market-demand.js"), "utf8");
  record("Import Demand Centers button in UI", mdJs.includes('data-md-action="import"'), "toolbar button");
  record("Import modal scaffold", mdJs.includes("openImportModal"), "modal function");

  report.manualSignInRequired = [
    `Open ${BASE}/market-demand.html?dealId=${DEAL_ID} signed in — page loads without console errors`,
    "My Deals → deal row More → Market Demand opens correct URL",
    "Demand centers table shows 3 rows for seeded deal",
    "Nearby hotel supply shows 1 row",
    "Summary cards and demand mix match API data",
    "Generate Demand Snapshot works from UI",
    "Refresh works from UI",
    "Import Demand Centers → paste JSON → Preview → Save Selected",
    "Empty state: use a deal with no linked demand centers",
  ];

  console.log(JSON.stringify(report, null, 2));
  if (report.fail) process.exit(1);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
