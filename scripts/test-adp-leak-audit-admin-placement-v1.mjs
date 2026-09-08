/**
 * Gate: AI Demand Leak Audit admin placement — left nav, report picker, isolation.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  getAdminLeakAuditReportCatalog,
} from "../api/admin-adp-leak-audits.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(__dirname, "..");

function assert(cond, msg) {
  if (!cond) throw new Error(msg);
}

function read(rel) {
  return fs.readFileSync(path.join(root, rel), "utf8");
}

function mockRes() {
  const out = { statusCode: 200, body: null };
  return {
    out,
    status(code) {
      out.statusCode = code;
      return this;
    },
    json(payload) {
      out.body = payload;
      return this;
    },
  };
}

async function main() {
  const appJs = read("public/app.js");
  assert(appJs.includes("adpLeakAuditAdmin"), "leak audit nav flag missing in app.js");
  assert(appJs.includes("route: '/admin/adp-leak-audits/reports'"), "Audit Reports nav missing");
  assert(appJs.includes("route: '/admin/adp-leak-audits/new'"), "New Audit nav missing");
  assert(appJs.includes("route: '/admin/adp-leak-audits/portfolios'"), "Portfolio Audits nav missing");
  assert(appJs.includes("route: '/admin/adp-leak-audits/samples'"), "Sample Reports nav missing");
  assert(appJs.includes("route: '/admin/adp-leak-audits/converted'"), "Converted Leads nav missing");
  // Leak Audits live under Admin Resources (not Market Intelligence ADP only)
  const marketIdx = appJs.indexOf("label: 'Market Intelligence'");
  const adminResIdx = appJs.indexOf("label: 'Admin Resources'");
  const reportsNavIdx = appJs.indexOf("route: '/admin/adp-leak-audits/reports'");
  assert(adminResIdx > 0, "Admin Resources nav group missing");
  assert(reportsNavIdx > adminResIdx, "expected Leak Audits under Admin Resources");
  assert(adminResIdx > marketIdx, "expected Admin Resources after Market Intelligence block");
  const marketSlice = appJs.slice(marketIdx, marketIdx + 1200);
  assert(
    !marketSlice.includes("adp-leak-audits"),
    "Leak Audits must not live inside Market Intelligence children"
  );

  const reportsHtml = read("public/admin/adp-leak-audits-reports.html");
  assert(reportsHtml.includes("AI Demand Leak Audits"), "reports page title");
  assert(
    reportsHtml.includes(
      "Limited diagnostics used to review how a hotel appears in AI answers before a paid ADP pilot."
    ),
    "reports subtitle"
  );
  assert(reportsHtml.includes('id="alaReportSelect"'), "report dropdown");
  assert(reportsHtml.includes("Load Report"), "Load Report button");
  assert(reportsHtml.includes("Open Web Report"), "Open Web Report button");
  assert(reportsHtml.includes("Download PDF"), "Download PDF button");
  assert(reportsHtml.includes("Copy Share Link"), "Copy Share Link button");
  assert(reportsHtml.includes("Mark Sent"), "Mark Sent button");
  assert(reportsHtml.includes("Promote to Paid ADP Pilot"), "Promote button");
  assert(reportsHtml.includes('id="alaPreviewFrame"'), "shared preview iframe");
  assert(reportsHtml.includes('name="robots" content="noindex, nofollow"'), "noindex on admin reports");

  const reportsJs = read("public/js/admin-adp-leak-audits-reports.js");
  assert(reportsJs.includes("report-catalog"), "loads report catalog API");
  assert(reportsJs.includes("previewUrl"), "uses catalog preview URLs");
  assert(reportsJs.includes("confirmed: true"), "promote requires confirmation");
  assert(reportsJs.includes("contentWindow.print"), "PDF uses shared renderer print");

  const gate = read("public/js/support-admin-gate.js");
  assert(gate.includes("requireAdpMonthlyReviewAdmin: requireAdmin"), "admin gate alias");

  const server = read("server.js");
  assert(server.includes("getAdminLeakAuditReportCatalog"), "catalog handler wired");
  assert(server.includes('/admin/adp-leak-audits/reports'), "reports route");
  assert(
    /app\.get\("\/admin\/adp-leak-audits"[\s\S]{0,120}redirect\(302,\s*"\/admin\/adp-leak-audits\/reports"\)/.test(
      server
    ),
    "root admin leak path redirects to reports"
  );

  const sitemap = read("public/sitemap.xml");
  assert(!sitemap.includes("adp-leak-audit"), "leak audits must not be in public sitemap");
  assert(!sitemap.includes("adp-leak-audits"), "admin leak audits must not be in sitemap");

  // Catalog unit: samples present with required label shape
  const res = mockRes();
  getAdminLeakAuditReportCatalog({ query: {} }, res);
  assert(res.out.statusCode === 200 && res.out.body?.ok, "catalog ok");
  const rows = res.out.body.reports || [];
  assert(rows.length >= 2, "expected at least Cambridge + Dovetail samples");
  const cambridge = rows.find((r) => String(r.displayName || "").includes("Cambridge"));
  const dovetail = rows.find((r) => String(r.displayName || "").includes("Dovetail"));
  assert(cambridge, "Cambridge sample in catalog");
  assert(dovetail, "Dovetail portfolio sample in catalog");
  assert(
    /— Single-property — Sample —/.test(cambridge.label),
    `Cambridge label format: ${cambridge.label}`
  );
  assert(/— Portfolio — Sample —/.test(dovetail.label), `Dovetail label format: ${dovetail.label}`);
  assert(cambridge.previewUrl === "/adp-leak-audit/sample", "Cambridge preview URL");
  assert(dovetail.previewUrl === "/adp-leak-audit/sample-portfolio", "Portfolio preview URL");
  assert(cambridge.canPromote === false && cambridge.canMarkSent === false, "samples not promotable");

  const filtered = mockRes();
  getAdminLeakAuditReportCatalog({ query: { reportType: "portfolio" } }, filtered);
  assert(
    (filtered.out.body.reports || []).every((r) => r.reportType === "portfolio"),
    "portfolio filter"
  );
  assert(
    (filtered.out.body.reports || []).some((r) => r.catalogId.includes("portfolio")),
    "portfolio filter returns portfolio sample"
  );

  const sampleOnly = mockRes();
  getAdminLeakAuditReportCatalog({ query: { reportType: "sample" } }, sampleOnly);
  assert(
    (sampleOnly.out.body.reports || []).every((r) => r.isSample),
    "sample reportType filter"
  );

  // Owner ADP dashboard must not embed leak audits as default tab
  const ownerAdp = read("public/owner-ai-demand.html");
  assert(!ownerAdp.includes("adp-leak-audit"), "paid ADP page must not embed leak audits");

  // Paid ADP admin remains reachable; Leak Audits are a sibling admin group
  const marketIntel = appJs.indexOf("label: 'Market Intelligence'");
  const marketBlock = appJs.slice(marketIntel, marketIntel + 2000);
  assert(
    marketBlock.includes("AI Demand Positioning") || marketBlock.includes("ai-demand"),
    "paid ADP nav under Market Intelligence unchanged"
  );

  // dealalitydemo / governed admin elevation wired for left-nav visibility
  const meJs = read("api/me.js");
  assert(
    meJs.includes("elevateGovernedPlatformAdmin"),
    "/api/me elevates governed platform admins (dealalitydemo)"
  );
  const assignments = JSON.parse(
    read("data/ai-demand-positioning/owner-property-access/assignments.v1.json")
  );
  assert(
    (assignments.adminEmails || [])
      .map((e) => String(e).toLowerCase())
      .includes("dealalitydemo@dealality.com"),
    "dealalitydemo in assignments adminEmails"
  );

  const reviewsHtml = read("public/app/admin/ai-demand-reviews.html");
  assert(
    reviewsHtml.includes("Open AI Demand Leak Audits"),
    "fallback Open AI Demand Leak Audits on AI Demand Reviews"
  );

  assert(reportsJs.includes('get("debug") === "nav"'), "reports ?debug=nav diagnostics");

  assert(appJs.includes("NAV_CONFIG_SOURCE"), "NAV_CONFIG_SOURCE in app.js");
  assert(
    appJs.includes("'/admin/adp-leak-audits/reports'"),
    "Audit Reports route resolves in ROUTES/NAV"
  );

  console.log("PASS test-adp-leak-audit-admin-placement-v1");
}

main().catch((err) => {
  console.error("FAIL", err.message || err);
  process.exit(1);
});
