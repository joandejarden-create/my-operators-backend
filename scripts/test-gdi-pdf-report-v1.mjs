#!/usr/bin/env node
/**
 * GDI PDF Report Engine V1 — data, selection, safety, and PDF smoke tests.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  getGdiPdfReportData,
  buildGdiPdfFilename,
} from "../lib/group-demand-intelligence/reports/gdi-pdf-report-data-v1.js";
import { buildGdiPdfDocumentHtml } from "../lib/group-demand-intelligence/reports/gdi-pdf-report-html-v1.js";
import { generateGdiReportPdfV1 } from "../lib/group-demand-intelligence/reports/generate-gdi-report-pdf-v1.mjs";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

const HOTELS = {
  bethesda: "recLuxvwwxID7U2B8",
  renaissance: "recG66DQJKP2c0UNh",
  hilton: "rec35fExUxCClpOP6",
  nownow: "recGkME49yYuxQl0u",
  radisson: "recUOyzOXn2Zdp98I",
};

const APPROVED_TOP5_HINTS = [
  /potomac.?memorial/i,
  /acts|ts27/i,
  /nice.?2027/i,
  /ahima/i,
  /acc.?legislative/i,
];

const DISCLAIMER =
  "Estimated room-revenue scenarios based on current assumptions and available opportunity data. These are not forecasts.";

let failures = 0;
function check(name, cond, detail = "") {
  if (cond) {
    console.log(`  PASS  ${name}`);
  } else {
    failures += 1;
    console.error(`  FAIL  ${name}${detail ? " — " + detail : ""}`);
  }
}

function assertNoInternalLeak(text, label) {
  check(
    `${label}: no markdown markers`,
    !/(^|\n)\s*#{1,3}\s|```|\*\*\*/.test(text)
  );
  check(
    `${label}: no internal IDs`,
    !/\b(gdi_opp_|gdi_mkt_|eventSeriesId|tbl[A-Za-z0-9]+)\b/.test(text)
  );
  check(
    `${label}: no Surfe`,
    !/\bsurfe\b/i.test(text)
  );
  check(
    `${label}: no Airtable/Jev jargon`,
    !/\b(airtable|jev\b|strict-readiness|canonical.?id)\b/i.test(text)
  );
}

async function testBethesdaData() {
  console.log("\n== Bethesda data ==");
  const data = await getGdiPdfReportData(HOTELS.bethesda);
  check("available", data.available === true, data.reason);
  check("hotel identity", /bethesda/i.test(data.hotel?.name || ""));
  check("ready count > 0", (data.executiveSummary?.customerReadyCount || 0) > 0);
  check(
    "ready section only READY",
    (data.topOpportunities || []).every((c) => c.classification === "READY")
  );
  check(
    "watch section only FUTURE_WATCH",
    (data.futureWatch || []).every((c) => c.classification === "FUTURE_WATCH")
  );
  check("top5 length <= 5", (data.immediatePursuits || []).length <= 5);
  check("action set <= 16", (data.topOpportunities || []).length <= 16);
  check(
    "deterministic top5 size",
    (data.immediatePursuits || []).length ===
      Math.min(5, data.topOpportunities?.length || 0)
  );

  const top5 = (data.immediatePursuits || []).map((c) => c.opportunity);
  const matchCount = APPROVED_TOP5_HINTS.filter((re) =>
    top5.some((t) => re.test(t))
  ).length;
  check(
    `top5 overlap with approved package (>=3 of 5 hints; got ${matchCount})`,
    matchCount >= 3,
    top5.join(" | ")
  );

  if (data.executiveSummary?.revenueScenario) {
    check(
      "revenue disclaimer exact",
      data.executiveSummary.revenueScenario.disclaimer === DISCLAIMER
    );
    check(
      "revenue not zero-fake",
      data.executiveSummary.revenueScenario.base > 0
    );
  } else {
    check("revenue omitted when unsupported (ok)", true);
  }

  for (const c of data.topOpportunities || []) {
    if (c.who?.kind === "NAMED") {
      check(`named contact present: ${c.opportunity.slice(0, 40)}`, !!c.who.name);
    } else {
      check(
        `functional fallback: ${c.opportunity.slice(0, 40)}`,
        /conference|meetings|team/i.test(c.who?.title || "")
      );
    }
  }

  const html = buildGdiPdfDocumentHtml(data, { cssText: "body{}" });
  assertNoInternalLeak(html, "bethesda html");
  check("html has GDI title", /GROUP &amp; DEMAND INTELLIGENCE/.test(html));
  check("html has hotel name", html.includes(data.hotel.name));
  if (data.executiveSummary?.revenueScenario) {
    check("html has disclaimer", html.includes(DISCLAIMER));
  }

  return data;
}

async function testGenericHotels() {
  console.log("\n== Generic hotel data ==");
  const results = {};
  for (const [key, hotelId] of Object.entries(HOTELS)) {
    if (key === "bethesda") continue;
    const data = await getGdiPdfReportData(hotelId);
    results[key] = {
      hotelId,
      available: !!data?.available,
      ready: data?.executiveSummary?.customerReadyCount ?? 0,
      watch: data?.executiveSummary?.futureWatchCount ?? 0,
      hotelName: data?.hotel?.name || data?.hotelName || hotelId,
      reason: data?.reason || null,
    };
    check(
      `${key}: loads without throw`,
      data != null
    );
    if (data.available) {
      check(
        `${key}: hotel name present`,
        !!(data.hotel?.name && data.hotel.name !== hotelId)
      );
      check(
        `${key}: no Bethesda bleed in name`,
        !/bethesda/i.test(data.hotel.name)
      );
      const html = buildGdiPdfDocumentHtml(data, { cssText: "" });
      assertNoInternalLeak(html, key);
    } else {
      check(`${key}: clear unavailable reason`, !!(data.reason && data.reason.length > 10));
    }
  }
  return results;
}

async function testBethesdaPdf(data) {
  console.log("\n== Bethesda PDF generation ==");
  const sampleDir = path.join(
    ROOT,
    "reports/group-demand-intelligence/gdi-pdf-report-v1/samples"
  );
  fs.mkdirSync(sampleDir, { recursive: true });
  const outPath = path.join(sampleDir, "pending_bethesda.pdf");
  const result = await generateGdiReportPdfV1({
    hotelId: HOTELS.bethesda,
    persist: true,
    outPath,
    pagesDir: path.join(sampleDir, "bethesda-pages"),
  });
  check("pdf ok", result.ok === true, result.reason);
  check("pdf non-zero", (result.byteLength || 0) > 1000);
  check("pdf magic", result.buffer?.slice(0, 5).toString() === "%PDF-");
  check("page count > 0", (result.pageCountHint || 0) > 0);
  check("gate cover", !!result.gate?.hasCover);
  check("gate executive", !!result.gate?.hasExecutive);
  check("gate no markdown", !result.gate?.hasMarkdown);
  check("gate no internal id", !result.gate?.hasInternalId);

  const finalName = result.filename || buildGdiPdfFilename(data.hotel.name, data.reportMetadata.reportDate);
  const dest = path.join(sampleDir, finalName);
  if (fs.existsSync(outPath)) fs.renameSync(outPath, dest);
  else if (result.buffer) fs.writeFileSync(dest, result.buffer);
  check("sample file exists", fs.existsSync(dest));

  return { result, samplePath: dest };
}

async function testFilename() {
  console.log("\n== Filename ==");
  const fn = buildGdiPdfFilename("Bethesda Marriott", "2026-09-30");
  check(
    "deterministic human-readable",
    fn === "Dealality_GDI_Bethesda_Marriott_2026-09-30.pdf"
  );
}

async function testAdminWiring() {
  console.log("\n== Admin wiring ==");
  const html = fs.readFileSync(
    path.join(ROOT, "public/app/admin/ai-demand-admin.html"),
    "utf8"
  );
  const adminJs = fs.readFileSync(
    path.join(ROOT, "public/js/admin-ai-demand-admin.js"),
    "utf8"
  );
  const gdiJs = fs.readFileSync(
    path.join(ROOT, "public/js/admin-gdi-reports.js"),
    "utf8"
  );
  const api = fs.readFileSync(path.join(ROOT, "api/admin-gdi-reports.js"), "utf8");
  const server = fs.readFileSync(path.join(ROOT, "server.js"), "utf8");
  check("admin tab present", /data-tab="gdi-reports"/.test(html));
  check("admin panel present", /id="adaPanelGdiReports"/.test(html));
  check("admin script include", /admin-gdi-reports\.js/.test(html));
  check("tab allowlist", /"gdi-reports"\s*:\s*true/.test(adminJs));
  check("generate button handler", /gdiRptGenerate/.test(gdiJs));
  check("API generate route export", /postAdminGdiReportGenerate/.test(api));
  check(
    "server routes registered",
    /group-demand-intelligence\/hotels\/:hotelId\/report-pdf/.test(server)
  );
  check(
    "ADP action plan panel untouched",
    /id="adaPanelActionPlan"/.test(html) && /admin-adp-action-plan\.js/.test(html)
  );
}

async function main() {
  console.log("GDI PDF Report Engine V1 — tests");
  await testFilename();
  await testAdminWiring();
  const data = await testBethesdaData();
  const generic = await testGenericHotels();
  const pdf = await testBethesdaPdf(data);

  const out = {
    generatedAt: new Date().toISOString(),
    failures,
    bethesda: {
      ready: data.executiveSummary?.customerReadyCount,
      high: data.executiveSummary?.highPriority,
      medium: data.executiveSummary?.mediumPriority,
      watch: data.executiveSummary?.futureWatchCount,
      named: data.executiveSummary?.namedContactCoveragePct,
      publicPath: data.executiveSummary?.publicContactPathCoveragePct,
      top5: (data.immediatePursuits || []).map((c) => c.opportunity),
      revenueDisclaimer:
        data.executiveSummary?.revenueScenario?.disclaimer || null,
      samplePath: pdf.samplePath,
      pageCountHint: pdf.result?.pageCountHint,
      byteLength: pdf.result?.byteLength,
    },
    generic,
  };
  const reportDir = path.join(
    ROOT,
    "reports/group-demand-intelligence/gdi-pdf-report-v1"
  );
  fs.mkdirSync(reportDir, { recursive: true });
  fs.writeFileSync(
    path.join(reportDir, "TEST_RESULTS.json"),
    JSON.stringify(out, null, 2),
    "utf8"
  );

  console.log(`\nFailures: ${failures}`);
  if (failures > 0) process.exit(1);
  console.log("ALL PASS");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
