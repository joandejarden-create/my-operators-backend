#!/usr/bin/env node
/**
 * Phase 4 — Operating Companies for Consideration validation (static + unit).
 *   node scripts/validate-operator-alignment-companies.mjs
 */
import { readFileSync, existsSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import {
  assessOperatorDataCompleteness,
  scoreToCompanyAlignmentBand,
  alignmentSignalsFromBreakdown,
} from "../lib/operator-alignment-company-utils.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");

const BANNED = [
  /Recommended Operators/i,
  /Best Operator Matches/i,
  /Preferred Operators/i,
  /Top Operators/i,
  /Dealality recommends/i,
  /Best operator/i,
  /Preferred operator/i,
];

let failed = 0;

function assert(cond, msg) {
  if (!cond) {
    console.error("FAIL:", msg);
    failed += 1;
  } else {
    console.log("ok:", msg);
  }
}

function scanNoBanned(relPaths) {
  for (const rel of relPaths) {
    const text = readFileSync(join(root, rel), "utf8");
    for (const re of BANNED) {
      if (re.test(text)) {
        assert(false, "banned phrase " + re + " in " + rel);
        return;
      }
    }
  }
  assert(true, "no banned advisory phrases in company-level assets");
}

function testBandMapping() {
  assert(
    scoreToCompanyAlignmentBand(82, { scorable: true, level: "sufficient" }) === "Strong Alignment Signals",
    "score 82 → Strong"
  );
  assert(
    scoreToCompanyAlignmentBand(40, { scorable: true, level: "sufficient" }) === "Limited Alignment Signals",
    "score 40 → Limited"
  );
  assert(
    scoreToCompanyAlignmentBand(90, { scorable: false, level: "insufficient" }) === "Insufficient Data",
    "not scorable → Insufficient Data"
  );
}

function testCompleteness() {
  const good = assessOperatorDataCompleteness(
    {
      companyName: "Acme Hotels",
      specificMarkets: ["Mexico", "Caribbean"],
      chainScale: ["Upscale"],
      primaryServices: ["Full Management"],
    },
    { profile: true, platform: true, commercial: false, governance: false }
  );
  assert(good.scorable, "sufficient prefill is scorable");
  const weak = assessOperatorDataCompleteness({}, { profile: false, platform: false });
  assert(!weak.scorable, "empty prefill not scorable");
}

function testSignalsNeutral() {
  const signals = alignmentSignalsFromBreakdown({
    geographyMarkets: { label: "Geography & Markets", score: 80 },
  });
  assert(signals.length > 0, "breakdown produces signals");
  assert(!/recommend/i.test(signals.join(" ")), "signals avoid recommend language");
}

function testRoutesAndWiring() {
  const server = readFileSync(join(root, "server.js"), "utf8");
  const serverUpload = readFileSync(join(root, "server.upload-ready.js"), "utf8");
  assert(server.includes("/api/operator-alignment-snapshot/:dealId/companies"), "companies route in server.js");
  assert(serverUpload.includes("/api/operator-alignment-snapshot/:dealId/companies"), "companies route in server.upload-ready.js");
  assert(server.includes("getOperatorAlignmentSnapshotCompanies"), "companies handler imported");
  const companiesIdx = server.indexOf('"/api/operator-alignment-snapshot/:dealId/companies"');
  const api404Idx = server.indexOf('app.use("/api", (req, res) =>');
  assert(companiesIdx > 0 && api404Idx > companiesIdx, "companies route registered before API 404 fallback in server.js");
  const companiesIdxU = serverUpload.indexOf('"/api/operator-alignment-snapshot/:dealId/companies"');
  const api404IdxU = serverUpload.indexOf('app.use("/api", (req, res) =>');
  assert(companiesIdxU > 0 && api404IdxU > companiesIdxU, "companies route before API 404 fallback in server.upload-ready.js");
  const html = readFileSync(join(root, "public/operator-alignment-snapshot.html"), "utf8");
  const md = readFileSync(join(root, "public/my-deals.html"), "utf8");
  const js = readFileSync(join(root, "public/js/operator-alignment-snapshot.js"), "utf8");
  assert(html.includes("/companies"), "standalone page fetches companies");
  assert(html.includes("attachCompaniesSnapshot"), "standalone always attaches companies payload");
  assert(md.includes("/companies"), "My Deals fetches companies");
  assert(md.includes("attachCompaniesSnapshot"), "My Deals uses attachCompaniesSnapshot when available");
  assert(js.includes("Operating Companies for Consideration"), "standalone section title present");
  assert(js.includes("COMPANIES_GATED_PRIMARY"), "gated primary copy constant");
  assert(js.includes("COMPANIES_GATED_SUPPORT"), "gated support copy constant");
  assert(js.includes("buildOutputNote"), "dynamic final limitation note helper");
  assert(js.includes("OUTPUT_NOTE_WITH_COMPANIES"), "companies-available footer variant");
  assert(js.includes("OUTPUT_NOTE_PROFILE_ONLY"), "profile-only footer variant");
  assert(
    !js.includes("does not evaluate named operators"),
    "legacy static footer (named operators) removed from renderer"
  );
  assert(js.includes("renderCompaniesForConsiderationSection"), "full companies section renderer");
  assert(js.includes("renderCompaniesGatedBlock"), "gated companies block renderer");
  assert(js.includes("attachCompaniesSnapshot"), "attachCompaniesSnapshot helper");
  assert(js.includes("[OAS companies QA]"), "dev-only companies QA console log");
  assert(js.includes("companiesFetchUserMessage"), "status-aware companies fetch error helper");
  assert(js.includes("fetchCompaniesApiPack"), "companies fetch pack helper");
  assert(js.includes("endpoint was not found"), "404-specific companies message");
  assert(js.includes("requires a signed-in session"), "401/403-specific companies message");
  assert(js.includes("server error"), "500-specific companies message");
  assert(
    !js.includes('could not be loaded (API route not found)'),
    "does not collapse all failures into API route not found parenthetical"
  );
  assert(!js.includes("op-1"), "renderer has no mock operator id op-1");
  assert(html.includes("oas-cover-265"), "standalone cache-bust query on OAS assets");
  assert(js.includes("cloneNode(true)"), "print clones live snapshot like BAS");
  assert(!js.includes("buildPrintHtml"), "no linear print document builder");
  assert(!js.includes("rebuildPrintCover"), "print matches BAS (no custom cover rebuild)");
  assert(js.includes("buildCompanyOwnerRationale"), "company-specific owner rationale");
  assert(js.includes("buildCurrentReviewStatus"), "dynamic current review status");
  assert(/oas-operator-detail-title[\s\S]{0,600}oas-operator-detail-meta/.test(js), "detail card company title before score");
  assert(js.includes("oas-operator-detail-title"), "visible detail card title element");
  const coverFn = js.match(/function renderCover[\s\S]*?return html;\s*\n\s*}/);
  assert(coverFn && coverFn[0].indexOf("bas-cover-disclaimer") < coverFn[0].lastIndexOf("bas-cover-hero"), "cover disclaimer before logo in single section");
  assert(!js.includes("before advancing — Compares"), "no raw scoring Compares fragments emitted");
  assert(js.includes("renderPage1OperatorNarrative"), "Brand Assessment narrative page");
  assert(js.includes("Operating Companies for Owner Review"), "owner review companies table");
  assert(!/buildHtml[\s\S]*renderDocumentBody/.test(js), "full snapshot does not use scroll grid body");
  assert(js.includes("resolveCompanyDisplayName"), "company display name resolver");
  assert(js.includes("data-oas-company-name"), "company name marker in card DOM");
  assert(js.includes("oas-card-header__title-row"), "card title row before markets");
  assert(js.includes("OAS_PRINT_GAP_LIMIT"), "capped key data gaps");
  assert(js.includes("Key Follow-Ups"), "combined closing section");
  assert(js.includes("oas-brief-card"), "light OAS summary cards (not BAS dark brief)");
  assert(js.includes("oas-card-grid--two-col"), "two-column card grid");
  assert(js.includes("OAS_PRINT_COMPANY_LIMIT"), "print company limit constant");
  assert(js.includes("Showing "), "showing N of M companies copy");
  assert(!js.includes("Alignment detail score:"), "no Alignment detail score label");
  assert(js.includes("Informational score:"), "informational score label");
  assert(js.includes("humanizeCompanyAlignmentSignal"), "owner-facing signal phrasing");
  assert(js.includes("renderMarketChips"), "compact market chips");
  assert(js.includes("collectCommonDataGaps"), "common data gaps summarization");
  assert(js.includes("oas-company-card--limited-extra"), "screen-only extra company cards");
  assert(js.includes("OAS_COMPANY_SECTION_NOTE"), "single section-level company note");
  assert(!js.includes('renderList("Company-level alignment signals"'), "no repeated company-level alignment signals list heading");
}

assert(existsSync(join(root, "lib/operator-alignment-company-utils.js")), "company utils exists");
testBandMapping();
testCompleteness();
testSignalsNeutral();
testRoutesAndWiring();
scanNoBanned([
  "lib/operator-alignment-company-utils.js",
  "public/js/operator-alignment-snapshot.js",
  "api/operator-alignment-snapshot.js",
]);

if (failed > 0) {
  console.error("\n" + failed + " failure(s)");
  process.exit(1);
}
console.log("\nOperator Alignment companies validation passed.");
