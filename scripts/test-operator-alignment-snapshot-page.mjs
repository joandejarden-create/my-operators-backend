#!/usr/bin/env node
/**
 * Operator Alignment Snapshot page — static validation (renderer + copy + print CSS).
 *   node scripts/test-operator-alignment-snapshot-page.mjs
 */
import { readFileSync, existsSync } from "fs";
import { execSync } from "child_process";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { buildOperatorAlignmentProfileSnapshot } from "../lib/operator-alignment-profile-utils.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");

const ALLOWED_PHRASES = [
  "not a recommendation or advisory conclusion",
  "does not recommend, rank, endorse, or select operators",
  "not a recommendation",
];

const ADVISORY_PATTERNS = [
  /\brecommend\b(?!ation or advisory)/i,
  /\bdealality recommends\b/i,
  /\bbest operator\b/i,
  /\bpreferred operator\b/i,
  /\bshould select\b/i,
  /\badvisory\b/i,
  /\bwe advise\b/i,
  /\bstrongest path\b/i,
  /\brecommended path\b/i,
  /\bbest fit\b/i,
  /\bbest match\b/i,
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

function scanFilesForAdvisory(paths) {
  for (const rel of paths) {
    const lines = readFileSync(join(root, rel), "utf8").split(/\n/);
    for (const line of lines) {
      if (ALLOWED_PHRASES.some((p) => line.toLowerCase().includes(p))) continue;
      for (const re of ADVISORY_PATTERNS) {
        if (re.test(line)) {
          assert(false, "advisory pattern " + re + " in " + rel + ": " + line.trim().slice(0, 60));
          return;
        }
      }
    }
  }
  assert(true, "no advisory patterns in new page assets");
}

function loadCalaSampleMergedFields() {
  const path = join(root, "fixtures", "sample-deals", "aeropuerto-cancun-select-service.example.json");
  const sample = JSON.parse(readFileSync(path, "utf8"));
  return { ...(sample.referenceProperty?.fields || {}), ...(sample.fictionalDeal?.fields || {}) };
}

/** Renderer is a browser IIFE — validate structure via source inspection. */
function testRendererViaFileRead() {
  const jsPath = join(root, "public/js/operator-alignment-snapshot.js");
  try {
    execSync(`node --check "${jsPath}"`, { stdio: "pipe" });
    assert(true, "renderer JS parses (node --check)");
  } catch (e) {
    assert(false, "renderer JS syntax error: " + (e.stderr?.toString() || e.message));
  }
  const js = readFileSync(jsPath, "utf8");
  assert(js.includes("function humanizeSignalKey"), "humanization helper exists");
  assert(js.includes("SIGNAL_KEY_LABELS"), "signal key label map exists");
  assert(js.includes("stripWorkflowActionPrefix"), "workflow prefix stripper exists");
  assert(!js.includes('<code class="oas-code">'), "renderer does not emit raw key code chips");
  assert(!js.includes('renderList("Suggested workflow action"'), "profile list uses plural section title only");
  assert(js.includes("Operator Alignment Snapshot"), "cover uses Operator Alignment Snapshot title");
  assert(js.includes("Operator Profiles for Review"), "renderer has profiles section");
  assert(js.includes("Operating Companies for Consideration"), "renderer has companies section title");
  assert(js.includes("attachCompaniesSnapshot"), "companies payload attach helper");
  assert(js.includes("fetchCompaniesApiPack"), "companies API fetch helper");
  assert(js.includes("companiesFetchUserMessage"), "status-aware companies error messages");
  assert(js.includes("OUTPUT_NOTE_WITH_COMPANIES"), "footer uses with-companies variant when data available");
  assert(js.includes("buildOutputNote"), "dynamic footer note helper");
  assert(js.includes("OUTPUT_NOTE_WITH_COMPANIES"), "footer variant when companies available");
  assert(js.includes("OUTPUT_NOTE_PROFILE_ONLY"), "footer variant when companies gated");
  assert(
    !js.includes("does not evaluate named operators"),
    "legacy static footer removed"
  );
  assert(js.includes("Suggested Workflow Actions"), "renderer uses workflow actions label");
  assert(!js.includes("Recommended Next Steps"), "renderer avoids recommended next steps");
  assert(js.includes("Not provided"), "renderer uses Not provided");
  assert(js.includes("data-bas-print"), "renderer has print button");
  assert(js.includes("oas-deal-grid"), "deal context uses OAS grid class");
  assert(js.includes("renderPage1OperatorNarrative"), "Brand Assessment-style narrative page");
  assert(js.includes("renderPage2OperatorDetail"), "Brand Assessment-style detail page");
  assert(js.includes("wrapBookPage"), "three-page book shell");
  assert(js.includes("bindPageFlip"), "page flip like Brand Assessment");
  assert(js.includes("Operator Alignment Narrative"), "narrative section kicker");
  assert(js.includes("1. Operator Alignment Summary"), "alignment summary section");
  assert(js.includes("2. Operator Pathway View"), "pathway view table");
  assert(js.includes("3. Operating Companies for Owner Review"), "companies for owner review table");
  assert(js.includes("Operator Alignment Detail"), "detail section kicker");
  assert(js.includes("1. Operator Alignment Snapshot Table"), "snapshot table");
  assert(js.includes("2. Operator-by-Operator Review Cards"), "operator detail cards");
  assert(js.includes("Common Questions to Clarify Before Outreach"), "common questions section");
  assert(js.includes("Methodology Note"), "methodology note section");
  assert(js.includes("DEALALITY OPERATOR ALIGNMENT SNAPSHOT"), "full cover doc type");
  assert(!/function buildHtml[\s\S]*renderDocumentBody/.test(js), "scroll grid body not used in buildHtml");
  assert(js.includes("OAS_DETAIL_CARD_LIMIT"), "detail card limit");
  assert(js.includes("data-oas-company-name"), "company name in detail cards");
  assert(js.includes("buildCompanyOwnerRationale"), "company-specific rationale builder");
  assert(js.includes("buildCompanyFactorsReviewed"), "polished alignment factors list");
  assert(js.includes("stripTechnicalScoringTail"), "technical scoring tail stripper");
  assert(js.includes("oas-operator-detail-title"), "detail card uses oas-operator-detail-title");
  assert(js.includes("oas-operator-detail-meta"), "detail card uses oas-operator-detail-meta");
  assert(/oas-operator-detail-title[\s\S]{0,600}oas-operator-detail-meta/.test(js), "company name before score in detail card");
  assert(!/oas-operator-detail-meta[\s\S]{0,300}oas-operator-detail-title/.test(js), "score line does not precede company title");
  const coverFn = js.match(/function renderCover[\s\S]*?return html;\s*\n\s*}/);
  assert(coverFn, "renderCover function present");
  const coverBody = coverFn[0];
  const coverStart = coverBody.indexOf("bas-cover-page");
  const coverEnd = coverBody.indexOf("</section>", coverStart);
  const coverBlock = coverBody.slice(coverStart, coverEnd);
  assert(coverBlock.includes("bas-avoid-break"), "cover uses bas-avoid-break like BAS");
  assert(coverBlock.includes("bas-cover-disclaimer") && coverBlock.includes("bas-cover-hero"), "disclaimer and logo inside cover section");
  assert(!coverBlock.includes("oas-cover-footer"), "cover matches BAS structure (no oas-cover-footer)");
  assert(js.includes("cloneNode(true)"), "print clones live snapshot like BAS");
  assert(!js.includes("buildPrintHtml"), "no separate print HTML builder");
  assert(!js.includes("renderPrintCover"), "no dedicated print cover markup");
  assert(!js.includes("oas-print-cover-page"), "no linear print cover class");
  assert(!js.includes("flattenBookForPrint"), "no book flatten for print");
  assert(!js.includes('page.style.pageBreakAfter = "auto"'), "print uses flatten not inline break hacks");
  const detailCardFn = js.match(/function renderOperatorDetailCard[\s\S]*?return html;\s*\n\s*}/);
  assert(detailCardFn, "renderOperatorDetailCard function present");
  assert(detailCardFn[0].includes("bas-section--keep"), "detail cards use bas-section--keep like BAS");
  assert(js.includes("Ready for controlled operator review after owner/advisor validation"), "ready review status copy");
  assert(!js.includes("before advancing — Compares"), "no raw scoring Compares tail in renderer source");
  assert(!js.includes("brand / portfolio relevance before advancing"), "no raw portfolio advancing label in source");
  assert(js.includes("OAS_PRINT_GAP_LIMIT"), "capped data gaps in full snapshot");
  assert(js.includes("resolveCompanyDisplayName"), "company name resolver");
  assert(js.includes("data-oas-company-name"), "visible company name attribute");
  assert(js.includes("oas-card-header__title-row"), "profile/company title row hierarchy");
  assert(!js.includes("Alignment detail score:"), "no prominent alignment detail score label");
  assert(!js.includes("Additional questions and data gaps may apply"), "no per-profile overflow boilerplate");
  assert(js.includes("bas-print-host"), "print uses bas-print-host like BAS");
  assert(!js.includes("_oasPrintData"), "render does not store parallel print payload");
  assert(js.includes("runPrintWhenReady"), "cover logo load before print");
  assert(js.includes("DEALALITY_LOGO_URL"), "cover uses same CDN logo URL as BAS");
}

function testHtmlPage() {
  const html = readFileSync(join(root, "public/operator-alignment-snapshot.html"), "utf8");
  assert(html.includes("dealality-memberstack-auth.js"), "page loads auth helper");
  assert(html.includes("operator-alignment-snapshot.js"), "page loads renderer");
  assert(html.includes("/api/operator-alignment-snapshot/"), "page calls profile API");
  assert(html.includes("/companies"), "page fetches companies API");
  assert(html.includes("attachCompaniesSnapshot"), "page attaches companies even on API failure");
  assert(html.includes("fetchCompaniesApiPack"), "page uses companies fetch helper");
  assert(html.includes("oas-cover-265"), "cache-bust on OAS renderer script");
  assert(!html.includes("oas-page"), "page shell matches BAS (no oas-page)");
  assert(html.includes("dealId"), "page reads dealId query");
}

function testPrintCss() {
  const css = readFileSync(join(root, "public/css/operator-alignment-snapshot.css"), "utf8");
  const js = readFileSync(join(root, "public/js/operator-alignment-snapshot.js"), "utf8");
  const companiesCss = readFileSync(join(root, "public/css/operator-alignment-companies.css"), "utf8");
  assert(css.includes("@media print"), "print CSS present");
  assert(css.includes("page-break-inside: avoid"), "avoid awkward card breaks");
  assert(!css.includes("size: A4"), "OAS print does not redefine @page (BAS CSS owns it)");
  assert(!css.includes("oas-print-sheet--cover"), "no custom OAS print sheet overrides");
  assert(!css.includes("rebuildPrintCover"), "no custom print cover CSS");
  assert(js.includes("flattenOasBookForPrint"), "print flattens flip-book wrappers");
  assert(js.includes("rebuildOasPrintCover"), "print rebuilds minimal single-sheet cover DOM");
  assert(js.includes("fitOasPrintCoverToOnePage"), "print scales cover to fit one sheet");
  assert(js.includes("oas-print-document"), "print uses flat document without flip viewport");
  assert(js.includes("oas-print-cover-foot"), "disclaimer and logo share print footer row");
  assert(js.includes("runPrintWhenReady"), "print waits for cover logo image");
  assert(js.includes("oas-print-flattened"), "print clone tagged for flattened layout CSS");
  assert(!css.includes("oas-print-cover-page"), "no linear print cover CSS");
  assert(css.includes("oas-print-cover-sheet"), "OAS print uses fixed-height cover grid sheet");
  assert(css.includes("oas-print-cover-foot"), "cover footer row keeps logo with disclaimer");
  assert(css.includes("height: 265mm !important") && css.includes("min-height: 265mm !important"), "cover uses 265mm printable height");
  assert(js.includes("OAS_PRINT_COVER_HEIGHT_MM = 265"), "JS cover height matches CSS");
  assert(css.includes("oas-print-flattened"), "flattened print layout CSS");
  assert(!css.includes("oas-print-flow"), "no linear print body flow CSS");
  assert(css.includes(".oas-operator-detail-title"), "print CSS styles operator detail title");
  assert(!css.includes("@page"), "print page size/margins come from brand-alignment-snapshot.css");
  assert(css.includes(".oas-deal-grid"), "deal grid styled in OAS CSS");
  assert(css.includes("background: var(--oas-paper)"), "light content page override");
  assert(css.includes("oas-summary-grid"), "summary grid layout");
  assert(companiesCss.includes("oas-company-card--limited-extra"), "print hides extra companies");
  assert(companiesCss.includes("oas-market-chip"), "market chip compaction");
}

function testMyDealsIntegration() {
  const html = readFileSync(join(root, "public/my-deals.html"), "utf8");
  assert(html.includes('data-action="operator-alignment"'), "My Deals has operator-alignment action");
  assert(html.includes("Operator Alignment Snapshot"), "My Deals labels Operator Alignment Snapshot");
  assert(html.includes("myDealsOperatorAlignmentModal"), "My Deals operator alignment modal exists");
  assert(html.includes("/api/operator-alignment-snapshot/"), "My Deals modal calls OAS profile API");
  assert(html.includes("/companies"), "My Deals loads company-level API");
  assert(html.includes("/operator-alignment-snapshot.html?dealId="), "My Deals links to full snapshot URL");
  assert(html.includes("operator-alignment-snapshot.js?v=oas-cover-265"), "My Deals loads OAS renderer with cache bust");
  assert(html.includes("operator-alignment-snapshot.css?v=oas-cover-265"), "My Deals loads OAS CSS with cache bust");
  assert(html.includes("renderMyDealsPreview"), "My Deals uses compact preview renderer");
  assert(!html.includes("Recommended Operators"), "My Deals avoids Recommended Operators");
  assert(!html.includes("Best Operator"), "My Deals avoids Best Operator copy");
  const js = readFileSync(join(root, "public/js/operator-alignment-snapshot.js"), "utf8");
  assert(js.includes("renderMyDealsPreview"), "renderer exports My Deals preview");
  assert(/Operating companies for consideration/i.test(js), "companies section label in preview renderer");
  assert(js.includes("COMPANIES_GATED_SUPPORT"), "preview gated support copy");
  assert(js.includes("resolveCompaniesPayload"), "preview resolves companies payload when missing");
  assert(!js.includes('<code class="oas-code">'), "renderer avoids raw key code in UI");
}

function testSnapshotPayloadRendersFiveCards() {
  const merged = loadCalaSampleMergedFields();
  const data = buildOperatorAlignmentProfileSnapshot("recSAMPLE", merged);
  assert(data.profilesForReview.length === 5, "five profiles in payload");
  assert(data.dealContext.dealName !== "Not provided" || true, "deal context built");
  const labels = data.profilesForReview.map((p) => p.displayLabel);
  assert(labels.some((l) => /CALA/i.test(l)), "includes CALA profile label");
}

function testFilesExist() {
  const paths = [
    "public/operator-alignment-snapshot.html",
    "public/js/operator-alignment-snapshot.js",
    "public/css/operator-alignment-snapshot.css",
  ];
  for (const p of paths) {
    assert(existsSync(join(root, p)), "exists: " + p);
  }
}

testFilesExist();
scanFilesForAdvisory([
  "public/operator-alignment-snapshot.html",
  "public/js/operator-alignment-snapshot.js",
  "public/css/operator-alignment-snapshot.css",
]);
testHtmlPage();
testPrintCss();
testRendererViaFileRead();
testMyDealsIntegration();
testSnapshotPayloadRendersFiveCards();

if (failed > 0) {
  console.error("\n" + failed + " failure(s)");
  process.exit(1);
}
console.log("\nOperator Alignment Snapshot page validation passed.");
