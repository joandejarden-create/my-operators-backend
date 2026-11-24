#!/usr/bin/env node
/**
 * P0 GDI inventory visibility R1 — permanent regression protection.
 * STAGED remains default; missing-STAGED + research → founder lifecycle UI;
 * customers never get unfinalized research; no publication mutation.
 *
 *   npm run test:gdi-inventory-visibility-r1
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { inspectFounderPreviewPublicationAvailability } from "../lib/group-demand-intelligence/publication-package/gdi-founder-preview-availability-v1.js";
import { resolvePreviewOpportunitiesFromPublicationPackage } from "../lib/group-demand-intelligence/publication-package/gdi-share-approved-package-resolver-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const results = [];
function check(name, fn) {
  try {
    fn();
    results.push({ name, ok: true });
    console.log(`PASS ${name}`);
  } catch (err) {
    results.push({ name, ok: false, error: String(err.message || err) });
    console.error(`FAIL ${name}: ${err.message}`);
  }
}

const appJs = fs.readFileSync(
  path.join(ROOT, "public/js/group-demand-intelligence/app.js"),
  "utf8",
);
const shellJs = fs.readFileSync(path.join(ROOT, "public/app.js"), "utf8");
const apiJs = fs.readFileSync(
  path.join(ROOT, "api/group-demand-intelligence.js"),
  "utf8",
);
const html = fs.readFileSync(
  path.join(ROOT, "public/group-demand-intelligence.html"),
  "utf8",
);
const css = fs.readFileSync(
  path.join(ROOT, "public/css/group-demand-intelligence.css"),
  "utf8",
);

const NO_STAGED = Object.freeze([
  ["rec35fExUxCClpOP6", "Hilton New York Times Square", { disc: 45, qual: 44 }],
  ["rec2PVBDavppGpenm", "AC Hotel A Coruña", null],
  ["recIwaP1etgx2g9nA", "Cambridge Beaches", null],
  ["rec82D9zpB8fede1I", "Castillo Hotel Son Vida", null],
  ["recGkME49yYuxQl0u", "NOW NOW NOHO", null],
  ["recjhQdAUiSyxfCqE", "Sheraton Mallorca Arabella", null],
  ["recFaxTEFF9ILHWC9", "The Westin Grand München", null],
]);

const STAGED_PROTECTED = Object.freeze([
  ["rece0NdV9FCnaFlVS", "Condesa DF", 4],
  ["recaokE8psIbuVeuH", "LaLuna", 3],
  ["rec1t35e3sPerzxYu", "Memmo Príncipe Real", 11],
  ["recLuxvwwxID7U2B8", "Bethesda Marriott", 67],
]);

const CATALOG_25 = Object.freeze([
  "rec2PVBDavppGpenm",
  "recLuxvwwxID7U2B8",
  "recIwaP1etgx2g9nA",
  "rec82D9zpB8fede1I",
  "rece0NdV9FCnaFlVS",
  "recRkGGNKslJptFIL",
  "rec35fExUxCClpOP6",
  "recaokE8psIbuVeuH",
  "rec1t35e3sPerzxYu",
  "recC2kbCaS48jFjFF",
  "recGkME49yYuxQl0u",
  "recH9h3McgMNFQEla",
  "recUOyzOXn2Zdp98I",
  "recG66DQJKP2c0UNh",
  "recCAGqkERSXgiku4",
  "recjhQdAUiSyxfCqE",
  "recKRJjcPnb4tVDDS",
  "rec0eTEnCqlsGJJt9",
  "recFaxTEFF9ILHWC9",
  "recdmo0CnhIZAwHGC",
  "rece0or38cxo3Fymb",
  "recgMYovrrZDJMqzX",
  "recXh4NgF8sDbi9cQ",
  "recnPB3Q02NO8Ma1o",
  "recrPQcZg7SFARRb2",
]);

check("staged_remains_localhost_default", () => {
  assert.match(appJs, /if \(isLocalFounderHost\(\)\) return "staged"/);
  assert.doesNotMatch(appJs, /if \(isLocalFounderHost\(\)\) return "research"/);
  assert.match(shellJs, /isLocalFounderShellHost\(\) \? 'staged'/);
  assert.doesNotMatch(shellJs, /isLocalFounderShellHost\(\) \? 'research'/);
});

check("missing_staged_founder_copy_and_view_research", () => {
  assert.match(
    appJs,
    /No finalized opportunities are available for this property yet\. Research opportunities are available for founder review\./,
  );
  assert.match(appJs, /VIEW RESEARCH/);
  assert.match(appJs, /data-gdi-recovery="view-research"/);
  assert.match(appJs, /canAccessFounderResearchInventory/);
  assert.match(appJs, /Qualified in research/);
  assert.match(appJs, /Finalized STAGED/);
  assert.match(appJs, /Last finalized publication/);
  assert.match(appJs, /Last research:/);
});

check("lifecycle_states_distinguished", () => {
  assert.match(appJs, /resolveInventoryLifecycleState/);
  assert.match(appJs, /NO_RESEARCH_AVAILABLE/);
  assert.match(appJs, /RESEARCH_AVAILABLE_NOT_FINALIZED/);
  assert.match(appJs, /QUALIFIED_RESEARCH_AVAILABLE/);
  assert.match(appJs, /FINALIZED_STAGED_AVAILABLE/);
  assert.match(appJs, /DATA_LOAD_ERROR/);
  assert.match(appJs, /data-gdi-lifecycle-state/);
  assert.match(appJs, /loadError/);
});

check("research_mode_banner_and_return_to_staged", () => {
  assert.match(appJs, /RESEARCH \/ NOT FINALIZED/);
  assert.match(appJs, /data-gdi-recovery="view-staged"/);
  assert.match(appJs, /View Finalized STAGED/);
  assert.match(appJs, /founderResearchModeBannerHtml/);
  assert.match(css, /\.gdi-research-mode-banner/);
  assert.match(css, /\.gdi-inventory-counts/);
});

check("research_link_preserves_hotel_via_live_query", () => {
  assert.match(appJs, /applyFounderInventoryMode\("research"\)/);
  assert.match(
    appJs,
    /url\.searchParams\.set\("inventory", "research"\)[\s\S]{0,80}set\("live", "1"\)/,
  );
  assert.match(appJs, /hotelId: state\.hotelId/);
});

check("customers_cannot_force_live_research", () => {
  assert.match(apiJs, /RESEARCH_INVENTORY_FORBIDDEN/);
  assert.match(apiJs, /Unfinalized research inventory is founder\/admin only/);
  assert.match(appJs, /canAccessFounderResearchInventory/);
  // Customer empty copy must not advertise research CTA without founder gate.
  const emptyFn = appJs.indexOf("function inventoryUnavailableEmptyHtml");
  assert.ok(emptyFn >= 0);
  const slice = appJs.slice(emptyFn, emptyFn + 1800);
  assert.match(slice, /canAccessFounderResearchInventory/);
  assert.match(slice, /VIEW RESEARCH/);
});

check("no_automatic_publication_or_pointer_changes", () => {
  assert.doesNotMatch(appJs, /stageForProduction|writePointer\(|promoteToCurrent/);
  assert.match(apiJs, /STAGED_PUBLICATION_PREVIEW_UNAVAILABLE/);
  assert.match(
    apiJs,
    /Refusing to fall back to another hotel, CURRENT published inventory, or live research/,
  );
});

check("hilton_times_square_research_without_staged", () => {
  const id = "rec35fExUxCClpOP6";
  const avail = inspectFounderPreviewPublicationAvailability({ hotelId: id });
  assert.equal(avail.hasStagedPublication, false);
  assert.equal(avail.discoveredCount, 45);
  assert.equal(avail.qualifiedInResearchCount, 44);
  assert.equal(avail.finalizedStagedCount, 0);
  assert.equal(avail.hasResearchInventory, true);
  const staged = resolvePreviewOpportunitiesFromPublicationPackage({
    hotelId: id,
    preferStaged: true,
  });
  assert.equal(staged.ok, false);
  assert.equal(staged.mode, "FAIL_CLOSED");
});

check("seven_no_staged_hotels_have_research_visibility", () => {
  for (const [id, name, expect] of NO_STAGED) {
    const avail = inspectFounderPreviewPublicationAvailability({ hotelId: id });
    assert.equal(avail.hasStagedPublication, false, name);
    assert.ok(
      avail.hasResearchInventory || avail.discoveredCount > 0,
      `${name} expected research inventory`,
    );
    if (expect) {
      assert.equal(avail.discoveredCount, expect.disc, `${name} discovered`);
      assert.equal(
        avail.qualifiedInResearchCount,
        expect.qual,
        `${name} qualified`,
      );
    }
    const staged = resolvePreviewOpportunitiesFromPublicationPackage({
      hotelId: id,
      preferStaged: true,
    });
    assert.equal(staged.ok, false, name);
  }
});

check("protected_staged_hotels_preserve_counts", () => {
  for (const [id, name, count] of STAGED_PROTECTED) {
    const avail = inspectFounderPreviewPublicationAvailability({ hotelId: id });
    assert.equal(avail.hasStagedPublication, true, name);
    const staged = resolvePreviewOpportunitiesFromPublicationPackage({
      hotelId: id,
      preferStaged: true,
    });
    assert.equal(staged.ok, true, name);
    assert.equal(
      String(staged.meta?.previewSource || "").toUpperCase(),
      "STAGED",
      name,
    );
    const n = Array.isArray(staged.opportunities)
      ? staged.opportunities.length
      : 0;
    assert.equal(n, count, `${name} staged count`);
    assert.equal(avail.finalizedStagedCount, count, `${name} avail count`);
  }
});

check("portfolio_25_hotels_lifecycle_matrix", () => {
  const matrix = [];
  for (const id of CATALOG_25) {
    const avail = inspectFounderPreviewPublicationAvailability({ hotelId: id });
    assert.equal(avail.hotelId, id);
    assert.ok("hasStagedPublication" in avail);
    assert.ok("hasResearchInventory" in avail);
    assert.ok("discoveredCount" in avail);
    assert.ok("qualifiedInResearchCount" in avail);
    assert.ok("finalizedStagedCount" in avail);
    let lifecycle = "NO_RESEARCH_AVAILABLE";
    if (avail.hasStagedPublication && avail.finalizedStagedCount > 0) {
      lifecycle = "FINALIZED_STAGED_AVAILABLE";
    } else if (avail.qualifiedInResearchCount > 0) {
      lifecycle = "QUALIFIED_RESEARCH_AVAILABLE";
    } else if (avail.hasResearchInventory || avail.discoveredCount > 0) {
      lifecycle = "RESEARCH_AVAILABLE_NOT_FINALIZED";
    }
    matrix.push({
      hotelId: id,
      lifecycle,
      discovered: avail.discoveredCount,
      qualifiedInResearch: avail.qualifiedInResearchCount,
      finalizedStaged: avail.finalizedStagedCount,
      hasStaged: avail.hasStagedPublication,
    });
  }
  assert.equal(matrix.length, 25);
  const outDir = path.join(
    ROOT,
    "reports/gdi-portfolio-inventory-visibility-r1",
  );
  fs.mkdirSync(outDir, { recursive: true });
  fs.writeFileSync(
    path.join(outDir, "PORTFOLIO_COUNT_MATRIX.json"),
    JSON.stringify({ generatedAt: new Date().toISOString(), hotels: matrix }, null, 2),
  );
});

check("cache_bust_r1", () => {
  assert.match(html, /app\.js\?v=gdi-inventory-visibility-r1/);
  assert.match(html, /group-demand-intelligence\.css\?v=gdi-inventory-visibility-r1/);
});

const failed = results.filter((r) => !r.ok);
console.log(
  JSON.stringify(
    {
      contract: "gdi-inventory-visibility-r1",
      passed: results.filter((r) => r.ok).length,
      failed: failed.length,
      results,
      safety: {
        paidResearch: false,
        airtableWrites: false,
        publicationChanges: false,
        customerResearchExposure: false,
      },
    },
    null,
    2,
  ),
);
if (failed.length) {
  console.error("FAILED:", failed.map((f) => f.name).join(", "));
  process.exit(1);
}
console.log("\nOK test:gdi-inventory-visibility-r1");
