#!/usr/bin/env node
/**
 * Restore canonical GDI E2E → STAGED → PRODUCTION localhost default.
 * $0 / no Airtable / no publication mutation.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { resolvePreviewOpportunitiesFromPublicationPackage } from "../lib/group-demand-intelligence/publication-package/gdi-share-approved-package-resolver-v1.js";
import { inspectFounderPreviewPublicationAvailability } from "../lib/group-demand-intelligence/publication-package/gdi-founder-preview-availability-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const appJs = fs.readFileSync(
  path.join(ROOT, "public/js/group-demand-intelligence/app.js"),
  "utf8",
);
const html = fs.readFileSync(
  path.join(ROOT, "public/group-demand-intelligence.html"),
  "utf8",
);
const template = fs.readFileSync(
  path.join(
    ROOT,
    "lib/group-demand-intelligence/template/gdi-full-e2e-hotel-template-v1.js",
  ),
  "utf8",
);

const results = [];
function check(name, fn) {
  try {
    fn();
    results.push({ name, ok: true });
    console.log(`PASS ${name}`);
  } catch (e) {
    results.push({ name, ok: false, error: String(e.message || e) });
    console.error(`FAIL ${name}: ${e.message || e}`);
  }
}

check("localhost_defaults_to_staged", () => {
  assert.match(appJs, /if \(isLocalFounderHost\(\)\) return "staged"/);
  assert.doesNotMatch(appJs, /if \(isLocalFounderHost\(\)\) return "research"/);
});

check("research_remains_explicit_diagnostic", () => {
  assert.match(appJs, /inventory === "research"/);
  assert.match(appJs, /q\.push\("live=1"\)/);
  assert.match(
    appJs,
    /never as a silent substitute for STAGED|Research remains available only via explicit/,
  );
});

check("missing_staged_fail_closed", () => {
  assert.match(appJs, /STAGED_PUBLICATION_PREVIEW_UNAVAILABLE/);
  assert.match(appJs, /preview=staged/);
});

check("full_e2e_template_requires_staged_localhost", () => {
  assert.match(
    template,
    /authenticated GDI loads STAGED on localhost \(or \?preview=staged\)/,
  );
});

check("bethesda_staged_and_current_intact", () => {
  const avail = inspectFounderPreviewPublicationAvailability({
    hotelId: "recLuxvwwxID7U2B8",
  });
  assert.equal(avail.hasStagedPublication, true);
  assert.equal(avail.hasCurrentPublication, true);
  const staged = resolvePreviewOpportunitiesFromPublicationPackage({
    hotelId: "recLuxvwwxID7U2B8",
    preferStaged: true,
  });
  assert.equal(staged.ok, true);
  assert.ok((staged.opportunities || []).length > 0);
});

check("design_hotels_have_no_staged_package", () => {
  for (const id of [
    "rece0NdV9FCnaFlVS",
    "recaokE8psIbuVeuH",
    "rec1t35e3sPerzxYu",
  ]) {
    const avail = inspectFounderPreviewPublicationAvailability({ hotelId: id });
    assert.equal(avail.hasStagedPublication, false, id);
    const staged = resolvePreviewOpportunitiesFromPublicationPackage({
      hotelId: id,
      preferStaged: true,
    });
    assert.equal(staged.ok, false, id);
  }
});

check("cache_marker_bumped_for_app_js", () => {
  assert.match(
    html,
    /app\.js\?v=gdi-restore-canonical-staged-workflow-v1/,
  );
});

const failed = results.filter((r) => !r.ok);
const out = {
  contract: "gdi-restore-canonical-staged-workflow-v1",
  at: new Date().toISOString(),
  passed: results.filter((r) => r.ok).length,
  failed: failed.length,
  results,
};
fs.mkdirSync(
  path.join(ROOT, "reports/gdi-restore-canonical-staged-workflow-v1"),
  { recursive: true },
);
fs.writeFileSync(
  path.join(
    ROOT,
    "reports/gdi-restore-canonical-staged-workflow-v1/TEST_RESULTS.json",
  ),
  JSON.stringify(out, null, 2),
);
if (failed.length) {
  console.error(`FAILED ${failed.length}`);
  process.exit(1);
}
console.log(`\nOK ${results.length} restore-staged gates`);
