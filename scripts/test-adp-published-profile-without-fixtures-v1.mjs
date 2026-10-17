/**
 * ADP client reports must resolve from the shipped published manifest when
 * fixtures/ are absent (lean production). Share validity, missing property,
 * and missing report stay distinct. GDI files are not required.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import ignore from "ignore";
import { fileURLToPath } from "node:url";
import {
  loadPropertyProfile,
  profileFromPublishedManifest,
} from "../lib/ai-demand-positioning/data-model.js";
import { getPublishedOwnerReport } from "../lib/ai-demand-positioning/published-read-service.js";
import { resolveAdpSharePropertyDisplay } from "../lib/ai-demand-positioning/share/adp-share-property-display-v1.js";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const published = path.join(root, "data/ai-demand-positioning/published");
const emptyFixtures = path.join(os.tmpdir(), "adp-no-fixtures-" + Date.now());
fs.mkdirSync(emptyFixtures, { recursive: true });

const hotels = [
  ["adp_renaissance_times_square", "Renaissance New York Times Square Hotel"],
  ["adp_westin_tampa_waterside", "The Westin Tampa Waterside"],
  ["adp_wyndham_san_diego_bayside", "Wyndham San Diego Bayside"],
];

for (const [propertyId, name] of hotels) {
  const profile = loadPropertyProfile(propertyId, {
    fixturesDir: emptyFixtures,
    publishedDir: published,
  });
  assert.ok(profile, `${propertyId} profile from published manifest`);
  assert.equal(profile.name, name);
  assert.equal(profile.propertyId, propertyId);
  const report = await getPublishedOwnerReport(propertyId);
  assert.equal(report.ok, true, `${propertyId} published report`);
  assert.equal(report.payload?.property?.name, name);
  assert.equal(report.payload?.property?.propertyId, propertyId);
}

const missingProperty = loadPropertyProfile("adp_not_a_real_property", {
  fixturesDir: emptyFixtures,
  publishedDir: published,
});
assert.equal(missingProperty, null);
const missingPropertyRead = await getPublishedOwnerReport("adp_not_a_real_property");
assert.equal(missingPropertyRead.ok, false);
assert.equal(missingPropertyRead.error, "property_not_found");

const manifestOnly = fs.mkdtempSync(path.join(os.tmpdir(), "adp-manifest-only-"));
const gapId = "adp_manifest_without_report";
fs.mkdirSync(path.join(manifestOnly, gapId), { recursive: true });
fs.writeFileSync(
  path.join(manifestOnly, gapId, "manifest.json"),
  JSON.stringify({
    propertyId: gapId,
    propertyName: "Manifest Only Hotel",
    city: "Test City",
  })
);
const gapProfile = profileFromPublishedManifest(gapId, manifestOnly);
assert.equal(gapProfile.name, "Manifest Only Hotel");
const gapRead = await getPublishedOwnerReport(gapId, { profile: gapProfile });
assert.equal(gapRead.ok, false);
assert.equal(gapRead.error, "no_monitoring_data");
assert.notEqual(gapRead.error, "property_not_found");

const display = resolveAdpSharePropertyDisplay("adp_westin_tampa_waterside");
assert.equal(display.name, "The Westin Tampa Waterside");
assert.match(display.label, /Westin Tampa Waterside/);

const ig = ignore().add(fs.readFileSync(path.join(root, ".railwayignore"), "utf8"));
assert.equal(
  ig.ignores("fixtures/ai-demand-positioning/westin-tampa-waterside-property-profile.json"),
  true
);
for (const [propertyId] of hotels) {
  const manifestRel = `data/ai-demand-positioning/published/${propertyId}/manifest.json`;
  assert.equal(ig.ignores(manifestRel), false, manifestRel);
  const manifest = JSON.parse(fs.readFileSync(path.join(root, manifestRel), "utf8"));
  const reportRel = `data/ai-demand-positioning/published/${propertyId}/${manifest.reportFile}`;
  assert.equal(ig.ignores(reportRel), false, reportRel);
  assert.equal(fs.existsSync(path.join(root, reportRel)), true);
}

console.log(JSON.stringify({ ok: true, hotels: hotels.map(([id]) => id) }));
