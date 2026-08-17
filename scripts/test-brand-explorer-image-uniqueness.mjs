#!/usr/bin/env node
/**
 * Image uniqueness unit + optional live brand checks.
 * Unit portion does not require Airtable.
 */
import assert from "assert";
import {
  buildImageIdentity,
  evaluateImageUniqueness,
  evaluateImageUniquenessForTest,
  pickDistinctImageAssets,
  extractSourceImageId,
  GALLERY_DISTINCT_MIN,
} from "../lib/partner-intelligence/brand-explorer-image-uniqueness.js";

function testScene7CropsCollapse() {
  const a = "https://digital.ihg.com/is/image/ihg/hotel-indigo-coral-reef:Crop-1x1?wid=800";
  const b = "https://digital.ihg.com/is/image/ihg/hotel-indigo-coral-reef:Crop-16x9?wid=1600";
  const ia = buildImageIdentity(a, { slotKey: "materials.gallery.1" });
  const ib = buildImageIdentity(b, { slotKey: "materials.gallery.2" });
  assert.equal(ia.duplicateGroupId, ib.duplicateGroupId, "Scene7 crops must share duplicate group");
  assert.ok(extractSourceImageId(a).includes("hotel-indigo-coral-reef"));
}

function testIhgHyphenAspectRatioVariantsCollapse() {
  const a = "https://digital.ihg.com/is/image/ihg/voco-ciudad-de-mexico-10958437436-2x1";
  const b = "https://digital.ihg.com/is/image/ihg/voco-ciudad-de-mexico-10958437436-4x3";
  const c = "https://digital.ihg.com/is/image/ihg/voco-ciudad-de-mexico-11141397083-2x1";
  const ia = buildImageIdentity(a, { slotKey: "materials.gallery.3" });
  const ib = buildImageIdentity(b, { slotKey: "materials.gallery.4" });
  const ic = buildImageIdentity(c, { slotKey: "materials.gallery.5" });
  assert.equal(
    ia.duplicateGroupId,
    ib.duplicateGroupId,
    "IHG -2x1 / -4x3 aspect variants must share duplicate group"
  );
  assert.notEqual(
    ia.duplicateGroupId,
    ic.duplicateGroupId,
    "Different Scene7 asset numbers must stay distinct"
  );
  // After Airtable upload, filename still carries the aspect suffix.
  const airA = "https://v5.airtableusercontent.com/v3/u/1/1/1/aaa/bbb/hashA";
  const airB = "https://v5.airtableusercontent.com/v3/u/1/1/1/ccc/ddd/hashB";
  const fa = buildImageIdentity(airA, { filename: "voco-ciudad-de-mexico-10958437436-2x1" });
  const fb = buildImageIdentity(airB, { filename: "voco-ciudad-de-mexico-10958437436-4x3" });
  assert.equal(
    fa.duplicateGroupId,
    fb.duplicateGroupId,
    "IHG aspect variants must collide via Airtable attachment filenames"
  );
}

function testAccorSizeVariantsCollapse() {
  const a = "https://www.ahstatic.com/photos/abcd1234_ho_00_p_1024x768.jpg";
  const b = "https://www.ahstatic.com/photos/abcd1234_ho_00_p_2048x1536.jpg";
  assert.equal(
    buildImageIdentity(a).duplicateGroupId,
    buildImageIdentity(b).duplicateGroupId,
    "Accor size variants must collide"
  );
}

function testAccorDifferentScenesDistinct() {
  const a = "https://www.ahstatic.com/photos/b1r7_ho_00_p_1024x768.jpg";
  const b = "https://www.ahstatic.com/photos/b1r7_ho_01_p_1024x768.jpg";
  assert.notEqual(
    buildImageIdentity(a).duplicateGroupId,
    buildImageIdentity(b).duplicateGroupId,
    "Accor ho_00 vs ho_01 are different photographs"
  );
}

function testSixSlotsThreeDistinctFails() {
  const urlA = "https://cdn.example.com/photos/beach_ho_00_p_1024x768.jpg";
  const urlB = "https://cdn.example.com/photos/lobby_ho_00_p_1024x768.jpg";
  const urlC = "https://cdn.example.com/photos/room_ho_00_p_1024x768.jpg";
  const rows = [
    { slotKey: "materials.gallery.1", imageUrl: urlA, title: "Exterior" },
    { slotKey: "materials.gallery.2", imageUrl: urlA, title: "Guest Room" },
    { slotKey: "materials.gallery.3", imageUrl: urlB, title: "Public Space" },
    { slotKey: "materials.gallery.4", imageUrl: urlB, title: "F&B" },
    { slotKey: "materials.gallery.5", imageUrl: urlC, title: "Design Detail" },
    { slotKey: "materials.gallery.6", imageUrl: urlC, title: "Property Setting" },
    { slotKey: "overview.scenario.1", imageUrl: urlA, title: "S1" },
    { slotKey: "overview.scenario.2", imageUrl: urlB, title: "S2" },
    { slotKey: "overview.scenario.3", imageUrl: urlC, title: "S3" },
    { slotKey: "footprint.openings", imageUrl: urlA, title: "P1", recordId: "r1" },
    { slotKey: "footprint.openings", imageUrl: urlB, title: "P2", recordId: "r2" },
    { slotKey: "footprint.openings", imageUrl: urlC, title: "P3", recordId: "r3" },
  ];
  const result = evaluateImageUniqueness({ brandSlug: "fixture-dup", presentationRows: rows });
  assert.equal(result.gallerySlotCount, 6);
  assert.equal(result.galleryDistinctCount, 3);
  assert.equal(result.pass, false, "must fail when only 3 distinct gallery images fill 6 slots");
  const t = evaluateImageUniquenessForTest(result);
  assert.equal(t.pass, false);
  assert.ok(t.failures.some((f) => /gallery_distinct_short/.test(f)));
}

function testSixDistinctPasses() {
  const rows = [];
  for (let i = 1; i <= 6; i++) {
    rows.push({
      slotKey: `materials.gallery.${i}`,
      imageUrl: `https://cdn.example.com/photos/prop${i}_ho_00_p_1024x768.jpg`,
      title: `Gallery ${i}`,
    });
  }
  for (let i = 1; i <= 3; i++) {
    rows.push({
      slotKey: `overview.scenario.${i}`,
      imageUrl: `https://cdn.example.com/photos/scen${i}_ho_00_p_1024x768.jpg`,
      title: `S${i}`,
    });
    rows.push({
      slotKey: "footprint.openings",
      imageUrl: `https://cdn.example.com/photos/open${i}_ho_00_p_1024x768.jpg`,
      title: `P${i}`,
      recordId: `r${i}`,
    });
  }
  const result = evaluateImageUniqueness({ brandSlug: "fixture-ok", presentationRows: rows });
  assert.equal(result.galleryDistinctCount, GALLERY_DISTINCT_MIN);
  assert.equal(result.pass, true);
}

function testSlhSizeVariantsCollapse() {
  const a = "https://slh.com/media/coral-reef-club-78484-xl-1.jpg";
  const b = "https://slh.com/media/coral-reef-club-78484-l-1.jpg";
  const c = "https://slh.com/media/coral-reef-club-78484-s-6.jpg";
  const ia = buildImageIdentity(a);
  const ib = buildImageIdentity(b);
  const ic = buildImageIdentity(c);
  assert.equal(ia.duplicateGroupId, ib.duplicateGroupId);
  assert.equal(ib.duplicateGroupId, ic.duplicateGroupId);
}

function testFilenamePreferredOverAirtableUrl() {
  const airA = "https://v5.airtableusercontent.com/v3/u/1/1/1/aaa/bbb/hashA";
  const airB = "https://v5.airtableusercontent.com/v3/u/1/1/1/ccc/ddd/hashB";
  const ia = buildImageIdentity(airA, { filename: "coral-reef-club-78484-xl-1.jpg" });
  const ib = buildImageIdentity(airB, { filename: "coral-reef-club-78484-l-1.jpg" });
  assert.equal(ia.duplicateGroupId, ib.duplicateGroupId, "filename size variants must collide across Airtable URLs");
}

function testPickDistinctSkipsDuplicates() {
  const assets = [
    { imageUrl: "https://cdn.example.com/photos/a_ho_00_p_1024x768.jpg" },
    { imageUrl: "https://cdn.example.com/photos/a_ho_00_p_2048x1536.jpg" },
    { imageUrl: "https://cdn.example.com/photos/b_ho_00_p_1024x768.jpg" },
    { imageUrl: "https://cdn.example.com/photos/c_ho_00_p_1024x768.jpg" },
  ];
  const picked = pickDistinctImageAssets(assets, 3);
  assert.equal(picked.length, 3);
  assert.notEqual(
    picked[0]._imageIdentity.duplicateGroupId,
    picked[1]._imageIdentity.duplicateGroupId
  );
}

function parseBrands(argv) {
  const idx = argv.indexOf("--brands");
  if (idx >= 0 && argv[idx + 1]) {
    return argv[idx + 1].split(",").map((s) => s.trim()).filter(Boolean);
  }
  return [];
}

async function maybeLive(brands) {
  if (!brands.length) return;
  if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID) {
    console.log("[skip live] Airtable credentials not configured");
    return;
  }
  const { auditBrandImageUniqueness } = await import(
    "../lib/partner-intelligence/brand-explorer-image-uniqueness-audit.js"
  );
  let failed = false;
  for (const slug of brands) {
    const r = await auditBrandImageUniqueness(slug);
    const line = `${slug}: pass=${r.pass} galleryDistinct=${r.galleryDistinctCount}/${r.gallerySlotCount}`;
    if (r.pass) console.log(`[PASS] ${line}`);
    else {
      failed = true;
      console.log(`[FAIL] ${line}`);
      for (const f of r.findings || []) console.log(`  - ${f.id}: ${f.detail}`);
    }
  }
  if (failed) process.exit(3);
}

function main() {
  testScene7CropsCollapse();
  testIhgHyphenAspectRatioVariantsCollapse();
  testAccorSizeVariantsCollapse();
  testAccorDifferentScenesDistinct();
  testSlhSizeVariantsCollapse();
  testFilenamePreferredOverAirtableUrl();
  testSixSlotsThreeDistinctFails();
  testSixDistinctPasses();
  testPickDistinctSkipsDuplicates();
  console.log("[PASS] brand-explorer image uniqueness (unit)");
  return maybeLive(parseBrands(process.argv.slice(2)));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
