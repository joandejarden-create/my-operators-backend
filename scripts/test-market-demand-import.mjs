#!/usr/bin/env node
/**
 * Market Demand import + duplicate detection tests.
 *   node scripts/test-market-demand-import.mjs
 */
import "../load-env.js";
import {
  normalizeDemandCenterName,
  nameSimilarity,
  isDuplicateCandidate,
  validateImportItem,
  buildImportPreview,
  filterSelectedImportItems,
  VALID_DEMAND_CATEGORIES,
} from "../lib/market-demand/import-validation.js";
import { DEMAND_CATEGORIES_SEED_ROWS } from "../lib/market-demand/demand-categories-seed-data.js";
import {
  postPreviewDemandCenterImport,
  postImportDemandCenters,
} from "../api/market-demand.js";

let failed = 0;
function assert(cond, msg) {
  if (!cond) {
    console.error("FAIL:", msg);
    failed += 1;
  } else {
    console.log("ok:", msg);
  }
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

function testNormalizeName() {
  assert(normalizeDemandCenterName("  Sample Airport! ") === "sample airport", "normalize name");
}

function testDuplicateDetection() {
  const existing = { name: "Sample International Airport", category: "Transportation", sourcePlaceId: "place-1" };
  assert(
    isDuplicateCandidate({ name: "Sample International Airport", category: "Transportation" }, existing).duplicate,
    "exact normalized name duplicate"
  );
  assert(
    isDuplicateCandidate({ name: "Other", category: "X", sourcePlaceId: "place-1" }, existing).duplicate,
    "same place id duplicate"
  );
  assert(
    isDuplicateCandidate({ name: "International Airport Sample", category: "Transportation" }, existing).duplicate ||
      nameSimilarity("International Airport Sample", existing.name) >= 0.85,
    "similar name same category"
  );
}

function testInvalidCategory() {
  const v = validateImportItem({ name: "Test", category: "Not A Real Category" }, 0);
  assert(!v.valid, "invalid category rejected");
  assert(v.errors.some((e) => /category/i.test(e)), "category error message");
}

function testEmptyImportPayload() {
  const req = { params: { dealId: "recTESTdeal01" }, body: { demandCenters: [] } };
  const res = mockRes();
  return postImportDemandCenters(req, res).then(() => {
    assert(res.out.statusCode === 400, "empty import → 400");
    assert(res.out.body?.error === "validation_failed", "empty import error code");
  });
}

function testPreviewNoWrite() {
  const req = {
    params: { dealId: "rec6JMTqtSUn1ygtd" },
    body: {
      demandCenters: [
        {
          name: "Duplicate Test Airport Preview Only",
          category: "Transportation",
          sourcePlaceId: "preview-only-place",
        },
      ],
    },
  };
  const res = mockRes();
  return postPreviewDemandCenterImport(req, res).then(() => {
    assert(res.out.statusCode === 200, "preview status 200");
    assert(res.out.body?.preview === true, "preview flag true");
    assert(Array.isArray(res.out.body?.previewRows), "previewRows array");
  });
}

function testSelectedIndicesFilter() {
  const items = [{ name: "A" }, { name: "B" }, { name: "C" }];
  const filtered = filterSelectedImportItems(items, [0, 2]);
  assert(filtered.length === 2 && filtered[0].name === "A" && filtered[1].name === "C", "selected indices filter");
}

function testBuildPreviewRejected() {
  const preview = buildImportPreview([{ name: "", category: "Leisure" }], []);
  assert(preview.rejected.length === 1, "missing name rejected in preview");
  assert(preview.previewRows[0].importStatus === "rejected", "rejected status");
}

function testSeedDataCategories() {
  assert(DEMAND_CATEGORIES_SEED_ROWS.length === 10, "10 seed categories");
  const names = DEMAND_CATEGORIES_SEED_ROWS.map((r) => r.category);
  assert(names.includes("Other"), "Other in seed");
  assert(
    VALID_DEMAND_CATEGORIES.length >= DEMAND_CATEGORIES_SEED_ROWS.length,
    "valid import categories cover seed rows"
  );
}

async function main() {
  testNormalizeName();
  testDuplicateDetection();
  testInvalidCategory();
  testSelectedIndicesFilter();
  testBuildPreviewRejected();
  testSeedDataCategories();
  await testEmptyImportPayload();
  await testPreviewNoWrite();

  if (failed) {
    console.error("\n" + failed + " test(s) failed");
    process.exit(1);
  }
  console.log("\nAll market demand import tests passed.");
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
