#!/usr/bin/env node
/**
 * Smoke tests for Brand Explorer Comparison Studio assets (no Airtable writes).
 */
import { readFileSync, existsSync } from "fs";
import { join } from "path";
import assert from "assert";

const root = process.cwd();
const mustExist = [
  "public/brand-explorer-compare.html",
  "public/js/brand-explorer-compare-studio.js",
  "public/css/brand-explorer-compare-studio.css",
  "public/brand-explorer-phase2e-preview/curio-collection-quality-candidate.json",
  "public/brand-explorer-phase2e-preview/ascend-quality-candidate.json",
  "public/brand-explorer-phase2e-preview/curio-collection-quality-scorecard.json",
  "public/brand-explorer-phase2e-preview/ascend-quality-scorecard.json",
  "api/brand-explorer-compare-review.js",
];

for (const rel of mustExist) {
  assert.ok(existsSync(join(root, rel)), `missing ${rel}`);
}

const html = readFileSync(join(root, "public/brand-explorer-compare.html"), "utf8");
assert.ok(html.includes("brand-explorer-compare-studio.js"));
assert.ok(html.includes("becsPairedDoc"), "Phase 2G paired-subsection document root required");

const appJs = readFileSync(join(root, "public/app.js"), "utf8");
assert.ok(appJs.includes("Brand Explorer Comparison"));
assert.ok(appJs.includes("/brand-explorer-compare"));

const compareJs = readFileSync(join(root, "public/js/brand-explorer-compare-studio.js"), "utf8");
assert.ok(compareJs.includes("becs-pair-cols"));
assert.ok(compareJs.includes("matchItems"));

const server = readFileSync(join(root, "server.js"), "utf8");
assert.ok(server.includes("postBrandExplorerCompareReview"));
assert.ok(server.includes("/api/brand-explorer-compare/review"));

const curio = JSON.parse(
  readFileSync(join(root, "public/brand-explorer-phase2e-preview/curio-collection-quality-candidate.json"), "utf8"),
);
assert.equal(curio.brandBasicsRecordId, "receQkxgjlezsc1xg");
assert.ok(curio.brandExplorer?.blocks?.length > 50);

const ascend = JSON.parse(
  readFileSync(join(root, "public/brand-explorer-phase2e-preview/ascend-quality-candidate.json"), "utf8"),
);
assert.equal(ascend.brandBasicsRecordId, "reclkgOzvAcBheUSo");
assert.ok(ascend.brandExplorer?.blocks?.length > 50);

console.log(
  JSON.stringify(
    {
      ok: true,
      curioBlocks: curio.brandExplorer.blocks.length,
      ascendBlocks: ascend.brandExplorer.blocks.length,
      airtableWrites: 0,
    },
    null,
    2,
  ),
);
