#!/usr/bin/env node
/**
 * Phase 2H gates — depth composition, footprint SoT, founder regressions, renderer exports present in source.
 */
import assert from "assert";
import { readFileSync, existsSync } from "fs";
import { join } from "path";
import {
  composeWhyValue,
  composeFootprint,
  composeOperatingModel,
  composeLoyalty,
  enrichCandidateWithDepth,
  loadEvidencePack,
} from "../lib/brand-explorer/be-phase2h-depth-composition-v1.js";
import {
  evaluateFounderRegressionCases,
  evaluateSubstantiveParity,
} from "../lib/brand-explorer/be-phase2h-substantive-parity-gates-v1.js";
import { aggregateOpenPipelineCounts, mapCountryToFootprintRegion } from "../lib/brand-explorer/be-airtable-integration-sot-v1.js";

const root = process.cwd();
let failed = 0;
function check(name, fn) {
  try {
    fn();
    console.log("PASS", name);
  } catch (e) {
    failed += 1;
    console.error("FAIL", name, e.message || e);
  }
}

const pack = loadEvidencePack("reports/brand-explorer/phase2e-quality-curio/01-evidence-pack.json");
assert.ok(pack, "curio evidence pack required");

check("why_value_has_multiple_distinct_claims", () => {
  const why = composeWhyValue(pack, { brandName: "Curio Collection by Hilton", parentCompany: "Hilton Worldwide" });
  assert.ok(why.insightCount >= 4, `expected >=4 insights, got ${why.insightCount}`);
  assert.ok(!/Focus this card|Evidence-grounded/i.test(why.body));
});

check("operating_model_leaves_are_distinct", () => {
  const ops = composeOperatingModel(pack, { brandName: "Curio Collection by Hilton", parentCompany: "Hilton Worldwide" });
  const bodies = Object.values(ops.leaves).map((l) => l.body);
  const uniq = new Set(bodies);
  assert.strictEqual(uniq.size, bodies.length, "ops leaves must not share identical bodies");
});

check("loyalty_owner_lenses_distinct", () => {
  const loy = composeLoyalty(pack, { brandName: "Curio Collection by Hilton", parentCompany: "Hilton Worldwide" });
  const pnl = loy.blocks.find((b) => b.slotKey === "loyalty.implications.pnl");
  const ops = loy.blocks.find((b) => b.slotKey === "loyalty.implications.ops");
  assert.ok(pnl && ops);
  assert.notStrictEqual(pnl.body, ops.body);
});

check("footprint_regions_do_not_repeat_brand_hpc_totals", () => {
  const fp = composeFootprint(pack, { brandName: "Curio Collection by Hilton" });
  for (const b of fp.blocks.filter((x) => /^footprint\.region\./.test(x.slotKey))) {
    assert.ok(
      !/HPC Open hotels matched=\d+/i.test(b.body) || /Regional filter applied|Coverage limited|Directional presence/i.test(b.body),
      `${b.slotKey} must not stamp unqualified brand-wide HPC`,
    );
  }
});

check("hpc_aggregate_exposes_byRegion", () => {
  const agg = aggregateOpenPipelineCounts([
    { radarStatus: "Open", Country: "United States", roomsKeys: 100 },
    { radarStatus: "Open", Country: "Colombia", roomsKeys: 80 },
    { radarStatus: "Pipeline", Country: "Spain", roomsKeys: 120 },
  ]);
  assert.ok(agg.byRegion);
  assert.strictEqual(mapCountryToFootprintRegion("Colombia"), "cala");
  assert.strictEqual(agg.byRegion.am.openHotels, 1);
  assert.strictEqual(agg.byRegion.cala.openHotels, 1);
  assert.strictEqual(agg.byRegion.eu.pipelineHotels, 1);
});

check("enrich_and_regression_suite", () => {
  const base = JSON.parse(
    readFileSync(join(root, "public/brand-explorer-phase2e-preview/curio-collection-phase2g-candidate.json"), "utf8"),
  );
  const baselineSnap = JSON.parse(
    readFileSync(join(root, "public/brand-explorer-phase2e-preview/curio-frozen-baseline-snapshot.json"), "utf8"),
  );
  const baseline = (baselineSnap.presentation?.blocks || []).map((r, i) => ({
    slotKey: r.slotKey,
    title: r.title,
    body: r.body,
    imageUrl: r.imageUrl || null,
    sort: i + 1,
  }));
  const { enriched } = enrichCandidateWithDepth(base, pack, {
    brandName: "Curio Collection by Hilton",
    parentCompany: "Hilton Worldwide",
  });
  const regressions = evaluateFounderRegressionCases(baseline, enriched.brandExplorer.blocks);
  const regional = regressions.find((r) => r.id === "regional_hpc_repeated");
  assert.ok(regional && regional.failed === false, "regional HPC stamp regression must not fail after Phase 2H");
  const opsEmpty = regressions.find((r) => r.id === "operating_model_emptied");
  assert.ok(opsEmpty && opsEmpty.failed === false, "operating model must not be emptied");
  const parity = evaluateSubstantiveParity({
    baselineBlocks: baseline,
    candidateBlocks: enriched.brandExplorer.blocks,
  });
  assert.ok(parity.metrics.priorityScored >= 5);
});

check("atelier_exports_present_in_source", () => {
  const src = readFileSync(join(root, "public/js/brand-explorer-atelier-from-api.js"), "utf8");
  assert.ok(src.includes("renderTabInnerHtml"));
  assert.ok(src.includes("extractSubsectionHtml"));
  assert.ok(src.includes("brandShellFromBlocks"));
});

check("compare_studio_loads_atelier", () => {
  const html = readFileSync(join(root, "public/brand-explorer-compare.html"), "utf8");
  assert.ok(html.includes("brand-explorer-atelier-from-api.js"));
  const js = readFileSync(join(root, "public/js/brand-explorer-compare-studio.js"), "utf8");
  assert.ok(js.includes("renderProductColumn"));
  assert.ok(js.includes("phase2h-candidate"));
});

check("phase2h_candidates_written_when_repair_run", () => {
  // Soft check: if candidates exist, validate shape; repair script creates them.
  const p = join(root, "public/brand-explorer-phase2e-preview/curio-collection-phase2h-candidate.json");
  if (!existsSync(p)) {
    console.log("SKIP phase2h candidate on disk (run be-phase2h-component-parity-repair-v1)");
    return;
  }
  const c = JSON.parse(readFileSync(p, "utf8"));
  assert.strictEqual(c.candidateVersion, "phase2h-component-parity");
  const why = (c.brandExplorer?.blocks || []).find((b) => b.slotKey === "overview.why_value");
  assert.ok(why && why.body.split(/\n+/).filter(Boolean).length >= 3);
});

if (failed) {
  console.error(`\n${failed} Phase 2H test(s) failed`);
  process.exit(1);
}
console.log("\nAll Phase 2H checks passed");
