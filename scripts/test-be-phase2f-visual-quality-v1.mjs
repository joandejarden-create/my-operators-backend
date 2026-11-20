#!/usr/bin/env node
/**
 * Phase 2F regression: founder screenshot failure classes.
 * Defective fixture must FAIL; repaired Curio/Ascend candidates must improve.
 */
import assert from "assert";
import { readFileSync, existsSync } from "fs";
import { join } from "path";
import { evaluateCandidateOverlay, VISUAL_FAIL } from "../lib/brand-explorer/be-phase2f-visual-quality-gates-v1.js";
import { repairCandidatePresentation } from "../lib/brand-explorer/be-phase2f-presentation-repair-v1.js";
import { titleLooksLikeParagraph, loadContentPresentationContract } from "../lib/brand-explorer/be-phase2f-content-presentation-contract-v1.js";

const root = process.cwd();

assert.ok(existsSync(join(root, "configs/brand-explorer-content-presentation-contract-v1.json")));
const contract = loadContentPresentationContract();
assert.equal(contract.version, "BE_CONTENT_PRESENTATION_CONTRACT_V1");

const defective = JSON.parse(
  readFileSync(join(root, "fixtures/brand-explorer-phase2f/defective-founder-failures.json"), "utf8"),
);
const bad = evaluateCandidateOverlay(defective, { brandName: "Fixture Defective Brand" });
assert.equal(bad.metrics.founderApprovalBlocked, true, "defective must block founder approval");
assert.ok(bad.failureRegister.some((f) => f.code === VISUAL_FAIL.PARAGRAPH_IN_TITLE), "paragraph in title");
assert.ok(bad.failureRegister.some((f) => f.code === VISUAL_FAIL.MISSING_REQUIRED_IMAGE), "missing image");
assert.ok(
  bad.failureRegister.some((f) => f.code === VISUAL_FAIL.NEAR_DUPLICATE || f.code === VISUAL_FAIL.GENERIC_REPEATED),
  "repeated economics / near-duplicate",
);
assert.ok(bad.failureRegister.some((f) => f.code === VISUAL_FAIL.LOGO_AS_GALLERY), "logo as gallery");
assert.ok(titleLooksLikeParagraph(defective.brandExplorer.blocks[0].title));

const repairedDefective = repairCandidatePresentation(defective, {
  brandName: "Fixture Defective Brand",
  parentCompany: "Parent Co",
  evidencePack: {
    evidence: [
      { id: "official.image.1", kind: "imagery", claim: "https://stories-editor.hilton.com/wp-content/uploads/2023/07/Curio-Collection-by-Hilton.webp" },
      { id: "official.image.2", kind: "imagery", claim: "https://stories-editor.hilton.com/wp-content/uploads/2022/11/The-Morrow-Hotel-Curio-Collection-by-Hilton_1.jpg" },
      { id: "official.image.3", kind: "imagery", claim: "https://stories-editor.hilton.com/wp-content/uploads/2026/09/Hale-Hokuala-Kauai-Curio-Collection-by-Hilton-Exterior-with-Pool-and-Ocean.jpg" },
    ],
  },
});
assert.ok(
  repairedDefective.scorecard.metrics.visualQualityPassRate > bad.metrics.visualQualityPassRate,
  "repair must improve visual pass rate on defective fixture",
);

const mustExist = [
  "public/js/brand-explorer-compare-studio.js",
  "lib/brand-explorer/be-phase2f-visual-quality-gates-v1.js",
  "lib/brand-explorer/be-phase2f-presentation-repair-v1.js",
  "docs/brand-explorer/BE_PHASE2F_VISUAL_QUALITY_RCA.md",
];
for (const rel of mustExist) assert.ok(existsSync(join(root, rel)), rel);

const compareJs = readFileSync(join(root, "public/js/brand-explorer-compare-studio.js"), "utf8");
assert.ok(compareJs.includes("phase2f-visual-repair"));
assert.ok(compareJs.includes("Section-locked") || compareJs.includes("viewMode"));
assert.ok(compareJs.includes("matchCollectionItems"));
assert.ok(compareJs.includes("Changes only") || compareJs.includes("renderChangesOnly"));

const curioPath = join(root, "public/brand-explorer-phase2e-preview/curio-collection-phase2f-candidate.json");
const ascendPath = join(root, "public/brand-explorer-phase2e-preview/ascend-phase2f-candidate.json");
if (existsSync(curioPath) && existsSync(ascendPath)) {
  const curio = JSON.parse(readFileSync(curioPath, "utf8"));
  const ascend = JSON.parse(readFileSync(ascendPath, "utf8"));
  const curioScore = evaluateCandidateOverlay(curio, { brandName: "Curio Collection by Hilton" });
  const ascendScore = evaluateCandidateOverlay(ascend, { brandName: "Ascend Hotel Collection" });
  assert.ok(curioScore.metrics.visualQualityPassRate > 0.5, "curio repaired should clear majority visual gates");
  assert.ok(ascendScore.metrics.visualQualityPassRate > 0.5, "ascend repaired should clear majority visual gates");
  const scen = (curio.brandExplorer.blocks || []).filter((b) => /^overview\.scenario\./.test(b.slotKey));
  assert.ok(scen.every((b) => b.imageUrl), "curio overview scenarios require images after repair");
  assert.ok(!titleLooksLikeParagraph(scen[0]?.title || ""), "scenario titles remain short");
}

console.log(
  JSON.stringify(
    {
      ok: true,
      defectiveFailures: bad.failureRegister.length,
      defectiveVisualPass: bad.metrics.visualQualityPassRate,
      repairedFixtureVisualPass: repairedDefective.scorecard.metrics.visualQualityPassRate,
      airtableWrites: 0,
      founderApproved: false,
    },
    null,
    2,
  ),
);
