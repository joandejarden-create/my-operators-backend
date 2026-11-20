#!/usr/bin/env node
/**
 * Factory 2.0 named batch five — pack + loyalty-six + registry smoke.
 */
import { readFileSync, existsSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { listNamedBatchFive } from "../lib/brand-explorer/factory-2/be-factory-2-depth-research-packs-v1.js";
import { buildDepthGapFillBlocks } from "../lib/brand-explorer/factory-2/be-factory-2-depth-candidate-builder-v1.js";
import { evaluateFactoryPermanentStandardsGate } from "../lib/brand-explorer/be-factory-permanent-loyalty-region-standards-v1.js";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");
let failed = 0;
function assert(c, m) {
  if (!c) {
    failed += 1;
    console.error("FAIL:", m);
  } else console.log("OK:", m);
}

const packs = listNamedBatchFive();
assert(packs.length === 5, "five depth packs registered");

const ids = new Set(packs.map((p) => p.brandId));
assert(ids.has("rec9aZp7GHtzUEg0c"), "AC Hotels id");
assert(ids.has("recahVIW4aCx0Ao84"), "Moxy id");
assert(ids.has("recDwzv86TWnz2gGB"), "Vignette id");
assert(ids.has("recsggfbKlJbjeRP9"), "Canopy id");
assert(ids.has("recvvmiyReHhiKdoK"), "EVEN id");

for (const pack of packs) {
  assert(pack.researchSpendUsd === 0, `${pack.slug} paid budget 0`);
  assert((pack.loyaltyProof || []).length === 6, `${pack.slug} has 6 loyalty proof cards`);
  const blocks = buildDepthGapFillBlocks(pack);
  const gate = evaluateFactoryPermanentStandardsGate({
    blocks,
    loyaltyParticipates: true,
    requireLoyaltyProof: true,
  });
  assert(gate.ok, `${pack.slug} permanent standards on overlay`);
  const frozen = join(
    ROOT,
    "public/brand-explorer-phase2e-preview",
    `${pack.slug}-factory2-frozen-approval-payload-v1.json`
  );
  assert(existsSync(frozen), `${pack.slug} frozen payload exists`);
}

const registry = readFileSync(
  join(ROOT, "public/js/brand-explorer-staged-candidate-registry.js"),
  "utf8"
);
assert(registry.includes("receQkxgjlezsc1xg"), "Curio preserved in registry");
assert(registry.includes("reclkgOzvAcBheUSo"), "Ascend preserved in registry");
assert(registry.includes("recEJCTDj1zrsjPM6"), "Autograph preserved in registry");
assert(registry.includes("FOUNDER_APPROVED_WRITEBACK"), "Curio/Ascend approval status preserved");
for (const pack of packs) {
  assert(registry.includes(pack.brandId), `registry has ${pack.slug}`);
}

const review = join(ROOT, "reports/brand-explorer/factory-2-0/13-named-batch-five-consolidated-review.json");
assert(existsSync(review), "consolidated review package exists");

if (failed) {
  console.error(`\n${failed} failed`);
  process.exit(1);
}
console.log("\nNamed batch five tests passed");
