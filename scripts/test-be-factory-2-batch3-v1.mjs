#!/usr/bin/env node
import { readFileSync, existsSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import {
  listBatch3,
  BATCH_3_IDS,
} from "../lib/brand-explorer/factory-2/be-factory-2-depth-research-packs-v1.js";
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

assert(BATCH_3_IDS.length === 5, "BATCH_3_IDS length 5");
const packs = listBatch3();
assert(packs.length === 5, "listBatch3 returns 5");

const registry = readFileSync(
  join(ROOT, "public/js/brand-explorer-staged-candidate-registry.js"),
  "utf8"
);
assert(registry.includes("function byId(id)"), "byId intact");
assert(!/if \(!want\) if \(\/\^voco/.test(registry), "byId not corrupted");
assert(registry.includes("FOUNDER_APPROVED_WRITEBACK"), "Curio/Ascend preserved");
assert(registry.includes("recEJCTDj1zrsjPM6"), "Autograph preserved");

for (const pack of packs) {
  assert(pack.researchSpendUsd === 0, `${pack.slug} $0`);
  assert((pack.loyaltyProof || []).length === 6, `${pack.slug} six proofs`);
  const gate = evaluateFactoryPermanentStandardsGate({
    blocks: buildDepthGapFillBlocks(pack),
    loyaltyParticipates: true,
    requireLoyaltyProof: true,
  });
  assert(gate.ok, `${pack.slug} standards`);
  assert(
    existsSync(
      join(ROOT, "public/brand-explorer-phase2e-preview", `${pack.slug}-factory2-frozen-approval-payload-v1.json`)
    ),
    `${pack.slug} frozen payload`
  );
  assert(registry.includes(pack.brandId), `registry ${pack.slug}`);
}

assert(
  existsSync(join(ROOT, "reports/brand-explorer/factory-2-0/21-batch3-consolidated-review.json")),
  "consolidated review"
);
assert(
  existsSync(join(ROOT, "reports/brand-explorer/factory-2-0/20-batch3-identity-confirm.json")),
  "identity confirm"
);

if (failed) {
  console.error(`\n${failed} failed`);
  process.exit(1);
}
console.log("\nBatch 3 tests passed");
