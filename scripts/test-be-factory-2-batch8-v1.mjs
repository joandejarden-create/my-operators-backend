#!/usr/bin/env node
import { existsSync, readFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import {
  listBatch8,
  BATCH_8_IDS,
  BWH_PROOF,
  SONESTA_PROOF,
  WYNDHAM_PROOF,
} from "../lib/brand-explorer/factory-2/be-factory-2-depth-research-packs-v1.js";
import { buildDepthGapFillBlocks } from "../lib/brand-explorer/factory-2/be-factory-2-depth-candidate-builder-v1.js";
import { measureEmptyBrandCompletion } from "../lib/brand-explorer/factory-2/be-factory-2-empty-brand-completion-slots-v1.js";
import { DEMAND_SCENARIO_REQUIRED_COUNT } from "../lib/brand-explorer/be-factory-permanent-demand-scenarios-six-v1.js";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");

function assert(cond, msg) {
  if (!cond) {
    console.error("FAIL:", msg);
    process.exit(1);
  }
  console.log("OK:", msg);
}

assert(BATCH_8_IDS.length === 20, "BATCH_8_IDS length 20");
const packs = listBatch8();
assert(packs.length === 20, "listBatch8 returns 20");
assert(BWH_PROOF("Best Western").length === 6, "BWH_PROOF six");
assert(SONESTA_PROOF("Sonesta Select").length === 6, "SONESTA_PROOF six");
assert(WYNDHAM_PROOF("La Quinta").length === 6, "WYNDHAM_PROOF six");

const demandSignatures = new Set();
for (const p of packs) {
  assert(p.loyaltyProof?.length === 6, `${p.slug} loyalty six`);
  assert(p.demandScenarios?.length === DEMAND_SCENARIO_REQUIRED_COUNT, `${p.slug} demand six`);
  assert(/FDD|disclosure/i.test(p.feeNote), `${p.slug} FDD language`);
  assert(!/\b\d{1,2}(\.\d+)?\s*%\s*(royalty|franchise)/i.test(p.feeNote), `${p.slug} no invented %`);
  demandSignatures.add(p.demandScenarios.map((d) => `${d.title}|${d.fit}`).join("||"));
  const blocks = buildDepthGapFillBlocks(p);
  const m = measureEmptyBrandCompletion(blocks);
  assert(m.completionPct >= 85, `${p.slug} completion ${m.completionPct}%`);
  const demandBlocks = blocks.filter((b) => b.slotKey === "commercial.demand");
  assert(demandBlocks.length === 6, `${p.slug} demand blocks 6`);
}
assert(demandSignatures.size >= 10, `brand-specific demand sets (${demandSignatures.size})`);

const identity = JSON.parse(
  readFileSync(join(ROOT, "reports/brand-explorer/factory-2-0/80-batch8-identity-confirm.json"), "utf8")
);
assert(identity.ok === true, "identity ok");
assert(identity.newBrandBasicsCreatesProposed === 0, "no Basics creates");
assert(identity.rows.length === 20, "identity 20 rows");

const reviewPath = join(ROOT, "reports/brand-explorer/factory-2-0/81-batch8-consolidated-review.json");
assert(existsSync(reviewPath), "consolidated review exists");
const review = JSON.parse(readFileSync(reviewPath, "utf8"));
assert(review.summary.processed === 20, "processed 20");
assert(review.summary.blocked === 0, "none blocked");
assert(review.summary.researchSpendUsdTotal === 0, "spend 0");

const registry = readFileSync(
  join(ROOT, "public/js/brand-explorer-staged-candidate-registry.js"),
  "utf8"
);
assert(registry.includes("receQkxgjlezsc1xg"), "Curio preserved");
assert(registry.includes("la-quinta-by-wyndham"), "la quinta entry");
assert(registry.includes("americas-best-value-inn"), "abvi entry");
assert(registry.includes("aiden-by-best-western"), "aiden entry");

for (const pack of packs) {
  assert(
    existsSync(
      join(
        ROOT,
        "public/brand-explorer-phase2e-preview",
        `${pack.slug}-factory2-frozen-approval-payload-v1.json`
      )
    ),
    `${pack.slug} frozen`
  );
  assert(registry.includes(pack.brandId), `registry ${pack.slug}`);
}

assert(existsSync(join(ROOT, "reports/brand-explorer/factory-2-0/81-batch8-founder-dashboard.md")), "dashboard md");
console.log("test-be-factory-2-batch8-v1: PASS");
