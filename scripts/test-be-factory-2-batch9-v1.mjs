#!/usr/bin/env node
import { existsSync, readFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import {
  listBatch9,
  BATCH_9_IDS,
  BONVOY_PROOF,
  HONORS_PROOF,
  HYATT_PROOF,
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

assert(BATCH_9_IDS.length === 20, "BATCH_9_IDS length 20");
assert(BATCH_9_IDS.includes("rec50qkCj6yt9fMPg"), "Delta Hotels ID included");
const packs = listBatch9();
assert(packs.length === 20, "listBatch9 returns 20");
assert(BONVOY_PROOF("Fairfield").length === 6, "BONVOY_PROOF six");
assert(HONORS_PROOF("Hampton").length === 6, "HONORS_PROOF six");
assert(HYATT_PROOF("Hyatt Regency").length === 6, "HYATT_PROOF six");

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
  readFileSync(join(ROOT, "reports/brand-explorer/factory-2-0/90-batch9-identity-confirm.json"), "utf8")
);
assert(identity.ok === true, "identity ok");
assert(identity.newBrandBasicsCreatesProposed === 0, "no Basics creates");
assert(identity.rows.length === 20, "identity 20 rows");
assert(identity.deltaHotelsResolvedId === "rec50qkCj6yt9fMPg", "Delta resolved");

const reviewPath = join(ROOT, "reports/brand-explorer/factory-2-0/91-batch9-consolidated-review.json");
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
assert(registry.includes("delta-hotels-by-marriott"), "delta entry");
assert(registry.includes("hampton-by-hilton"), "hampton entry");
assert(registry.includes("city-express-by-marriott"), "city express entry");

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
}

console.log("PASS: Batch 9 Factory 2.0 gates");
