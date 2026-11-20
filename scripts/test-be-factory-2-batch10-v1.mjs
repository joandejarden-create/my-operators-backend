#!/usr/bin/env node
import { existsSync, readFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import {
  listBatch10,
  BATCH_10_IDS,
  ACCOR_PROOF,
  CHOICE_PROOF,
  MEMBERSHIP_NETWORK_PROOF,
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

assert(BATCH_10_IDS.length === 20, "BATCH_10_IDS length 20");
assert(BATCH_10_IDS.includes("recwl5JOYxlChuCAr"), "Preferred ID included");
assert(BATCH_10_IDS.includes("recjjSnY2opb8P4DG"), "SLH ID included");
const packs = listBatch10();
assert(packs.length === 20, "listBatch10 returns 20");
assert(ACCOR_PROOF("Pullman").length === 6, "ACCOR_PROOF six");
assert(CHOICE_PROOF("Quality Inn").length === 6, "CHOICE_PROOF six");
assert(MEMBERSHIP_NETWORK_PROOF("Preferred").length === 6, "MEMBERSHIP_NETWORK_PROOF six");

const demandSignatures = new Set();
for (const p of packs) {
  assert(p.loyaltyProof?.length === 6, `${p.slug} loyalty six`);
  assert(p.demandScenarios?.length === DEMAND_SCENARIO_REQUIRED_COUNT, `${p.slug} demand six`);
  assert(/FDD|disclosure|membership/i.test(p.feeNote), `${p.slug} disclosure language`);
  assert(!/\b\d{1,2}(\.\d+)?\s*%\s*(royalty|franchise)/i.test(p.feeNote), `${p.slug} no invented %`);
  demandSignatures.add(p.demandScenarios.map((d) => `${d.title}|${d.fit}`).join("||"));
  const blocks = buildDepthGapFillBlocks(p);
  const m = measureEmptyBrandCompletion(blocks);
  assert(m.completionPct >= 85, `${p.slug} completion ${m.completionPct}%`);
}
assert(demandSignatures.size >= 10, `brand-specific demand sets (${demandSignatures.size})`);

const preferred = packs.find((p) => p.slug === "preferred-hotels-and-resorts");
const slh = packs.find((p) => p.slug === "small-luxury-hotels-of-the-world");
assert(/membership/i.test(preferred.feeNote), "Preferred membership fee language");
assert(/membership/i.test(slh.feeNote), "SLH membership fee language");
assert(!/franchise FDD royalty table/i.test(preferred.feeNote) || /not a traditional franchise/i.test(preferred.feeNote), "Preferred not franchise table");

const identity = JSON.parse(
  readFileSync(join(ROOT, "reports/brand-explorer/factory-2-0/100-batch10-identity-confirm.json"), "utf8")
);
assert(identity.ok === true, "identity ok");
assert(identity.newBrandBasicsCreatesProposed === 0, "no Basics creates");
assert(identity.rows.length === 20, "identity 20 rows");
assert((identity.substitutions || []).length === 0, "no substitutions");

const reviewPath = join(ROOT, "reports/brand-explorer/factory-2-0/101-batch10-consolidated-review.json");
assert(existsSync(reviewPath), "consolidated review exists");
const review = JSON.parse(readFileSync(reviewPath, "utf8"));
assert(review.summary.processed === 20, "processed 20");
assert(review.summary.blocked === 0, "none blocked");
assert(review.summary.researchSpendUsdTotal === 0, "spend 0");
assert(existsSync(join(ROOT, "reports/brand-explorer/factory-2-0/101-batch10-founder-dashboard.md")), "founder dashboard");

const registry = readFileSync(join(ROOT, "public/js/brand-explorer-staged-candidate-registry.js"), "utf8");
assert(registry.includes("handwritten-collection"), "handwritten registry");
assert(registry.includes("preferred-hotels-and-resorts"), "preferred registry");
assert(registry.includes("small-luxury-hotels-of-the-world"), "slh registry");

for (const pack of packs) {
  assert(
    existsSync(
      join(ROOT, "public/brand-explorer-phase2e-preview", `${pack.slug}-factory2-frozen-approval-payload-v1.json`)
    ),
    `frozen payload ${pack.slug}`
  );
}

console.log("test-be-factory-2-batch10-v1: PASS");
