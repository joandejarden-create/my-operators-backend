#!/usr/bin/env node
import { existsSync, readFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import {
  listBatch6,
  BATCH_6_IDS,
} from "../lib/brand-explorer/factory-2/be-factory-2-depth-research-packs-v1.js";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");

function assert(cond, msg) {
  if (!cond) {
    console.error("FAIL:", msg);
    process.exit(1);
  }
  console.log("OK:", msg);
}

assert(BATCH_6_IDS.length === 10, "BATCH_6_IDS length 10");
const packs = listBatch6();
assert(packs.length === 10, "listBatch6 returns 10");

const registry = readFileSync(
  join(ROOT, "public/js/brand-explorer-staged-candidate-registry.js"),
  "utf8"
);
assert(!/\r,/.test(registry), "registry no CR");
assert(registry.includes("receQkxgjlezsc1xg"), "Curio preserved");
assert(registry.includes("recEJCTDj1zrsjPM6"), "Autograph preserved");
assert(registry.includes("ritz-carlton"), "ritz-carlton entry");
assert(registry.includes("six-senses"), "six-senses entry");
const byIdBlock = registry.slice(registry.indexOf("function byId"), registry.indexOf("function bySlug"));
const resolveBlock = registry.slice(
  registry.indexOf("function resolveForBrand"),
  registry.indexOf("global.DealalityBrandExplorerStagedCandidates")
);
assert(resolveBlock.includes("ritz[- ]?carlton"), "ritz resolver in resolveForBrand");
assert(!byIdBlock.includes("ritz[- ]?carlton"), "ritz resolver not in byId");

const identity = JSON.parse(
  readFileSync(join(ROOT, "reports/brand-explorer/factory-2-0/50-batch6-identity-confirm.json"), "utf8")
);
assert(identity.ok === true, "identity confirm ok");
assert(identity.newBrandBasicsCreatesProposed === 0, "no new Brand Basics creates");
assert(
  identity.rows.every((r) => r.classification === "EXISTING_CANONICAL"),
  "all EXISTING_CANONICAL"
);

const review = JSON.parse(
  readFileSync(
    join(ROOT, "reports/brand-explorer/factory-2-0/51-batch6-consolidated-review.json"),
    "utf8"
  )
);
assert(review.summary.processed === 10, "processed 10");
assert(review.summary.blocked === 0, "none blocked");
assert(review.summary.researchSpendUsdTotal === 0, "spend 0");

for (const pack of packs) {
  const stagePath = join(ROOT, "data/brand-explorer/factory-2-0/brands", pack.brandId, "stage.json");
  assert(existsSync(stagePath), `${pack.slug} stage`);
  const stage = JSON.parse(readFileSync(stagePath, "utf8"));
  assert(stage.researchSpend?.totalUsd === 0, `${pack.slug} $0`);
  assert(stage.certification?.permanentStandards?.ok === true, `${pack.slug} standards`);
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
  if (pack.slug === "hyatt-house") {
    assert(/select-service|extended-stay/i.test(pack.feeGeography), "hyatt-house select-service fee note");
  }
  if (["ritz-carlton", "st-regis", "edition-hotels", "six-senses"].includes(pack.slug)) {
    assert(/do not invent franchise/i.test(pack.feeNote), `${pack.slug} no invented franchise fees`);
  }
}

assert(
  existsSync(join(ROOT, "reports/brand-explorer/factory-2-0/51-batch6-consolidated-review.md")),
  "consolidated review md"
);
assert(
  existsSync(join(ROOT, "reports/brand-explorer/factory-2-0/52-batch6-canonical-creation-plan.md")),
  "creation plan"
);

console.log("\nBatch 6 tests passed");
