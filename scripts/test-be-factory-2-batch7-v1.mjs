#!/usr/bin/env node
import { existsSync, readFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import {
  listBatch7,
  BATCH_7_IDS,
  CHOICE_PROOF,
  WYNDHAM_PROOF,
} from "../lib/brand-explorer/factory-2/be-factory-2-depth-research-packs-v1.js";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");

function assert(cond, msg) {
  if (!cond) {
    console.error("FAIL:", msg);
    process.exit(1);
  }
  console.log("OK:", msg);
}

assert(BATCH_7_IDS.length === 10, "BATCH_7_IDS length 10");
const packs = listBatch7();
assert(packs.length === 10, "listBatch7 returns 10");
assert(CHOICE_PROOF("Sleep Inn").length === 6, "CHOICE_PROOF six");
assert(WYNDHAM_PROOF("Super 8").length === 6, "WYNDHAM_PROOF six");

const registry = readFileSync(
  join(ROOT, "public/js/brand-explorer-staged-candidate-registry.js"),
  "utf8"
);
assert(!/\r,/.test(registry), "registry no CR");
assert(registry.includes("receQkxgjlezsc1xg"), "Curio preserved");
assert(registry.includes("garner-hotels"), "garner entry");
assert(registry.includes("travelodge-by-wyndham"), "travelodge entry");
const byIdBlock = registry.slice(registry.indexOf("function byId"), registry.indexOf("function bySlug"));
const resolveBlock = registry.slice(registry.indexOf("function resolveForBrand"));
assert(resolveBlock.includes("sleep inn"), "sleep inn resolver in resolveForBrand");
assert(!byIdBlock.includes("sleep inn"), "sleep inn resolver not in byId");

const identity = JSON.parse(
  readFileSync(join(ROOT, "reports/brand-explorer/factory-2-0/60-batch7-identity-confirm.json"), "utf8")
);
assert(identity.ok === true, "identity ok");
assert(identity.newBrandBasicsCreatesProposed === 0, "no Basics creates");

const review = JSON.parse(
  readFileSync(
    join(ROOT, "reports/brand-explorer/factory-2-0/61-batch7-consolidated-review.json"),
    "utf8"
  )
);
assert(review.summary.processed === 10, "processed 10");
assert(review.summary.blocked === 0, "none blocked");
assert(review.summary.researchSpendUsdTotal === 0, "spend 0");

for (const pack of packs) {
  const stage = JSON.parse(
    readFileSync(
      join(ROOT, "data/brand-explorer/factory-2-0/brands", pack.brandId, "stage.json"),
      "utf8"
    )
  );
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
  assert(/FDD|disclosure/i.test(pack.feeNote), `${pack.slug} FDD language`);
  if (pack.loyaltyProgram === "Choice Privileges") {
    assert(pack.sources.choicePrivileges, `${pack.slug} choicePrivileges source`);
  }
  if (pack.loyaltyProgram === "Wyndham Rewards") {
    assert(pack.sources.wyndhamRewards, `${pack.slug} wyndhamRewards source`);
  }
}

assert(
  existsSync(join(ROOT, "reports/brand-explorer/factory-2-0/61-batch7-consolidated-review.md")),
  "review md"
);
console.log("\nBatch 7 tests passed");
