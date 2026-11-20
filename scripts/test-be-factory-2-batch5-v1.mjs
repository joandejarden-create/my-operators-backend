#!/usr/bin/env node
import { existsSync, readFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import {
  listBatch5,
  BATCH_5_IDS,
} from "../lib/brand-explorer/factory-2/be-factory-2-depth-research-packs-v1.js";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");

function assert(cond, msg) {
  if (!cond) {
    console.error("FAIL:", msg);
    process.exit(1);
  }
  console.log("OK:", msg);
}

assert(BATCH_5_IDS.length === 10, "BATCH_5_IDS length 10");
const packs = listBatch5();
assert(packs.length === 10, "listBatch5 returns 10");

const registry = readFileSync(
  join(ROOT, "public/js/brand-explorer-staged-candidate-registry.js"),
  "utf8"
);
assert(!/\r,/.test(registry), "registry no CR");
assert(registry.includes("receQkxgjlezsc1xg"), "Curio/Ascend preserved");
assert(registry.includes("recEJCTDj1zrsjPM6"), "Autograph preserved");
assert(registry.includes("jw-marriott"), "jw-marriott resolver/entry");
assert(registry.includes("hyatt-place"), "hyatt-place resolver/entry");

const identity = JSON.parse(
  readFileSync(join(ROOT, "reports/brand-explorer/factory-2-0/40-batch5-identity-confirm.json"), "utf8")
);
assert(identity.ok === true, "identity confirm ok");
assert(identity.newBrandBasicsCreates === 0, "no new Brand Basics creates");
assert(
  identity.rows.every((r) => r.classification === "EXISTING_CANONICAL"),
  "all EXISTING_CANONICAL"
);

const review = JSON.parse(
  readFileSync(
    join(ROOT, "reports/brand-explorer/factory-2-0/41-batch5-consolidated-review.json"),
    "utf8"
  )
);
assert(review.summary.processed === 10, "processed 10");
assert(review.summary.blocked === 0, "none blocked");
assert(review.summary.researchSpendUsdTotal === 0, "spend 0");

for (const pack of packs) {
  const stagePath = join(
    ROOT,
    "data/brand-explorer/factory-2-0/brands",
    pack.brandId,
    "stage.json"
  );
  assert(existsSync(stagePath), `${pack.slug} stage`);
  const stage = JSON.parse(readFileSync(stagePath, "utf8"));
  assert(stage.researchSpend?.totalUsd === 0, `${pack.slug} $0`);
  assert(
    (stage.candidate?.brandExplorer?.blocks || []).filter((b) => b.slotKey === "loyalty.proof")
      .length === 6 ||
      (stage.certification?.permanentStandards?.loyalty?.ok === true),
    `${pack.slug} six proofs / standards`
  );
  assert(stage.certification?.permanentStandards?.ok === true, `${pack.slug} standards`);
  assert(
    existsSync(
      join(ROOT, "public/brand-explorer-phase2e-preview", `${pack.slug}-factory2-frozen-approval-payload-v1.json`)
    ),
    `${pack.slug} frozen`
  );
  assert(registry.includes(pack.brandId), `registry ${pack.slug}`);
  // Hyatt Place must not use luxury fee geography copy
  if (pack.slug === "hyatt-place") {
    assert(/select-service/i.test(pack.feeGeography), "hyatt-place select-service fee note");
    assert(!/luxury fee exhibits/i.test(pack.feeNote) || /Do not apply luxury/i.test(pack.feeNote), "hyatt-place no luxury fee import");
  }
}

assert(
  existsSync(join(ROOT, "reports/brand-explorer/factory-2-0/41-batch5-consolidated-review.md")),
  "consolidated review md"
);
assert(
  existsSync(join(ROOT, "reports/brand-explorer/factory-2-0/42-batch5-canonical-creation-plan.md")),
  "creation plan"
);

console.log("\nBatch 5 tests passed");
