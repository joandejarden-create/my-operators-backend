#!/usr/bin/env node
/**
 * Gate: empty-CURRENT Factory depth candidates must reach Autograph slot-parity bar.
 */
import assert from "assert";
import { getDepthPack } from "../lib/brand-explorer/factory-2/be-factory-2-depth-research-packs-v1.js";
import { buildDepthGapFillBlocks } from "../lib/brand-explorer/factory-2/be-factory-2-depth-candidate-builder-v1.js";
import {
  measureEmptyBrandCompletion,
  EMPTY_BRAND_REQUIRED_SLOTS,
  DIFFERENTIATOR_MIN_BULLETS,
  countPresentationBullets,
  buildCurioBarDifferentiatorBodies,
} from "../lib/brand-explorer/factory-2/be-factory-2-empty-brand-completion-slots-v1.js";

const SIX_SENSES = "rec4ckn09CLLmppCe";
const JW = "recj5BCSpWqMEEBDg";

function checkBrand(brandId, label) {
  const pack = getDepthPack(brandId);
  assert.ok(pack, `${label}: pack missing`);
  const blocks = buildDepthGapFillBlocks(pack);
  const m = measureEmptyBrandCompletion(blocks);
  assert.ok(
    m.completionPct >= 85,
    `${label}: completion ${m.completionPct}% < 85%. Missing: ${m.missing.slice(0, 15).join(", ")}`
  );
  const keys = new Set(blocks.map((b) => b.slotKey));
  for (const must of [
    "operations.model.primary_model",
    "operations.flexibility.design",
    "valueOwners.lifecycle.1",
    "valueOwners.scenario.1",
    "footprint.region.am",
    "economics.opening.step.1",
    "overview.proof.1",
    "insight.summary",
  ]) {
    assert.ok(keys.has(must), `${label}: missing ${must}`);
  }
  // No invented fee percentages
  const feeBodies = blocks
    .filter((b) => /^economics\.fee/.test(b.slotKey || ""))
    .map((b) => `${b.title}\n${b.body}`)
    .join("\n");
  assert.ok(
    /confirm|disclosure|exhibit|not staged|not invent/i.test(feeBodies),
    `${label}: fee language must stay disclosure-gated`
  );
  assert.ok(
    !/\b\d{1,2}(\.\d+)?\s*%\s*(royalty|franchise|fee)/i.test(feeBodies),
    `${label}: must not invent royalty/franchise percentages`
  );
  const idBody = blocks.find((b) => b.slotKey === "overview.differentiators.identity")?.body || "";
  const comBody =
    blocks.find((b) => b.slotKey === "overview.differentiators.commercial")?.body || "";
  assert.ok(
    countPresentationBullets(idBody) >= DIFFERENTIATOR_MIN_BULLETS,
    `${label}: identity differentiators need ${DIFFERENTIATOR_MIN_BULLETS} bullets (got ${countPresentationBullets(idBody)})`
  );
  assert.ok(
    countPresentationBullets(comBody) >= DIFFERENTIATOR_MIN_BULLETS,
    `${label}: commercial differentiators need ${DIFFERENTIATOR_MIN_BULLETS} bullets (got ${countPresentationBullets(comBody)})`
  );
  assert.ok(m.differentiatorBarPass, `${label}: differentiator bar must pass`);
  console.log(
    `OK ${label}: blocks=${blocks.length} completion=${m.completionPct}% gallery=${m.galleryWithImage}/6 required=${EMPTY_BRAND_REQUIRED_SLOTS.length} diff=${countPresentationBullets(idBody)}/${countPresentationBullets(comBody)}`
  );
  return m;
}

const renaissanceBodies = buildCurioBarDifferentiatorBodies(
  getDepthPack("recmj7cNIUbYPKVJl")
);
assert.ok(
  countPresentationBullets(renaissanceBodies.identity) === DIFFERENTIATOR_MIN_BULLETS,
  "Renaissance identity helper emits 4"
);
assert.ok(
  countPresentationBullets(renaissanceBodies.commercial) === DIFFERENTIATOR_MIN_BULLETS,
  "Renaissance commercial helper emits 4"
);

checkBrand(SIX_SENSES, "Six Senses");
checkBrand(JW, "JW Marriott");
checkBrand("recmj7cNIUbYPKVJl", "Renaissance");
console.log("test-be-factory-2-empty-brand-completion-v1: PASS");
