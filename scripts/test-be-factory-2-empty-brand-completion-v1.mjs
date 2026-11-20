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
  console.log(
    `OK ${label}: blocks=${blocks.length} completion=${m.completionPct}% gallery=${m.galleryWithImage}/6 required=${EMPTY_BRAND_REQUIRED_SLOTS.length}`
  );
  return m;
}

checkBrand(SIX_SENSES, "Six Senses");
checkBrand(JW, "JW Marriott");
console.log("test-be-factory-2-empty-brand-completion-v1: PASS");
