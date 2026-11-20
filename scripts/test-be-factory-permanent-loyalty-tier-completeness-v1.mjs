#!/usr/bin/env node
/**
 * Permanent Elite Tier completeness — positive/negative fixtures.
 * Distinct from loyalty.proof exact-six (BE-QL-LOYALTY-SIX-001).
 */
import assert from "assert";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  BE_FACTORY_PERMANENT_LOYALTY_TIER_COMPLETENESS_V1,
  LOYALTY_TIER_FAIL,
  buildLoyaltyEliteBlocksFromProgram,
  decideLoyaltyEliteCandidateApply,
  getCanonicalLoyaltyProgram,
  resolveLoyaltyProgramId,
  validateLoyaltyEliteTier,
} from "../lib/brand-explorer/be-factory-permanent-loyalty-tier-completeness-v1.js";
import { assembleCumulativeProposal } from "../lib/brand-explorer/be-cumulative-proposal-assembler-v1.js";
import { evaluateFactoryPermanentStandardsGate } from "../lib/brand-explorer/be-factory-permanent-loyalty-region-standards-v1.js";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const fixDir = path.join(root, "fixtures/brand-explorer-factory-permanent-standards");

assert.equal(
  BE_FACTORY_PERMANENT_LOYALTY_TIER_COMPLETENESS_V1,
  "BE_FACTORY_PERMANENT_LOYALTY_TIER_COMPLETENESS_V1"
);

assert.equal(resolveLoyaltyProgramId({ loyaltyProgram: "Marriott Bonvoy" }), "marriott_bonvoy");
assert.equal(resolveLoyaltyProgramId({ loyaltyProgram: "my Barceló" }), "my_barcelo");
assert.equal(
  resolveLoyaltyProgramId({
    loyaltyProgram: "Preferred Hotels & Resorts membership network (not a franchise loyalty program)",
  }),
  "non_tiered_network"
);
assert.ok(getCanonicalLoyaltyProgram("marriott_bonvoy").tiers.length >= 6);
assert.equal(buildLoyaltyEliteBlocksFromProgram("marriott_bonvoy").length, 6);
assert.equal(buildLoyaltyEliteBlocksFromProgram("hilton_honors").length, 5);
assert.equal(buildLoyaltyEliteBlocksFromProgram("my_barcelo").length, 3);

function load(name) {
  return JSON.parse(fs.readFileSync(path.join(fixDir, name), "utf8"));
}

// Negative: collapse four → one generic
{
  const fix = load("failing-loyalty-elite-collapsed-four-to-one.json");
  const d = decideLoyaltyEliteCandidateApply(fix.currentBlocks, fix.candidateBlocks, {
    programId: fix.programId,
  });
  assert.equal(d.applyCandidate, false, "collapsed elite must not apply");
  assert.equal(d.reason, fix.expectReason);
  assert.ok(d.flags.includes(LOYALTY_TIER_FAIL.COLLAPSED) || d.flags.includes(LOYALTY_TIER_FAIL.REGRESSION));
  const assembled = assembleCumulativeProposal(fix.currentBlocks, fix.candidateBlocks);
  const elite = assembled.blocks.filter((b) => b.slotKey === "loyalty.elite");
  assert.equal(elite.length, 4, "CURRENT four tiers must be retained");
  assert.ok(!elite.some((b) => /elite recognition/i.test(b.title)));
}

// Negative: removes valid tier
{
  const fix = load("failing-loyalty-elite-removes-valid-tier.json");
  const d = decideLoyaltyEliteCandidateApply(fix.currentBlocks, fix.candidateBlocks, {
    programId: fix.programId,
  });
  assert.equal(d.applyCandidate, false);
  assert.equal(d.reason, fix.expectReason);
  const assembled = assembleCumulativeProposal(fix.currentBlocks, fix.candidateBlocks);
  assert.equal(
    assembled.blocks.filter((b) => b.slotKey === "loyalty.elite").length,
    4,
    "must retain CURRENT four Honors tiers"
  );
}

// Positive fixtures
for (const name of [
  "passing-loyalty-elite-five-tiers.json",
  "passing-loyalty-elite-three-tiers-barcelo.json",
  "passing-loyalty-elite-member-plus-elites-hyatt.json",
  "passing-loyalty-elite-non-tiered-network.json",
  "passing-loyalty-elite-lifetime-invitation-labeled.json",
]) {
  const fix = load(name);
  const v = validateLoyaltyEliteTier(fix.blocks, { programId: fix.programId });
  assert.equal(v.ok, true, `${name} must validate ok: ${JSON.stringify(v.reasons)}`);
}

{
  const fix = load("passing-loyalty-elite-no-program.json");
  const v = validateLoyaltyEliteTier(fix.blocks, {
    programId: fix.programId,
    participates: false,
  });
  assert.equal(v.ok, true);
}

// Permanent standards gate composes elite lesson
{
  const fix = load("passing-loyalty-elite-lifetime-invitation-labeled.json");
  const proof = Array.from({ length: 6 }, (_, i) => ({
    slotKey: "loyalty.proof",
    title: `Benefit ${i + 1}`,
    body: `Meaningful benefit card ${i + 1} for participating loyalty program.`,
  }));
  const gate = evaluateFactoryPermanentStandardsGate({
    blocks: [...proof, ...fix.blocks],
    loyaltyParticipates: true,
    requireLoyaltyProof: true,
    loyaltyProgramId: "marriott_bonvoy",
    brandName: "Autograph Collection",
  });
  assert.equal(gate.ok, true, JSON.stringify(gate.loyaltyElite));
  assert.ok(gate.lessons.includes("BE-QL-LOYALTY-TIER-COMPLETENESS-001"));
  assert.equal(gate.loyaltyElite.ok, true);
}

{
  const bad = load("failing-loyalty-elite-collapsed-four-to-one.json");
  const gate = evaluateFactoryPermanentStandardsGate({
    blocks: bad.candidateBlocks,
    loyaltyParticipates: true,
    loyaltyProgramId: "marriott_bonvoy",
    skipLoyaltyEliteGate: false,
  });
  assert.equal(gate.loyaltyElite.ok, false, "generic single elite must fail elite gate");
  assert.equal(gate.ok, false);
}

console.log(
  JSON.stringify(
    {
      ok: true,
      contract: BE_FACTORY_PERMANENT_LOYALTY_TIER_COMPLETENESS_V1,
      lesson: "BE-QL-LOYALTY-TIER-COMPLETENESS-001",
      negativeFixturesBlocked: true,
      positiveFixturesPass: true,
      distinctFromLoyaltyProofExactSix: true,
    },
    null,
    2
  )
);
