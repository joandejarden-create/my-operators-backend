#!/usr/bin/env node
/**
 * Permanent Factory loyalty-six + regional acronym standards — positive/negative fixtures.
 */
import assert from "assert";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  BE_FACTORY_PERMANENT_LOYALTY_REGION_STANDARDS_V1,
  decideLoyaltyProofCandidateApply,
  evaluateFactoryPermanentStandardsGate,
  findBadRegionalHeadingTokens,
  normalizeRegionalAcronymsInHeading,
  validateLoyaltyProofExactSix,
} from "../lib/brand-explorer/be-factory-permanent-loyalty-region-standards-v1.js";
import { assembleCumulativeProposal } from "../lib/brand-explorer/be-cumulative-proposal-assembler-v1.js";
import { toEditorialTitleCase } from "../lib/brand-explorer/be-editorial-title-case-v1.js";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const fixDir = path.join(root, "fixtures/brand-explorer-factory-permanent-standards");

assert.equal(
  BE_FACTORY_PERMANENT_LOYALTY_REGION_STANDARDS_V1,
  "BE_FACTORY_PERMANENT_LOYALTY_REGION_STANDARDS_V1"
);

assert.equal(toEditorialTitleCase("Apac"), "APAC");
assert.equal(toEditorialTitleCase("Emea"), "EMEA");
assert.equal(toEditorialTitleCase("Cala"), "CALA");
assert.equal(toEditorialTitleCase("Latam"), "LATAM");
assert.equal(normalizeRegionalAcronymsInHeading("Apac corridor"), "APAC corridor");
assert.deepEqual(findBadRegionalHeadingTokens("Apac"), ["Apac"]);
assert.deepEqual(findBadRegionalHeadingTokens("APAC"), []);

const single = JSON.parse(
  fs.readFileSync(path.join(fixDir, "failing-loyalty-proof-single-stub.json"), "utf8")
);
const singleDecide = decideLoyaltyProofCandidateApply(single.currentBlocks, single.candidateBlocks);
assert.equal(singleDecide.applyCandidate, false, "single Proof Point must not apply");
assert.equal(singleDecide.reason, single.expectReason);
const singleAssembled = assembleCumulativeProposal(single.currentBlocks, single.candidateBlocks);
assert.equal(
  singleAssembled.provenanceBySlot["loyalty.proof"].applyCandidate,
  false,
  "assembler must block single Proof Point"
);
assert.equal(
  (singleAssembled.blocks.filter((b) => b.slotKey === "loyalty.proof") || []).length,
  6,
  "CURRENT six must be retained"
);

const partial = JSON.parse(
  fs.readFileSync(path.join(fixDir, "failing-loyalty-proof-partial-three.json"), "utf8")
);
const partialDecide = decideLoyaltyProofCandidateApply(partial.currentBlocks, partial.candidateBlocks);
assert.equal(partialDecide.applyCandidate, false);
assert.equal(partialDecide.reason, "loyalty_proof_requires_exact_six_cards");
const partialAssembled = assembleCumulativeProposal([], partial.candidateBlocks);
assert.ok(
  !partialAssembled.blocks.some((b) => b.slotKey === "loyalty.proof"),
  "incomplete loyalty.proof must not enter publication blocks over empty CURRENT"
);

const regionFail = JSON.parse(
  fs.readFileSync(path.join(fixDir, "failing-region-acronym-title-case.json"), "utf8")
);
const regionGate = evaluateFactoryPermanentStandardsGate({
  blocks: regionFail.blocks,
  headings: regionFail.headings,
  loyaltyParticipates: false,
});
assert.equal(regionGate.region.ok, false, "Apac/Emea headings must fail region gate");
assert.equal(regionGate.blocked, true);

const passFix = JSON.parse(
  fs.readFileSync(path.join(fixDir, "passing-loyalty-six-and-apac.json"), "utf8")
);
const passGate = evaluateFactoryPermanentStandardsGate({
  blocks: passFix.blocks,
  headings: passFix.headings,
  loyaltyParticipates: true,
  requireLoyaltyProof: true,
});
assert.equal(passGate.ok, true, "exact six + APAC must pass");
assert.equal(passGate.loyalty.count, 6);
assert.equal(validateLoyaltyProofExactSix(passFix.blocks).ok, true);

// Factory 2.0 inheritance marker
assert.deepEqual(passGate.factoryInheritance, ["1.0", "2.0"]);

console.log(
  JSON.stringify(
    {
      ok: true,
      contract: BE_FACTORY_PERMANENT_LOYALTY_REGION_STANDARDS_V1,
      negativeFixturesBlocked: true,
      positiveFixturePass: true,
      assemblerRetainsSix: true,
      factoryInheritance: passGate.factoryInheritance,
    },
    null,
    2
  )
);
