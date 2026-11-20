#!/usr/bin/env node
/**
 * Permanent Loyalty at a Glance KPI completeness — fixtures + SureStay regression.
 */
import assert from "assert";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  BE_FACTORY_PERMANENT_LOYALTY_GLANCE_KPI_V1,
  LOYALTY_GLANCE_FAIL,
  buildLoyaltyGlanceKpiBlocks,
  decideLoyaltyGlanceKpiCandidateApply,
  estimateLoyaltyMix,
  validateLoyaltyGlanceKpis,
} from "../lib/brand-explorer/be-factory-permanent-loyalty-glance-kpi-v1.js";
import { assembleCumulativeProposal } from "../lib/brand-explorer/be-cumulative-proposal-assembler-v1.js";
import { evaluateFactoryPermanentStandardsGate } from "../lib/brand-explorer/be-factory-permanent-loyalty-region-standards-v1.js";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const fixDir = path.join(root, "fixtures/brand-explorer-factory-permanent-standards");

assert.equal(
  BE_FACTORY_PERMANENT_LOYALTY_GLANCE_KPI_V1,
  "BE_FACTORY_PERMANENT_LOYALTY_GLANCE_KPI_V1"
);

function load(name) {
  return JSON.parse(fs.readFileSync(path.join(fixDir, name), "utf8"));
}

// SureStay all-N/A must fail
{
  const fix = load("failing-loyalty-glance-all-na-surestay.json");
  const v = validateLoyaltyGlanceKpis(fix.blocks);
  assert.equal(v.ok, false);
  assert.ok(v.flags.includes(fix.expectFlag));
  assert.equal(v.classification, "KPI_MISSING");
}

// Content regression: N/A over verified CURRENT
{
  const fix = load("failing-loyalty-glance-regression-na-over-current.json");
  const d = decideLoyaltyGlanceKpiCandidateApply(fix.currentBlocks, fix.candidateBlocks);
  assert.equal(d.applyCandidate, false);
  assert.equal(d.reason, fix.expectReason);
  const assembled = assembleCumulativeProposal(fix.currentBlocks, fix.candidateBlocks);
  const kpis = assembled.blocks.filter((b) => /^loyalty\.kpi\./.test(b.slotKey));
  assert.equal(kpis.length, 4);
  assert.ok(kpis.every((b) => !/^n\/?a$/i.test(b.body)), "must retain CURRENT verified KPIs");
}

// Four verified Bonvoy
{
  const fix = load("passing-loyalty-glance-four-verified-bonvoy.json");
  assert.equal(validateLoyaltyGlanceKpis(fix.blocks).ok, true);
}

// Estimated mix BWR
{
  const fix = load("passing-loyalty-glance-estimated-mix-bwr.json");
  const v = validateLoyaltyGlanceKpis(fix.blocks);
  assert.equal(v.ok, true);
  assert.equal(v.verifiedCount, 4);
}

// Builder produces four meaningful KPIs for SureStay / BWR
{
  const built = buildLoyaltyGlanceKpiBlocks({
    programId: "best_western_rewards",
    brandName: "SureStay Plus by Best Western",
    chainScale: "Economy",
  });
  assert.equal(built.ok, true, JSON.stringify(built));
  assert.equal(built.blocks.length, 4);
  assert.equal(built.blocks[0].body, "64M");
  assert.ok(/Estimated Loyalty Mix/i.test(built.blocks[3].title));
  assert.ok(/\d/.test(built.blocks[3].body));
  const v = validateLoyaltyGlanceKpis(built.blocks);
  assert.equal(v.ok, true);
}

// Marriott uses verified program mix
{
  const mix = estimateLoyaltyMix({ programId: "marriott_bonvoy", brandName: "Westin" });
  assert.equal(mix.kind, "verified_program");
  assert.ok(/68%/.test(mix.value));
}

// Economy vs luxury bands differ
{
  const econ = estimateLoyaltyMix({
    programId: "best_western_rewards",
    brandName: "SureStay Plus by Best Western",
    chainScale: "Economy",
  });
  const lux = estimateLoyaltyMix({
    programId: "hilton_honors",
    brandName: "Waldorf Astoria Hotels & Resorts",
  });
  assert.equal(econ.kind, "estimated");
  assert.equal(lux.kind, "estimated");
  assert.notEqual(econ.displayValue, lux.displayValue);
}

// Permanent standards gate composes glance lesson
{
  const built = buildLoyaltyGlanceKpiBlocks({
    programId: "marriott_bonvoy",
    brandName: "AC Hotels by Marriott",
  });
  const proof = Array.from({ length: 6 }, (_, i) => ({
    slotKey: "loyalty.proof",
    title: `Benefit ${i + 1}`,
    body: `Meaningful benefit card ${i + 1}.`,
  }));
  const gate = evaluateFactoryPermanentStandardsGate({
    blocks: [...proof, ...built.blocks],
    loyaltyParticipates: true,
    requireLoyaltyProof: true,
    requireLoyaltyGlance: true,
    loyaltyProgramId: "marriott_bonvoy",
  });
  assert.equal(gate.loyaltyGlance.ok, true, JSON.stringify(gate.loyaltyGlance));
  assert.ok(gate.lessons.includes("BE-QL-LOYALTY-GLANCE-KPI-001"));
}

{
  const fix = load("failing-loyalty-glance-all-na-surestay.json");
  const gate = evaluateFactoryPermanentStandardsGate({
    blocks: fix.blocks,
    loyaltyParticipates: true,
    requireLoyaltyGlance: true,
  });
  assert.equal(gate.loyaltyGlance.ok, false);
  assert.equal(gate.ok, false);
}

console.log(
  JSON.stringify(
    {
      ok: true,
      contract: BE_FACTORY_PERMANENT_LOYALTY_GLANCE_KPI_V1,
      lesson: "BE-QL-LOYALTY-GLANCE-KPI-001",
      surestayAllNaBlocked: true,
      estimatedMixAuthorized: true,
      contentRegressionBlocked: true,
      failCodes: Object.values(LOYALTY_GLANCE_FAIL),
    },
    null,
    2
  )
);
