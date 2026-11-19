#!/usr/bin/env node
import assert from "node:assert/strict";
import {
  RECONSTRUCTION_TARGETS,
  buildUniversalEvidencePack,
  applyBrandLoyaltyLocalization,
  validateAtelierMount,
  assessSourceAttribution,
} from "../lib/brand-explorer/be-phase2e-universal-reconstruction-v1.js";
import { buildProductionTemplateInventory } from "../lib/brand-explorer/be-phase2d-template-inventory-v1.js";
import {
  generateEnhancedCandidateFromEvidence,
  evaluateEnhancedQuality,
} from "../lib/brand-explorer/be-phase2d-curio-quality-recovery-v1.js";

assert.ok(RECONSTRUCTION_TARGETS.ascend.recordId === "reclkgOzvAcBheUSo");
assert.ok(RECONSTRUCTION_TARGETS.curio.loyalty.programName === "Hilton Honors");
assert.ok(RECONSTRUCTION_TARGETS.ascend.loyalty.programName === "Choice Privileges");

const inventory = buildProductionTemplateInventory();
const pack = await buildUniversalEvidencePack(RECONSTRUCTION_TARGETS.ascend, {
  allowLiveHpc: false,
  allowOfficialFetch: false,
});
assert.ok(pack.evidence.length >= 8, `expected Ascend evidence, got ${pack.evidence.length}`);
assert.equal(pack.usedPresentationAsInput, false);
assert.ok(pack.evidence.some((e) => /Choice Privileges/i.test(e.claim)));

let candidate = generateEnhancedCandidateFromEvidence(pack, {
  brandName: RECONSTRUCTION_TARGETS.ascend.name,
  parentCompany: RECONSTRUCTION_TARGETS.ascend.parentCompany,
  inventory,
});
candidate = applyBrandLoyaltyLocalization(candidate, pack, RECONSTRUCTION_TARGETS.ascend);

const hero = candidate.rows.find((r) => r.slotKey === "loyalty.hero_title");
assert.ok(hero && /Choice Privileges/i.test(hero.body), "Ascend loyalty hero must be Choice Privileges");
assert.ok(!candidate.rows.some((r) => /hilton honors/i.test(r.body)), "no Hilton Honors in Ascend candidate");

const quality = evaluateEnhancedQuality(candidate, pack, inventory);
assert.ok(quality.scores.applicableFieldCompleteness >= 0.85, `Ascend completeness ${quality.scores.applicableFieldCompleteness}`);

const atelier = validateAtelierMount(candidate, RECONSTRUCTION_TARGETS.ascend);
assert.equal(atelier.loyaltyContamination, "OK");
assert.ok(atelier.status.startsWith("PASS"), atelier.status);

const attribution = assessSourceAttribution(candidate, pack);
assert.ok(attribution.financeOpsGroundingRate >= 0.8);

console.log(
  JSON.stringify(
    {
      ok: true,
      ascendApplicable: quality.scores.applicableFieldCompleteness,
      atelier: atelier.status,
      grounding: attribution.financeOpsGroundingRate,
      evidence: pack.evidence.length,
    },
    null,
    2,
  ),
);
