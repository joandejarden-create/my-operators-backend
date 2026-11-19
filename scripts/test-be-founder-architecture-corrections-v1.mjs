#!/usr/bin/env node
/**
 * Founder architecture corrections + lessons registry + failing fixture suite.
 * Does NOT regenerate brand content. Gate 2 must remain blocked.
 */
import assert from "assert";
import {
  loadArchitectureCorrections,
  loadCanonicalSubsectionInventory,
  loadFounderQualityLessonsRegistry,
  validateArchitectureContractIntegrity,
  runArchitectureFailingFixtureSuite,
  summarizeLessonsRegistry,
  evaluateArchitectureBlocks,
  BE_FOUNDER_ARCHITECTURE_CORRECTIONS_V1,
  BE_FOUNDER_QUALITY_LESSONS_REGISTRY_V1,
} from "../lib/brand-explorer/be-founder-architecture-corrections-v1.js";
import { isGate1FounderApproved } from "../lib/brand-explorer/be-permanent-content-standards-v1.js";
import { readFileSync, existsSync } from "fs";
import { join } from "path";

let failed = 0;
function check(name, fn) {
  try {
    fn();
    console.log("PASS", name);
  } catch (e) {
    failed += 1;
    console.error("FAIL", name, e.message || e);
  }
}

check("architecture_contract_loads", () => {
  const c = loadArchitectureCorrections({ reload: true });
  assert.strictEqual(c.version, BE_FOUNDER_ARCHITECTURE_CORRECTIONS_V1);
  assert.strictEqual(c.gatePolicy.gate2ContentReconstruction, "BLOCKED");
});

check("inventory_frozen_from_live_curio", () => {
  const inv = loadCanonicalSubsectionInventory({ reload: true });
  assert.ok(inv.sections.length >= 30);
  assert.strictEqual(inv.lifecycleRecovery.liveRecoveredCount, 6);
  assert.deepStrictEqual(inv.lifecycleRecovery.orderedTitles, [
    "Evaluation",
    "Conversion Design",
    "Pre-Opening",
    "Opening",
    "Ramp-Up",
    "Ongoing",
  ]);
});

check("lifecycle_exact_6_not_invented_7", () => {
  const arch = loadArchitectureCorrections({ reload: true });
  const life = arch.subsections.supportAcrossTheLifecycle;
  assert.strictEqual(life.count, 6);
  assert.strictEqual(life.founderAssertedCount, 7);
  assert.strictEqual(life.liveRecoveredCount, 6);
  assert.match(life.resolution, /DO_NOT_INVENT/);
  assert.strictEqual(life.conflictStatus, "OPEN");
  assert.strictEqual(life.conflictId, "BE-CONFLICT-LIFECYCLE-6-VS-7");
});

check("owner_buying_dual_propositions_encoded", () => {
  const arch = loadArchitectureCorrections({ reload: true });
  const ob = arch.subsections.whatTheOwnerIsReallyBuying;
  assert.strictEqual(ob.requiredPropositions.length, 2);
  assert.strictEqual(ob.forbidReduceToOwnerOutcomesOnly, true);
});

check("lessons_registry_phase2c_through_arch", () => {
  const reg = loadFounderQualityLessonsRegistry({ reload: true });
  assert.strictEqual(reg.version, BE_FOUNDER_QUALITY_LESSONS_REGISTRY_V1);
  const ids = new Set(reg.lessons.map((l) => l.lessonId));
  for (const id of [
    "BE-QL-2C-001",
    "BE-QL-2H-A-001",
    "BE-QL-ARCH-005",
    "BE-QL-ARCH-006",
    "BE-QL-LOCAL-PRODUCT-PARITY-001",
  ]) {
    assert.ok(ids.has(id), `missing ${id}`);
  }
  const summary = summarizeLessonsRegistry();
  assert.strictEqual(summary.verifiedImproperly.length, 0);
  assert.ok(summary.total >= 15);
});

check("contract_integrity_and_gate2_blocked", () => {
  const r = validateArchitectureContractIntegrity();
  assert.strictEqual(r.ok, true, JSON.stringify(r.failures));
  assert.strictEqual(r.gate1FounderApproved, false);
  assert.strictEqual(isGate1FounderApproved(), false);
});

check("gate1_evidence_preserved", () => {
  const root = process.cwd();
  assert.ok(existsSync(join(root, "reports/brand-explorer/phase2h-a-renderer-parity/00-payload-manifest.json")));
  assert.ok(existsSync(join(root, "public/brand-explorer-phase2e-preview/curio-gate1-renderer-parity-payload.json")));
  // Visual Gate 1 founder artifact may exist after PR #465; must remain visual-only (excludes Gate 2 / Airtable writes).
  const gate1Path = join(root, "reports/brand-explorer/phase2h-a-renderer-parity/GATE1_FOUNDER_APPROVED.json");
  if (existsSync(gate1Path)) {
    const g1 = JSON.parse(readFileSync(gate1Path, "utf8"));
    assert.strictEqual(g1.disposition, "FOUNDER_APPROVED");
    assert.ok(Array.isArray(g1.scope?.excludes));
    assert.ok(g1.scope.excludes.some((x) => /Gate 2/i.test(String(x))));
    assert.ok(g1.scope.excludes.some((x) => /Airtable|publication/i.test(String(x))));
  }
});

check("live_curio_lifecycle_passes_exact_6", () => {
  const payload = JSON.parse(
    readFileSync("public/brand-explorer-phase2e-preview/curio-gate1-renderer-parity-payload.json", "utf8"),
  );
  const blocks = payload.brandExplorer?.blocks || [];
  const ev = evaluateArchitectureBlocks(blocks);
  assert.ok(!ev.failures.includes("lifecycle_invented_seventh_stage"));
  assert.ok(!ev.failures.some((f) => f.startsWith("lifecycle_count:")));
});

check("mandatory_failing_fixture_suite", () => {
  const suite = runArchitectureFailingFixtureSuite();
  assert.ok(suite.results.length >= 6, "expected >=6 failing fixtures");
  assert.strictEqual(suite.ok, true, JSON.stringify(suite.results.filter((r) => !r.passed), null, 2));
});

if (failed) {
  console.error(`\n${failed} founder architecture check(s) failed`);
  process.exit(1);
}
console.log("\nFounder architecture corrections OK — Gate 2 remains blocked; lifecycle frozen at live EXACT 6");
