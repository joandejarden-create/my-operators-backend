#!/usr/bin/env node
import assert from "node:assert/strict";
import {
  buildCoverageRow,
  summarizeCoverageLedger,
  coverageUnitId,
  RETRIEVAL_STATUS,
} from "../lib/hotel-intelligence/sec-edgar/sec-discovery-coverage-ledger-v3.js";
import {
  buildBrandNeutralDiscoveryProcess,
  buildIncrementalIntelPerDollarRoadmap,
  NON_RESTRICTIVE_SEED_HINTS,
  DISCOVERY_SOURCE_CATEGORIES,
} from "../lib/hotel-intelligence/sec-edgar/sec-discovery-expansion-design-v1.js";
import { assertModePermissions } from "../lib/hotel-intelligence/hotel-intelligence-factory/modes-v1.js";
import {
  HIF_STAGE,
  DEFAULT_RESEARCH_STAGES,
  DISCOVERY_EXPANSION_STAGES,
} from "../lib/hotel-intelligence/hotel-intelligence-factory/stages-v1.js";

let passed = 0;
function test(name, fn) {
  try {
    fn();
    passed += 1;
    console.log(`PASS ${name}`);
  } catch (err) {
    console.error(`FAIL ${name}`);
    console.error(err);
    process.exitCode = 1;
  }
}

test("brand_names_are_not_ceilings", () => {
  assert.equal(NON_RESTRICTIVE_SEED_HINTS.brandNamesAreNotCeilings, true);
  assert.equal(NON_RESTRICTIVE_SEED_HINTS.issuerRegistryIsNotExhaustive, true);
  assert.equal(NON_RESTRICTIVE_SEED_HINTS.queryFamilyExhaustionIsNotCoverageComplete, true);
});

test("coverage_row_stable_id", () => {
  const a = coverageUnitId({ cik: "1040829", accession: "0001-20-000001", documentId: "EX-10.1" });
  const b = coverageUnitId({ cik: "0001040829", accession: "000120000001", documentId: "EX-10.1" });
  assert.equal(a, b);
  const row = buildCoverageRow({
    cik: "1040829",
    accession: "0001-20-000001",
    documentId: "EX-10.1",
    filingType: "EX-10",
    retrievalStatus: RETRIEVAL_STATUS.QUEUED,
  });
  assert.equal(row.coverageUnitId, a);
  assert.equal(row.exhaustiveClaimForbidden, undefined);
});

test("summary_forbids_exhaustive_claim", () => {
  const s = summarizeCoverageLedger([
    buildCoverageRow({ cik: "1", accession: "a", documentId: "p" }),
  ]);
  assert.equal(s.exhaustiveClaimForbidden, true);
  assert.equal(s.unitCount, 1);
});

test("roadmap_prioritizes_ex10_before_intl", () => {
  const r = buildIncrementalIntelPerDollarRoadmap();
  assert.equal(r.orderedWorkstreams[0].id, "EX10_HMA_FRANCHISE_EXHIBIT_HARVEST");
  assert.ok(r.orderedWorkstreams.some((w) => w.id === "INTL_NON_SEC_ADAPTERS"));
  assert.ok(DISCOVERY_SOURCE_CATEGORIES.length >= 6);
});

test("discover_expand_allowed_opt_in_not_default", () => {
  assert.equal(HIF_STAGE.DISCOVER_EXPAND, "DISCOVER_EXPAND");
  assert.ok(!DEFAULT_RESEARCH_STAGES.includes("DISCOVER_EXPAND"));
  assert.ok(DISCOVERY_EXPANSION_STAGES.includes("DISCOVER_EXPAND"));
  const ok = assertModePermissions({
    mode: "Research",
    stages: ["DISCOVER_EXPAND", "REPORT"],
  });
  assert.equal(ok.ok, true);
  const bad = assertModePermissions({
    mode: "CANONICAL_PROMOTION",
    stages: ["DISCOVER_EXPAND"],
  });
  assert.equal(bad.ok, false);
});

test("process_reuses_factory_boundaries", () => {
  const p = buildBrandNeutralDiscoveryProcess();
  assert.equal(p.factoryIntegration.proposedStage, "DISCOVER_EXPAND");
  assert.equal(p.factoryIntegration.promotionUnchanged, true);
  assert.equal(p.factoryIntegration.customerBlockedDefault, true);
});

console.log(`\n${passed} discovery-expansion tests passed`);
