/**
 * Unit tests for Operator Explorer Tab Factory evaluate + fixture flatten.
 * No Airtable writes. Uses fixtures/ for baseline operators.
 */
import assert from "node:assert/strict";
import {
  flattenOperatorFixtureToPrefill,
  loadOperatorFixturePayload,
} from "../lib/partner-intelligence/operator-explorer-fixture-payload.js";
import {
  evaluateOperatorTabFactoryForTest,
  evaluateOperatorTabFactoryFromPayload,
} from "../lib/partner-intelligence/operator-explorer-tab-factory-evaluate.js";
import { runOperatorTabFactoryAudit } from "../lib/partner-intelligence/operator-explorer-tab-factory-audit.js";

function main() {
  // Empty payload → hard fails (PI allowGapCopy must not waive Tab Factory emptiness)
  const empty = evaluateOperatorTabFactoryForTest({}, "empty-op");
  assert.equal(empty.auditComplete, true);
  assert.equal(empty.auditPass, false);
  assert.ok(empty.failFindings > 10, "empty payload should fail many required fields");

  // Synthetic thick payload for a few keys still fails overall (incomplete),
  // but classified fields should pass when present.
  const partial = evaluateOperatorTabFactoryFromPayload({
    operatorSlug: "partial",
    prefill: {
      companyName: "Test Operator CALA",
      companyHistory:
        "Founded for regional hotel management with owner-aligned reporting and brand-approved operating depth across gateway markets.",
      op_commercial_engine_json: {
        intro: "Commercial engine",
        items: [
          { title: "Revenue", description: "In-market pacing and channel mix discipline for CALA assets." },
          { title: "Sales", description: "Regional sales coverage with brand and owner cadence." },
        ],
      },
    },
    source: "fixtures",
  });
  const history = partial.findings.find((f) => f.prefillKey === "companyHistory");
  assert.ok(history);
  assert.equal(history.status, "pass");
  const commercial = partial.findings.find((f) => f.prefillKey === "op_commercial_engine_json");
  assert.ok(commercial);
  assert.equal(commercial.status, "pass");

  // Fixture flatten / load
  const flat = flattenOperatorFixtureToPrefill({
    _meta: { x: 1 },
    profileFields: {
      companyHistory: "Arbor Lodging was founded in 2006 and built a vertically integrated platform for owners.",
    },
    lead_org_structure_json: [{ title: "CALA", description: "Regional leadership hub." }],
  });
  assert.ok(flat.companyHistory);
  assert.ok(Array.isArray(flat.lead_org_structure_json));

  const arborFixtures = loadOperatorFixturePayload("arbor-lodging-cala");
  assert.equal(arborFixtures.slug, "arbor-lodging-cala");
  assert.ok(arborFixtures.fixtureFiles.length >= 3, "Arbor should have multiple fixture packs");
  assert.ok(arborFixtures.keyCount > 5, "Arbor fixture map should expose prefill keys");

  const heFixtures = loadOperatorFixturePayload("hotel-equities-cala");
  assert.equal(heFixtures.slug, "hotel-equities-cala");
  assert.ok(heFixtures.fixtureFiles.length >= 3);
  assert.ok(heFixtures.keyCount > 5);

  console.log(
    JSON.stringify(
      {
        ok: true,
        emptyFails: empty.failFindings,
        arborFixtureKeys: arborFixtures.keyCount,
        heFixtureKeys: heFixtures.keyCount,
        arborFiles: arborFixtures.fixtureFiles.length,
        heFiles: heFixtures.fixtureFiles.length,
      },
      null,
      2
    )
  );
}

async function runFixtureAuditSmoke() {
  const report = await runOperatorTabFactoryAudit({
    operators: ["arbor-lodging-cala", "hotel-equities-cala"],
    source: "fixtures",
  });
  assert.equal(report.operatorResults.length, 2);
  assert.equal(report.auditComplete, true);
  assert.equal(report.summary.operatorsAudited, 2);
  // Goldens are not required to be auditPass yet — fixtures may not cover full registry.
  // Smoke asserts structure only.
  for (const o of report.operatorResults) {
    assert.ok(Array.isArray(o.tabSummaries));
    assert.equal(o.tabSummaries.length, 10);
    assert.ok(typeof o.failFindings === "number");
  }
  console.log(
    JSON.stringify(
      {
        fixtureAuditSmoke: true,
        auditPass: report.auditPass,
        totalFailFindings: report.summary.totalFailFindings,
        perOperator: report.operatorResults.map((o) => ({
          slug: o.operatorSlug,
          fails: o.failFindings,
          auditPass: o.auditPass,
        })),
      },
      null,
      2
    )
  );
}

try {
  main();
  await runFixtureAuditSmoke();
} catch (err) {
  console.error("[test:operator-explorer-tab-factory-audit]", err?.stack || err?.message || err);
  process.exitCode = 1;
}
