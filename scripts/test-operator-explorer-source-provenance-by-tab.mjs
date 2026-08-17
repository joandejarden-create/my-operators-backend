/**
 * Unit tests for Operator Explorer source provenance by tab.
 */
import assert from "node:assert/strict";
import {
  collectFixtureProvenanceSources,
  evaluateOperatorSourceProvenanceByTab,
  CANONICAL_OPERATOR_SOURCE_RULES,
} from "../lib/partner-intelligence/operator-explorer-source-provenance-by-tab.js";
import { runOperatorSourceProvenanceAudit } from "../lib/partner-intelligence/operator-explorer-source-provenance-by-tab-audit.js";

function main() {
  assert.ok(CANONICAL_OPERATOR_SOURCE_RULES["arbor-lodging-cala"]);
  assert.ok(CANONICAL_OPERATOR_SOURCE_RULES["hotel-equities-cala"]);

  const empty = evaluateOperatorSourceProvenanceByTab({
    operatorSlug: "arbor-lodging-cala",
    sources: [],
  });
  assert.equal(empty.pass, false);
  assert.ok(empty.failures.length > 0);

  const thirdOnly = evaluateOperatorSourceProvenanceByTab({
    operatorSlug: "arbor-lodging-cala",
    sources: [
      {
        sourceTitle: "Press",
        sourceUrl: "https://www.hotelinvestmenttoday.com/example",
        origin: "test",
      },
    ],
  });
  assert.equal(thirdOnly.pass, false);

  const ok = evaluateOperatorSourceProvenanceByTab({
    operatorSlug: "arbor-lodging-cala",
    sources: [
      {
        sourceTitle: "Arbor official",
        sourceUrl: "https://www.arborlodging.com/platforms",
        origin: "test",
      },
      {
        sourceTitle: "Press",
        sourceUrl: "https://www.hotelinvestmenttoday.com/example",
        origin: "test",
      },
    ],
  });
  assert.equal(ok.pass, true, ok.failures.join(","));

  const arborSources = collectFixtureProvenanceSources("arbor-lodging-cala");
  assert.ok(arborSources.length >= 1);
  assert.ok(arborSources.some((s) => /arborlodging\.com/i.test(s.host || s.sourceUrl)));

  const heSources = collectFixtureProvenanceSources("hotel-equities-cala");
  assert.ok(heSources.some((s) => /hotelequities\.com/i.test(s.host || s.sourceUrl)));

  console.log(
    JSON.stringify(
      {
        ok: true,
        arborFixtureSources: arborSources.length,
        heFixtureSources: heSources.length,
      },
      null,
      2
    )
  );
}

async function smoke() {
  const report = await runOperatorSourceProvenanceAudit({
    operators: ["arbor-lodging-cala", "hotel-equities-cala"],
    source: "fixtures",
  });
  assert.equal(report.operatorResults.length, 2);
  assert.equal(report.auditPass, true, JSON.stringify(report.summary));
  console.log(
    JSON.stringify(
      {
        smoke: true,
        auditPass: report.auditPass,
        perOperator: report.operatorResults.map((o) => ({
          slug: o.operatorSlug,
          pass: o.pass,
          sources: o.sourceCount,
        })),
      },
      null,
      2
    )
  );
}

try {
  main();
  await smoke();
} catch (err) {
  console.error("[test:operator-explorer-source-provenance-by-tab]", err?.stack || err?.message || err);
  process.exitCode = 1;
}
