/**
 * Read-only checks for Scout opportunity signal generation.
 *
 * Usage: node scripts/test-scout-opportunity-signals.mjs
 */
import "../load-env.js";
import { buildOpportunitySignalsReport, SIGNAL_TYPES } from "../lib/scout/opportunity-signals.js";

const CASES = [
  {
    label: "Mexico + Choice parent gap",
    query: {
      country: "Mexico",
      parentCompany: "Choice Hotels International, Inc.",
      includePipeline: "1",
      limit: 50,
    },
    expectSignalTypes: ["parent_company_market_gap"],
    expectMinSignals: 1,
  },
  {
    label: "Colombia (multi-signal)",
    query: { country: "Colombia", includePipeline: "1", limit: 100 },
    expectMinSignals: 1,
  },
  {
    label: "Market Mexican Caribbean",
    query: { market: "Mexican Caribbean", includePipeline: "1", limit: 50 },
    expectMinSignals: 1,
  },
  {
    label: "Courtyard brand gap",
    query: {
      brand: "Courtyard by Marriott",
      parentCompany: "Marriott International",
      includePipeline: "1",
      limit: 50,
    },
    expectSignalTypes: ["brand_market_gap"],
    expectMinSignals: 1,
  },
  {
    label: "independent_conversion_cluster only",
    query: { country: "Mexico", signalType: "independent_conversion_cluster", limit: 20 },
    expectSignalTypes: ["independent_conversion_cluster"],
    expectMinSignals: 1,
  },
  {
    label: "large_independent_asset minRooms=150",
    query: { country: "Mexico", signalType: "large_independent_asset", minRooms: "150", limit: 20 },
    expectSignalTypes: ["large_independent_asset"],
    expectMinSignals: 1,
  },
];

function assert(cond, msg) {
  if (!cond) throw new Error(msg);
}

function assertSignalShape(signal) {
  const required = [
    "signalId",
    "signalType",
    "title",
    "confidence",
    "actionability",
    "priorityScore",
    "reason",
    "supportingMetrics",
    "recommendedAction",
    "source",
  ];
  for (const key of required) {
    assert(key in signal, `signal missing ${key}`);
  }
  assert(signal.source.readOnly === true, "signal.source.readOnly expected true");
  assert(SIGNAL_TYPES.includes(signal.signalType), `unknown signalType ${signal.signalType}`);
  assert(signal.priorityScore >= 0 && signal.priorityScore <= 100, "priorityScore out of range");
}

async function runCase(testCase) {
  console.log("\n---", testCase.label, "---");
  const report = await buildOpportunitySignalsReport(testCase.query);

  assert(report.ok, report.error || "report failed");
  assert(report.source?.readOnly === true, "expected readOnly source");
  assert(report.source?.writes === false, "expected no writes");
  assert(report.source?.marketField === "Market", "marketField should be Market");
  assert(report.source?.submarketField === "Submarket", "submarketField should be Submarket");

  console.log("summary", report.summary);
  console.log("warnings", report.warnings);
  if (report.signals[0]) {
    console.log("sample signal", {
      signalType: report.signals[0].signalType,
      title: report.signals[0].title,
      priorityScore: report.signals[0].priorityScore,
    });
  }

  assert(report.summary.signalsReturned === report.signals.length, "summary count mismatch");
  assert(report.signals.length <= (testCase.query.limit ? parseInt(testCase.query.limit, 10) : 100));

  if (testCase.expectMinSignals != null) {
    assert(
      report.signals.length >= testCase.expectMinSignals,
      `expected >= ${testCase.expectMinSignals} signals, got ${report.signals.length}`
    );
  }

  if (testCase.expectSignalTypes) {
    const types = new Set(report.signals.map((s) => s.signalType));
    for (const t of testCase.expectSignalTypes) {
      assert(types.has(t), `expected signal type ${t} in results`);
    }
  }

  for (const sig of report.signals.slice(0, 5)) {
    assertSignalShape(sig);
  }

  if (testCase.query.signalType === "large_independent_asset") {
    const minRooms = parseInt(testCase.query.minRooms || "100", 10);
    for (const sig of report.signals) {
      assert(
        (sig.supportingMetrics?.rooms || 0) >= minRooms,
        `large asset signal below minRooms ${minRooms}`
      );
    }
  }
}

async function main() {
  if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID_ALT) {
    console.error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID_ALT in .env");
    process.exit(1);
  }

  let failed = 0;
  const countsByType = new Map();

  for (const c of CASES) {
    try {
      await runCase(c);
      const report = await buildOpportunitySignalsReport(c.query);
      for (const row of report.summary.bySignalType) {
        countsByType.set(row.label, (countsByType.get(row.label) || 0) + row.count);
      }
      console.log("PASS:", c.label);
    } catch (e) {
      failed += 1;
      console.error("FAIL:", c.label, e.message);
    }
  }

  console.log("\n--- aggregate signal type counts (from test cases) ---");
  console.log([...countsByType.entries()].sort((a, b) => b[1] - a[1]));

  process.exit(failed > 0 ? 1 : 0);
}

main();
