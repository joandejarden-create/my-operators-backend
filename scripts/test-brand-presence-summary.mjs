/**
 * Read-only checks for Brand Alias Mapping + GET /api/brand-presence-summary logic.
 * Requires AIRTABLE_API_KEY, AIRTABLE_BASE_ID_ALT, and Brand Alias Mapping table seeded.
 *
 * Usage: node scripts/test-brand-presence-summary.mjs
 */
import "../load-env.js";
import { resolveBrandAffiliationMatchers } from "../lib/hotel-census/brand-alias-resolve.js";
import { aggregateCensusPresenceSummary } from "../lib/hotel-census/aggregate-presence-summary.js";

const CASES = [
  {
    label: "Courtyard by Marriott (Dealality canonical)",
    requested: "Courtyard by Marriott",
    parentCompany: "Marriott International",
    expectMatchersInclude: ["Courtyard", "Courtyard by Marriott"],
    expectCanonical: "Courtyard by Marriott",
    expectMinOpenHotels: 45,
    expectNoAliasUnavailable: true,
  },
  {
    label: "AC Hotels by Marriott",
    requested: "AC Hotels by Marriott",
    parentCompany: "Marriott International",
    expectMatchersInclude: [
      "AC Hotels by Marriott",
      "AC Hotel by Marriott",
      "AC Hotels",
    ],
    expectCanonical: "AC Hotels by Marriott",
    expectMinOpenHotels: 15,
    expectNoAliasUnavailable: true,
  },
  {
    label: "Unmatched brand (fallback)",
    requested: "Totally Fake Brand XYZ 999",
    parentCompany: "",
    expectFallback: true,
  },
];

function assert(cond, msg) {
  if (!cond) throw new Error(msg);
}

async function runCase(testCase) {
  console.log("\n---", testCase.label, "---");
  const resolution = await resolveBrandAffiliationMatchers(
    testCase.requested,
    testCase.parentCompany
  );
  console.log("resolution", JSON.stringify(resolution, null, 2));

  assert(resolution.ok, "resolution failed");

  if (testCase.expectNoAliasUnavailable) {
    assert(
      !resolution.warnings.some((w) => w.startsWith("ALIAS_TABLE_UNAVAILABLE")),
      "alias table should be available"
    );
    assert(resolution.usedAliasTable, "expected alias table matchers");
  }

  if (testCase.expectFallback) {
    assert(
      !resolution.usedAliasTable ||
        resolution.warnings.some((w) => w.startsWith("NO_ALIAS")),
      "expected fallback warning for unmatched brand"
    );
  } else {
    assert(
      resolution.usedAliasTable || resolution.warnings.length > 0,
      "expected alias table or explicit fallback warning"
    );
  }
  if (testCase.expectCanonical) {
    assert(
      resolution.canonicalBrandName === testCase.expectCanonical,
      `canonical expected ${testCase.expectCanonical} got ${resolution.canonicalBrandName}`
    );
  }
  for (const m of testCase.expectMatchersInclude || []) {
    assert(
      resolution.affiliationMatchers.includes(m),
      `expected matcher ${m} in ${JSON.stringify(resolution.affiliationMatchers)}`
    );
  }

  const summary = await aggregateCensusPresenceSummary({
    affiliationMatchers: resolution.affiliationMatchers,
    parentCompany: testCase.parentCompany,
  });
  console.log("metrics", summary.metrics);
  console.log("warnings", resolution.warnings);
  assert(summary.ok, summary.error || "aggregate failed");
  assert(
    !resolution.affiliationMatchers.includes("Independent"),
    "Independent must not be in matchers"
  );

  if (testCase.expectMinOpenHotels != null) {
    assert(
      summary.metrics.totalOpenHotels >= testCase.expectMinOpenHotels,
      `expected >= ${testCase.expectMinOpenHotels} open hotels, got ${summary.metrics.totalOpenHotels}`
    );
  }
}

async function main() {
  if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID_ALT) {
    console.error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID_ALT in .env");
    process.exit(1);
  }

  let failed = 0;
  for (const c of CASES) {
    try {
      await runCase(c);
      console.log("PASS:", c.label);
    } catch (e) {
      failed += 1;
      console.error("FAIL:", c.label, e.message);
    }
  }

  process.exit(failed > 0 ? 1 : 0);
}

main();
