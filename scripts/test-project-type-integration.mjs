#!/usr/bin/env node
/**
 * Smoke tests for canonical Project Type handling.
 *   node scripts/test-project-type-integration.mjs
 *   node scripts/test-project-type-integration.mjs --deal-id recXXX
 */
import "dotenv/config";
import {
  PROJECT_TYPE_CANONICAL_OPTIONS,
  normalizeProjectTypeLabel,
  resolveProjectTypeKind,
  mapLegacyLandGreenfieldProjectType,
} from "../lib/project-type.js";
import {
  deriveCapabilityAreas,
  buildClarifications,
  capabilityIdsForProjectTypeKind,
} from "../lib/operator-capability-rules.js";
import { buildReadinessFromFields } from "../api/deal-readiness-review.js";
import { inferReadinessContext } from "../api/deal-readiness-context.js";
import { fetchDealWithMergedLinkedRecords } from "../api/my-deals.js";
import { postBrandAlignmentSnapshot } from "../api/brand-alignment-snapshot.js";
import { postOperatorCapabilitySnapshot } from "../api/operator-capability-snapshot.js";

let failed = 0;

function assert(cond, msg) {
  if (!cond) {
    console.error("FAIL:", msg);
    failed += 1;
  } else {
    console.log("ok:", msg);
  }
}

function testNormalization() {
  assert(
    normalizeProjectTypeLabel("Renovation / repositioning (open hotel)") === "Renovation / Repositioning",
    "legacy renovation maps to canonical"
  );
  assert(
    normalizeProjectTypeLabel("Land / greenfield only") === "New Build",
    "legacy land/greenfield maps to New Build label"
  );
  assert(
    normalizeProjectTypeLabel("Acquisition of operating hotel") === "Existing Operating Hotel",
    "legacy acquisition maps to Existing Operating Hotel for reads only"
  );
  assert(resolveProjectTypeKind("Mixed-Use Hospitality Project") === "mixed_use", "mixed-use kind");
  assert(resolveProjectTypeKind("Other / To Be Confirmed") === "other_tbc", "other tbc kind");
  assert(
    PROJECT_TYPE_CANONICAL_OPTIONS.length === 7 &&
      !PROJECT_TYPE_CANONICAL_OPTIONS.includes("Land / greenfield only"),
    "canonical list has 7 options without land/greenfield"
  );
}

function testCapabilityRules() {
  const newBuild = deriveCapabilityAreas({ "Project Type": "New Build" });
  assert(
    newBuild.some((c) => c.id === "pre_opening" && c.strength === "inferred"),
    "New Build infers pre-opening"
  );
  const conversion = deriveCapabilityAreas({ "Project Type": "Conversion / Reflag" });
  assert(
    conversion.some((c) => c.id === "conversion_pip"),
    "Conversion / Reflag infers conversion PIP"
  );
  const reno = deriveCapabilityAreas({ "Project Type": "Renovation / Repositioning" });
  assert(
    reno.some((c) => c.id === "commercial_repositioning"),
    "Renovation / Repositioning infers commercial repositioning"
  );
  const existing = deriveCapabilityAreas({ "Project Type": "Existing Operating Hotel" });
  assert(
    existing.some((c) => c.id === "accounting_reporting"),
    "Existing Operating Hotel infers reporting"
  );
  const otherIds = capabilityIdsForProjectTypeKind("other_tbc");
  assert(otherIds.length === 0, "Other / To Be Confirmed has no inferred capability IDs");
  const clar = buildClarifications({
    "Project Type": "Other / To Be Confirmed",
    "Who should receive bids for this project?": "Third-Party Operators Only (Management)",
  });
  assert(
    clar.some((c) => /confirm project type/i.test(c)),
    "Other / To Be Confirmed adds clarification"
  );
}

function testBackfillLand() {
  const withSignals = mapLegacyLandGreenfieldProjectType({
    "Stage of Development": "Land Under Control Only",
    "Zoned for Hotel Development": "Yes",
  });
  assert(withSignals.value === "New Build", "land/greenfield + site signals → New Build");
  const weak = mapLegacyLandGreenfieldProjectType({});
  assert(weak.value === "Other / To Be Confirmed", "land/greenfield without signals → Other");
}

function testReadinessContexts() {
  for (const pt of PROJECT_TYPE_CANONICAL_OPTIONS) {
    const ctx = inferReadinessContext({ "Project Type": pt, Country: "Mexico" });
    assert(ctx.projectTypeContext !== "unknown", `readiness context for ${pt}`);
  }
  const legacy = inferReadinessContext({ "Project Type": "Conversion", Country: "US" });
  assert(legacy.projectTypeContext === "conversion_reflag", "legacy Conversion string");
}

async function testApis(dealId) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) {
    console.warn("skip API tests: no AIRTABLE credentials");
    return;
  }
  let id = dealId;
  if (!id) {
    const Airtable = (await import("airtable")).default;
    const base = new Airtable({ apiKey }).base(baseId);
    const rows = await base(process.env.AIRTABLE_TABLE_DEALS || "Deals")
      .select({ maxRecords: 1 })
      .firstPage();
    id = rows[0]?.id;
  }
  if (!id) {
    console.warn("skip API tests: no deal id");
    return;
  }

  const resOcs = { statusCode: 200, body: null };
  const mockRes = {
    status(c) {
      resOcs.statusCode = c;
      return mockRes;
    },
    json(b) {
      resOcs.body = b;
      return mockRes;
    },
  };
  await postOperatorCapabilitySnapshot({ body: { dealId: id } }, mockRes);
  assert(resOcs.body?.success === true, "operator capability snapshot API");
  assert(Array.isArray(resOcs.body?.capabilityAreas), "snapshot returns capabilityAreas");

  const resBas = { statusCode: 200, body: null };
  const mockBas = {
    status(c) {
      resBas.statusCode = c;
      return mockBas;
    },
    json(b) {
      resBas.body = b;
      return mockBas;
    },
  };
  await postBrandAlignmentSnapshot(
    { body: { dealId: id, brandUniverse: "owner_preferred_then_pipeline", maxBrands: 3 } },
    mockBas
  );
  assert(resBas.body?.success === true, "brand alignment snapshot API");

  const full = await fetchDealWithMergedLinkedRecords(baseId, apiKey, id);
  const readiness = buildReadinessFromFields(full.deal.fields || {});
  assert(readiness.success !== false, "readiness review builds");
}

async function main() {
  const dealIdx = process.argv.indexOf("--deal-id");
  const dealId = dealIdx >= 0 ? process.argv[dealIdx + 1] : "";

  testNormalization();
  testCapabilityRules();
  testBackfillLand();
  testReadinessContexts();
  await testApis(dealId);

  if (failed) {
    console.error("\n" + failed + " test(s) failed");
    process.exit(1);
  }
  console.log("\nAll project-type integration checks passed.");
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
