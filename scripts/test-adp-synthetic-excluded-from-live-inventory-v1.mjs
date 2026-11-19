#!/usr/bin/env node
/**
 * Gate: adp_smoke_test_synthetic never enters live ADP inventory discovery.
 */
import assert from "node:assert/strict";
import { existsSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { listPublishedPropertyIds } from "../lib/ai-demand-positioning/published-snapshot.js";
import { buildPublishedAdpReviewCoverageV1 } from "../lib/ai-demand-positioning/monthly-review/admin/published-review-coverage-v1.js";
import { listMonthlyReviewUniversePropertyIds } from "../lib/ai-demand-positioning/monthly-review/resolve-adp-monthly-review-eligibility-v1.js";
import { isAdpSyntheticOrTestPropertyId } from "../lib/ai-demand-positioning/governance/adp-operational-registry-hygiene-v1.js";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
const SYNTH = "adp_smoke_test_synthetic";

assert.equal(isAdpSyntheticOrTestPropertyId(SYNTH), true);

const liveSeed = join(root, "fixtures/ai-demand-positioning/published", SYNTH);
const isolated = join(
  root,
  "fixtures/ai-demand-positioning/test-only/published",
  SYNTH
);
assert.equal(existsSync(liveSeed), false, "synthetic must not live under published seed");
assert.equal(existsSync(isolated), true, "synthetic fixture preserved under test-only");

const published = listPublishedPropertyIds();
assert.ok(!published.includes(SYNTH));
assert.ok(!published.some((id) => isAdpSyntheticOrTestPropertyId(id)));
assert.equal(published.length, 38);

const universe = listMonthlyReviewUniversePropertyIds();
assert.ok(!universe.includes(SYNTH));

const coverage = buildPublishedAdpReviewCoverageV1({});
assert.equal(coverage.universeCount, 38);
assert.equal(coverage.reviewReadyCount, 38);
assert.equal(coverage.missingCount, 0);
assert.equal(coverage.blockedCount, 0);
assert.ok(!(coverage.properties || []).some((p) => p.propertyId === SYNTH));

console.log(
  JSON.stringify(
    {
      ok: true,
      gate: "test:adp-synthetic-excluded-from-live-inventory-v1",
      publishedCount: published.length,
      coverage: {
        universe: coverage.universeCount,
        ready: coverage.reviewReadyCount,
        missing: coverage.missingCount,
        blocked: coverage.blockedCount,
      },
      isolatedFixture: isolated.replace(/\\/g, "/"),
    },
    null,
    2
  )
);
