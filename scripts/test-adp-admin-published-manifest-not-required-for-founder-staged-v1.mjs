/**
 * test:adp-admin-published-manifest-not-required-for-founder-staged-v1
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { loadPublishedManifest } from "../lib/ai-demand-positioning/published-snapshot.js";
import { resolveAdpMonthlyReviewEligibilityV1 } from "../lib/ai-demand-positioning/monthly-review/resolve-adp-monthly-review-eligibility-v1.js";

const ID = "adp_radisson_blu_toronto_downtown";
const legacy = path.join("data/ai-demand-positioning/published", ID, "manifest.json");
const durable = path.join("data/adp-durable-published/current", ID, "manifest.json");
assert.ok(fs.existsSync(durable), "durable current must exist for this gate");

const manifest = loadPublishedManifest(ID);
assert.ok(manifest, "manifest resolves");
const elig = resolveAdpMonthlyReviewEligibilityV1(ID);
assert.equal(elig.eligible, true);
assert.ok(!(elig.blockers || []).includes("published_manifest_missing"));

// If legacy missing, resolution must be durable
if (!fs.existsSync(legacy)) {
  assert.equal(manifest._adpManifestSource, "durable_published_current");
  assert.equal(elig.manifestResolution, "durable_published_current");
}

console.log("PASS test:adp-admin-published-manifest-not-required-for-founder-staged-v1");
