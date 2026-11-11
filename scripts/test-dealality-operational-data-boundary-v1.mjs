import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import {
  MIGRATION_PLAN_V1,
  classifyTrackedPath,
  isOperationalClass,
  resolveGdiStorageBoundary,
} from "../lib/dealality/operational-data-boundary-v1.js";
import { mergePublishedInventoryIntoOpportunities } from "../lib/group-demand-intelligence/durable-publication/gdi-published-inventory-registry-v1.js";

function assert(cond, message) {
  if (!cond) throw new Error(message);
}

assert(classifyTrackedPath("lib/server-side.js") === "source", "lib is source");
assert(classifyTrackedPath("fixtures/example.json") === "source_fixture", "fixtures stay tracked");
assert(isOperationalClass(classifyTrackedPath("data/group-demand-intelligence/hotels/x/opportunities.json")), "gdi store");
assert(isOperationalClass(classifyTrackedPath("config/client-share/gdi-share-registry/active-tokens.json")), "share registry");
assert(isOperationalClass(classifyTrackedPath("reports/example.json")), "reports");
assert(MIGRATION_PLAN_V1.status === "PLAN_ONLY_PENDING_APPROVAL", "plan is not an apply");

const boundary = resolveGdiStorageBoundary({});
assert(boundary.overrideConfigured === false, "default root stays configured");
assert(boundary.usesExternalRoot === false, "default root is the repo tree");
const external = resolveGdiStorageBoundary({ GDI_DATA_ROOT: path.join(os.tmpdir(), "dealality-gdi-boundary") });
assert(external.usesExternalRoot === true, "override leaves the repo tree");
assert(external.defaultRepoRoot === boundary.defaultRepoRoot, "default location is unchanged");

const root = fs.mkdtempSync(path.join(os.tmpdir(), "gdi-boundary-"));
const merged = mergePublishedInventoryIntoOpportunities(
  "rec-boundary-test",
  [{ id: "research-only-row" }],
  { root, strictPublishedOnly: true }
);
assert(Array.isArray(merged) && merged.length === 0, "research rows do not publish by fallback");

console.log(JSON.stringify({ ok: true, plan: MIGRATION_PLAN_V1.id, researchLeakBlocked: true }));
