#!/usr/bin/env node
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const doc = path.join(ROOT, "docs/data-intelligence/gdi-discovery-quality-pivot-plan-v1.md");
const summary = path.join(
  ROOT,
  "reports/gdi-discovery-quality-pivot-plan-v1/PLAN_SUMMARY.json",
);

assert.ok(fs.existsSync(doc), "plan doc missing");
assert.ok(fs.existsSync(summary), "plan summary missing");
const j = JSON.parse(fs.readFileSync(summary, "utf8"));
assert.equal(j.schema, "gdi_discovery_quality_pivot_plan_v1");
assert.equal(j.phase2bDecision, "STOP");
assert.equal(j.bottlenecks.length, 3);
assert.equal(j.firstTestableSlice.paidUsd, 0);
assert.ok(
  j.firstTestableSlice.excludesOpportunityIds.includes("gdi_lmc_00562bf81b1e1957"),
);
assert.ok(
  j.firstTestableSlice.excludesOpportunityIds.includes(
    "gdi_opp_2027_ddaa_annual_conference_8",
  ),
);
const md = fs.readFileSync(doc, "utf8");
assert.match(md, /Three highest-impact discovery bottlenecks/);
assert.match(md, /gdi-discovery-intake-disposition-v1/);
assert.doesNotMatch(md, /Expand Phase 2B/);

console.log(
  JSON.stringify(
    {
      ok: true,
      bottlenecks: j.bottlenecks.map((b) => b.id),
      firstSlice: j.firstTestableSlice.id,
    },
    null,
    2,
  ),
);
