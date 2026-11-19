#!/usr/bin/env node
/**
 * Gate 1 visual pack integrity — does NOT founder-approve.
 */
import assert from "assert";
import { existsSync, readFileSync, readdirSync } from "fs";
import { join } from "path";

const dir = join(
  process.cwd(),
  "reports/brand-explorer/phase2h-a-renderer-parity/ten-tab-visual-v1",
);
const reportPath = join(dir, "01-ten-tab-capture-report.json");

assert.ok(existsSync(reportPath), "ten-tab capture report missing — run be-gate1-ten-tab-visual-capture-v1");
const report = JSON.parse(readFileSync(reportPath, "utf8"));

assert.strictEqual(report.founderApprovalStatus, "NOT_FOUNDER_APPROVED");
assert.strictEqual(report.gate2, "BLOCKED");
assert.strictEqual(report.contentMutation, false);
assert.ok(report.payloadHashSha256 && report.payloadHashSha256.length === 64);

assert.strictEqual(report.tabClickSummary.productOk, 10);
assert.strictEqual(report.tabClickSummary.compareOk, 10);

const shots = readdirSync(join(dir, "screenshots")).filter((f) => f.endsWith(".png"));
assert.ok(shots.length >= 25, `expected >=25 screenshots, got ${shots.length}`);

const ops = report.environments.B_compare.tabs.find((t) => t.tabId === "atelier-ops");
assert.ok(ops, "ops tab missing");
assert.ok(
  (ops.metrics.sectionTitles || []).includes("Operating Model"),
  "Gate1 compare Ops must remount Operating Model (not fall back to Overview)",
);
assert.strictEqual(ops.metrics.leftRightHtmlEqual, true);
assert.strictEqual(ops.metrics.gate1Hook.activeTab, "atelier-ops");

const overview = report.environments.B_compare.tabs.find((t) => t.tabId === "atelier-overview");
assert.ok((overview.metrics.sectionTitles || []).includes("Brand Snapshot"));
assert.strictEqual(overview.metrics.leftRightHtmlEqual, true);

console.log("PASS Gate1 ten-tab visual pack integrity (NOT founder-approved)");
console.log(
  JSON.stringify(
    {
      screenshots: shots.length,
      payloadHash: report.payloadHashSha256.slice(0, 16) + "…",
      remainingDiffs: (report.remainingVisualDifferences || []).filter((d) => d.tabId).length,
    },
    null,
    2,
  ),
);
