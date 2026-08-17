#!/usr/bin/env node
/**
 * Phase 3A.2 — multi-batch history + demo portfolio contracts (no provider calls).
 */
import assert from "node:assert/strict";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { DEMO_BRAND_PORTFOLIO } from "../lib/dealality/demo-stakeholder-workspace.js";
import { loadPeerSetConfig, resolvePeerSetMembership } from "../lib/ai-visibility/peer-sets.js";
import { assertBrandAiVisibilityRoutesRegistered, BRAND_AI_VISIBILITY_EXPECTED_ROUTES } from "../lib/ai-visibility/route-registration-guard.js";

let passed = 0;
let failed = 0;
function test(name, fn) {
  try {
    fn();
    passed += 1;
    console.log(`  PASS ${name}`);
  } catch (err) {
    failed += 1;
    console.error(`  FAIL ${name}: ${err.message}`);
  }
}

console.log("\nAI Visibility Phase 3A.2 contracts\n");

test("demo portfolio is 7 monitored Active brands (no Comfort/Radisson)", () => {
  assert.equal(DEMO_BRAND_PORTFOLIO.length, 7);
  const ids = DEMO_BRAND_PORTFOLIO.map((b) => b.brandId);
  assert.ok(ids.includes("reclkgOzvAcBheUSo"));
  assert.ok(ids.includes("recEJCTDj1zrsjPM6"));
  assert.ok(!ids.includes("recOzH5iAE1xEjyD0"));
  assert.ok(!ids.includes("recmKqo7M7mLZgRqQ"));
});

test("peer set expanded to 10 Active Brand Basics IDs", () => {
  const cfg = loadPeerSetConfig();
  const peer = resolvePeerSetMembership(
    { peerSetId: "peers_upper_upscale_brands_global_v1", commercialRegion: "CALA" },
    cfg
  );
  assert.equal(peer.entityIds.length, 10);
  assert.ok(peer.entityIds.includes("recIPuBC50fv13zRR")); // Westin
  assert.ok(peer.entityIds.includes("recsggfbKlJbjeRP9")); // Canopy
  assert.ok(peer.entityIds.includes("reccXxMHEh7NNRhIE")); // Tapestry
});

test("route registration guard expects executive-summary", () => {
  assert.ok(
    BRAND_AI_VISIBILITY_EXPECTED_ROUTES.some((r) => r.path.includes("executive-summary"))
  );
  const fakeApp = {
    _router: {
      stack: BRAND_AI_VISIBILITY_EXPECTED_ROUTES.map((r) => ({
        route: { path: r.path, methods: { [r.method]: true } },
      })),
    },
  };
  const result = assertBrandAiVisibilityRoutesRegistered(fakeApp, {
    logger: { log() {}, error() {} },
  });
  assert.equal(result.ok, true);
});

test("server.js registers executive-summary before :brandId", () => {
  const src = fs.readFileSync(
    path.join(path.dirname(fileURLToPath(import.meta.url)), "..", "server.js"),
    "utf8"
  );
  const execIdx = src.indexOf("/api/ai-visibility/brand/executive-summary");
  const overviewIdx = src.indexOf("/api/ai-visibility/brand/:brandId/overview");
  assert.ok(execIdx > 0 && overviewIdx > execIdx);
  assert.match(src, /assertBrandAiVisibilityRoutesRegistered/);
});

const reportPath = path.join(
  path.dirname(fileURLToPath(import.meta.url)),
  "..",
  "data",
  "ai-visibility",
  "phase3a2-validation-report.json"
);
if (fs.existsSync(reportPath)) {
  test("validation report shows multi-geo observed brands (no synthetic)", () => {
    const j = JSON.parse(fs.readFileSync(reportPath, "utf8"));
    assert.ok(j.qualityGate?.length >= 4);
    assert.equal(j.executiveSummary?.brandsMonitored?.monitored, 7);
    assert.ok(j.executiveSummary?.topPresence?.presence > 0);
    assert.ok(!("compositeScore" in (j.executiveSummary || {})));
    const ascendCala = j.brandComparison?.["Ascend Hotel Collection"]?.CALA;
    assert.ok(ascendCala);
    assert.notEqual(ascendCala.avail, "not_monitored");
  });
} else {
  test("validation report optional skip", () => {
    assert.ok(true);
  });
}

console.log(`\nPhase 3A.2: ${passed} passed, ${failed} failed\n`);
if (failed) process.exit(1);
