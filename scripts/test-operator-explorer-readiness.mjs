import test from "node:test";
import assert from "node:assert/strict";
import { classifyExplorerReadiness } from "../lib/operator-explorer/readiness.js";

test("Research content-complete is gated from publishable", () => {
  const r = classifyExplorerReadiness({
    namedAssignmentCount: 3,
    distinctCountryCount: 2,
    distinctBrandNameCount: 2,
    track: 1,
    recordPurpose: "Research",
  });
  assert.equal(r.contentComplete, true);
  assert.equal(r.contentCompleteButLifecycleGated, true);
  assert.equal(r.explorerPublishable, false);
});

test("Production content-complete is publishable", () => {
  const r = classifyExplorerReadiness({
    namedAssignmentCount: 3,
    distinctCountryCount: 2,
    distinctBrandNameCount: 2,
    track: 1,
    recordPurpose: "Production",
  });
  assert.equal(r.explorerPublishable, true);
  assert.equal(r.usefulness, "Useful Profile");
});

test("Track 2 without BMC is thin", () => {
  const r = classifyExplorerReadiness({
    namedAssignmentCount: 3,
    distinctCountryCount: 2,
    distinctBrandNameCount: 1,
    track: 2,
    hasBrandManagedCapability: false,
    recordPurpose: "Production",
  });
  assert.equal(r.contentComplete, false);
  assert.equal(r.explorerPublishable, false);
});
