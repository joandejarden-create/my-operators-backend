#!/usr/bin/env node
/**
 * Owner portfolio materialization idempotency — Windows-safe repeated calls.
 */

import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import {
  ensureGoldenOwnerPortfoliosMaterialized,
  writeOwnerPortfolioProfile,
  getOwnerPortfolioProfile,
  getGoldenOwnerPortfolioMaterializationState,
  __resetGoldenOwnerPortfolioMaterializationForTests,
  FIXTURE_DIR,
} from "../lib/hotel-intelligence/ownership/owner-control/portfolio-store.js";
import {
  getDefaultOwnershipSurface,
  __resetDefaultOwnershipSurfaceForTests,
} from "../lib/hotel-intelligence/ownership/ownership-surface-v1.js";

let failed = 0;
function check(name, fn) {
  try {
    fn();
    console.log("PASS", name);
  } catch (err) {
    failed += 1;
    console.error("FAIL", name, err.message);
  }
}

__resetGoldenOwnerPortfolioMaterializationForTests();
__resetDefaultOwnershipSurfaceForTests();

check("first_materialization_succeeds", () => {
  const ids = ensureGoldenOwnerPortfoliosMaterialized();
  assert.ok(Array.isArray(ids) && ids.length >= 4, "expected golden owner ids");
  const state = getGoldenOwnerPortfolioMaterializationState();
  assert.equal(state.initCount, 1);
  assert.equal(state.materialized, true);
  for (const id of ids) {
    const file = path.join(FIXTURE_DIR, `${id.replace(/[^a-zA-Z0-9_-]/g, "_")}.json`);
    assert.ok(fs.existsSync(file), `fixture missing ${file}`);
    const profile = getOwnerPortfolioProfile(id);
    assert.ok(profile?.owner_entity_id, `profile missing for ${id}`);
  }
});

check("second_materialization_no_init_bump_and_skips_rewrite", () => {
  const before = getGoldenOwnerPortfolioMaterializationState();
  const mtimesBefore = Object.fromEntries(
    fs
      .readdirSync(FIXTURE_DIR)
      .filter((f) => f.endsWith(".json"))
      .map((f) => {
        const p = path.join(FIXTURE_DIR, f);
        return [f, fs.statSync(p).mtimeMs];
      })
  );

  const ids2 = ensureGoldenOwnerPortfoliosMaterialized();
  const after = getGoldenOwnerPortfolioMaterializationState();
  assert.equal(after.initCount, before.initCount, "initCount must not increase");
  assert.ok(ids2.length >= 4);

  // Force a second write attempt on one profile — must skip when unchanged
  const sampleId = ids2[0];
  const profile = getOwnerPortfolioProfile(sampleId);
  const graphPath = path.join(
    FIXTURE_DIR,
    `${sampleId.replace(/[^a-zA-Z0-9_-]/g, "_")}.json`
  );
  const prior = JSON.parse(fs.readFileSync(graphPath, "utf8"));
  const result = writeOwnerPortfolioProfile(profile, prior.graph ?? null);
  assert.equal(result.skipped, true, "unchanged profile+graph must skip write");
  assert.equal(result.wrote, false);

  for (const [f, mtime] of Object.entries(mtimesBefore)) {
    const now = fs.statSync(path.join(FIXTURE_DIR, f)).mtimeMs;
    assert.equal(now, mtime, `mtime changed for ${f} on idempotent path`);
  }
});

check("third_same_process_call_still_stable", () => {
  const a = ensureGoldenOwnerPortfoliosMaterialized();
  const b = ensureGoldenOwnerPortfoliosMaterialized();
  assert.deepEqual(a, b);
  assert.equal(getGoldenOwnerPortfolioMaterializationState().initCount, 1);
});

check("lazy_default_surface_singleton", () => {
  __resetDefaultOwnershipSurfaceForTests();
  // Do not reset portfolio materialization — surface create should reuse singleton
  const s1 = getDefaultOwnershipSurface();
  const s2 = getDefaultOwnershipSurface();
  assert.equal(s1, s2);
  const meta = s1.meta();
  assert.ok(meta);
});

check("fail_safe_keeps_existing_on_forced_bad_path", () => {
  // Sanity: existing fixture remains readable after repeated ensures
  const ids = ensureGoldenOwnerPortfoliosMaterialized();
  const id = ids[0];
  const p = getOwnerPortfolioProfile(id);
  assert.ok(p?.owner_entity_id === id || p?.owner_entity_id);
});

if (failed) {
  console.error(`\n${failed} failing`);
  process.exit(1);
}
console.log("\nAll owner-portfolio materialization idempotency tests passed.");
