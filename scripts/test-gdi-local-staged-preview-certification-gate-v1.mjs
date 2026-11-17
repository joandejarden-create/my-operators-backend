#!/usr/bin/env node
/**
 * Permanent local staged-preview certification gate.
 * Uses YOTEL Geneva Lake staged package as the live fixture; asserts general
 * fail-closed behavior for unknown hotels.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  GDI_LOCAL_STAGED_PREVIEW_CERTIFICATION_GATE_V1,
  certifyLocalStagedPreviewResolution,
} from "../lib/group-demand-intelligence/publication-package/gdi-local-staged-preview-certification-gate-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const HOTEL = "recrPQcZg7SFARRb2";
const EXP_FP =
  "353430092ceea5626c85e04bffbeeb84d0738bf7a5382c5177d5fdf37413eba3";
const PRIOR_FP =
  "41a1b1ca0c34eb5da4b2569db499743ef1721f52376d141c379677e4cd6c054f";

const results = [];
function check(name, fn) {
  try {
    fn();
    results.push({ name, ok: true });
  } catch (err) {
    results.push({ name, ok: false, error: String(err?.message || err) });
  }
}

check("gate_id_stable", () => {
  assert.equal(
    GDI_LOCAL_STAGED_PREVIEW_CERTIFICATION_GATE_V1,
    "gdi-local-staged-preview-certification-gate-v1",
  );
});

check("yotel_staged_resolves_exact_fingerprint_and_counts", () => {
  const staged = JSON.parse(
    fs.readFileSync(
      path.join(
        ROOT,
        "lib/group-demand-intelligence/bundled-publication-packages",
        HOTEL,
        "STAGED.json",
      ),
      "utf8",
    ),
  );
  assert.equal(staged.contentFingerprintSha256, EXP_FP);
  assert.notEqual(staged.contentFingerprintSha256, PRIOR_FP);

  const cert = certifyLocalStagedPreviewResolution({
    hotelId: HOTEL,
    root: path.join(
      ROOT,
      "lib/group-demand-intelligence/bundled-publication-packages",
    ),
    expectedFingerprintSha256: EXP_FP,
  });
  assert.equal(cert.ok, true, JSON.stringify(cert.fails || []));
  assert.equal(cert.previewSource, "STAGED");
  assert.equal(cert.contentFingerprintSha256, EXP_FP);
  assert.equal(cert.inventoryCounts.all, 42);
  assert.equal(cert.inventoryCounts.hotel, 34);
  assert.equal(cert.inventoryCounts.competitive, 8);
  assert.equal(cert.opportunityIds.length, 42);
  assert.equal(cert.browserVerifiedEligible, true);
});

check("expected_id_set_must_match_exactly", () => {
  const cert = certifyLocalStagedPreviewResolution({
    hotelId: HOTEL,
    root: path.join(
      ROOT,
      "lib/group-demand-intelligence/bundled-publication-packages",
    ),
    expectedFingerprintSha256: EXP_FP,
    expectedOpportunityIds: ["not-a-real-id"],
  });
  assert.equal(cert.ok, false);
  assert.ok(
    (cert.fails || []).some((f) => f.code === "OPPORTUNITY_ID_SET_MISMATCH"),
  );
});

check("unknown_hotel_fail_closed", () => {
  const cert = certifyLocalStagedPreviewResolution({
    hotelId: "recDOESNOTEXIST0000000000",
    root: path.join(
      ROOT,
      "lib/group-demand-intelligence/bundled-publication-packages",
    ),
  });
  assert.equal(cert.ok, false);
  assert.ok(
    (cert.fails || []).some((f) => f.code === "STAGED_POINTER_MISSING"),
  );
  assert.equal(cert.browserVerifiedEligible, false);
});

check("wrong_expected_fingerprint_fails", () => {
  const cert = certifyLocalStagedPreviewResolution({
    hotelId: HOTEL,
    root: path.join(
      ROOT,
      "lib/group-demand-intelligence/bundled-publication-packages",
    ),
    expectedFingerprintSha256: PRIOR_FP,
  });
  assert.equal(cert.ok, false);
  assert.ok(
    (cert.fails || []).some(
      (f) => f.code === "STAGED_POINTER_FINGERPRINT_MISMATCH",
    ),
  );
});

const failed = results.filter((r) => !r.ok);
console.log(
  JSON.stringify(
    {
      contract: GDI_LOCAL_STAGED_PREVIEW_CERTIFICATION_GATE_V1,
      ok: failed.length === 0,
      passed: results.filter((r) => r.ok).length,
      failed: failed.length,
      results,
    },
    null,
    2,
  ),
);
process.exit(failed.length ? 1 : 0);
