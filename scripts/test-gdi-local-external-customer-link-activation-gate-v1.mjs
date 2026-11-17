#!/usr/bin/env node
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { evaluateLocalExternalCustomerLinkActivationGate } from "../lib/group-demand-intelligence/share/gdi-local-external-customer-link-activation-gate-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const results = [];

function check(name, fn) {
  try {
    fn();
    results.push({ name, ok: true });
  } catch (err) {
    results.push({ name, ok: false, error: String(err.message || err) });
  }
}

check("yotel_gate_passes_for_staged_42", () => {
  const gate = evaluateLocalExternalCustomerLinkActivationGate("recrPQcZg7SFARRb2", {
    expectedFingerprint:
      "353430092ceea5626c85e04bffbeeb84d0738bf7a5382c5177d5fdf37413eba3",
    expectedCounts: { all: 42, hotelMarket: 34, cmi: 8 },
  });
  assert.equal(gate.ok, true, JSON.stringify(gate.fails, null, 2));
  assert.equal(gate.inventory.all, 42);
});

check("gate_is_hotel_agnostic_module", () => {
  const src = fs.readFileSync(
    path.join(
      ROOT,
      "lib/group-demand-intelligence/share/gdi-local-external-customer-link-activation-gate-v1.js",
    ),
    "utf8",
  );
  assert.doesNotMatch(src, /recrPQcZg7SFARRb2/);
  assert.match(src, /evaluateLocalExternalCustomerLinkActivationGate/);
});

check("activate_script_requires_exact_phrase", () => {
  const src = fs.readFileSync(
    path.join(ROOT, "scripts/gdi-activate-yotel-geneva-35343009-approved.mjs"),
    "utf8",
  );
  assert.match(src, /FOUNDER_APPROVED_FOR_PUBLICATION \$\{APPROVED_FP\}/);
  assert.match(
    src,
    /353430092ceea5626c85e04bffbeeb84d0738bf7a5382c5177d5fdf37413eba3/,
  );
  assert.match(src, /assertLocalExternalCustomerLinkActivationGate/);
  assert.match(src, /41a1b1ca0c34eb5da4b2569db499743ef1721f52376d141c379677e4cd6c054f/);
});

check("fingerprint_mismatch_fails_gate", () => {
  const gate = evaluateLocalExternalCustomerLinkActivationGate("recrPQcZg7SFARRb2", {
    expectedFingerprint: "0".repeat(64),
    expectedCounts: { all: 42, hotelMarket: 34, cmi: 8 },
  });
  assert.equal(gate.ok, false);
  assert.ok(gate.fails.some((f) => f.code === "FINGERPRINT_MISMATCH"));
});

const failed = results.filter((r) => !r.ok);
console.log(
  JSON.stringify(
    {
      contract: "gdi-local-external-customer-link-activation-gate-v1",
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
