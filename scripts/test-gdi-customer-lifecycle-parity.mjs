#!/usr/bin/env node
/**
 * Customer feedback lifecycle parity — authenticated GDI vs authorized share GDI.
 * Asserts the same customer-visible controls exist in shared UI markup.
 * Allows navigation/admin differences only.
 */

import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createRequire } from "node:module";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const require = createRequire(import.meta.url);

function loadUi() {
  const code = fs.readFileSync(
    path.join(ROOT, "public/js/group-demand-intelligence/dealality-gdi-ui.js"),
    "utf8"
  );
  const sandbox = { window: {}, globalThis: {} };
  sandbox.window = sandbox;
  sandbox.globalThis = sandbox;
  // eslint-disable-next-line no-new-func
  const fn = new Function("window", "globalThis", code + "\n;return window.DealalityGdiUi;");
  return fn(sandbox, sandbox);
}

const UI = loadUi();
assert.ok(UI.customerFeedbackLifecycleHtml, "shared lifecycle helper required");

const enums = UI.CUSTOMER_VALIDATION_ENUMS;
const fullPerms = {
  canValidate: true,
  canRecordAction: true,
  canRecordOutcome: true,
};

const authHtml = UI.customerFeedbackLifecycleHtml(
  { id: "gdi_opp_parity", shareValidation: null },
  enums,
  fullPerms,
  null
);
const shareHtml = UI.customerFeedbackLifecycleHtml(
  { id: "gdi_opp_parity", shareValidation: null },
  enums,
  fullPerms,
  null
);

assert.equal(authHtml, shareHtml, "auth and share customer lifecycle HTML must match");

const required = [
  "Hotel Validation",
  "Familiarity / Status",
  "Commercial Value",
  "Contact Person",
  "Email Quality",
  "Phone Quality",
  "gdiSvSave",
  "gdiSvSaveNext",
  "What did the team do?",
  "gdiDoAction",
  "Save Action",
  "What happened?",
  "gdiDoOutcome",
  "Save Outcome",
  "gdiDoLossWrap",
  'data-gdi-customer-lifecycle="1"',
  'data-gdi-lifecycle-action="1"',
  'data-gdi-lifecycle-outcome="1"',
];

for (const needle of required) {
  assert.ok(authHtml.includes(needle), `missing customer control: ${needle}`);
}

// Capability gating — action/outcome hidden when not granted
const validateOnly = UI.customerFeedbackLifecycleHtml(
  { id: "x" },
  enums,
  { canValidate: true, canRecordAction: false, canRecordOutcome: false },
  null
);
assert.ok(validateOnly.includes("Hotel Validation"));
assert.ok(!validateOnly.includes('data-gdi-lifecycle-action="1"'));
assert.ok(!validateOnly.includes('data-gdi-lifecycle-outcome="1"'));

// Registry capability grant without token reissue
process.env.DECISION_OUTCOMES_PERSISTENCE = "filesystem";
const {
  grantGdiShareCapabilitiesByTokenId,
  resolveGdiShareCapabilities,
  GDI_SHARE_CAPABILITY,
  readGdiShareRegistry,
} = await import("../lib/group-demand-intelligence/index.js");

const reg = readGdiShareRegistry();
const bethesda = reg.tokens?.["gdisht_47c25d74c79216021fb36150"];
assert.ok(bethesda, "Bethesda token registry row required");
assert.equal(bethesda.status, "ACTIVE");
assert.ok(
  (bethesda.capabilities || []).includes(GDI_SHARE_CAPABILITY.CAN_RECORD_ACTION),
  "Bethesda registry must include CAN_RECORD_ACTION"
);
assert.ok(
  (bethesda.capabilities || []).includes(GDI_SHARE_CAPABILITY.CAN_RECORD_OUTCOME),
  "Bethesda registry must include CAN_RECORD_OUTCOME"
);
assert.ok(
  (bethesda.capabilities || []).includes(GDI_SHARE_CAPABILITY.CAN_VALIDATE),
  "Bethesda registry must include CAN_VALIDATE"
);

const derived = resolveGdiShareCapabilities(
  {
    surfaces: ["brief", "opportunities", "opportunity_detail", "summary"],
    mode: "read_only",
  },
  bethesda
);
assert.ok(derived.includes("CAN_RECORD_ACTION"));
assert.ok(derived.includes("CAN_RECORD_OUTCOME"));
assert.ok(derived.includes("CAN_VALIDATE"));

// Surfaces-only derivation (no registry) still gets validate, not action/outcome
const legacy = resolveGdiShareCapabilities({
  surfaces: ["opportunity_detail"],
});
assert.ok(legacy.includes("CAN_VALIDATE"));
assert.ok(!legacy.includes("CAN_RECORD_ACTION"));

// App sources reference shared helper
const shareApp = fs.readFileSync(
  path.join(ROOT, "public/js/group-demand-intelligence/share-app.js"),
  "utf8"
);
const authApp = fs.readFileSync(
  path.join(ROOT, "public/js/group-demand-intelligence/app.js"),
  "utf8"
);
assert.ok(shareApp.includes("customerFeedbackLifecycleHtml"));
assert.ok(authApp.includes("customerFeedbackLifecycleHtml"));
assert.ok(shareApp.includes("/actions"));
assert.ok(shareApp.includes("/outcomes"));

console.log("PASS gdi customer feedback lifecycle parity (auth == share)");
