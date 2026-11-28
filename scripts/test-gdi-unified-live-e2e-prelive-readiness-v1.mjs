#!/usr/bin/env node
/**
 * Regression for the pre-live readiness gate (pure logic + static wiring).
 * Does not spawn Live/DryRun CLIs (the full gate does: npm run gdi:unified-live-e2e-prelive-readiness-v1).
 * No network, no spend.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  evaluateReadiness,
  buildProposal,
  selectFreshHotel,
  READINESS,
  FRESH_HOTEL_CANDIDATES,
} from "./gdi-unified-live-e2e-prelive-readiness-v1.mjs";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const WYNDHAM = "recnPB3Q02NO8Ma1o";

// READY only when every check passes; otherwise PARTIAL with an outstanding list + exit 1
const ready = evaluateReadiness([{ id: "a", label: "A", pass: true }, { id: "b", label: "B", pass: true }]);
assert.equal(ready.status, READINESS.READY);
assert.equal(ready.exitCode, 0);
assert.deepEqual(ready.outstanding, []);
const partial = evaluateReadiness([{ id: "a", label: "A", pass: true }, { id: "weekly_dry_run", label: "Weekly", pass: false }]);
assert.equal(partial.status, READINESS.PARTIAL);
assert.equal(partial.exitCode, 1);
assert.ok(partial.outstanding.some((o) => /WEEKLY_DRY_RUN_NOT_PASSING/.test(o)));
assert.equal(evaluateReadiness([{ id: "x", label: "X", pass: undefined }]).status, READINESS.PARTIAL, "non-true is not a pass");

// Fresh hotel selection: real hotel, never Wyndham, no STAGED/CURRENT
assert.ok(!FRESH_HOTEL_CANDIDATES.includes(WYNDHAM));
const sel = await selectFreshHotel();
assert.ok(sel, "a fresh hotel candidate exists");
assert.notEqual(sel.hotelId, WYNDHAM);
assert.ok(/^rec[A-Za-z0-9]{14}$/.test(sel.hotelId));
const pkgDir = path.join(ROOT, "lib/group-demand-intelligence/bundled-publication-packages", sel.hotelId);
assert.ok(!fs.existsSync(path.join(pkgDir, "STAGED.json")) && !fs.existsSync(path.join(pkgDir, "CURRENT.json")));

// Proposal content contract
const { json, md } = buildProposal(sel, { date: "20261128" });
assert.equal(json.authorized, false);
assert.equal(json.limits.exaUsdCeiling, 25);
assert.equal(json.limits.findymailCredits, 15);
assert.equal(json.limits.surfe, "FORBIDDEN");
assert.match(json.authorizationToken.proposed, /^FOUNDER_GO_GDI_LIVE_E2E_[A-Z0-9_]+_20261128$/);
assert.match(md, /PROPOSED LIMITS — NOT AUTHORIZATION TO SPEND/);
assert.ok(md.includes(sel.hotelId));
assert.ok(json.commands.liveFull.includes(`-HotelId ${sel.hotelId}`));
assert.ok(json.commands.liveFull.includes("-HardCeilingUsd 25"));
assert.ok(!json.commands.liveFull.includes(WYNDHAM), "no Wyndham identity in the fresh-hotel command");

// Static wiring
const pkg = JSON.parse(fs.readFileSync(path.join(ROOT, "package.json"), "utf8"));
for (const name of [
  "gdi:weekly-refresh",
  "test:gdi-weekly-refresh-dry-run-v1",
  "gdi:clean-airtable-reconstruction-cert-v1",
  "test:gdi-clean-airtable-reconstruction-cert-v1",
  "gdi:unified-live-e2e-prelive-readiness-v1",
  "test:gdi-unified-live-e2e-prelive-readiness-v1",
]) {
  assert.ok(pkg.scripts[name], `npm script ${name}`);
}
assert.ok(fs.existsSync(path.join(ROOT, "docs/ai-build-system/GDI_CANONICAL_POWERSHELL_OPERATIONS_RUNBOOK_V1.md")));

console.log("OK test:gdi-unified-live-e2e-prelive-readiness-v1");
