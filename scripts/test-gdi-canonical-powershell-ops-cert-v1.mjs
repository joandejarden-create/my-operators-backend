#!/usr/bin/env node
/**
 * Regression: GDI PowerShell ops suite — hotel-agnostic Checkpoint A defaults,
 * Full E2E Findymail flag passthrough, runbook + inventory presence.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

const common = fs.readFileSync(
  path.join(ROOT, "scripts/gdi/_GdiV3Common.ps1"),
  "utf8",
);
assert.doesNotMatch(
  common,
  /AuthorizationId = 'FOUNDER_GO_WYNDHAM_FINAL_COMMERCIAL_CONTINUATION/,
  "Checkpoint A must not default Wyndham AuthorizationId",
);
assert.doesNotMatch(
  common,
  /gdi-wyndham-who-how-v3-tranche-20261127\/01-TRANCHE_MANIFEST\.json/,
  "Checkpoint A must not default Wyndham Manifest path",
);
assert.match(common, /No hotel-specific default is permitted/);
assert.match(common, /No Wyndham-hardcoded default is permitted/);

const full = fs.readFileSync(
  path.join(ROOT, "scripts/gdi/Invoke-GdiFullE2E.ps1"),
  "utf8",
);
assert.match(full, /AuthorizeFindymail/);
assert.match(full, /-AuthorizeFindymail:\$AuthorizeFindymail/);

const required = [
  "scripts/gdi/Invoke-GdiDiscovery.ps1",
  "scripts/gdi/Invoke-GdiDeepResearch.ps1",
  "scripts/gdi/Invoke-GdiWhoHow.ps1",
  "scripts/gdi/Invoke-GdiContactResearch.ps1",
  "scripts/gdi/Invoke-GdiQualification.ps1",
  "scripts/gdi/Invoke-GdiFullE2E.ps1",
  "scripts/gdi/Invoke-GdiValidateFullE2E.ps1",
  "scripts/gdi/Invoke-GdiExecutiveSummary.ps1",
  "scripts/gdi/run-gdi-full-e2e-v3-stage-cli-v1.mjs",
  "docs/ai-build-system/GDI_CANONICAL_POWERSHELL_OPERATIONS_RUNBOOK_V1.md",
  "scripts/gdi-canonical-powershell-ops-cert-v1.mjs",
];
for (const rel of required) {
  assert.ok(fs.existsSync(path.join(ROOT, rel)), rel);
}

const cli = fs.readFileSync(
  path.join(ROOT, "scripts/gdi/run-gdi-full-e2e-v3-stage-cli-v1.mjs"),
  "utf8",
);
assert.doesNotMatch(
  cli,
  /Live paid execution for this stage is not enabled/,
  "generic V3 CLI must no longer blanket-refuse Live",
);
assert.doesNotMatch(cli, /process\.exit\(3\)/);
assert.match(cli, /runFullE2eV3Live/);
for (const flagName of [
  "--authorization-id",
  "--hard-ceiling-usd",
  "--manifest",
  "--plan-research-persistence",
  "--authorize-research-persistence",
  "--exact-write-set-authorization",
  "--apply-research-persistence",
  "--skip-discovery",
  "--skip-deep",
  "--skip-who-how",
  "--skip-contact",
  "--skip-qualification",
  "--skip-staged-progression",
  "--activate-staged-pointer",
  "--no-activate-staged-pointer",
]) {
  assert.ok(cli.includes(flagName), `CLI parses ${flagName}`);
}

for (const param of [
  "AuthorizationId",
  "HardCeilingUsd",
  "Manifest",
  "PlanResearchPersistence",
  "AuthorizeResearchPersistence",
  "ExactWriteSetAuthorization",
  "ApplyResearchPersistence",
  "SkipWhoHow",
  "SkipStagedProgression",
]) {
  assert.match(common, new RegExp(`\\$${param}\\b`), `common helper accepts ${param}`);
  assert.match(full, new RegExp(`-${param}\\b`), `Full E2E forwards ${param}`);
}
assert.doesNotMatch(
  full,
  /refuses Live|remains refused/i,
  "Full E2E notes must not claim Live is refused",
);

console.log("OK test:gdi-canonical-powershell-ops-cert-v1");
