#!/usr/bin/env node
/**
 * Permanent gate: FULL E2E → STAGED → FOUNDER APPROVAL → CURRENT
 *   npm run test:gdi-canonical-e2e-to-staged-lifecycle-v1
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  GDI_CANONICAL_E2E_TO_STAGED_LIFECYCLE_V1,
  GDI_CANONICAL_PUBLICATION_LIFECYCLE,
  sanitizeOpportunityForStagedPackage,
  stageQualifiedInventoryFromE2e,
} from "../lib/group-demand-intelligence/publication-package/gdi-canonical-e2e-to-staged-lifecycle-v1.js";
import { createGdiPublicationPackageStore } from "../lib/group-demand-intelligence/publication-package/index.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const WY = "recnPB3Q02NO8Ma1o";
const BETH = "recLuxvwwxID7U2B8";

let failed = 0;
function check(name, fn) {
  try {
    fn();
    console.log(`PASS ${name}`);
  } catch (err) {
    failed += 1;
    console.error(`FAIL ${name}: ${err && err.message ? err.message : err}`);
  }
}

function read(rel) {
  return fs.readFileSync(path.join(ROOT, rel), "utf8");
}

check("lifecycle_contract_sequence", () => {
  assert.equal(
    GDI_CANONICAL_E2E_TO_STAGED_LIFECYCLE_V1,
    "gdi-canonical-e2e-to-staged-lifecycle-v1",
  );
  assert.deepEqual(GDI_CANONICAL_PUBLICATION_LIFECYCLE.sequence, [
    "FULL_E2E",
    "STAGED",
    "FOUNDER_APPROVAL",
    "CURRENT",
  ]);
  assert.equal(GDI_CANONICAL_PUBLICATION_LIFECYCLE.autoStageAfterAuthorizedE2e, true);
  assert.equal(
    GDI_CANONICAL_PUBLICATION_LIFECYCLE.proposedQualifiedIsNotFounderGate,
    true,
  );
  assert.equal(
    GDI_CANONICAL_PUBLICATION_LIFECYCLE.stagedNeverAutoPublishesCurrent,
    true,
  );
});

check("sanitize_strips_founder_preview_meta", () => {
  const row = sanitizeOpportunityForStagedPackage({
    id: "opp_a",
    title: "A",
    founderProposedPreview: true,
    founderReviewBucket: "proposed",
    hotelMarketScopeEligible: true,
  });
  assert.equal(row.id, "opp_a");
  assert.equal(row.founderProposedPreview, undefined);
  assert.equal(row.founderReviewBucket, undefined);
});

check("dry_run_stage_does_not_mutate_pointers", () => {
  const store = createGdiPublicationPackageStore({});
  const beforeS = store.readPointer(WY, "STAGED");
  const beforeC = store.readPointer(WY, "CURRENT");
  const result = stageQualifiedInventoryFromE2e({
    hotelId: WY,
    hotelName: "Wyndham San Diego Bayside",
    opportunities: [
      {
        id: "dry_run_only_opp",
        title: "Dry run",
        hotelMarketScopeEligible: true,
        primaryCommercialEligible: true,
      },
    ],
    dryRun: true,
    activate: false,
    runId: "lifecycle_test_dry",
  });
  assert.equal(result.ok, true);
  assert.equal(result.activated, false);
  assert.equal(result.dryRun, true);
  const afterS = store.readPointer(WY, "STAGED");
  const afterC = store.readPointer(WY, "CURRENT");
  assert.equal(
    beforeS?.publicationVersion || null,
    afterS?.publicationVersion || null,
  );
  assert.equal(
    beforeC?.publicationVersion || null,
    afterC?.publicationVersion || null,
  );
});

check("wyndham_staged_is_locked_26_current_null", () => {
  const store = createGdiPublicationPackageStore({});
  const staged = store.readPointer(WY, "STAGED");
  const current = store.readPointer(WY, "CURRENT");
  assert.ok(staged?.publicationVersion);
  assert.equal(current?.publicationVersion || null, null);
  const loaded = store.loadPackage(WY, staged.publicationVersion);
  assert.equal(loaded.ok, true);
  assert.equal(loaded.package.opportunityCount, 26);
  assert.equal(loaded.package.opportunities.length, 26);
  // CLEAN11 historical package preserved
  assert.ok(
    fs.existsSync(
      path.join(
        ROOT,
        "lib/group-demand-intelligence/bundled-publication-packages",
        WY,
        "gdi_hotel_wide_fqo_recnPB3Q02NO8Ma1o_20261125_3283ee92",
        "package.json",
      ),
    ),
  );
});

check("bethesda_pointers_preserved", () => {
  const store = createGdiPublicationPackageStore({});
  const staged = store.readPointer(BETH, "STAGED");
  assert.ok(staged?.publicationVersion, "Bethesda STAGED must remain");
});

check("orchestrator_and_ps1_wire_staged_progression", () => {
  const orch = read(
    "lib/group-demand-intelligence/workflow/gdi-full-e2e-v3-orchestrator-v1.js",
  );
  assert.match(orch, /stageQualifiedInventoryFromE2e/);
  assert.match(orch, /06-staged-progression\.json/);
  const entry = read(
    "lib/group-demand-intelligence/workflow/gdi-canonical-full-e2e-entry-v1.js",
  );
  assert.match(entry, /stageQualifiedInventoryFromE2e/);
  assert.match(entry, /GDI_CANONICAL_PUBLICATION_LIFECYCLE/);
  const ps1 = read("scripts/gdi/Invoke-GdiFullE2E.ps1");
  assert.match(ps1, /FULL E2E → STAGED → FOUNDER APPROVAL → CURRENT|FULL_E2E → STAGED/);
  assert.match(ps1, /CURRENT never/);
});

check("docs_and_constitution_record_lifecycle", () => {
  const constitution = read("docs/ai-build-system/DEALALITY_PRODUCT_CONSTITUTION.md");
  assert.match(constitution, /FULL E2E → STAGED → FOUNDER APPROVAL → CURRENT/);
  const bd = read("docs/ai-build-system/BUILD_DECISIONS.md");
  assert.match(bd, /gdi-canonical-e2e-to-staged-lifecycle-v1/);
  assert.ok(
    fs.existsSync(
      path.join(ROOT, "docs/ai-build-system/GDI_CANONICAL_E2E_TO_STAGED_LIFECYCLE_V1.md"),
    ),
  );
});

check("ui_no_proposed_founder_gate_default_staged", () => {
  const app = read("public/js/group-demand-intelligence/app.js");
  assert.match(app, /isLocalFounderHost\(\)\) return "staged"/);
  assert.doesNotMatch(app, /founderInventoryModeSelectorHtml\(/);
  assert.match(app, /Obsolete proposed\/hold\/continuation/);
  const html = read("public/app.html");
  assert.doesNotMatch(html, /data-gdi-shell-inventory="proposed"/);
});

if (failed) {
  console.error(`\n${failed} failure(s)`);
  process.exit(1);
}
console.log("\nOK test:gdi-canonical-e2e-to-staged-lifecycle-v1");
