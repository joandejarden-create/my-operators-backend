#!/usr/bin/env node
/**
 * Permanent diagnostic regressions for Bethesda gold-standard replay V1.
 * Does not mutate STAGED/CURRENT. $0 only.
 */
import assert from "assert";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { createHash } from "crypto";
import { evaluateFinalizedQualifiedOpportunity } from "../lib/group-demand-intelligence/workflow/gdi-finalized-qualified-opportunity-contract-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const HOTEL = "recLuxvwwxID7U2B8";
const GOLD =
  "gdi_pub_recLuxvwwxID7U2B8_20261113_local_master_v2";
const REPORT = path.join(
  ROOT,
  "reports/gdi-bethesda-gold-standard-replay-v1",
);

function sha(p) {
  return createHash("sha256").update(fs.readFileSync(p)).digest("hex");
}

function pass(name) {
  console.log(`  PASS ${name}`);
}

{
  const pkgPath = path.join(
    ROOT,
    "lib/group-demand-intelligence/bundled-publication-packages",
    HOTEL,
    GOLD,
    "package.json",
  );
  const pkg = JSON.parse(fs.readFileSync(pkgPath, "utf8"));
  assert.equal(pkg.opportunities.length, 67);
  pass("gold_package_has_67_opportunities");
}

{
  const staged = path.join(
    ROOT,
    "lib/group-demand-intelligence/bundled-publication-packages",
    HOTEL,
    "STAGED.json",
  );
  const current = path.join(
    ROOT,
    "lib/group-demand-intelligence/bundled-publication-packages",
    HOTEL,
    "CURRENT.json",
  );
  assert.equal(
    sha(staged),
    "f2a4f70b3fb104ee8c59c91648c35e5062de6f29565800a0015f55a2e875e6aa",
  );
  assert.equal(
    sha(current),
    "db4eb0ca7d3e5b08ed160293f085b06260096347e7720f26c4dca6ca4606ce9c",
  );
  const stagedJson = JSON.parse(fs.readFileSync(staged, "utf8"));
  assert.equal(stagedJson.publicationVersion || stagedJson.version, GOLD);
  pass("staged_current_pointers_match_freeze_checksums");
}

{
  const gold = JSON.parse(
    fs.readFileSync(path.join(REPORT, "BETHESDA_GOLD_STANDARD_67.json"), "utf8"),
  );
  assert.equal(gold.opportunityCount, 67);
  assert.equal(gold.opportunities.length, 67);
  assert.ok(gold.opportunities.every((o) => o.canonicalId));
  pass("gold_standard_manifest_complete");
}

{
  const pkg = JSON.parse(
    fs.readFileSync(
      path.join(
        ROOT,
        "lib/group-demand-intelligence/bundled-publication-packages",
        HOTEL,
        GOLD,
        "package.json",
      ),
      "utf8",
    ),
  );
  const first = pkg.opportunities[0];
  const evalRaw = evaluateFinalizedQualifiedOpportunity(first);
  // Historical presentation packages often lack top-level sourceUrls.
  assert.equal(evalRaw.finalizedQualified, false);
  assert.ok(evalRaw.reasons.includes("MISSING_EVIDENCE_URLS"));
  pass("fqo_fails_gold_presentation_without_toplevel_sourceurls");
}

{
  const matrix = JSON.parse(
    fs.readFileSync(
      path.join(REPORT, "BETHESDA_67_REPLAY_PARITY_MATRIX.json"),
      "utf8",
    ),
  );
  assert.equal(matrix.metrics.originalIdsReconciled, 67);
  assert.ok(matrix.metrics.technicalRegressionCount >= 1);
  pass("parity_matrix_reconciles_all_67_ids");
}

{
  const ptr = JSON.parse(
    fs.readFileSync(path.join(REPORT, "POINTER_PRESERVATION.json"), "utf8"),
  );
  assert.equal(ptr.stagedUnchanged, true);
  assert.equal(ptr.currentUnchanged, true);
  pass("replay_did_not_mutate_pointers");
}

{
  const html = path.join(REPORT, "ISOLATED_DUAL_PANE_PREVIEW.html");
  assert.ok(fs.existsSync(html));
  const body = fs.readFileSync(html, "utf8");
  assert.ok(body.includes("LEFT — Approved historical"));
  assert.ok(body.includes("RIGHT — Current E2E replay"));
  pass("isolated_dual_pane_preview_exists");
}

{
  const src = fs.readFileSync(
    path.join(
      ROOT,
      "lib/group-demand-intelligence/workflow/gdi-finalized-qualified-opportunity-contract-v1.js",
    ),
    "utf8",
  );
  assert.ok(src.includes("thesis.length >="));
  assert.ok(src.includes("action.length >="));
  assert.ok(/Westin New York at Times Square/.test(src));
  pass("fqo_defects_documented_in_source_for_remediation");
}

console.log("\nOK test-gdi-bethesda-gold-standard-replay-v1");
