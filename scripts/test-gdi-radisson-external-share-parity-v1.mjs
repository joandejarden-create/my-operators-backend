#!/usr/bin/env node
/**
 * Permanent GDI Full E2E gate: Radisson auth↔share package parity + Bethesda non-regression.
 *
 *   npm run test:gdi-radisson-external-share-parity-v1
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  assessAuthSharePackageParity,
  assertAuthShareSimulatedParity,
} from "../lib/group-demand-intelligence/share/gdi-auth-share-package-parity-v1.js";
import {
  resolveOpportunityFromApprovedPackage,
  resolveShareOpportunitiesFromApprovedPackage,
} from "../lib/group-demand-intelligence/publication-package/gdi-share-approved-package-resolver-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const HOTEL = "recH9h3McgMNFQEla";
const BETHESDA = "recLuxvwwxID7U2B8";
const FHLIP = "rx_d_2400ebe4c2bb";
const EXPECTED_FP =
  "533f2642f8c74af881ff525c2335f9a5471f5ef1d04ac16f7646131176f8706d";

const appJs = fs.readFileSync(
  path.join(ROOT, "public/js/group-demand-intelligence/app.js"),
  "utf8",
);
const shareJs = fs.readFileSync(
  path.join(ROOT, "public/js/group-demand-intelligence/share-app.js"),
  "utf8",
);

let failed = 0;
function check(name, fn) {
  try {
    fn();
    console.log(`PASS ${name}`);
  } catch (err) {
    failed += 1;
    console.error(`FAIL ${name}: ${err.message}`);
  }
}

check("shared_ui_modules_exist", () => {
  assert.match(appJs, /opportunitiesApiPath|detailUrl/);
  assert.match(shareJs, /group-demand-intelligence\/share\/hotels/);
  assert.ok(
    fs.existsSync(
      path.join(ROOT, "public/group-demand-intelligence-share.html"),
    ),
  );
});

const radisson = assessAuthSharePackageParity(HOTEL);
check("radisson_staged_fingerprint", () => {
  assert.equal(radisson.auth.fingerprint, EXPECTED_FP);
  assert.equal(radisson.auth.inventory.all, 66);
  assert.equal(radisson.auth.inventory.hotelMarket, 50);
  assert.equal(radisson.auth.inventory.cmi, 16);
  assert.equal(radisson.auth.inventory.scaling, 0);
  assert.equal(radisson.auth.inventory.localCatering, 6);
  assert.equal(radisson.auth.inventory.contactBuyer, 3);
});

check("radisson_simulated_share_parity", () => {
  assertAuthShareSimulatedParity(radisson);
});

check("radisson_live_share_not_silently_staged", () => {
  // Ordinary share must not invent CURRENT from STAGED.
  assert.equal(radisson.liveShare.mode, "LEGACY");
  assert.equal(radisson.liveShare.matchesStagedFingerprint, false);
  assert.equal(radisson.founderApprovalRequired, true);
  assert.equal(radisson.productionExternalReady, false);
});

check("radisson_all_66_staged_details", () => {
  const fails = [];
  for (const id of radisson.auth.opportunityIds) {
    const d = resolveOpportunityFromApprovedPackage({
      hotelId: HOTEL,
      opportunityId: id,
      preview: "staged",
      preferStaged: true,
    });
    if (!(d.ok && d.opportunity?.id === id)) fails.push(id);
    if (d.meta?.contentFingerprintSha256 !== EXPECTED_FP) fails.push(id + ":fp");
  }
  assert.equal(fails.length, 0, JSON.stringify(fails.slice(0, 8)));
});

check("fhlip_fixture", () => {
  const d = resolveOpportunityFromApprovedPackage({
    hotelId: HOTEL,
    opportunityId: FHLIP,
    preview: "staged",
    preferStaged: true,
  });
  assert.equal(d.ok, true);
  assert.equal(d.opportunity?.id, FHLIP);
  assert.match(String(d.opportunity?.displayTitle || d.opportunity?.title || ""), /FHLIP/i);
});

const bethesda = assessAuthSharePackageParity(BETHESDA);
check("bethesda_share_non_regression", () => {
  assert.equal(bethesda.liveShare.ok, true);
  assert.equal(bethesda.liveShare.mode, "PACKAGE");
  assert.ok((bethesda.liveShare.inventory?.all || 0) >= 60);
  const share = resolveShareOpportunitiesFromApprovedPackage({
    hotelId: BETHESDA,
  });
  assert.equal(share.ok, true);
  assert.equal(share.mode, "PACKAGE");
  assert.equal(
    share.meta?.contentFingerprintSha256,
    bethesda.liveShare.fingerprint,
  );
});

check("parity_module_wired", () => {
  assert.ok(
    fs.existsSync(
      path.join(
        ROOT,
        "lib/group-demand-intelligence/share/gdi-auth-share-package-parity-v1.js",
      ),
    ),
  );
});

if (failed) {
  console.error(`\n${failed} failure(s)`);
  process.exit(1);
}
console.log(
  JSON.stringify(
    {
      ok: true,
      radissonFingerprint: EXPECTED_FP,
      simulatedParityOk: true,
      founderApprovalRequired: true,
      bethesdaShareCount: bethesda.liveShare.inventory?.all ?? null,
    },
    null,
    2,
  ),
);
console.log("\nOK test:gdi-radisson-external-share-parity-v1");
