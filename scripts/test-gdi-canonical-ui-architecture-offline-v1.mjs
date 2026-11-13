#!/usr/bin/env node
/**
 * Offline permanent gates for GDI canonical customer UI architecture.
 * Safe for CI (no live server / Railway / Playwright required).
 *
 *   npm run test:gdi-canonical-ui-architecture-offline
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { attachPackageCustomerPresentation } from "../lib/group-demand-intelligence/publication-package/gdi-package-customer-presentation-attach-v1.js";
import {
  buildCanonicalCustomerPresentationContract,
  diffCanonicalCustomerPresentation,
  attachCanonicalCustomerPresentationContract,
} from "../lib/group-demand-intelligence/presentation/gdi-canonical-customer-presentation-contract-v1.js";
import {
  describeSvgIconStateContract,
  SUPPORTED_REVENUE_STREAM_ICON_FIXTURES,
  resolveCanonicalRevenueStreamIconStates,
} from "../lib/group-demand-intelligence/presentation/gdi-svg-icon-state-contract-v1.js";
import {
  createDifferenceRecord,
  upsertDifference,
  openMaterialCount,
  RESOLUTION_STATUS,
  DIFFERENCE_CATEGORIES,
} from "../lib/group-demand-intelligence/presentation/gdi-component-difference-register-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
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

const EXPECTED_FP =
  "441773211494cebaf50c03ef8921ccac6c57f31ac5cb2398585352b425e36f7f";

check("html_cache_marker_architecture_v1", () => {
  for (const rel of [
    "public/group-demand-intelligence.html",
    "public/group-demand-intelligence-share.html",
  ]) {
    const html = fs.readFileSync(path.join(ROOT, rel), "utf8");
    assert.match(
      html,
      /dealality-gdi-ui\.js\?v=gdi-contact-subtitle-freeze-v1/,
      rel,
    );
  }
});

check("shared_ui_prefers_canonical_contract", () => {
  const ui = fs.readFileSync(
    path.join(ROOT, "public/js/group-demand-intelligence/dealality-gdi-ui.js"),
    "utf8",
  );
  assert.match(ui, /function readCanonicalCustomerPresentation/);
  assert.match(ui, /fromCanonicalContract:\s*true/);
  assert.match(ui, /canon\.contact\.name/);
  assert.match(ui, /canon\.commercialAction\.pillLabel/);
});

check("svg_icon_state_fixtures", () => {
  for (const fx of SUPPORTED_REVENUE_STREAM_ICON_FIXTURES) {
    const got = resolveCanonicalRevenueStreamIconStates(fx.class);
    assert.deepEqual(got, fx.expected, fx.id);
  }
  const desc = describeSvgIconStateContract();
  assert.equal(desc.contractId, "GDI_SVG_ICON_STATE_CONTRACT_V1");
});

check("lorna_kipfer_canonical_contract_parity", () => {
  const row = attachPackageCustomerPresentation({
    id: "gdi_opp_regulatory_information_conference_20260930",
    title: "Regulatory Information Conference",
    primaryContact: {
      name: "Lorna Kipfer",
      professionalTitle: "RIC Conference Contact",
      organizationName: "U.S. Nuclear Regulatory Commission",
    },
    revenueStreamClass: "COMBINED_GUESTROOMS_EVENT_FNB",
    guestroomThesis: "Documented attendee lodging for RIC week.",
    salesActionPillLabel: "Qualify Opportunity",
    salesActionKey: "QUALIFY_OPPORTUNITY",
  });
  const contract = row.canonicalCustomerPresentation;
  assert.equal(contract.contact.name, "Lorna Kipfer");
  assert.equal(contract.contact.role, "RIC Conference Contact");
  assert.doesNotMatch(String(contract.contact.role), /Group Travel/i);
  assert.equal(contract.icons.guestrooms, "active");
  assert.equal(contract.icons.eventFnb, "muted");
  assert.equal(contract.commercialAction.pillLabel, "Qualify Opportunity");

  // Auth and share must build the same contract from the same attached row.
  const a = buildCanonicalCustomerPresentationContract(row, {
    contentFingerprintSha256: EXPECTED_FP,
  });
  const b = buildCanonicalCustomerPresentationContract({ ...row }, {
    contentFingerprintSha256: EXPECTED_FP,
  });
  assert.equal(diffCanonicalCustomerPresentation(a, b).length, 0);
});

check("difference_register_categories_and_resolve", () => {
  assert.ok(DIFFERENCE_CATEGORIES.includes("SVG_STATE_DIFFERENCE"));
  assert.ok(DIFFERENCE_CATEGORIES.includes("CONTACT_FIELD_DIFFERENCE"));
  let register = { differences: [] };
  register = upsertDifference(
    register,
    createDifferenceRecord({
      differenceId: "TEST__SVG__demo",
      category: "SVG_STATE_DIFFERENCE",
      severity: "MATERIAL",
      resolutionStatus: RESOLUTION_STATUS.OPEN,
      opportunityId: "demo",
      componentName: "guestrooms",
    }),
  );
  assert.equal(openMaterialCount(register), 1);
  register.differences[0].resolutionStatus = RESOLUTION_STATUS.RESOLVED;
  assert.equal(openMaterialCount(register), 0);
});

check("visual_baseline_manifest_and_pngs", () => {
  const dir = path.join(ROOT, "fixtures/gdi-canonical-ui-visual-baselines");
  const manifest = JSON.parse(
    fs.readFileSync(path.join(dir, "MANIFEST.json"), "utf8"),
  );
  assert.equal(manifest.publicationFingerprint, EXPECTED_FP);
  for (const surface of Object.values(manifest.surfaces)) {
    for (const key of ["overview", "ricCard"]) {
      const file = path.join(dir, surface[key]);
      assert.ok(fs.existsSync(file), file);
      assert.ok(fs.statSync(file).size > 1000, file);
    }
  }
});

check("attach_is_idempotent_for_contract", () => {
  const once = attachPackageCustomerPresentation({
    id: "idem",
    title: "Demo",
    revenueStreamClass: "GUESTROOMS_ONLY",
    guestroomThesis: "Rooms.",
  });
  const twice = attachCanonicalCustomerPresentationContract(once, {});
  assert.ok(twice.canonicalCustomerPresentation);
  assert.equal(
    twice.canonicalCustomerPresentation.icons.guestrooms,
    "active",
  );
});

check("permanent_register_file_present", () => {
  const p = path.join(
    ROOT,
    "reports/gdi-component-difference-register/COMPONENT_DIFFERENCE_REGISTER.json",
  );
  assert.ok(fs.existsSync(p));
  const reg = JSON.parse(fs.readFileSync(p, "utf8"));
  assert.ok(Array.isArray(reg.differences));
});

if (failed) {
  console.error(`\n${failed} failing assertion(s)`);
  process.exit(1);
}
console.log("\nOK test:gdi-canonical-ui-architecture-offline");
