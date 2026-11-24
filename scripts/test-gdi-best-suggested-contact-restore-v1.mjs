#!/usr/bin/env node
/**
 * $0 certification tests — best suggested contact restore V1.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  evaluateVerifiedCommercialRoute,
  evaluateContactCompletenessForFinalization,
} from "../lib/group-demand-intelligence/presentation/gdi-verified-commercial-route-v1.js";
import { looksLikePersonName } from "../lib/group-demand-intelligence/secondary-named-contact-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

function read(p) {
  return fs.readFileSync(path.join(ROOT, p), "utf8");
}

function plausible(n) {
  return looksLikePersonName(n) && String(n).split(/\s+/).length >= 2;
}

let passed = 0;
function check(name, fn) {
  try {
    fn();
    passed += 1;
    console.log(`PASS ${name}`);
  } catch (err) {
    console.error(`FAIL ${name}: ${err.message}`);
    process.exitCode = 1;
  }
}

check("1_named_person_finalization_complete", () => {
  const r = evaluateContactCompletenessForFinalization(
    {
      primaryContact: {
        name: "Daniel Roy",
        role: "Senior Strategic Sourcing Partner",
        organization: "Salesforce",
      },
    },
    { looksLikePersonName: plausible },
  );
  assert.equal(r.complete, true);
  assert.equal(r.mode, "NAMED_PERSON");
});

check("2_verified_name_without_email_ok", () => {
  const r = evaluateContactCompletenessForFinalization(
    {
      primaryContact: {
        name: "Paulo Pereira",
        title: "VP Operations Portugal",
        email: null,
      },
    },
    { looksLikePersonName: plausible },
  );
  assert.equal(r.complete, true);
  assert.ok(!r.primaryContact?.email);
});

check("3_person_reuse_identity_shape", () => {
  assert.equal(plausible("Maria João Oliveira"), true);
  assert.equal(plausible("Organization contact desk"), false);
});

check("4_generic_homepage_rejected", () => {
  const r = evaluateVerifiedCommercialRoute({
    label: "Official Nestlé organizing / lodging channel",
    url: "https://www.nestle.com/",
  });
  assert.equal(r.ok, false);
  assert.equal(r.code, "GENERIC_HOMEPAGE_REJECTED");
});

check("5_valid_department_contact_route", () => {
  const r = evaluateVerifiedCommercialRoute({
    label: "Official WAPOR lodging desk",
    url: "https://wapor.org/events/2026-conference/housing-and-lodging/",
  });
  assert.equal(r.ok, true);
});

check("6_no_invented_buyer_from_junk", () => {
  const r = evaluateContactCompletenessForFinalization(
    { primaryContact: { name: "Survey Software" } },
    { looksLikePersonName: plausible },
  );
  assert.equal(r.complete, false);
});

check("7_restricted_contact_not_complete_via_name_alone_if_gap", () => {
  // Restriction is enforced at display layer; completeness still needs plausible name.
  const r = evaluateContactCompletenessForFinalization(
    { primaryContact: { name: "Buyer Not Yet Identified" } },
    { looksLikePersonName: plausible },
  );
  assert.equal(r.complete, false);
});

check("8_best_contact_ranking_prefers_named_over_route", () => {
  const r = evaluateContactCompletenessForFinalization(
    {
      primaryContact: { name: "Maurice Saad", role: "Conference Chair" },
      approvedCommercialRoute: "Official SpiceCon organizing / lodging channel",
      commercialContactRoute: "https://example.com/",
    },
    { looksLikePersonName: plausible },
  );
  assert.equal(r.mode, "NAMED_PERSON");
});

check("9_no_contact_research_required_on_tiles", () => {
  const ui = read("public/js/group-demand-intelligence/dealality-gdi-ui.js");
  assert.match(ui, /kind: "empty"/);
  assert.match(ui, /leave tile contact footer blank/i);
  const fnStart = ui.indexOf("function compactTileContactHtml");
  assert.ok(fnStart > 0);
  const fnSlice = ui.slice(fnStart, fnStart + 500);
  assert.match(fnSlice, /return ""/);
  // Must not emit the label as customer-visible HTML fallback text.
  assert.doesNotMatch(fnSlice, /esc\([^)]*Contact Research Required/);
  assert.doesNotMatch(
    fnSlice,
    /brand-card__contact-name--unidentified[\s\S]{0,80}Contact Research Required/,
  );
});

check("10_founder_gated_view_research_on_missing_staged", () => {
  const app = read("public/js/group-demand-intelligence/app.js");
  assert.match(
    app,
    /No finalized opportunities are available for this property/,
  );
  assert.match(app, /canAccessFounderResearchInventory/);
  assert.match(
    app,
    /inventoryUnavailableEmptyHtml[\s\S]{0,2000}view-research/,
  );
  assert.doesNotMatch(
    app,
    /Switch to Research to review hotel research findings/,
  );
});

check("11_historical_package_contract_documented", () => {
  const script = read("scripts/gdi-design-hotels-best-contact-restage-v1.mjs");
  assert.match(script, /priorVersionPreserved|HISTORICAL_STAGED_MISSING|priorPackagePreserved/);
});

check("12_restage_excludes_ineligible", () => {
  const script = read("scripts/gdi-design-hotels-best-contact-restage-v1.mjs");
  assert.match(script, /excluded\.push/);
  assert.match(script, /NAMED_PERSON/);
});

check("13_no_auto_customer_publication", () => {
  const script = read("scripts/gdi-design-hotels-best-contact-restage-v1.mjs");
  assert.doesNotMatch(script, /publishCustomerVisible/);
  assert.match(script, /currentPublication: false/);
});

check("14_bethesda_packages_not_targeted", () => {
  const script = read("scripts/gdi-design-hotels-best-contact-restage-v1.mjs");
  assert.doesNotMatch(script, /recLuxvwwxID7U2B8/);
});

check("15_identity_continuity_helpers_present", () => {
  const script = read("scripts/gdi-design-hotels-best-contact-restage-v1.mjs");
  assert.match(script, /enrichFromInventory/);
  assert.match(script, /bestSuggestedContactRestored/);
});

console.log(`\n${passed} checks evaluated`);
if (process.exitCode) {
  console.error("CERTIFICATION_FAILED");
  process.exit(1);
}
console.log("CERTIFICATION_PASS");
