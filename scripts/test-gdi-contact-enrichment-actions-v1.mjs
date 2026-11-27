#!/usr/bin/env node
/**
 * Regression: List contact three-channel presentation + unboxed Actions +
 * identity recovery from LinkedIn slug (Lily Yuan).
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { hydrateOpportunityContacts } from "../lib/group-demand-intelligence/publication-package/gdi-wyndham-contact-hydration-rematerialize-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
let failed = 0;

function check(name, fn) {
  try {
    fn();
    console.log(`PASS ${name}`);
  } catch (err) {
    failed += 1;
    console.error(`FAIL ${name}`);
    console.error(err);
  }
}

check("identity_lily_yuan_from_linkedin_slug", () => {
  const { opportunity, fieldChanges } = hydrateOpportunityContacts({
    id: "disc_0fa36c29506f",
    opportunityId: "disc_0fa36c29506f",
    primaryContact: {
      name: "Lily Y.",
      title: "Senior Director, Supply Chain Management",
      organization: "Bora Biologics",
      sourceUrl: "https://linkedin.com/in/lilyyuan26",
    },
  });
  assert.equal(opportunity.primaryContact.name, "Lily Yuan");
  assert.match(
    fieldChanges.join(","),
    /IDENTITY_RESOLVED_FROM_LINKEDIN_SLUG/,
  );
  assert.equal(
    opportunity.primaryContact.linkedinUrl,
    "https://linkedin.com/in/lilyyuan26",
  );
});

check("identity_suzanne_k_not_invented", () => {
  const { opportunity, fieldChanges } = hydrateOpportunityContacts({
    id: "disc_ecba7545037a",
    primaryContact: {
      name: "Suzanne K.",
      title: "Global Travel & Expense Manager",
      sourceUrl: "https://linkedin.com/in/suzanne-k-549a9822",
    },
  });
  assert.equal(opportunity.primaryContact.name, "Suzanne K.");
  assert.doesNotMatch(
    fieldChanges.join(","),
    /IDENTITY_RESOLVED_FROM_LINKEDIN_SLUG/,
  );
});

check("list_module_three_channel_order_and_missing_linkedin", () => {
  const src = fs.readFileSync(
    path.join(ROOT, "public/js/group-demand-intelligence/gdi-commercial-list-table-v1.js"),
    "utf8",
  );
  const idxEmail = src.indexOf("ICON_EMAIL");
  const idxPhone = src.indexOf("ICON_PHONE");
  const idxLiMissing = src.indexOf("LinkedIn Not Available");
  assert.ok(idxEmail > 0 && idxPhone > idxEmail);
  assert.ok(idxLiMissing > idxPhone);
  assert.match(src, /LinkedIn Profile/);
});

check("actions_css_unboxed", () => {
  const css = fs.readFileSync(
    path.join(ROOT, "public/css/group-demand-intelligence.css"),
    "utf8",
  );
  assert.match(css, /unboxed SVG treatment/);
  assert.match(
    css,
    /\.gdi-page \.gdi-list-actions \.action-icon \{[\s\S]*?border:\s*none;/,
  );
});

check("surfe_findymail_not_in_list_ui", () => {
  const src = fs.readFileSync(
    path.join(ROOT, "public/js/group-demand-intelligence/gdi-commercial-list-table-v1.js"),
    "utf8",
  );
  assert.doesNotMatch(src, /surfe|Surfe|findymail|Findymail/i);
});

if (failed) {
  console.error(`\n${failed} failure(s)`);
  process.exit(1);
}
console.log("\nOK test:gdi-contact-enrichment-actions-v1");
