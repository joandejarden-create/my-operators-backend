#!/usr/bin/env node
/**
 * Permanent gate: abbreviated surnames are not fully verified identities,
 * presentation fields sync after LinkedIn-slug recovery, Findymail blocked.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import {
  classifyPersonIdentityCompleteness,
  isAbbreviatedPersonName,
  isFullyVerifiedPersonName,
  preferCompletePersonName,
  syncOpportunityContactPresentationNames,
  auditAbbreviatedContactIdentities,
  PERSON_IDENTITY_COMPLETENESS,
} from "../lib/group-demand-intelligence/presentation/gdi-abbreviated-contact-identity-v1.js";
import { hydrateOpportunityContacts } from "../lib/group-demand-intelligence/publication-package/gdi-wyndham-contact-hydration-rematerialize-v1.js";
import { assessFindymailEmailEligibility } from "../lib/group-demand-intelligence/workflow/gdi-findymail-stage4-enrichment-v1.js";
import {
  evaluateWhoHowAbbreviatedIdentityGate,
  evaluateStage4AbbreviatedIdentityGate,
  evaluateHotelAbbreviatedIdentityGates,
} from "../lib/group-demand-intelligence/workflow/gdi-abbreviated-identity-stage-gates-v1.js";
import { splitContactPersonName } from "../lib/group-demand-intelligence/presentation/gdi-contact-identity-preflight-v1.js";
import { isIncompletePersonName } from "../lib/group-demand-intelligence/presentation/gdi-content-hydration-presentation-gate-v1.js";

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

check("classifier_abbreviated_vs_full", () => {
  assert.equal(isAbbreviatedPersonName("Lily Y."), true);
  assert.equal(isAbbreviatedPersonName("Suzanne K."), true);
  assert.equal(isAbbreviatedPersonName("Suzanne K"), true);
  assert.equal(isAbbreviatedPersonName("Lily Yuan"), false);
  assert.equal(isFullyVerifiedPersonName("Lily Yuan"), true);
  assert.equal(isFullyVerifiedPersonName("Suzanne K."), false);
  assert.equal(
    classifyPersonIdentityCompleteness("Lily Y."),
    PERSON_IDENTITY_COMPLETENESS.ABBREVIATED_SURNAME,
  );
  assert.equal(isIncompletePersonName("Suzanne K."), true);
});

check("prefer_complete_over_abbreviated", () => {
  assert.equal(
    preferCompletePersonName("Lily Yuan", "Lily Y."),
    "Lily Yuan",
  );
  assert.equal(
    preferCompletePersonName("Suzanne K.", "Suzanne K."),
    "Suzanne K.",
  );
});

check("hydrate_syncs_lily_presentation_fields", () => {
  const { opportunity, fieldChanges } = hydrateOpportunityContacts({
    id: "disc_0fa36c29506f",
    primaryContact: {
      name: "Lily Y.",
      title: "Senior Director, Supply Chain Management",
      organization: "Bora Biologics",
      sourceUrl: "https://linkedin.com/in/lilyyuan26",
      email: "lily.yuan@borabiologics.com",
    },
    contactName: "Lily Y.",
    tileContactPreview: {
      kind: "named",
      name: "Lily Y.",
      role: "Senior Director, Supply Chain Management",
    },
    recommendedNextStep: "Contact Lily Y.; ask the qualification questions.",
  });
  assert.equal(opportunity.primaryContact.name, "Lily Yuan");
  assert.equal(opportunity.contactName, "Lily Yuan");
  assert.equal(opportunity.tileContactPreview.name, "Lily Yuan");
  assert.match(opportunity.recommendedNextStep, /Lily Yuan/);
  assert.ok(fieldChanges.some((c) => /presentation:/.test(c)));
  assert.equal(
    opportunity.primaryContact.identityCompleteness,
    PERSON_IDENTITY_COMPLETENESS.FULL,
  );
});

check("suzanne_not_invented_marked_partial", () => {
  const { opportunity, fieldChanges } = hydrateOpportunityContacts({
    id: "disc_ecba7545037a",
    primaryContact: {
      name: "Suzanne K.",
      title: "Global Travel & Expense Manager",
      sourceUrl: "https://linkedin.com/in/suzanne-k-549a9822",
    },
    secondaryContact: {
      name: "Sebastian Biagioni",
      title: "Facilities Expansion Manager",
    },
  });
  assert.equal(opportunity.primaryContact.name, "Suzanne K.");
  assert.equal(
    opportunity.primaryContact.identityStatus,
    "PARTIAL_IDENTITY_ABBREVIATED_SURNAME",
  );
  assert.equal(opportunity.primaryContact.identityVerified, false);
  assert.equal(opportunity.primaryContact.findymailEligible, false);
  assert.ok(
    fieldChanges.some((c) => /PARTIAL_IDENTITY_ABBREVIATED_SURNAME/.test(c)),
  );
  assert.doesNotMatch(fieldChanges.join(","), /IDENTITY_RESOLVED_FROM_LINKEDIN_SLUG/);
});

check("findymail_blocks_abbreviated", () => {
  const r = assessFindymailEmailEligibility(
    { name: "Suzanne K.", organization: "Saronic Technologies" },
    { organizationName: "Saronic Technologies", sources: [{ url: "https://saronic.com" }] },
  );
  assert.equal(r.eligible, false);
  assert.equal(r.reason, "IDENTITY_INCOMPLETE");

  const lily = assessFindymailEmailEligibility(
    {
      name: "Lily Yuan",
      organization: "Bora Biologics",
      email: "lily.yuan@borabiologics.com",
    },
    { organizationName: "Bora Biologics" },
  );
  assert.equal(lily.eligible, false);
  assert.equal(lily.reason, "EMAIL_ALREADY_PRESENT");
});

check("who_how_and_stage4_gates", () => {
  const whoBad = evaluateWhoHowAbbreviatedIdentityGate({
    fullName: "Suzanne K.",
    identityVerified: true,
  });
  assert.equal(whoBad.ok, false);

  const whoOk = evaluateWhoHowAbbreviatedIdentityGate({
    fullName: "Lily Yuan",
    identityVerified: true,
  });
  assert.equal(whoOk.ok, true);

  const s4 = evaluateStage4AbbreviatedIdentityGate(
    { name: "Suzanne K.", organization: "Saronic" },
    { organizationName: "Saronic", sources: [{ url: "https://saronic.com" }] },
  );
  assert.equal(s4.findymailEligible, false);
  assert.equal(s4.ok, true); // not eligible → gate ok (blocked correctly)
});

check("identity_preflight_splits_abbreviated_surname", () => {
  const parts = splitContactPersonName("Suzanne K.");
  assert.equal(parts.firstName, "Suzanne");
  assert.equal(parts.lastName, null);
  assert.equal(parts.surnameAbbreviated, true);
});

check("presentation_sync_and_hotel_audit", () => {
  const synced = syncOpportunityContactPresentationNames({
    primaryContact: { name: "Lily Yuan", fullName: "Lily Yuan" },
    contactName: "Lily Y.",
    tileContactPreview: { name: "Lily Y." },
  });
  assert.equal(synced.changed, true);
  assert.equal(synced.opportunity.contactName, "Lily Yuan");
  assert.equal(synced.opportunity.tileContactPreview.name, "Lily Yuan");

  const hotel = evaluateHotelAbbreviatedIdentityGates([
    synced.opportunity,
    {
      id: "suz",
      primaryContact: {
        name: "Suzanne K.",
        identityVerified: false,
        identityStatus: "PARTIAL_IDENTITY_ABBREVIATED_SURNAME",
      },
    },
  ]);
  assert.equal(hotel.presentationDrift.length, 0);
  assert.ok(hotel.audit.flagged >= 1);
});

check("ui_prefers_primary_over_abbrev_tile", () => {
  const src = fs.readFileSync(
    new URL(
      "../public/js/group-demand-intelligence/dealality-gdi-ui.js",
      import.meta.url,
    ),
    "utf8",
  );
  assert.match(src, /isAbbrevSurnameLocal/);
  assert.match(src, /Prefer fuller primaryContact name/);
});

if (failed) {
  console.error(`\n${failed} failure(s)`);
  process.exit(1);
}
console.log("\nOK test:gdi-abbreviated-contact-identity-v1");
