#!/usr/bin/env node
/**
 * Permanent gate: Stage 4 LinkedIn mapping + Wyndham contact hydration.
 *   npm run test:gdi-contact-research-hydration-v1
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  collectVerifiedChannelsForPerson,
  resolvePersonLinkedInUrl,
} from "../lib/group-demand-intelligence/workflow/gdi-contact-research-stage-v1.js";
import { applyWhoHowResultToOpportunity } from "../lib/group-demand-intelligence/workflow/gdi-who-how-agent-contract-v1.js";
import {
  auditContactCoverage,
  hydrateOpportunityContacts,
  rematerializeWyndhamStagedContactHydration,
  WYNDHAM_HOTEL_ID,
} from "../lib/group-demand-intelligence/publication-package/gdi-wyndham-contact-hydration-rematerialize-v1.js";
import { createGdiPublicationPackageStore } from "../lib/group-demand-intelligence/publication-package/index.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

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

check("resolve_linkedin_from_sourceUrl", () => {
  const li = resolvePersonLinkedInUrl({
    name: "David Aguirre",
    sourceUrl: "https://linkedin.com/in/david-aguirre-27020411",
  });
  assert.equal(li, "https://linkedin.com/in/david-aguirre-27020411");
});

check("stage4_dryrun_accepts_linkedin_sourceUrl_as_usable", () => {
  const collected = collectVerifiedChannelsForPerson({
    name: "David Aguirre",
    title: "Project Manager",
    sourceUrl: "https://linkedin.com/in/david-aguirre-27020411",
  });
  assert.equal(collected.usable.ok, true);
  assert.ok(
    collected.accepted.some((c) => c.channelType === "PERSON_LINKEDIN"),
  );
});

check("who_how_maps_linkedin_from_sourceUrl", () => {
  const next = applyWhoHowResultToOpportunity(
    {},
    {
      whoHowStatus: "VERIFIED_DIRECT_BUYER",
      directBuyer: {
        fullName: "Dede Kassel",
        currentTitle: "Corporate Travel Manager",
        organization: "Flatiron Construction",
        sourceUrl: "https://linkedin.com/in/dede-kassel-b333a122",
        verifiedLinkedIn: null,
        verifiedBusinessEmail: null,
        verifiedBusinessPhone: null,
        whyWeAreShowingThisContact: "Title confirms lodging authority",
      },
      entryPoints: [],
      at: new Date().toISOString(),
    },
  );
  assert.equal(
    next.primaryContact.linkedinUrl,
    "https://linkedin.com/in/dede-kassel-b333a122",
  );
});

check("hydrate_maps_linkedin_and_titles", () => {
  const { opportunity, fieldChanges } = hydrateOpportunityContacts({
    opportunityId: "disc_x",
    primaryContact: {
      name: "Kathryn Newstead",
      title: null,
      email: "knewstead@downtownsandiego.org",
      phone: "619-933-0032",
    },
  });
  assert.equal(opportunity.primaryContact.title, "Director of Events");
  assert.ok(fieldChanges.some((c) => c.includes("TITLE_RECOVERED")));
});

check("wyndham_staged_tip_has_linkedin_fields_after_hydration", () => {
  const store = createGdiPublicationPackageStore({});
  const staged = store.readPointer(WYNDHAM_HOTEL_ID, "STAGED");
  assert.ok(staged?.publicationVersion);
  assert.match(String(staged.publicationVersion), /20261127_/);
  const loaded = store.loadPackage(WYNDHAM_HOTEL_ID, staged.publicationVersion);
  assert.equal(loaded.ok, true);
  assert.equal(loaded.package.opportunityCount, 26);
  const audit = auditContactCoverage(loaded.package.opportunities);
  assert.equal(audit.totalOpportunities, 26);
  assert.equal(audit.namedContacts, 26);
  assert.equal(audit.verifiedTitles, 26);
  assert.equal(audit.verifiedPersonalLinkedIn, 14);
  assert.equal(
    audit.rows.filter((r) => r.linkedinFieldSet).length,
    14,
  );
  assert.equal(audit.verifiedEmail, 11);
  assert.equal(audit.verifiedTelephone, 7);
});

check("dto_passes_linkedinUrl", () => {
  // Dynamic import avoided — sync require via createRequire not needed; read source contract.
  const dtoSrc = fs.readFileSync(
    path.join(ROOT, "lib/group-demand-intelligence/opportunity-list-dto.js"),
    "utf8",
  );
  assert.match(dtoSrc, /linkedinUrl/);
  assert.match(dtoSrc, /resolveListLinkedInUrl/);
});

check("dry_run_rematerialize_ok", () => {
  const r = rematerializeWyndhamStagedContactHydration({ dryRun: true });
  assert.equal(r.ok, true);
  assert.equal(r.opportunityCount, 26);
});

check("list_html_cache_bust_updated", () => {
  const html = fs.readFileSync(
    path.join(ROOT, "public/group-demand-intelligence.html"),
    "utf8",
  );
  assert.match(html, /gdi-commercial-list-table-v1\.js\?v=gdi-list-cols-simplify-20261127/);
});

if (failed) {
  console.error(`\n${failed} failure(s)`);
  process.exit(1);
}
console.log("\nOK test:gdi-contact-research-hydration-v1");
