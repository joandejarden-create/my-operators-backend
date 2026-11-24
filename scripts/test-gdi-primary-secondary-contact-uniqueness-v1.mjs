#!/usr/bin/env node
/**
 * Permanent regression: primary and secondary must be distinct verified people.
 * $0 mocked — no paid providers.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  isSameVerifiedPerson,
  enforceDistinctPrimarySecondaryContacts,
  extractVerifiedNamedContacts,
  evaluateNamedContactCoverageStandard,
  attachNamedContactCoverageMetadata,
  NAMED_CONTACT_COVERAGE,
} from "../lib/group-demand-intelligence/workflow/gdi-named-contact-coverage-standard-v1.js";
import { attachSecondaryNamedContact } from "../lib/group-demand-intelligence/secondary-named-contact-v1.js";
import {
  selectBestCommercialContacts,
  projectSelectionOntoOpportunity,
} from "../lib/group-demand-intelligence/presentation/gdi-commercial-contact-selection-v1.js";
import { runBuyerAlternativePersonResearch } from "../lib/group-demand-intelligence/workflow/gdi-buyer-alternative-person-research-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(
  ROOT,
  "reports/gdi-primary-secondary-contact-uniqueness-v1",
);

let passed = 0;
function check(name, fn) {
  fn();
  passed += 1;
  console.log(`  PASS ${name}`);
}
async function checkAsync(name, fn) {
  await fn();
  passed += 1;
  console.log(`  PASS ${name}`);
}

check("1_isSameVerifiedPerson_by_name", () => {
  assert.equal(
    isSameVerifiedPerson({ name: "Jason Gooding" }, { name: "jason gooding" }),
    true,
  );
  assert.equal(
    isSameVerifiedPerson({ name: "Jason Gooding" }, { name: "Luísa Marques" }),
    false,
  );
});

check("2_enforce_clears_duplicate_secondary", () => {
  const { opportunity, clearedDuplicateSecondary } =
    enforceDistinctPrimarySecondaryContacts({
      primaryContact: { name: "Jason Gooding", role: "Organizer" },
      secondaryContact: { name: "Jason Gooding", role: "Organizer" },
    });
  assert.equal(clearedDuplicateSecondary, true);
  assert.equal(opportunity.secondaryContact, undefined);
  assert.equal(opportunity.primaryContact.name, "Jason Gooding");
});

check("3_extract_ignores_duplicate_secondary", () => {
  const { primary, secondary } = extractVerifiedNamedContacts({
    primaryContact: { name: "Filomena Marcelo", role: "Partner" },
    secondaryContact: { name: "Filomena Marcelo", role: "Partner" },
  });
  assert.equal(primary?.name, "Filomena Marcelo");
  assert.equal(secondary, null);
  const ev = evaluateNamedContactCoverageStandard({
    primaryContact: { name: "Filomena Marcelo", role: "Partner" },
    secondaryContact: { name: "Filomena Marcelo", role: "Partner" },
  });
  assert.equal(ev.namedCount, 1);
  assert.equal(ev.coverage, NAMED_CONTACT_COVERAGE.ONE_VERIFIED);
});

check("4_attachSecondary_does_not_preserve_duplicate", () => {
  const next = attachSecondaryNamedContact({
    organizationName: "MeetingPoint",
    primaryContact: { name: "Filomena Marcelo", role: "Partner" },
    secondaryContact: { name: "Filomena Marcelo", role: "Partner" },
    namedStakeholders: [
      { name: "Filomena Marcelo", title: "Partner", organization: "MeetingPoint" },
      {
        name: "Luísa Marques",
        title: "Partner",
        organization: "MeetingPoint",
        sourceUrl: "https://www.meetingpoint.pt/en/our-world/",
      },
    ],
  });
  assert.notEqual(
    String(next.secondaryContact?.name || "").toLowerCase(),
    "filomena marcelo",
  );
  if (next.secondaryContact?.name) {
    assert.equal(next.secondaryContact.name, "Luísa Marques");
  }
});

check("5_projectSelection_clears_duplicate_secondary", () => {
  const selection = selectBestCommercialContacts(
    [
      {
        name: "João Freire",
        title: "Organizing Committee Chair",
        organization: "IPBA",
        sourceUrl: "https://ipba2026lisbon.org/",
      },
    ],
    { organizationName: "IPBA" },
  );
  const projected = projectSelectionOntoOpportunity(
    {
      organizationName: "IPBA",
      secondaryContact: { name: "João Freire", role: "Chair" },
    },
    selection,
  );
  assert.equal(projected.opportunity.primaryContact?.name, "João Freire");
  assert.ok(
    !projected.opportunity.secondaryContact ||
      projected.opportunity.secondaryContact.name !== "João Freire",
  );
});

check("6_attachMetadata_does_not_fabricate_attempt", () => {
  const opp = attachNamedContactCoverageMetadata({
    primaryContact: { name: "Jordan Lee", role: "Travel Manager" },
  });
  assert.equal(opp.secondContactAttempted, false);
  const ev = evaluateNamedContactCoverageStandard(opp);
  assert.equal(ev.secondaryResearchStatus, "NOT_ATTEMPTED");
});

await checkAsync("7_alt_person_rejects_duplicate_secondary", async () => {
  const r = await runBuyerAlternativePersonResearch({
    organizationName: "MeetingPoint",
    primaryContact: {
      name: "Filomena Marcelo",
      title: "Partner",
      organization: "MeetingPoint",
      sourceUrl: "https://www.meetingpoint.pt/en/our-world/",
    },
    secondaryContact: {
      name: "Filomena Marcelo",
      title: "Partner",
      organization: "MeetingPoint",
    },
    namedStakeholders: [
      {
        name: "Filomena Marcelo",
        title: "Partner",
        organization: "MeetingPoint",
        sourceUrl: "https://www.meetingpoint.pt/en/our-world/",
      },
      {
        name: "Luísa Marques",
        title: "Partner",
        organization: "MeetingPoint",
        sourceUrl: "https://www.meetingpoint.pt/en/our-world/",
      },
    ],
  });
  assert.equal(r.opportunity.primaryContact?.name, "Filomena Marcelo");
  assert.notEqual(
    String(r.opportunity.secondaryContact?.name || "").toLowerCase(),
    "filomena marcelo",
  );
});

fs.mkdirSync(OUT, { recursive: true });
fs.writeFileSync(
  path.join(OUT, "CERTIFICATION.json"),
  JSON.stringify(
    {
      contract: "gdi-primary-secondary-contact-uniqueness-v1",
      at: new Date().toISOString(),
      passed,
      status: "CERTIFICATION_PASS",
    },
    null,
    2,
  ),
);
console.log(`\n${passed} checks evaluated\nCERTIFICATION_PASS`);
