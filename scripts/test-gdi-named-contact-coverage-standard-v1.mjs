#!/usr/bin/env node
/**
 * Permanent regression — min-1 / target-2 named contact coverage for Full E2E.
 * $0 mocked; no paid providers.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(ROOT, "reports/gdi-named-contact-coverage-standard-v1");

const {
  evaluateNamedContactCoverageStandard,
  assertFinalizedPackageNamedContactCoverage,
  attachNamedContactCoverageMetadata,
  FINALIZATION_CONTACT_DISPOSITION,
  SECONDARY_CONTACT_RESEARCH_STATUS,
} = await import(
  "../lib/group-demand-intelligence/workflow/gdi-named-contact-coverage-standard-v1.js"
);
const {
  evaluateContactCompletenessForFinalization,
} = await import(
  "../lib/group-demand-intelligence/presentation/gdi-verified-commercial-route-v1.js"
);
const {
  runRestoredContactInvestigationPass,
  isPlausibleBuyerPersonName,
} = await import(
  "../lib/group-demand-intelligence/workflow/gdi-original-full-e2e-execution-restore-v1.js"
);
const {
  runPublicationQualityGate,
} = await import(
  "../lib/group-demand-intelligence/publication-package/gdi-publication-quality-gate-v1.js"
);

const results = [];
function check(name, fn) {
  try {
    fn();
    results.push({ name, ok: true });
    console.log(`PASS ${name}`);
  } catch (err) {
    results.push({ name, ok: false, error: String(err?.message || err) });
    console.error(`FAIL ${name}: ${err?.message || err}`);
  }
}

check("1_zero_named_is_research_continuation", () => {
  const ev = evaluateNamedContactCoverageStandard({
    organizationName: "Nestlé",
    primaryContact: { name: null },
    commercialContactRoute: "https://www.nestle.com/contact",
  });
  assert.equal(ev.finalizeEligible, false);
  assert.equal(
    ev.disposition,
    FINALIZATION_CONTACT_DISPOSITION.RESEARCH_CONTINUATION,
  );
  const fin = evaluateContactCompletenessForFinalization({
    organizationName: "Nestlé",
    commercialContactRoute: "https://www.nestle.com/contact-us",
    approvedCommercialRoute: "Events contact",
  });
  assert.equal(fin.complete, false);
});

check("2_one_named_finalizes_with_secondary_attempt_doc", () => {
  const opp = attachNamedContactCoverageMetadata(
    {
      organizationName: "Acme",
      primaryContact: {
        name: "Jordan Lee",
        role: "Travel Manager",
        organization: "Acme",
      },
    },
    {
      secondContactAttempted: true,
      secondContactFound: false,
      secondContactDisposition: "NEEDS_ADDITIONAL_PUBLIC_RESEARCH",
    },
  );
  const ev = evaluateNamedContactCoverageStandard(opp);
  assert.equal(ev.namedCount, 1);
  assert.equal(
    ev.disposition,
    FINALIZATION_CONTACT_DISPOSITION.FINALIZED_ONE_NAMED,
  );
  assert.equal(
    ev.secondaryResearchStatus,
    SECONDARY_CONTACT_RESEARCH_STATUS.ATTEMPTED_NO_SECOND_VERIFIED,
  );
});

check("3_two_named_is_desired_coverage", () => {
  const ev = evaluateNamedContactCoverageStandard({
    organizationName: "Acme",
    primaryContact: {
      name: "Jordan Lee",
      role: "Travel Manager",
      organization: "Acme",
      buyerClassification: "VERIFIED_VIABLE_ALTERNATIVE",
    },
    secondaryContact: {
      name: "Sam Ortiz",
      role: "Director of Meetings",
      organization: "Acme",
      buyerClassification: "SECONDARY_CONTACT",
    },
  });
  assert.equal(ev.namedCount, 2);
  assert.equal(
    ev.disposition,
    FINALIZATION_CONTACT_DISPOSITION.FINALIZED_TWO_NAMED,
  );
});

check("4_package_gate_blocks_zero_named", () => {
  const gate = assertFinalizedPackageNamedContactCoverage([
    {
      id: "a",
      organizationName: "Acme",
      primaryContact: {
        name: "Jordan Lee",
        role: "Travel Manager",
        organization: "Acme",
        buyerClassification: "VERIFIED_VIABLE_ALTERNATIVE",
      },
      secondaryContactResearchStatus: "ATTEMPTED_NO_SECOND_VERIFIED",
      secondContactAttempted: true,
    },
    {
      id: "b",
      organizationName: "Nestlé",
      primaryContact: { name: null },
    },
  ]);
  assert.equal(gate.ok, false);
  assert.ok(
    gate.blockers.some((b) => b.code === "ZERO_VERIFIED_NAMED_CONTACTS"),
  );
});

check("5_package_gate_requires_secondary_attempt_docs_when_enabled", () => {
  const gate = assertFinalizedPackageNamedContactCoverage(
    [
      {
        id: "a",
        primaryContact: {
          name: "Jordan Lee",
          role: "Travel Manager",
          organization: "Acme",
          buyerClassification: "VERIFIED_VIABLE_ALTERNATIVE",
        },
      },
    ],
    { requireSecondaryAttemptDocumentation: true },
  );
  assert.equal(gate.ok, false);
  assert.ok(
    gate.blockers.some(
      (b) => b.code === "SECONDARY_CONTACT_RESEARCH_UNDOCUMENTED",
    ),
  );
});

check("6_contact_investigation_attaches_coverage_metadata", () => {
  const pass = runRestoredContactInvestigationPass({
    organizationName: "Acme Meetings Inc",
    primaryContact: {
      name: "Jordan Lee",
      role: "Meetings Director",
      organization: "Acme Meetings Inc",
    },
    evidence: [
      {
        url: "https://acme.example/team",
        statement:
          "Jordan Lee is Meetings Director at Acme Meetings Inc responsible for hotel sourcing.",
      },
    ],
  });
  assert.equal(pass.pass.secondContactAttempted, true);
  assert.ok(pass.opportunity.contactCoverage);
  assert.ok(
    isPlausibleBuyerPersonName(pass.opportunity.primaryContact?.name) ||
      pass.pass.contactResearchComplete === true,
  );
});

check("7_publication_quality_gate_blocks_zero_named_in_staged", () => {
  const q = runPublicationQualityGate({
    opportunities: [
      {
        id: "x",
        organizationName: "Org",
        whatIsHappening: "Event happening with lodging need for attendees.",
        potentialHotelBusiness: "Rooms for attendees during the conference.",
        whyThisHotel: "Location fits the event geography.",
        whyActNow: "Decision window is open.",
        recommendedNextStep: "Research outreach once a named buyer is verified.",
        primaryContact: { name: null },
      },
    ],
    enforceCommercialExpirationGate: false,
    enforceCommercialActionabilityGate: false,
  });
  assert.equal(q.ok, false);
  assert.ok(
    q.blocking.some((b) => b.code === "ZERO_VERIFIED_NAMED_CONTACTS"),
  );
});

check("8_org_route_alone_cannot_finalize", () => {
  const fin = evaluateContactCompletenessForFinalization({
    approvedCommercialRoute: "Housing bureau contact",
    commercialContactRoute: "https://example.org/housing",
  });
  assert.equal(fin.complete, false);
  assert.match(String(fin.mode || fin.reason || ""), /RESEARCH_CONTINUATION|INCOMPLETE|not sufficient/i);
});

check("9_named_without_email_still_satisfies_minimum", () => {
  const fin = evaluateContactCompletenessForFinalization({
    primaryContact: {
      name: "Ana Ribeiro",
      role: "Housing Manager",
      email: null,
      emailVerificationStatus: "NO_EMAIL",
    },
  });
  assert.equal(fin.complete, true);
  assert.equal(fin.mode, "NAMED_PERSON");
});

check("10_design_hotels_staged_packages_have_no_zero_named", () => {
  const hotels = [
    "rece0NdV9FCnaFlVS",
    "recaokE8psIbuVeuH",
    "rec1t35e3sPerzxYu",
  ];
  for (const hotelId of hotels) {
    const ptr = JSON.parse(
      fs.readFileSync(
        path.join(
          ROOT,
          "lib/group-demand-intelligence/bundled-publication-packages",
          hotelId,
          "STAGED.json",
        ),
        "utf8",
      ),
    );
    const pkg = JSON.parse(
      fs.readFileSync(
        path.join(
          ROOT,
          "lib/group-demand-intelligence/bundled-publication-packages",
          hotelId,
          ptr.publicationVersion,
          "package.json",
        ),
        "utf8",
      ),
    );
    const gate = assertFinalizedPackageNamedContactCoverage(
      pkg.opportunities || [],
      { requireSecondaryAttemptDocumentation: false },
    );
    assert.equal(
      gate.summary.zeroNamed,
      0,
      `${hotelId} has zero-named opportunities`,
    );
    assert.equal(gate.ok || gate.summary.zeroNamed === 0, true);
  }
});

fs.mkdirSync(OUT, { recursive: true });
const report = {
  contract: "gdi-named-contact-coverage-standard-v1",
  at: new Date().toISOString(),
  results,
  pass: results.every((r) => r.ok),
};
fs.writeFileSync(path.join(OUT, "CERTIFICATION.json"), JSON.stringify(report, null, 2));
console.log(`\n${results.length} checks evaluated`);
console.log(report.pass ? "CERTIFICATION_PASS" : "CERTIFICATION_FAIL");
process.exit(report.pass ? 0 : 1);
