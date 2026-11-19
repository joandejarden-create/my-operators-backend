/**
 * Regression: package + locked customer-facing paths must emit the shared
 * structured Who-to-Contact card DTO (scalingTwoContactPresentation) so the
 * main GDI drawer matches the approved comparison renderer.
 */
import assert from "node:assert/strict";
import { attachPackageCustomerPresentation } from "../lib/group-demand-intelligence/publication-package/gdi-package-customer-presentation-attach-v1.js";
import { prepareCustomerFacingOpportunity } from "../lib/group-demand-intelligence/share/gdi-customer-facing-opportunity-contract-v1.js";

const EUROGUSS_LIKE = Object.freeze({
  id: "opp-euroguss-mexico-2027",
  title: "EUROGUSS MEXICO 2027",
  organizationName: "NMNA",
  primaryContact: {
    name: "Gissella Palma",
    role: "Project Manager",
    organization: "NMNA",
    email: "gissella.palma@nmna.us",
    phone: null,
    linkedInUrl: "https://www.linkedin.com/in/example-gissella/",
    commercialContactPathLabel: "Named person: direct business email",
    whyContact:
      "Primary organizer contact for EUROGUSS Mexico hotel sourcing.",
    isNamedCommerciallyRelevant: true,
    buyerEligible: true,
  },
  secondaryContact: null,
});

function assertStructuredPrimary(label, opportunity) {
  const two = opportunity?.scalingTwoContactPresentation;
  assert.ok(two?.primary, `${label}: missing scalingTwoContactPresentation.primary`);
  assert.equal(two.primary.customerFacingNamed, true, `${label}: not named`);
  assert.match(
    String(two.primary.name || two.primary.displayName || ""),
    /Gissella Palma/i,
    `${label}: wrong name`,
  );
  assert.equal(
    two.primary.email,
    "gissella.palma@nmna.us",
    `${label}: verified email missing`,
  );
  assert.notEqual(
    two.primary.email,
    "alex.buyer@fixture.example",
    `${label}: mock fixture email leaked`,
  );
  assert.notEqual(
    two.primary.phone,
    "+1-202-555-0199",
    `${label}: mock fixture phone leaked`,
  );
}

const packaged = attachPackageCustomerPresentation(
  { ...EUROGUSS_LIKE },
  { packageId: "pkg-test" },
);
assertStructuredPrimary("package-attach", packaged);

const locked = prepareCustomerFacingOpportunity({
  ...EUROGUSS_LIKE,
  publishedPresentationLocked: true,
  gdiPublishedInventoryRegistry: true,
});
assertStructuredPrimary("locked-customer-facing", locked);

console.log(
  JSON.stringify(
    {
      ok: true,
      test: "gdi-drawer-contact-card-parity-v1",
      packageHasTwoContact: true,
      lockedHasTwoContact: true,
      mockFixtureBlocked: true,
    },
    null,
    2,
  ),
);
