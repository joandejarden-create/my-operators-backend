/**
 * Regression: package + locked customer-facing paths must emit the shared
 * structured Who-to-Contact card DTO (scalingTwoContactPresentation) so the
 * main GDI drawer matches the approved comparison renderer.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { attachPackageCustomerPresentation } from "../lib/group-demand-intelligence/publication-package/gdi-package-customer-presentation-attach-v1.js";
import { prepareCustomerFacingOpportunity } from "../lib/group-demand-intelligence/share/gdi-customer-facing-opportunity-contract-v1.js";
import { ensureDrawerContactParityPresentation } from "../lib/group-demand-intelligence/presentation/gdi-drawer-contact-parity-presentation-v1.js";

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

const root = path.dirname(fileURLToPath(import.meta.url));
const sheratonPath = path.join(
  root,
  "..",
  "data",
  "group-demand-intelligence",
  "hotels",
  "recCAGqkERSXgiku4",
  "opportunities.json",
);
let eurogussRecovery = null;
if (fs.existsSync(sheratonPath)) {
  const raw = JSON.parse(fs.readFileSync(sheratonPath, "utf8"));
  const opps = Array.isArray(raw) ? raw : raw.opportunities || [];
  const euro = opps.find((o) => o?.id === "paid3_565ce4588e6d");
  assert.ok(euro, "EUROGUSS fixture opportunity missing from Sheraton inventory");
  const recovered = await ensureDrawerContactParityPresentation(euro, {
    timeoutMs: 12000,
    maxUrls: 3,
  });
  const primary = recovered.scalingTwoContactPresentation?.primary;
  assert.equal(primary?.customerFacingNamed, true, "EUROGUSS named primary");
  assert.equal(
    primary?.email,
    "gissella.palma@nmna.us",
    "EUROGUSS public email recovered",
  );
  assert.match(
    String(primary?.phone || ""),
    /770/,
    "EUROGUSS public phone recovered",
  );
  assert.notEqual(primary?.email, "alex.buyer@fixture.example");
  eurogussRecovery = {
    email: primary.email,
    phone: primary.phone,
    reason: recovered.publicChannelRecoveryMeta?.reason || null,
  };
}

console.log(
  JSON.stringify(
    {
      ok: true,
      test: "gdi-drawer-contact-card-parity-v1",
      packageHasTwoContact: true,
      lockedHasTwoContact: true,
      mockFixtureBlocked: true,
      eurogussRecovery,
    },
    null,
    2,
  ),
);
