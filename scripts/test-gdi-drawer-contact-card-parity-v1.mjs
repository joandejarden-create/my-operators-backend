/**
 * Regression: package + locked + detail parity must emit structured
 * Who-to-Contact DTO from stored authorized data only — no outbound HTTP,
 * no mock Surfe fixture channels, missing channels stay missing.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { attachPackageCustomerPresentation } from "../lib/group-demand-intelligence/publication-package/gdi-package-customer-presentation-attach-v1.js";
import { prepareCustomerFacingOpportunity } from "../lib/group-demand-intelligence/share/gdi-customer-facing-opportunity-contract-v1.js";
import {
  ensureDrawerContactParityPresentation,
  stripMockFixtureContactChannels,
} from "../lib/group-demand-intelligence/presentation/gdi-drawer-contact-parity-presentation-v1.js";
import {
  recoverPublicContactChannels,
} from "../lib/group-demand-intelligence/presentation/gdi-public-contact-channel-hierarchy-v1.js";
import {
  validatePublicOutboundUrl,
  isPrivateOrLocalIp,
} from "../lib/group-demand-intelligence/presentation/gdi-safe-outbound-fetch-v1.js";

const EUROGUSS_LIKE = Object.freeze({
  id: "opp-euroguss-mexico-2027",
  title: "EUROGUSS MEXICO 2027",
  organizationName: "NMNA",
  primaryContact: {
    name: "Gissella Palma",
    role: "Project Manager",
    organization: "NMNA",
    email: "gissella.palma@nmna.us",
    phone: "+1 (770) 618-5833",
    linkedInUrl: "https://www.linkedin.com/in/example-gissella/",
    commercialContactPathLabel: "Named person: direct business email",
    whyContact:
      "Primary organizer contact for EUROGUSS Mexico hotel sourcing.",
    isNamedCommerciallyRelevant: true,
    buyerEligible: true,
    contactDataOrigin: "PUBLIC_SOURCE",
    channelVerificationStatus: "PUBLICLY_VERIFIED",
  },
  secondaryContact: null,
});

const EUROGUSS_NAME_ONLY = Object.freeze({
  id: "opp-euroguss-name-only",
  title: "EUROGUSS MEXICO 2027",
  organizationName: "NMNA",
  primaryContact: {
    name: "Gissella Palma",
    title: "Sales Manager",
    organization: "NürnbergMesse North America / EUROGUSS MEXICO team",
    contactClass: "RELEVANT_COMMERCIAL_ENTRY_POINT",
  },
  sources: [
    { url: "https://www.euroguss-mexico.com/contacts", statement: "contacts" },
  ],
});

function assertStructuredPrimary(label, opportunity, { requireChannels = true } = {}) {
  const two = opportunity?.scalingTwoContactPresentation;
  assert.ok(two?.primary, `${label}: missing scalingTwoContactPresentation.primary`);
  assert.equal(two.primary.customerFacingNamed, true, `${label}: not named`);
  assert.match(
    String(two.primary.name || two.primary.displayName || ""),
    /Gissella Palma/i,
    `${label}: wrong name`,
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
  if (requireChannels) {
    assert.equal(
      two.primary.email,
      "gissella.palma@nmna.us",
      `${label}: verified email missing`,
    );
  }
}

// --- Package + locked paths with stored verified channels ---
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

// --- Detail path: stored verified channels → structured card; no outbound ---
const fetchCalls = [];
const originalFetch = globalThis.fetch;
globalThis.fetch = async (...args) => {
  fetchCalls.push(args[0]);
  throw new Error("UNEXPECTED_OUTBOUND_FETCH_ON_DETAIL_PATH");
};
try {
  const fromStored = await ensureDrawerContactParityPresentation(
    { ...EUROGUSS_LIKE },
    { timeoutMs: 1, maxUrls: 5 },
  );
  assertStructuredPrimary("detail-stored-channels", fromStored);
  assert.equal(fromStored.publicChannelRecoveryMeta?.outboundFetch, false);
  assert.equal(fromStored.publicChannelRecoveryMeta?.reason, "STORED_DATA_ONLY");
  assert.equal(fetchCalls.length, 0, "detail path must not outbound-fetch");

  // Name-only inventory: still structured card; channels explicitly absent
  const nameOnly = await ensureDrawerContactParityPresentation({
    ...EUROGUSS_NAME_ONLY,
  });
  assertStructuredPrimary("detail-name-only", nameOnly, {
    requireChannels: false,
  });
  assert.equal(
    Boolean(nameOnly.scalingTwoContactPresentation?.primary?.email),
    false,
    "must not invent email when not stored",
  );
  assert.equal(
    Boolean(nameOnly.scalingTwoContactPresentation?.primary?.phone),
    false,
    "must not invent phone when not stored",
  );
  assert.equal(fetchCalls.length, 0, "name-only detail must not outbound-fetch");

  // Mock fixture channels stripped on customer path
  const withMock = await ensureDrawerContactParityPresentation({
    ...EUROGUSS_NAME_ONLY,
    primaryContact: {
      ...EUROGUSS_NAME_ONLY.primaryContact,
      email: "alex.buyer@fixture.example",
      phone: "+1-202-555-0199",
    },
  });
  assert.notEqual(
    withMock.scalingTwoContactPresentation?.primary?.email,
    "alex.buyer@fixture.example",
  );
  assert.equal(withMock.mockFixtureChannelsStripped, true);
  assert.equal(fetchCalls.length, 0);
} finally {
  globalThis.fetch = originalFetch;
}

const stripped = stripMockFixtureContactChannels({
  primaryContact: {
    name: "X",
    email: "alex.buyer@fixture.example",
    phone: "+1-202-555-0199",
  },
});
assert.equal(stripped.primaryContact.email, null);
assert.equal(stripped.primaryContact.phone, null);

// --- SSRF guards on research-only recovery path ---
assert.equal(isPrivateOrLocalIp("127.0.0.1"), true);
assert.equal(isPrivateOrLocalIp("10.0.0.1"), true);
assert.equal(isPrivateOrLocalIp("169.254.169.254"), true);
assert.equal(isPrivateOrLocalIp("192.168.1.1"), true);
assert.equal(isPrivateOrLocalIp("8.8.8.8"), false);

const loopback = await validatePublicOutboundUrl("http://127.0.0.1/contacts");
assert.equal(loopback.ok, false);
assert.match(String(loopback.reason), /blocked/);

const metadata = await validatePublicOutboundUrl(
  "http://169.254.169.254/latest/meta-data/",
);
assert.equal(metadata.ok, false);

const localhostName = await validatePublicOutboundUrl(
  "http://localhost/admin",
);
assert.equal(localhostName.ok, false);

const researchFetchCalls = [];
const recoveredBlocked = await recoverPublicContactChannels(
  {
    ...EUROGUSS_NAME_ONLY,
    sources: [
      { url: "http://127.0.0.1/secret" },
      { url: "http://169.254.169.254/latest/meta-data/" },
      { url: "http://192.168.0.5/contacts" },
    ],
  },
  {
    fetchImpl: async (url) => {
      researchFetchCalls.push(String(url));
      return {
        ok: true,
        status: 200,
        headers: { get: () => "text/html" },
        arrayBuffer: async () => Buffer.from("<html></html>"),
      };
    },
    skipDns: true,
  },
);
assert.equal(
  researchFetchCalls.length,
  0,
  "private/metadata URLs must not be fetched",
);
assert.ok(
  (recoveredBlocked.rejectedUrls || []).length >= 2,
  "rejectedUrls should record SSRF blocks",
);

// Redirect-to-private: first hop public (mocked), Location private → no follow fetch of private
const redirectCalls = [];
const recoveredRedirect = await recoverPublicContactChannels(
  {
    primaryContact: { name: "Gissella Palma", title: "Sales Manager" },
    sources: [{ url: "https://example.com/contacts" }],
  },
  {
    skipDns: true,
    fetchImpl: async (url) => {
      redirectCalls.push(String(url));
      if (String(url).includes("example.com")) {
        return {
          ok: false,
          status: 302,
          headers: {
            get: (h) =>
              String(h).toLowerCase() === "location"
                ? "http://127.0.0.1/steal"
                : null,
          },
          arrayBuffer: async () => Buffer.alloc(0),
        };
      }
      throw new Error(`UNEXPECTED_PRIVATE_REDIRECT_FETCH:${url}`);
    },
  },
);
assert.ok(
  redirectCalls.every((u) => !/127\.0\.0\.1|169\.254/.test(u)),
  "must not fetch redirect-to-private destination",
);
assert.ok(
  (recoveredRedirect.rejectedUrls || []).some((r) =>
    /blocked|redirect/i.test(String(r.reason)),
  ),
);

// Locked package bytes unchanged by attach (presentation-only fields OK;
// primaryContact email/phone identity preserved, no mock injection)
const lockedFrozen = {
  ...EUROGUSS_LIKE,
  publishedPresentationLocked: true,
  gdiPublishedInventoryRegistry: true,
  scalingTwoContactPresentation: {
    primary: {
      name: "Gissella Palma",
      displayName: "Gissella Palma",
      customerFacingNamed: true,
      email: "gissella.palma@nmna.us",
      phone: "+1 (770) 618-5833",
    },
  },
};
const lockedAgain = prepareCustomerFacingOpportunity(lockedFrozen);
assert.equal(
  lockedAgain.scalingTwoContactPresentation?.primary?.email,
  "gissella.palma@nmna.us",
);
assert.equal(
  lockedAgain.primaryContact?.email,
  EUROGUSS_LIKE.primaryContact.email,
);

// Main package vs comparison-style unify produce same structured card from same data
const mainDto = attachPackageCustomerPresentation({ ...EUROGUSS_LIKE });
const detailDto = await ensureDrawerContactParityPresentation({
  ...EUROGUSS_LIKE,
});
assert.equal(
  mainDto.scalingTwoContactPresentation?.primary?.email,
  detailDto.scalingTwoContactPresentation?.primary?.email,
);
assert.equal(
  mainDto.scalingTwoContactPresentation?.primary?.name,
  detailDto.scalingTwoContactPresentation?.primary?.name,
);

// Sheraton inventory EUROGUSS: name present; channels may be absent until
// durable persistence is certified — still must not outbound-fetch.
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
let eurogussInventory = null;
if (fs.existsSync(sheratonPath)) {
  const invFetchCalls = [];
  globalThis.fetch = async (...args) => {
    invFetchCalls.push(args[0]);
    throw new Error("UNEXPECTED_OUTBOUND_FETCH_ON_INVENTORY_DETAIL");
  };
  try {
    const raw = JSON.parse(fs.readFileSync(sheratonPath, "utf8"));
    const opps = Array.isArray(raw) ? raw : raw.opportunities || [];
    const euro = opps.find((o) => o?.id === "paid3_565ce4588e6d");
    assert.ok(euro, "EUROGUSS fixture opportunity missing from Sheraton inventory");
    const presented = await ensureDrawerContactParityPresentation(euro);
    assert.equal(
      presented.scalingTwoContactPresentation?.primary?.customerFacingNamed,
      true,
    );
    assert.match(
      String(
        presented.scalingTwoContactPresentation?.primary?.name ||
          presented.scalingTwoContactPresentation?.primary?.displayName ||
          "",
      ),
      /Gissella Palma/i,
    );
    assert.equal(invFetchCalls.length, 0);
    eurogussInventory = {
      hasStoredEmail: Boolean(
        presented.scalingTwoContactPresentation?.primary?.email,
      ),
      hasStoredPhone: Boolean(
        presented.scalingTwoContactPresentation?.primary?.phone,
      ),
      outboundFetch: presented.publicChannelRecoveryMeta?.outboundFetch,
      reason: presented.publicChannelRecoveryMeta?.reason,
    };
  } finally {
    globalThis.fetch = originalFetch;
  }
}

console.log(
  JSON.stringify(
    {
      ok: true,
      test: "gdi-drawer-contact-card-parity-v1",
      packageHasTwoContact: true,
      lockedHasTwoContact: true,
      detailStoredDataOnly: true,
      noOutboundOnDetail: true,
      mockFixtureBlocked: true,
      ssrfPrivateRejected: true,
      ssrfRedirectToPrivateRejected: true,
      eurogussInventory,
    },
    null,
    2,
  ),
);
