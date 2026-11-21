/**
 * Permanent regression gates for GDI Contact Actionability V1.
 */
import assert from "node:assert/strict";
import {
  CONTACT_ACTIONABILITY_TIER,
  classifyContactActionability,
  classifyEmailChannel,
  attachContactActionability,
} from "../lib/group-demand-intelligence/presentation/gdi-contact-actionability-v1.js";
import { toOpportunityListDto } from "../lib/group-demand-intelligence/opportunity-list-dto.js";
import { createGdiPublicationPackageStore } from "../lib/group-demand-intelligence/publication-package/gdi-publication-package-store-v1.js";
import { createGdiPublishedInventoryRegistry } from "../lib/group-demand-intelligence/durable-publication/gdi-published-inventory-registry-v1.js";
import { attachPackageCustomerPresentation } from "../lib/group-demand-intelligence/publication-package/gdi-package-customer-presentation-attach-v1.js";

function test(name, fn) {
  try {
    fn();
    console.log(`  PASS ${name}`);
  } catch (err) {
    console.error(`  FAIL ${name}`);
    throw err;
  }
}

console.log("test-gdi-contact-actionability-v1");

test("STRICT_EMAIL is not a publication gate flag", () => {
  const a = classifyContactActionability({
    opportunityId: "x",
    primaryContact: { name: "Jane Doe", title: "Meeting Planner", email: "jane@example.org" },
  });
  assert.equal(a.strictEmailPublicationGate, false);
  assert.equal(a.mutatesCustomerPublication, false);
  assert.equal(a.mutatesOpportunityEligibility, false);
});

test("person_specific email without sourceUrl is not verified", () => {
  const ch = classifyEmailChannel({
    name: "Jane Doe",
    email: "jane@acme.org",
    emailOwnership: "person_specific",
  });
  assert.equal(ch.verified, false);
  assert.equal(ch.status, "PERSON_SPECIFIC_WITHOUT_SOURCE_URL");
});

test("source-confirmed named email → SALES_READY", () => {
  const a = classifyContactActionability({
    opportunityId: "y",
    primaryContact: {
      name: "Lara Schneider",
      title: "Congress Organiser",
      email: "lara@wearemci.com",
      emailKind: "NAMED_DIRECT",
      sourceUrl: "https://example.com/contact",
      contactClass: "RELEVANT_COMMERCIAL_ENTRY_POINT",
    },
  });
  assert.equal(a.contactActionabilityTier, CONTACT_ACTIONABILITY_TIER.SALES_READY);
  assert.equal(a.buyerVsEntry, "RELEVANT_COMMERCIAL_ENTRY_POINT");
  assert.notEqual(a.buyerVsEntryLabel, "Direct buyer");
});

test("direct buyer label distinct from entry point", () => {
  const a = classifyContactActionability({
    opportunityId: "z",
    primaryContact: {
      name: "Alex Buyer",
      title: "Hotel Procurement Manager",
      email: "alex@org.org",
      emailKind: "NAMED_DIRECT",
      sourceUrl: "https://example.com/staff",
      contactClass: "DIRECT_BUYER",
    },
  });
  assert.equal(a.buyerVsEntry, "DIRECT_BUYER");
  assert.match(a.buyerVsEntryLabel, /Direct buyer/i);
});

test("named + phone + commercial remit → REACHABLE without verified email", () => {
  const a = classifyContactActionability({
    opportunityId: "p",
    primaryContact: {
      name: "Sam Planner",
      title: "Conference Housing Manager",
      phone: "+1 202 555 0100",
      contactClass: "DIRECT_BUYER",
    },
  });
  assert.equal(a.contactActionabilityTier, CONTACT_ACTIONABILITY_TIER.REACHABLE);
});

test("named without channel → CONTACT_RESEARCH_REQUIRED", () => {
  const a = classifyContactActionability({
    opportunityId: "r",
    primaryContact: { name: "Pat Name", title: "Director" },
  });
  assert.equal(
    a.contactActionabilityTier,
    CONTACT_ACTIONABILITY_TIER.CONTACT_RESEARCH_REQUIRED,
  );
});

test("no named person → CONTACT_IDENTIFICATION_REQUIRED", () => {
  const a = classifyContactActionability({
    opportunityId: "n",
    primaryContact: { name: "Events Team", email: "info@org.org" },
  });
  assert.equal(
    a.contactActionabilityTier,
    CONTACT_ACTIONABILITY_TIER.CONTACT_IDENTIFICATION_REQUIRED,
  );
});

test("inferred/generic mailbox never SALES_READY", () => {
  const a = classifyContactActionability({
    opportunityId: "g",
    primaryContact: {
      name: "Chris Host",
      title: "Event Manager",
      email: "info@example.org",
      sourceUrl: "https://example.org/contact",
      contactClass: "RELEVANT_COMMERCIAL_ENTRY_POINT",
    },
  });
  assert.notEqual(a.contactActionabilityTier, CONTACT_ACTIONABILITY_TIER.SALES_READY);
});

test("list DTO carries actionability without baking publication mutation", () => {
  const dto = toOpportunityListDto({
    opportunityId: "dto1",
    id: "dto1",
    title: "Test Opp",
    primaryContact: {
      name: "Lara Schneider",
      title: "Congress Organiser",
      email: "lara@wearemci.com",
      emailKind: "NAMED_DIRECT",
      sourceUrl: "https://example.com/contact",
      contactClass: "RELEVANT_COMMERCIAL_ENTRY_POINT",
    },
  });
  assert.equal(dto.contactActionabilityTier, "SALES_READY");
  assert.equal(dto.primaryContact.emailCustomerVerified, true);
  assert.equal(dto.buyerVsEntry, "RELEVANT_COMMERCIAL_ENTRY_POINT");
});

test("attach does not strip frozen primaryContact provenance fields", () => {
  const row = attachContactActionability({
    opportunityId: "prov",
    primaryContact: {
      name: "Jane Doe",
      email: "jane@x.org",
      emailOwnership: "person_specific",
      sourceUrl: null,
      title: "Planner",
    },
  });
  assert.equal(row.primaryContact.emailOwnership, "person_specific");
  assert.equal(row.contactEmailCustomerVerified, false);
  assert.equal(row.contactActionabilityTier, "CONTACT_RESEARCH_REQUIRED");
});

test("frozen customer-visible inventory remains 182 with tiers attached", () => {
  const store = createGdiPublicationPackageStore();
  const registry = createGdiPublishedInventoryRegistry();
  const hotelIds = [
    "recLuxvwwxID7U2B8",
    "recH9h3McgMNFQEla",
    "recrPQcZg7SFARRb2",
  ];
  let n = 0;
  const tiers = {
    SALES_READY: 0,
    REACHABLE: 0,
    CONTACT_RESEARCH_REQUIRED: 0,
    CONTACT_IDENTIFICATION_REQUIRED: 0,
  };
  for (const hotelId of hotelIds) {
    const loaded = store.loadCustomerVisiblePackage(hotelId);
    assert.equal(loaded.ok, true, hotelId);
    const attached = attachPackageCustomerPresentation(loaded.package.opportunities);
    for (const row of attached) {
      n += 1;
      assert.ok(row.contactActionabilityTier, row.opportunityId || row.id);
      tiers[row.contactActionabilityTier] =
        (tiers[row.contactActionabilityTier] || 0) + 1;
    }
  }
  let listed = [];
  try {
    listed = registry.listAllHotelsPublished({ includeRevoked: false }) || [];
  } catch {
    listed = [];
  }
  const seen = new Set();
  // Count only registry rows not already in CURRENT hotels above
  for (const r of listed) {
    if (hotelIds.includes(r.hotelId)) continue;
    const key = `${r.hotelId}::${r.opportunityId}`;
    if (seen.has(key)) continue;
    seen.add(key);
    n += 1;
    let snap = null;
    try {
      snap = registry.loadSnapshot(r.hotelId, r.opportunityId, r.publishedVersion);
    } catch {
      snap = null;
    }
    const nested = snap?.snapshot || snap?.opportunity || {};
    const attached = attachContactActionability({
      ...nested,
      opportunityId: r.opportunityId || nested.id,
      primaryContact: nested.primaryContact || {
        name: nested.contactName,
        email: nested.contactEmail,
        phone: nested.contactPhone,
        title: nested.contactRole,
      },
    });
    tiers[attached.contactActionabilityTier] =
      (tiers[attached.contactActionabilityTier] || 0) + 1;
  }
  assert.equal(n, 182, `expected 182 customer-visible, got ${n}`);
  console.log("  inventory freeze tiers", tiers);
});

console.log("test-gdi-contact-actionability-v1: PASS");
