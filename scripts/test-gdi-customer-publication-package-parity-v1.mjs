#!/usr/bin/env node
/**
 * Automated tests for GDI Customer Publication Package + parity gates.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import {
  buildCustomerPublicationPackage,
  fingerprintPublicationContent,
  verifyPublicationPackageIntegrity,
  comparePublicationParity,
  comparePublicationInventory,
  runPublicationQualityGate,
  createGdiPublicationPackageStore,
  resolveShareOpportunitiesFromApprovedPackage,
  mergeShareFeedbackOntoPackageOpportunities,
  validateFounderApprovalReceipt,
  assertPublicationGatesPassed,
  GDI_PUBLICATION_LIFECYCLE_STATE,
  remediateDuplicateOrgAssociationSentence,
  stripEmptyParentheses,
  dedupeTileClassificationSourcing,
  applyCustomerFacingContentRemediation,
  rewritePlaceholderUncertainty,
} from "../lib/group-demand-intelligence/publication-package/index.js";

function sampleOpp(id, overrides = {}) {
  return {
    id,
    displayTitle: overrides.displayTitle || `Opportunity ${id}`,
    title: overrides.title || `Opportunity ${id}`,
    organizationName: overrides.organizationName || "Example Org",
    whatIsHappening: overrides.whatIsHappening || "Example Org hosts a regional program in 2027.",
    whyThisHotel: overrides.whyThisHotel || "Fits catchment and meeting inventory.",
    whyActNow: overrides.whyActNow || "Planning window is open for 2027.",
    potentialHotelBusiness: overrides.potentialHotelBusiness || "Room block opportunity.",
    recommendedNextStep: overrides.recommendedNextStep || "Contact buyer to qualify demand.",
    hotelSourcingStatusLabel:
      overrides.hotelSourcingStatusLabel || "Target-hotel sourcing not established",
    tileClassificationLine:
      overrides.tileClassificationLine || "Example Org · Corporate Event",
    salesActionKey: overrides.salesActionKey || "QUALIFY_OPPORTUNITY",
    salesActionPillLabel: overrides.salesActionPillLabel || "Qualify Opportunity",
    inventoryPopulations: overrides.inventoryPopulations || ["SALES_OPPORTUNITIES"],
    hotelMarketScopeEligible: overrides.hotelMarketScopeEligible ?? true,
    competitiveMarketScopeEligible: overrides.competitiveMarketScopeEligible ?? false,
    primaryContact: { name: "Alex Buyer", title: "Director" },
    sources: [{ url: "https://example.com/event", title: "Event page" }],
    establishedFacts: ["Public event listing confirmed"],
    uncertainties: ["Hotel contracting owner not confirmed"],
    ...overrides,
  };
}

function buildPkg(opps, extra = {}) {
  return buildCustomerPublicationPackage({
    hotelId: "recTESTHOTEL00001",
    hotelName: "Test Hotel",
    opportunities: opps,
    publicationVersion: extra.publicationVersion || "gdi_pub_test_v1",
    applicationCompatibilityVersion: "gdi-customer-app-compat-v1",
    ...extra,
  });
}

// 1. Canonical package generation
{
  const opps = Array.from({ length: 67 }, (_, i) =>
    sampleOpp(`gdi_test_${String(i + 1).padStart(3, "0")}`, {
      inventoryPopulations:
        i < 10
          ? ["SALES_OPPORTUNITIES", "COMPETITIVE_MARKET_INTELLIGENCE"]
          : i < 61
            ? ["SALES_OPPORTUNITIES"]
            : ["COMPETITIVE_MARKET_INTELLIGENCE"],
      hotelMarketScopeEligible: i < 61,
      competitiveMarketScopeEligible: i < 10 || i >= 61,
    }),
  );
  const pkg = buildPkg(opps);
  assert.equal(pkg.opportunityCount, 67);
  assert.equal(pkg.inventoryCounts.all, 67);
  assert.equal(pkg.inventoryCounts.hotel, 61);
  assert.equal(pkg.inventoryCounts.competitive, 16); // 10 overlap + 6 competitive-only in this synthetic set
  assert.ok(pkg.contentFingerprintSha256);
  assert.equal(pkg.contentFingerprintSha256.length, 64);
  console.log("ok canonical package generation");
}

// 2. Stable deterministic hashing
{
  const opps = [sampleOpp("a"), sampleOpp("b")];
  const sortOrder = ["a", "b"];
  const a = buildPkg(opps, { publicationVersion: "hash_v1", sortOrder });
  const b = buildPkg([...opps].reverse(), {
    publicationVersion: "hash_v1",
    sortOrder,
  });
  assert.equal(a.contentFingerprintSha256, b.contentFingerprintSha256);
  const fp1 = fingerprintPublicationContent(a);
  const fp2 = fingerprintPublicationContent(b);
  assert.equal(fp1.contentFingerprintSha256, fp2.contentFingerprintSha256);
  // Different approved sort order must change fingerprint.
  const c = buildPkg(opps, {
    publicationVersion: "hash_v1",
    sortOrder: ["b", "a"],
  });
  assert.notEqual(a.contentFingerprintSha256, c.contentFingerprintSha256);
  console.log("ok stable deterministic hashing");
}

// 3–5. Exact identity / content / missing-extra
{
  const opps = Array.from({ length: 5 }, (_, i) => sampleOpp(`id_${i}`));
  const pkg = buildPkg(opps);
  const inv = comparePublicationInventory(pkg, {
    opportunities: opps,
    sortOrder: pkg.sortOrder,
  });
  assert.equal(inv.ok, true);
  const missing = comparePublicationInventory(pkg, {
    opportunities: opps.slice(0, 3),
  });
  assert.equal(missing.ok, false);
  assert.deepEqual(missing.missingOpportunityIds, ["id_3", "id_4"]);
  const extra = comparePublicationInventory(pkg, {
    opportunities: [...opps, sampleOpp("id_extra")],
  });
  assert.equal(extra.ok, false);
  assert.deepEqual(extra.extraOpportunityIds, ["id_extra"]);
  console.log("ok inventory identity / missing / extra");
}

// 6. Different source versions (fingerprint mismatch)
{
  const pkg = buildPkg([sampleOpp("x")]);
  const altered = {
    opportunities: [sampleOpp("x", { whatIsHappening: "Different wording entirely." })],
    sortOrder: pkg.sortOrder,
  };
  const parity = comparePublicationParity(pkg, altered);
  assert.equal(parity.ok, false);
  assert.equal(parity.content.ok, false);
  assert.equal(parity.fingerprintMatch, false);
  console.log("ok different source versions / wording fail");
}

// 7. Different frontend builds
{
  const pkg = buildPkg([sampleOpp("x")]);
  const parity = comparePublicationParity(
    pkg,
    {
      opportunities: pkg.opportunities,
      sortOrder: pkg.sortOrder,
      contentFingerprintSha256: pkg.contentFingerprintSha256,
    },
    { requireFrontendMatch: true, candidateFrontendBuild: "other-build" },
  );
  assert.equal(parity.frontendCompatibilityOk, false);
  assert.equal(parity.ok, false);
  console.log("ok different frontend builds fail");
}

// 8. Legacy Airtable fallback prevention (broken CURRENT pointer)
{
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "gdi-pubpkg-"));
  const store = createGdiPublicationPackageStore({ root: tmp });
  const pkg = buildPkg([sampleOpp("x")]);
  store.writePackage(pkg);
  store.writePointer(pkg.hotelId, "CURRENT", {
    publicationId: pkg.publicationId,
    publicationVersion: pkg.publicationVersion,
    contentFingerprintSha256: "0".repeat(64),
    lifecycleState: GDI_PUBLICATION_LIFECYCLE_STATE.CUSTOMER_PUBLISHED,
  });
  const resolved = resolveShareOpportunitiesFromApprovedPackage({
    hotelId: pkg.hotelId,
    store,
    env: {},
  });
  assert.equal(resolved.ok, false);
  assert.equal(resolved.mode, "FAIL_CLOSED");
  console.log("ok legacy Airtable fallback prevention");
}

// 9. Duplicate narrative detection
{
  const bad = sampleOpp("dup", {
    whatIsHappening:
      "Woman's Club of Bethesda is associated with Woman's Club of Bethesda — Preferred Lodging Partnership",
    organizationName: "Woman's Club of Bethesda",
    displayTitle: "Woman's Club of Bethesda",
    tileClassificationLine:
      "Woman's Club · Target-hotel sourcing not established",
    hotelSourcingStatusLabel: "Target-hotel sourcing not established",
  });
  const q = runPublicationQualityGate({ opportunities: [bad] });
  assert.equal(q.ok, false);
  assert.ok(q.blocking.some((f) => f.code === "DUPLICATE_ORGANIZATION_NAME"));
  assert.ok(q.blocking.some((f) => f.code === "REPEATED_SOURCING_STATUS"));
  const fixed = remediateDuplicateOrgAssociationSentence(
    bad.whatIsHappening,
    bad.organizationName,
  );
  assert.equal(fixed.includes("is associated with Woman's Club"), false);
  console.log("ok duplicate narrative detection");
}

// 10. Commercial recommendation consistency
{
  const conflict = sampleOpp("c1", {
    salesActionPillLabel: "Win Room Block",
    recommendedNextStep:
      "Official hotel sourcing for this edition appears closed. Do not pursue as an open room-block RFP.",
  });
  const q = runPublicationQualityGate({ opportunities: [conflict] });
  assert.ok(q.blocking.some((f) => f.code === "CONFLICTING_COMMERCIAL_ACTIONS"));
  console.log("ok commercial recommendation consistency");
}

// 11. Publication approval enforcement
{
  const pkg = buildPkg([sampleOpp("x")]);
  const badReceipt = validateFounderApprovalReceipt(
    { approved: true, publicationId: "wrong" },
    pkg,
  );
  assert.equal(badReceipt.ok, false);
  const good = validateFounderApprovalReceipt(
    {
      approved: true,
      publicationId: pkg.publicationId,
      publicationVersion: pkg.publicationVersion,
      contentFingerprintSha256: pkg.contentFingerprintSha256,
    },
    pkg,
  );
  assert.equal(good.ok, true);
  const gates = assertPublicationGatesPassed({
    inventory_parity: { ok: true },
    content_parity: { ok: true },
    presentation_parity: { ok: true },
    quality: { ok: true },
    fingerprint_integrity: { ok: true },
    founder_approval_fingerprint_match: { ok: true },
    // production_verification missing
  });
  assert.equal(gates.ok, false);
  assert.ok(gates.missing.includes("production_verification"));
  console.log("ok publication approval enforcement");
}

// 12. Existing feedback preservation
{
  const pkg = buildPkg([sampleOpp("x")]);
  const merged = mergeShareFeedbackOntoPackageOpportunities(pkg.opportunities, {
    items: [
      {
        opportunityId: "x",
        familiarityStatus: "Known",
        commercialValue: "High",
        note: "keep me",
        validatedAt: "2026-11-01T00:00:00.000Z",
      },
    ],
  });
  assert.equal(merged[0].shareValidation.familiarityStatus, "Known");
  assert.equal(merged[0].whatIsHappening, pkg.opportunities[0].whatIsHappening);
  const integrity = verifyPublicationPackageIntegrity(pkg);
  assert.equal(integrity.ok, true);
  console.log("ok feedback preservation");
}

// 13. Share-token permissions — package serve still requires share capability at API layer;
//     resolver itself only supplies package when CURRENT is CUSTOMER_PUBLISHED.
{
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "gdi-pubpkg-"));
  const store = createGdiPublicationPackageStore({ root: tmp });
  const pkg = buildPkg([sampleOpp("x")]);
  const pub = store.publishCustomerVisible(
    { ...pkg, approvalStatus: "FOUNDER_APPROVED" },
    { requireFounderApproval: true },
  );
  assert.equal(pub.ok, true);
  const resolved = resolveShareOpportunitiesFromApprovedPackage({
    hotelId: pkg.hotelId,
    store,
  });
  assert.equal(resolved.ok, true);
  assert.equal(resolved.mode, "PACKAGE");
  assert.equal(resolved.opportunities.length, 1);
  console.log("ok share package resolution / permissions boundary");
}

// 14. Production browser rendering — covered by rendered-text parity comparator
{
  const pkg = buildPkg([sampleOpp("x")]);
  const renderedFail = comparePublicationParity(pkg, {
    opportunities: [
      sampleOpp("x", {
        hotelSourcingStatusLabel:
          "Target-hotel sourcing not established · Target-hotel sourcing not established",
      }),
    ],
    sortOrder: pkg.sortOrder,
  });
  assert.equal(renderedFail.rendered.ok, false);
  console.log("ok rendered-content parity detects repeated sourcing");
}

// 15. Safe rollback
{
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "gdi-pubpkg-"));
  const store = createGdiPublicationPackageStore({ root: tmp });
  const pkg = buildPkg([sampleOpp("x")]);
  store.publishCustomerVisible(
    { ...pkg, approvalStatus: "FOUNDER_APPROVED" },
    { requireFounderApproval: true },
  );
  const withheld = store.withholdCustomerPublication(pkg.hotelId, "PARITY_FAILED");
  assert.equal(withheld.ok, true);
  const resolved = resolveShareOpportunitiesFromApprovedPackage({
    hotelId: pkg.hotelId,
    store,
  });
  // Withheld → legacy mode (no CURRENT customer publish), not silent Airtable for broken pointer
  assert.equal(resolved.mode, "LEGACY");
  assert.ok(store.loadPackage(pkg.hotelId, pkg.publicationVersion).ok);
  console.log("ok safe rollback / withhold");
}

// 16. Cross-hotel isolation
{
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "gdi-pubpkg-"));
  const store = createGdiPublicationPackageStore({ root: tmp });
  const a = buildCustomerPublicationPackage({
    hotelId: "recHOTELA",
    opportunities: [sampleOpp("a1")],
    publicationVersion: "v1",
  });
  const b = buildCustomerPublicationPackage({
    hotelId: "recHOTELB",
    opportunities: [sampleOpp("b1")],
    publicationVersion: "v1",
  });
  store.publishCustomerVisible(
    { ...a, approvalStatus: "FOUNDER_APPROVED" },
    { requireFounderApproval: true },
  );
  store.publishCustomerVisible(
    { ...b, approvalStatus: "FOUNDER_APPROVED" },
    { requireFounderApproval: true },
  );
  const ra = resolveShareOpportunitiesFromApprovedPackage({
    hotelId: "recHOTELA",
    store,
  });
  assert.equal(ra.opportunities[0].id, "a1");
  assert.notEqual(ra.package.hotelId, "recHOTELB");
  console.log("ok cross-hotel isolation");
}

// Negative: production 27 vs approved 67
{
  const approved = buildPkg(
    Array.from({ length: 67 }, (_, i) => sampleOpp(`n_${i}`)),
    { publicationVersion: "neg_67" },
  );
  const prod27 = {
    opportunities: Array.from({ length: 27 }, (_, i) => sampleOpp(`n_${i}`)),
  };
  const parity = comparePublicationParity(approved, prod27);
  assert.equal(parity.ok, false);
  assert.equal(parity.inventory.ok, false);
  assert.equal(parity.inventory.candidateCounts.all, 27);
  assert.equal(parity.inventory.approvedCounts.all, 67);
  assert.ok(parity.inventory.missingOpportunityIds.length >= 40);
  console.log("ok negative 27 vs 67 blocks certification");
}

// Negative: same count, different wording
{
  const approved = buildPkg([sampleOpp("w1"), sampleOpp("w2")]);
  const other = {
    opportunities: [
      sampleOpp("w1", { whatIsHappening: "Completely different commercial copy." }),
      sampleOpp("w2"),
    ],
    sortOrder: approved.sortOrder,
  };
  const parity = comparePublicationParity(approved, other);
  assert.equal(parity.ok, false);
  assert.equal(parity.content.ok, false);
  console.log("ok negative same-count wording mismatch fails");
}

// Negative: data matches but rendered repeats sourcing
{
  const approved = buildPkg([
    sampleOpp("r1", {
      hotelSourcingStatusLabel: "Target-hotel sourcing not established",
      tileClassificationLine: "Org · Event",
    }),
  ]);
  // Simulate external frontend concatenating sourcing twice into tile line
  const external = {
    opportunities: [
      {
        ...approved.opportunities[0],
        tileClassificationLine:
          "Org · Event · Target-hotel sourcing not established",
        hotelSourcingStatusLabel: "Target-hotel sourcing not established",
      },
    ],
    sortOrder: approved.sortOrder,
    contentFingerprintSha256: approved.contentFingerprintSha256,
  };
  const q = runPublicationQualityGate({ opportunities: external.opportunities });
  assert.equal(q.ok, false);
  assert.ok(q.blocking.some((f) => f.code === "REPEATED_SOURCING_STATUS"));
  const rendered = comparePublicationParity(approved, {
    ...external,
    // force content field mismatch on tile line
  });
  assert.equal(rendered.content.ok, false);
  console.log("ok negative rendered repeated sourcing fails");
}

// Empty parentheses helper
{
  assert.equal(stripEmptyParentheses("Bethesda Premier Cup ()"), "Bethesda Premier Cup");
  assert.equal(
    dedupeTileClassificationSourcing(
      "Org · Event · Target-hotel sourcing not established",
      "Target-hotel sourcing not established",
    ),
    "Org · Event",
  );
  console.log("ok empty-parens / tile dedupe helpers");
}

// Content remediation: org self-assoc, TBD, monitor alignment, sourcing boilerplate
{
  const remediated = applyCustomerFacingContentRemediation({
    id: "r1",
    organizationName: "Woman's Club of Bethesda",
    displayTitle: "Woman's Club of Bethesda",
    whatIsHappening:
      "Woman's Club of Bethesda is associated with Woman's Club of Bethesda — Preferred Lodging Partnership",
    whyActNow: "Destination TBD; hotel TBA",
    hotelSourcingStatusLabel: "Hotel Sourcing Unconfirmed",
    tileClassificationLine: "Org · Hotel Sourcing Unconfirmed",
    salesActionKey: "WIN_ROOM_BLOCK",
    salesActionPillLabel: "Win Room Block",
    recommendedNextStep:
      "Monitor for sourcing signals. do not claim open RFP or overflow without evidence",
  });
  assert.equal(
    remediated.whatIsHappening.includes("is associated with Woman's Club"),
    false,
  );
  assert.equal(/\bTBD\b|\bTBA\b/.test(remediated.whyActNow), false);
  // Defensive "monitor for sourcing / do not claim open RFP" → Qualify, not Win Room Block.
  assert.equal(remediated.salesActionPillLabel, "Qualify Opportunity");
  assert.equal(
    /hotel sourcing unconfirmed/i.test(remediated.whatIsHappening || ""),
    false,
  );
  assert.equal(rewritePlaceholderUncertainty("dates TBD"), "dates not yet announced");
  const q = runPublicationQualityGate({ opportunities: [remediated] });
  assert.equal(q.ok, true);
  console.log("ok customer-facing content remediation");
}

console.log("\nALL test-gdi-customer-publication-package-parity-v1 checks passed");
