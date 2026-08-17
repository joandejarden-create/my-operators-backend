#!/usr/bin/env node
/**
 * Unit tests: brand-specific source validation + auditPass semantics.
 * No live Airtable required.
 */
import assert from "assert";
import { evaluateBrandSpecificSourceValidation } from "../lib/partner-intelligence/brand-explorer-brand-specific-source-validation.js";
import {
  evaluateRenderedFieldCompletenessForTest,
} from "../lib/partner-intelligence/brand-explorer-rendered-field-completeness-evaluate.js";
import { resolveCanonicalBrandState } from "../lib/partner-intelligence/brand-explorer-os-state-machine.js";

function testIndigoRequiresHotelIndigoDomain() {
  const fail = evaluateBrandSpecificSourceValidation({
    brandSlug: "hotel-indigo",
    brandConfig: {
      slug: "hotel-indigo",
      consumerUrl: "https://www.ihg.com/hotelindigo/hotels/us/en/reservation",
      developmentUrl: "https://development.ihg.com/brand/hotel-indigo",
      officialSourceDomains: ["ihg.com", "development.ihg.com"],
      momentumSourceUrls: ["https://www.ihg.com/hotelindigo/hotels/us/en/reservation"],
    },
    registryAssets: [
      { sourcePageUrl: "https://www.ihg.com/hotelindigo/hotels/us/en/reservation" },
      { sourcePageUrl: "https://development.ihg.com/brand/hotel-indigo" },
      { sourcePageUrl: "https://www.ihgplc.com/" },
    ],
  });
  assert.equal(fail.pass, false, "Indigo without hotelindigo.com must fail");
  assert.ok(
    fail.missingRequiredBrandDomains.includes("hotelindigo.com"),
    "must report missing hotelindigo.com"
  );

  const pass = evaluateBrandSpecificSourceValidation({
    brandSlug: "hotel-indigo",
    brandConfig: {
      slug: "hotel-indigo",
      consumerUrl: "https://www.hotelindigo.com/",
      developmentUrl: "https://development.ihg.com/brand/hotel-indigo",
      officialSourceDomains: ["hotelindigo.com", "ihg.com", "ihgplc.com"],
      momentumSourceUrls: ["https://www.hotelindigo.com/", "https://www.ihgplc.com/"],
    },
    registryAssets: [
      { sourcePageUrl: "https://www.hotelindigo.com/hotels" },
      { sourcePageUrl: "https://www.hotelindigo.com/about" },
      { sourcePageUrl: "https://development.ihg.com/brand/hotel-indigo" },
    ],
  });
  assert.equal(pass.pass, true, "Indigo with hotelindigo.com + parent context should pass");
}

function testMGalleryRequiresBrandDomain() {
  const fail = evaluateBrandSpecificSourceValidation({
    brandSlug: "mgallery-collection",
    brandConfig: {
      consumerUrl: "https://group.accor.com/en/brands-and-experiences/mgallery",
      officialSourceDomains: ["group.accor.com", "all.accor.com"],
      momentumSourceUrls: ["https://group.accor.com/en/brands-and-experiences/mgallery"],
    },
  });
  assert.equal(fail.pass, false);
  assert.ok(fail.missingRequiredBrandDomains.includes("mgallery.accor.com"));

  const pass = evaluateBrandSpecificSourceValidation({
    brandSlug: "mgallery-collection",
    brandConfig: {
      consumerUrl: "https://mgallery.accor.com/",
      officialSourceDomains: ["mgallery.accor.com", "group.accor.com"],
      momentumSourceUrls: ["https://mgallery.accor.com/", "https://group.accor.com/"],
    },
  });
  assert.equal(pass.pass, true);
}

function testSlhRequiresSlhComNoFranchiseForce() {
  const pass = evaluateBrandSpecificSourceValidation({
    brandSlug: "small-luxury-hotels-of-the-world",
    brandConfig: {
      consumerUrl: "https://www.slh.com/",
      officialSourceDomains: ["slh.com"],
      momentumSourceUrls: ["https://www.slh.com/"],
    },
    brandApi: { brandPositioning: "Independent luxury consortium for distinctive hotels." },
  });
  assert.equal(pass.pass, true);

  const franchise = evaluateBrandSpecificSourceValidation({
    brandSlug: "small-luxury-hotels-of-the-world",
    brandConfig: {
      consumerUrl: "https://www.slh.com/",
      officialSourceDomains: ["slh.com"],
      momentumSourceUrls: ["https://www.slh.com/"],
    },
    brandApi: { brandPositioning: "Franchise agreement and FDD Item 19 disclosures." },
  });
  assert.equal(franchise.pass, false);
  assert.ok(franchise.failures.includes("slh_franchise_logic_forced"));
}

function testAuditPassRequiresZeroFails() {
  const withFails = evaluateRenderedFieldCompletenessForTest({
    auditPass: false,
    failFindings: 2,
    findings: [
      { fieldName: "Operating Model", status: "blank" },
      { fieldName: "Philosophy", status: "blank" },
    ],
  });
  assert.equal(withFails.pass, false);
  assert.ok(withFails.failures.length >= 2);

  const clean = evaluateRenderedFieldCompletenessForTest({
    auditPass: true,
    failFindings: 0,
    findings: [{ fieldName: "Operating Model", status: "pass" }],
  });
  assert.equal(clean.pass, true);
}

function testOsBlocksFounderWhenGatesFail() {
  const base = {
    brandExists: true,
    factoryConfigExists: true,
    sourceCoverageReady: true,
    galleryReady: true,
    propertyExamplesReady: true,
    visualAssetPackReady: true,
    liveInternalPreviewClean: true,
    residualPresentationDirty: false,
    externalQualityLockPass: true,
    externalFullProfileRendered: false,
    founderVisualReviewPassed: false,
    activeReleaseApproved: false,
  };

  const blocked = resolveCanonicalBrandState({
    ...base,
    brandSpecificSourceValidationPass: false,
    renderedFieldCompletenessPass: false,
    goldenContentQualityPass: true,
    tabFactoryAuditPass: false,
    sourceProvenanceByTabPass: false,
    noEmptyRenderedComponentsPass: false,
    imageDistinctivenessPass: true,
  });
  assert.equal(blocked.canonicalState, "draft_applied_with_defects");

  const ready = resolveCanonicalBrandState({
    ...base,
    brandSpecificSourceValidationPass: true,
    renderedFieldCompletenessPass: true,
    goldenContentQualityPass: true,
    tabFactoryAuditPass: true,
    sourceProvenanceByTabPass: true,
    noEmptyRenderedComponentsPass: true,
    imageDistinctivenessPass: true,
  });
  assert.equal(ready.canonicalState, "founder_review_ready");
}

function main() {
  testIndigoRequiresHotelIndigoDomain();
  testMGalleryRequiresBrandDomain();
  testSlhRequiresSlhComNoFranchiseForce();
  testAuditPassRequiresZeroFails();
  testOsBlocksFounderWhenGatesFail();
  console.log("[PASS] brand-explorer mandatory release gates (source + auditPass + OS state)");
}

main();
