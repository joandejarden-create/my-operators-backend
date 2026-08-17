#!/usr/bin/env node
/**
 * Unit checks for profile-governance publish readiness logic.
 */
import assert from "node:assert/strict";
import {
  assessSourceGate,
  assessTargetProtection,
  assessPackageReadiness,
  assessBrandExplorerGovernanceReadiness,
  buildPublishPackages,
  buildPublishScopeSlice,
  classifyGovernanceChange,
  detectSourceOriginConflict,
  GOVERNANCE_CHANGE_EQUIVALENT_STABLE,
  inferValidationStatus,
  isEquivalentStableLiveGovernance,
  isStableGovernanceChangeClass,
  mapPiRegionToProfile,
  mapSourceToProfileSourceType,
  classifySourceBasisBucket,
  isCompanyControlledPressSource,
  proposeProfileGovernance,
  resolveEntityKey,
} from "../lib/partner-intelligence/profile-governance-publish-readiness.js";
import {
  GOVERNANCE_VALIDATION_STATUS,
  GOVERNANCE_EXTERNAL_DISPLAY,
  GOVERNANCE_USAGE_PERMISSION,
} from "../lib/profile-governance/profile-governance-fields.js";

const CURIO = "receQkxgjlezsc1xg";
const US_FDD = "recy2pyEahF9UUsEk";
const NOISY_SRC = "recNOISY1";
const FACT_BRAND = "recCE4145q2u3ZCHH";
const FACT_PARENT = "recpaaTI64IIsm7hi";

// resolveEntityKey prefers brand link
{
  const r = resolveEntityKey({ brandId: "recBRAND123", operatorId: "recOP456" });
  assert.equal(r.entityType, "brand");
  assert.equal(r.recordId, "recBRAND123");
}

// stale source blocked
{
  const r = assessSourceGate({
    id: "recSRC1",
    status: "Stale",
    sourceQuality: "High",
    approvedForExplorerUse: "Yes",
  });
  assert.equal(r.ok, false);
  assert.ok(r.failures.includes("source_stale"));
}

// company validated profile protected
{
  const r = assessTargetProtection(
    {
      "Company Validated": true,
      "Validation Status": "Company Validated",
      "Usage Permission": "Platform Display Allowed",
    },
    "brand",
    "2026-01-01"
  );
  assert.equal(r.blocked, true);
  assert.ok(r.reasons.includes("company_validated_checkbox"));
}

// mixed origins conflict
{
  const c = detectSourceOriginConflict([
    { sourceOrigin: "Brand Provided" },
    { sourceOrigin: "Public Web" },
  ]);
  assert.equal(c.conflict, true);
}

// infer company published from brand provided + company-controlled source type
{
  const v = inferValidationStatus(
    [{ sourceOrigin: "Brand Provided", sourceType: "Brand Page" }],
    [{ extractionType: "Directly Stated" }]
  );
  assert.equal(v, GOVERNANCE_VALIDATION_STATUS.companyPublished);
}

// Website Capture maps to Company Website → Company Published even when origin is Public Web
{
  const v = inferValidationStatus(
    [
      { sourceOrigin: "Public Web", sourceType: "Website Capture" },
      { sourceOrigin: "Public Web", sourceType: "Website Capture" },
      { sourceOrigin: "Operator Provided", sourceType: "Website Capture" },
    ],
    [{ extractionType: "Directly Stated" }]
  );
  assert.equal(v, GOVERNANCE_VALIDATION_STATUS.companyPublished);
  assert.equal(mapSourceToProfileSourceType({ sourceType: "Website Capture" }), "Company Website");
  assert.equal(classifySourceBasisBucket({ sourceType: "Website Capture" }), "company");
}

// never company validated in proposal
{
  const { proposed } = proposeProfileGovernance({
    entityType: "operator",
    sources: [
      {
        id: "recS1",
        status: "Approved",
        sourceQuality: "High",
        approvedForExplorerUse: "Yes",
        sourceOrigin: "Brand Provided",
        sourceType: "Brand Page",
        region: "CALA",
        sourceTitle: "Operator site",
        lastReviewed: "2026-06-01",
      },
    ],
    facts: [
      {
        id: "recF1",
        humanReviewStatus: "Approved",
        approvedValue: "Test",
        extractionType: "Directly Stated",
        reviewedAt: "2026-06-01",
        publicVisibility: "Public",
      },
    ],
    publishedRows: [],
    piReviewDate: "2026-06-01",
  });
  assert.notEqual(proposed.validationStatus, GOVERNANCE_VALIDATION_STATUS.companyValidated);
  assert.equal(proposed.companyValidated, false);
}

// eligible package end-to-end
{
  const sources = [
    {
      id: "recS1",
      brandId: "recB1",
      profileType: "Brand",
      status: "Approved",
      sourceQuality: "High",
      approvedForExplorerUse: "Yes",
      sourceOrigin: "Public Web",
      sourceType: "Brand Page",
      region: "Global",
      sourceTitle: "Brand brochure",
      lastReviewed: "2026-06-15",
    },
  ];
  const facts = [
    {
      id: "recF1",
      brandId: "recB1",
      sourceRecordId: "recS1",
      humanReviewStatus: "Approved",
      approvedValue: "Published claim",
      extractionType: "Directly Stated",
      reviewedAt: "2026-06-15",
      publicVisibility: "Public",
    },
  ];
  const packages = buildPublishPackages({ sources, facts, published: [] });
  assert.equal(packages.length, 1);
  const assessment = assessPackageReadiness(packages[0], {
    id: "recB1",
    entityType: "brand",
    name: "Test Brand",
    fields: {},
  });
  assert.equal(assessment.eligible, true);
  assert.ok(assessment.proposal?.expectedGovernance?.displayLabel);
}

// region mapping
assert.equal(mapPiRegionToProfile("CALA"), "CALA-Specific");

// 1. Non-approved noisy sources do not block publish-scope eligibility
{
  const pkg = {
    entityKey: `brand:${CURIO}`,
    entityType: "brand",
    recordId: CURIO,
    sources: [
      {
        id: US_FDD,
        brandId: CURIO,
        status: "Extracted",
        sourceQuality: "High",
        approvedForExplorerUse: "Yes",
        sourceOrigin: "Brand Provided",
        sourceType: "FDD",
        region: "Global",
        sourceTitle: "2025 US Curio FDD",
        lastReviewed: "2026-07-06",
      },
      {
        id: NOISY_SRC,
        brandId: CURIO,
        status: "Extracted",
        sourceQuality: "Medium",
        approvedForExplorerUse: "No",
        sourceOrigin: "Public Web",
        sourceType: "Other",
        region: "Global",
        sourceTitle: "Third-party blog",
      },
    ],
    facts: [
      {
        id: FACT_BRAND,
        brandId: CURIO,
        sourceRecordId: US_FDD,
        fieldName: "be.identity.brandName",
        humanReviewStatus: "Approved",
        approvedValue: "Curio Collection by Hilton",
        extractionType: "Directly Stated",
        reviewedAt: "2026-07-06",
        publicVisibility: "Public",
      },
      {
        id: FACT_PARENT,
        brandId: CURIO,
        sourceRecordId: US_FDD,
        fieldName: "be.identity.parentCompany",
        humanReviewStatus: "Approved",
        approvedValue: "Hilton Worldwide",
        extractionType: "Directly Stated",
        reviewedAt: "2026-07-06",
        publicVisibility: "Public",
      },
    ],
    published: [],
  };
  const assessment = assessPackageReadiness(pkg, { id: CURIO, entityType: "brand", name: "Curio", fields: {} });
  assert.equal(assessment.eligible, true);
  assert.equal(assessment.excludedSourceCount, 1);
  assert.ok(
    assessment.fullPackageWarnings.some(
      (w) => w.includes("full_package_conflict") || w.includes("full_package_mixed_basis")
    )
  );
  assert.ok(!assessment.publishScopeBlockers.some((r) => r.includes("approved_for_explorer_use_no")));
  assert.ok(!assessment.publishScopeBlockers.some((r) => r.startsWith("conflict:")));
}

// 2. Mixed conflict in full package does not block when publish scope is single origin
{
  const slice = buildPublishScopeSlice({
    sources: [
      { id: US_FDD, approvedForExplorerUse: "Yes", sourceOrigin: "Brand Provided" },
      { id: NOISY_SRC, approvedForExplorerUse: "No", sourceOrigin: "Public Web" },
    ],
    facts: [],
  });
  assert.equal(slice.publishScopeSourceCount, 1);
  assert.equal(slice.excludedSourceCount, 1);
}

// 3. Mixed company-controlled + reviewed sources propose Source-Informed (not blocked)
{
  const pkg = {
    entityKey: "brand:recB2",
    entityType: "brand",
    recordId: "recB2",
    sources: [
      {
        id: "recS1",
        status: "Approved",
        sourceQuality: "High",
        approvedForExplorerUse: "Yes",
        sourceOrigin: "Public Web",
        sourceType: "Website Capture",
        lastReviewed: "2026-06-01",
      },
      {
        id: "recS2",
        status: "Approved",
        sourceQuality: "High",
        approvedForExplorerUse: "Yes",
        sourceOrigin: "Public Web",
        sourceType: "Case Study",
        lastReviewed: "2026-06-01",
      },
    ],
    facts: [
      {
        id: "recF1",
        sourceRecordId: "recS1",
        humanReviewStatus: "Approved",
        approvedValue: "A",
        publicVisibility: "Public",
      },
    ],
    published: [],
  };
  const assessment = assessPackageReadiness(pkg, { id: "recB2", entityType: "brand", fields: {} });
  assert.equal(assessment.eligible, true);
  assert.equal(assessment.proposal.proposed.validationStatus, GOVERNANCE_VALIDATION_STATUS.sourceInformed);
  assert.equal(assessment.proposal.expectedGovernance.displayLabel, "Source-Informed Profile");
  const conflict = detectSourceOriginConflict(pkg.sources);
  assert.equal(conflict.conflict, false);
  assert.equal(conflict.mixedBasis, true);
}

// 4. No approved Explorer-use sources blocks
{
  const pkg = {
    entityKey: "brand:recB3",
    entityType: "brand",
    recordId: "recB3",
    sources: [
      {
        id: "recS3",
        status: "Extracted",
        sourceQuality: "High",
        approvedForExplorerUse: "No",
        sourceOrigin: "Brand Provided",
      },
    ],
    facts: [],
    published: [],
  };
  const assessment = assessPackageReadiness(pkg, { id: "recB3", entityType: "brand", fields: {} });
  assert.equal(assessment.eligible, false);
  assert.ok(assessment.publishScopeBlockers.includes("no_approved_explorer_sources"));
}

// 5. Approved facts from non-approved sources do not count
{
  const slice = buildPublishScopeSlice({
    sources: [
      { id: US_FDD, approvedForExplorerUse: "Yes" },
      { id: NOISY_SRC, approvedForExplorerUse: "No" },
    ],
    facts: [
      { id: "recF1", sourceRecordId: NOISY_SRC, humanReviewStatus: "Approved" },
      { id: "recF2", sourceRecordId: US_FDD, humanReviewStatus: "Approved" },
    ],
  });
  assert.equal(slice.factsUsedForProposal.length, 1);
  assert.equal(slice.factsUsedForProposal[0].id, "recF2");
}

// 6. Rejected facts do not count
{
  const slice = buildPublishScopeSlice({
    sources: [{ id: US_FDD, approvedForExplorerUse: "Yes" }],
    facts: [
      { id: "recR1", sourceRecordId: US_FDD, humanReviewStatus: "Rejected" },
      { id: "recF2", sourceRecordId: US_FDD, humanReviewStatus: "Approved" },
    ],
  });
  assert.equal(slice.factsUsedForProposal.length, 1);
  assert.equal(slice.rejectedFactCount, 1);
}

// 7. Curio-like package not blocked by non-approved sources
{
  const assessment = assessPackageReadiness(
    {
      entityKey: `brand:${CURIO}`,
      entityType: "brand",
      recordId: CURIO,
      sources: [
        {
          id: US_FDD,
          status: "Extracted",
          sourceQuality: "High",
          approvedForExplorerUse: "Yes",
          sourceOrigin: "Brand Provided",
          sourceType: "FDD",
          region: "Global",
          sourceTitle: "2025 US Curio FDD",
          lastReviewed: "2026-07-06",
        },
        ...Array.from({ length: 14 }, (_, i) => ({
          id: `recNoise${i}`,
          status: "Extracted",
          sourceQuality: "Medium",
          approvedForExplorerUse: "No",
          sourceOrigin: i % 2 ? "Public Web" : "Brand Provided",
          sourceType: "Other",
        })),
      ],
      facts: [
        {
          id: FACT_BRAND,
          sourceRecordId: US_FDD,
          fieldName: "be.identity.brandName",
          humanReviewStatus: "Approved",
          approvedValue: "Curio Collection by Hilton",
          extractionType: "Directly Stated",
          publicVisibility: "Public",
        },
        {
          id: FACT_PARENT,
          sourceRecordId: US_FDD,
          fieldName: "be.identity.parentCompany",
          humanReviewStatus: "Approved",
          approvedValue: "Hilton Worldwide",
          extractionType: "Directly Stated",
          publicVisibility: "Public",
        },
      ],
      published: [],
    },
    { id: CURIO, entityType: "brand", fields: {} }
  );
  assert.equal(assessment.publishScopeSourceCount, 1);
  assert.equal(assessment.excludedSourceCount, 14);
  assert.equal(assessment.eligible, true);
  assert.ok(assessment.warnings.includes("sparse_publish_scope_fact_set"));
  assert.equal(assessment.proposal.proposed.confidenceLevel, "Medium");
  assert.ok(assessment.proposal.proposed.evidenceNotes.includes("Sparse publish scope"));
  assert.ok(assessment.proposal.proposed.evidenceNotes.includes("identity-only coverage"));
  assert.equal(assessment.proposal.expectedGovernance.displayLabel, "AI-Assisted Profile");
  assert.ok(!assessment.proposal.expectedGovernance.displaySubtitle.includes("Confidence:"));
}

// Sparse confidence: Curio-like package proposes Medium, not High
{
  const { proposed } = proposeProfileGovernance({
    entityType: "brand",
    sources: [
      {
        id: US_FDD,
        status: "Extracted",
        sourceQuality: "High",
        approvedForExplorerUse: "Yes",
        sourceOrigin: "Brand Provided",
        sourceType: "FDD",
        region: "Global",
        sourceTitle: "2025 US Curio FDD",
        lastReviewed: "2026-07-06",
      },
    ],
    facts: [
      {
        id: FACT_BRAND,
        fieldName: "be.identity.brandName",
        humanReviewStatus: "Approved",
        approvedValue: "Curio Collection by Hilton",
        extractionType: "Directly Stated",
        reviewedAt: "2026-07-06",
        publicVisibility: "Public",
      },
      {
        id: FACT_PARENT,
        fieldName: "be.identity.parentCompany",
        humanReviewStatus: "Approved",
        approvedValue: "Hilton Worldwide",
        extractionType: "Directly Stated",
        reviewedAt: "2026-07-06",
        publicVisibility: "Public",
      },
    ],
    publishedRows: [],
    piReviewDate: "2026-07-06",
  });
  assert.equal(proposed.confidenceLevel, "Medium");
  assert.notEqual(proposed.confidenceLevel, "High");
}

// High confidence requires non-identity substantive coverage
{
  const { proposed: identityOnly } = proposeProfileGovernance({
    entityType: "brand",
    sources: [
      {
        id: "recS1",
        status: "Approved",
        sourceQuality: "High",
        approvedForExplorerUse: "Yes",
        sourceOrigin: "Brand Provided",
        sourceType: "FDD",
        lastReviewed: "2026-06-01",
      },
    ],
    facts: [
      { fieldName: "be.identity.brandName", humanReviewStatus: "Approved", publicVisibility: "Public" },
      { fieldName: "be.identity.parentCompany", humanReviewStatus: "Approved", publicVisibility: "Public" },
      { fieldName: "be.identity.brandName", humanReviewStatus: "Approved", publicVisibility: "Public" },
    ],
    publishedRows: [],
    piReviewDate: "2026-06-01",
  });
  assert.equal(identityOnly.confidenceLevel, "Medium");
  assert.ok(identityOnly.evidenceNotes.includes("identity-only coverage"));
}

// Kimpton-like richer approved facts can still propose High
{
  const { proposed, expectedGovernance } = proposeProfileGovernance({
    entityType: "brand",
    sources: [
      {
        id: "recKimptonFdd",
        status: "Approved",
        sourceQuality: "High",
        approvedForExplorerUse: "Yes",
        sourceOrigin: "Brand Provided",
        sourceType: "FDD",
        region: "Global",
        sourceTitle: "Kimpton FDD",
        lastReviewed: "2026-06-01",
      },
    ],
    facts: [
      {
        fieldName: "be.identity.brandName",
        humanReviewStatus: "Approved",
        approvedValue: "Kimpton Hotels",
        extractionType: "Directly Stated",
        publicVisibility: "Public",
      },
      {
        fieldName: "be.identity.parentCompany",
        humanReviewStatus: "Approved",
        approvedValue: "IHG Hotels & Resorts",
        extractionType: "Directly Stated",
        publicVisibility: "Public",
      },
      {
        fieldName: "be.positioning.summary",
        humanReviewStatus: "Approved",
        approvedValue: "Boutique lifestyle hotels",
        extractionType: "Directly Stated",
        publicVisibility: "Public",
      },
      {
        fieldName: "be.overview.typicalUseCase",
        humanReviewStatus: "Approved",
        approvedValue: "Urban lifestyle conversions",
        extractionType: "Directly Stated",
        publicVisibility: "Public",
      },
    ],
    publishedRows: [],
    piReviewDate: "2026-06-01",
  });
  assert.equal(proposed.confidenceLevel, "High");
  assert.equal(proposed.validationStatus, GOVERNANCE_VALIDATION_STATUS.companyPublished);
  assert.equal(expectedGovernance.displayLabel, "AI-Assisted Profile");
  assert.ok(expectedGovernance.displaySubtitle.includes("Company Materials"));
  assert.ok(!proposed.evidenceNotes?.includes("identity-only coverage"));
}

// Hotel Equities-like operator package: official website captures → Company Published / AI-Assisted
{
  const HE = "recWPKu5laVZxsvpn";
  const { proposed, expectedGovernance } = proposeProfileGovernance({
    entityType: "operator",
    sources: [
      {
        id: "rectG9wdsAeL7u0FG",
        status: "Extracted",
        sourceQuality: "Medium",
        approvedForExplorerUse: "Yes",
        sourceOrigin: "Public Web",
        sourceType: "Website Capture",
        sourceTitle: "Hotel Equities home",
        lastReviewed: "2026-07-06",
      },
      {
        id: "rec9FSzLhaLPcPvtv",
        status: "Extracted",
        sourceQuality: "Medium",
        approvedForExplorerUse: "Yes",
        sourceOrigin: "Public Web",
        sourceType: "Website Capture",
        sourceTitle: "Hotel Equities Services",
        lastReviewed: "2026-07-06",
      },
      {
        id: "recy1oDTNe7kyQGbE",
        status: "Extracted",
        sourceQuality: "Medium",
        approvedForExplorerUse: "Yes",
        sourceOrigin: "Operator Provided",
        sourceType: "Website Capture",
        sourceTitle: "Hotel Equities CALA",
        lastReviewed: "2026-07-06",
      },
    ],
    facts: [
      {
        fieldName: "op.snapshot.companyName",
        humanReviewStatus: "Approved",
        approvedValue: "Hotel Equities",
        extractionType: "Directly Stated",
        publicVisibility: "Public",
      },
      {
        fieldName: "op.snapshot.companyDescription",
        humanReviewStatus: "Approved",
        approvedValue: "CALA division overview",
        extractionType: "Directly Stated",
        publicVisibility: "Public",
      },
      {
        fieldName: "op.snapshot.companyDescription",
        humanReviewStatus: "Approved",
        approvedValue: "Corporate overview",
        extractionType: "Directly Stated",
        publicVisibility: "Public",
      },
      {
        fieldName: "op.markets.regionsSupported",
        humanReviewStatus: "Approved",
        approvedValue: "Caribbean, Latin America",
        extractionType: "Directly Stated",
        publicVisibility: "Public",
      },
      {
        fieldName: "op.snapshot.primaryServiceModel",
        humanReviewStatus: "Approved",
        approvedValue: "Hotel Management",
        extractionType: "Directly Stated",
        publicVisibility: "Public",
      },
    ],
    publishedRows: [],
    piReviewDate: "2026-07-06",
  });
  assert.equal(proposed.validationStatus, GOVERNANCE_VALIDATION_STATUS.companyPublished);
  assert.equal(proposed.sourceType, "Company Website");
  assert.equal(proposed.confidenceLevel, "Medium");
  assert.equal(expectedGovernance.displayLabel, "AI-Assisted Profile");
  assert.ok(expectedGovernance.displaySubtitle.includes("Source Basis: Company Materials"));
  assert.ok(!expectedGovernance.displaySubtitle.includes("Confidence:"));
  assert.equal(proposed.companyValidated, false);

  const assessment = assessPackageReadiness(
    {
      entityKey: `operator:${HE}`,
      entityType: "operator",
      recordId: HE,
      sources: [
        {
          id: "rectG9wdsAeL7u0FG",
          operatorId: HE,
          status: "Extracted",
          sourceQuality: "Medium",
          approvedForExplorerUse: "Yes",
          sourceOrigin: "Public Web",
          sourceType: "Website Capture",
          lastReviewed: "2026-07-06",
        },
        {
          id: "rec9FSzLhaLPcPvtv",
          operatorId: HE,
          status: "Extracted",
          sourceQuality: "Medium",
          approvedForExplorerUse: "Yes",
          sourceOrigin: "Public Web",
          sourceType: "Website Capture",
          lastReviewed: "2026-07-06",
        },
        {
          id: "recy1oDTNe7kyQGbE",
          operatorId: HE,
          status: "Extracted",
          sourceQuality: "Medium",
          approvedForExplorerUse: "Yes",
          sourceOrigin: "Operator Provided",
          sourceType: "Website Capture",
          lastReviewed: "2026-07-06",
        },
      ],
      facts: [
        {
          id: "rec4OkNp3HErir1Tm",
          operatorId: HE,
          sourceRecordId: "rectG9wdsAeL7u0FG",
          fieldName: "op.snapshot.companyName",
          humanReviewStatus: "Approved",
          approvedValue: "Hotel Equities",
          publicVisibility: "Public",
        },
        {
          id: "rec5ZV7hxlyZz3eRk",
          operatorId: HE,
          sourceRecordId: "recy1oDTNe7kyQGbE",
          fieldName: "op.snapshot.companyDescription",
          humanReviewStatus: "Approved",
          approvedValue: "CALA overview",
          publicVisibility: "Public",
        },
        {
          id: "recg9JSrZm9gmFKcN",
          operatorId: HE,
          sourceRecordId: "rectG9wdsAeL7u0FG",
          fieldName: "op.snapshot.companyDescription",
          humanReviewStatus: "Approved",
          approvedValue: "Home overview",
          publicVisibility: "Public",
        },
        {
          id: "recQEsdNe6Z6yYl7R",
          operatorId: HE,
          sourceRecordId: "rectG9wdsAeL7u0FG",
          fieldName: "op.markets.regionsSupported",
          humanReviewStatus: "Approved",
          approvedValue: "Caribbean, Latin America, United States, Canada",
          publicVisibility: "Public",
        },
        {
          id: "recDasPN4e1SOJOUa",
          operatorId: HE,
          sourceRecordId: "rec9FSzLhaLPcPvtv",
          fieldName: "op.snapshot.primaryServiceModel",
          humanReviewStatus: "Approved",
          approvedValue: "Hotel Management",
          publicVisibility: "Public",
        },
      ],
      published: [],
    },
    {
      id: HE,
      entityType: "operator",
      fields: {
        "Validation Status": GOVERNANCE_VALIDATION_STATUS.sourceInformed,
        "Data Confidence Level": "Medium",
      },
    }
  );
  assert.equal(assessment.eligible, true);
  assert.equal(assessment.changeClass, "upgrade");
  assert.equal(assessment.proposal.proposed.validationStatus, GOVERNANCE_VALIDATION_STATUS.companyPublished);
}

// Unknown source type defaults to Source-Informed
{
  const v = inferValidationStatus([{ sourceOrigin: "Public Web", sourceType: "Other" }], []);
  assert.equal(v, GOVERNANCE_VALIDATION_STATUS.sourceInformed);
}

// Official company press kit (Public Web capture) → Company Materials / Company Published
{
  const pressKit = {
    sourceOrigin: "Public Web",
    sourceType: "Press Release",
    sourceTitle: "Radisson Blu Choice press kit Americas",
    sourceUrl: "https://media.choicehotels.com/Radisson-blu-press-kit",
  };
  assert.equal(isCompanyControlledPressSource(pressKit), true);
  assert.equal(mapSourceToProfileSourceType(pressKit), "Company Materials");
  assert.equal(classifySourceBasisBucket(pressKit), "company");

  const v = inferValidationStatus(
    [
      { sourceOrigin: "Public Web", sourceType: "Website Capture" },
      { sourceOrigin: "Public Web", sourceType: "Website Capture" },
      pressKit,
      { sourceOrigin: "Brand Provided", sourceType: "Development Brochure" },
    ],
    [{ extractionType: "Directly Stated" }]
  );
  assert.equal(v, GOVERNANCE_VALIDATION_STATUS.companyPublished);

  const { proposed, expectedGovernance } = proposeProfileGovernance({
    entityType: "brand",
    sources: [
      {
        id: "recWeb1",
        status: "Extracted",
        sourceQuality: "Medium",
        approvedForExplorerUse: "Yes",
        sourceOrigin: "Public Web",
        sourceType: "Website Capture",
        sourceTitle: "Consumer brand page",
        lastReviewed: "2026-07-06",
      },
      pressKit,
      {
        id: "recPdf1",
        status: "Extracted",
        sourceQuality: "High",
        approvedForExplorerUse: "Yes",
        sourceOrigin: "Brand Provided",
        sourceType: "Development Brochure",
        sourceTitle: "Development one-pager",
        lastReviewed: "2026-07-06",
      },
    ],
    facts: [
      {
        humanReviewStatus: "Approved",
        approvedValue: "Radisson Blu",
        extractionType: "Directly Stated",
        publicVisibility: "Public",
      },
    ],
    publishedRows: [],
    piReviewDate: "2026-07-06",
  });
  assert.equal(proposed.validationStatus, GOVERNANCE_VALIDATION_STATUS.companyPublished);
  assert.equal(expectedGovernance.displayLabel, "AI-Assisted Profile");
  assert.ok(expectedGovernance.displaySubtitle.includes("Source Basis: Company Materials"));
  const conflict = detectSourceOriginConflict([
    { sourceOrigin: "Public Web", sourceType: "Website Capture" },
    pressKit,
    { sourceOrigin: "Brand Provided", sourceType: "Development Brochure" },
  ]);
  assert.ok(!conflict.mixedBasis);
  assert.equal(conflict.conflict, false);
}

// Arbor-like reviewed public source remains Source-Informed
{
  const { proposed, expectedGovernance } = proposeProfileGovernance({
    entityType: "operator",
    sources: [
      {
        id: "recS1",
        status: "Approved",
        sourceQuality: "Medium",
        approvedForExplorerUse: "Yes",
        sourceOrigin: "Public Web",
        sourceType: "Press Release",
        region: "CALA",
        lastReviewed: "2026-06-10",
      },
    ],
    facts: [
      {
        humanReviewStatus: "Approved",
        approvedValue: "Arbor Lodging",
        extractionType: "Directly Stated",
        publicVisibility: "Public",
      },
    ],
    publishedRows: [],
    piReviewDate: "2026-06-10",
  });
  assert.equal(proposed.validationStatus, GOVERNANCE_VALIDATION_STATUS.sourceInformed);
  assert.equal(expectedGovernance.displayLabel, "Source-Informed Profile");
  assert.ok(expectedGovernance.displaySubtitle.includes("Source Basis: Reviewed Sources"));
}

// Choice mini-batch: live published governance stronger than PI cosmetic proposal → stable equivalence
{
  const live = {
    validationStatus: GOVERNANCE_VALIDATION_STATUS.companyPublished,
    usagePermission: GOVERNANCE_USAGE_PERMISSION.platformDisplayAllowed,
    sourceType: "Company PDF / Brochure",
    sourceRegion: "CALA-Specific",
    confidenceLevel: "High",
    lastReviewedDate: "2026-07-06",
    externalDisplayStatus: GOVERNANCE_EXTERNAL_DISPLAY.showTrustLabel,
    companyValidated: false,
    internalNotes: "PI profile-governance publish 2026-07-06 (brand:recOzH5iAE1xEjyD0).",
  };
  const proposed = {
    validationStatus: GOVERNANCE_VALIDATION_STATUS.companyPublished,
    usagePermission: GOVERNANCE_USAGE_PERMISSION.platformDisplayAllowed,
    sourceType: "Company Materials",
    sourceRegion: null,
    confidenceLevel: "Medium",
    lastReviewedDate: "2026-07-06",
    externalDisplayStatus: GOVERNANCE_EXTERNAL_DISPLAY.showTrustLabel,
    companyValidated: false,
    internalNotes: "PI publish readiness audit proposal — not written.",
  };
  assert.equal(isEquivalentStableLiveGovernance(live, proposed, "brand"), true);
  assert.equal(classifyGovernanceChange(live, proposed, { entityType: "brand" }), GOVERNANCE_CHANGE_EQUIVALENT_STABLE);
  assert.equal(isStableGovernanceChangeClass(GOVERNANCE_CHANGE_EQUIVALENT_STABLE), true);

  const assessment = assessPackageReadiness(
    {
      entityKey: "brand:recOzH5iAE1xEjyD0",
      entityType: "brand",
      recordId: "recOzH5iAE1xEjyD0",
      sources: [
        {
          id: "recZFPfGRo5C9FF2Q",
          brandId: "recOzH5iAE1xEjyD0",
          status: "Extracted",
          sourceQuality: "High",
          approvedForExplorerUse: "Yes",
          sourceOrigin: "Brand Provided",
          sourceType: "Development Brochure",
          region: "CALA",
          lastReviewed: "2026-07-06",
        },
        {
          id: "recxm2Jxqvi2n2I8K",
          brandId: "recOzH5iAE1xEjyD0",
          status: "Extracted",
          sourceQuality: "Medium",
          approvedForExplorerUse: "Yes",
          sourceOrigin: "Public Web",
          sourceType: "Website Capture",
          region: "CALA",
          lastReviewed: "2026-07-06",
        },
        {
          id: "recRbi8CjS8BVt4Z3",
          brandId: "recOzH5iAE1xEjyD0",
          status: "Extracted",
          sourceQuality: "Medium",
          approvedForExplorerUse: "Yes",
          sourceOrigin: "Public Web",
          sourceType: "Press Release",
          sourceTitle: "Comfort press kit",
          sourceUrl: "https://media.choicehotels.com/comfort-press-kit",
          region: "CALA",
          lastReviewed: "2026-07-06",
        },
      ],
      facts: [
        {
          id: "recF1",
          brandId: "recOzH5iAE1xEjyD0",
          sourceRecordId: "recZFPfGRo5C9FF2Q",
          fieldName: "be.identity.brandName",
          humanReviewStatus: "Approved",
          approvedValue: "Comfort Inn & Suites",
          publicVisibility: "Public",
        },
        {
          id: "recF2",
          brandId: "recOzH5iAE1xEjyD0",
          sourceRecordId: "recZFPfGRo5C9FF2Q",
          fieldName: "be.identity.parentCompany",
          humanReviewStatus: "Approved",
          approvedValue: "Choice Hotels International",
          publicVisibility: "Public",
        },
        {
          id: "recF3",
          brandId: "recOzH5iAE1xEjyD0",
          sourceRecordId: "recxm2Jxqvi2n2I8K",
          fieldName: "be.overview.developmentModel",
          humanReviewStatus: "Approved",
          approvedValue: "Franchise development",
          publicVisibility: "Public",
        },
        {
          id: "recF4",
          brandId: "recOzH5iAE1xEjyD0",
          sourceRecordId: "recRbi8CjS8BVt4Z3",
          fieldName: "be.loyalty.programName",
          humanReviewStatus: "Approved",
          approvedValue: "Choice Privileges",
          publicVisibility: "Public",
        },
      ],
      published: [],
    },
    {
      id: "recOzH5iAE1xEjyD0",
      entityType: "brand",
      name: "Comfort Inn & Suites",
      fields: {
        "Validation Status": live.validationStatus,
        "Usage Permission": live.usagePermission,
        "Source Type": live.sourceType,
        "Source Region": live.sourceRegion,
        "Confidence Level": live.confidenceLevel,
        "Last Reviewed Date": live.lastReviewedDate,
        "External Display Status": live.externalDisplayStatus,
        "Company Validated": false,
        "Internal Notes": live.internalNotes,
      },
    }
  );
  assert.equal(assessment.eligible, true);
  assert.ok(isStableGovernanceChangeClass(assessment.changeClass));
}

// Real trust-label mismatch remains conflict (Source-Informed proposal vs Company Published live)
{
  const live = {
    validationStatus: GOVERNANCE_VALIDATION_STATUS.companyPublished,
    usagePermission: GOVERNANCE_USAGE_PERMISSION.platformDisplayAllowed,
    sourceType: "Company PDF / Brochure",
    confidenceLevel: "High",
    externalDisplayStatus: GOVERNANCE_EXTERNAL_DISPLAY.showTrustLabel,
    companyValidated: false,
  };
  const proposed = {
    validationStatus: GOVERNANCE_VALIDATION_STATUS.sourceInformed,
    usagePermission: GOVERNANCE_USAGE_PERMISSION.platformDisplayAllowed,
    sourceType: "Press Release",
    confidenceLevel: "Medium",
    externalDisplayStatus: GOVERNANCE_EXTERNAL_DISPLAY.showTrustLabel,
    companyValidated: false,
  };
  assert.equal(isEquivalentStableLiveGovernance(live, proposed, "brand"), false);
  assert.equal(classifyGovernanceChange(live, proposed, { entityType: "brand" }), "downgrade");
}

// Brand Explorer governance: rejected/internal facts do not block when public approved facts are source-backed
{
  const src = {
    id: "recSRC1",
    status: "Approved",
    sourceQuality: "High",
    approvedForExplorerUse: "Yes",
  };
  const liveState = {
    sources: [src],
    facts: [
      {
        id: "recF1",
        fieldName: "be.identity.brandName",
        explorerType: "Brand Explorer",
        humanReviewStatus: "Approved",
        publicVisibility: "Public",
        sourceRecordId: "recSRC1",
        extractedValue: "Kimpton Hotels",
        approvedValue: "Kimpton Hotels",
        sourceQuality: "High",
        confidenceLevel: "High",
      },
      {
        id: "recF2",
        fieldName: "be.overview.thin",
        explorerType: "Brand Explorer",
        humanReviewStatus: "Rejected",
        publicVisibility: "Internal Only",
        sourceRecordId: "recSRC1",
        extractedValue: "internal draft",
      },
    ],
  };
  const r = assessBrandExplorerGovernanceReadiness(liveState);
  assert.equal(r.governedPlatformReady, true);
  assert.equal(r.breakdown.rejectedInternal, 1);
  assert.equal(r.breakdown.pendingPublic, 0);
}

// Pending public facts still block explorer governance
{
  const r = assessBrandExplorerGovernanceReadiness({
    sources: [
      {
        id: "recSRC1",
        status: "Approved",
        sourceQuality: "High",
        approvedForExplorerUse: "Yes",
      },
    ],
    facts: [
      {
        id: "recF1",
        fieldName: "be.identity.brandName",
        humanReviewStatus: "Pending",
        publicVisibility: "Public",
        sourceRecordId: "recSRC1",
      },
    ],
  });
  assert.equal(r.governedPlatformReady, false);
  assert.ok(r.blockers.some((b) => b.startsWith("pending_public_explorer_facts")));
}

// expansion backlog target detection for Final QA scoring scope
{
  const { isExpansionBacklogBrandTarget } = await import(
    "../lib/partner-intelligence/brand-explorer-final-qa-auditor.js"
  );
  assert.equal(
    isExpansionBacklogBrandTarget({ resolution: { resolutionSource: "expansion_backlog" } }),
    true
  );
  assert.equal(
    isExpansionBacklogBrandTarget({ resolution: { resolutionSource: "active_registry" } }),
    false
  );
  assert.equal(
    isExpansionBacklogBrandTarget({
      slug: "radisson-individuals-by-choice",
      resolution: { resolutionSource: "record_id" },
    }),
    true
  );
}

console.log("test-partner-intelligence-publish-readiness: ok");
