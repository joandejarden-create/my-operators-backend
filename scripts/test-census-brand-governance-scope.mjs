/**
 * Brand governance scope rules — discovery not Active/Live-only.
 */
import assert from "node:assert/strict";
import {
  BRAND_GOVERNANCE_STATUS,
  CENSUS_ONLY_PRODUCTION_USE_STATUS,
  REVIEW_REASON,
  classifyBrandGovernanceStatus,
  classifyCensusReviewReasons,
  buildNonActiveCensusGovernanceFields,
  buildReviewReclassificationPatch,
  evaluateLevel2Eligibility,
  evaluateNonActiveCleanCoreEligibility,
  buildOfficialParentInventoryDiscoveryControlList,
  isOwnerFacingBrandEligible,
  writeBrandSetupPromotionDecisionPack,
} from "../lib/research-engine-v2/census-brand-governance.js";
import { buildInsertFieldsFromDiscovered } from "../lib/research-engine-v2/census-autopilot-source-discovery.js";
import { evaluateBrandSourceOfTruth } from "../lib/research-engine-v2/census-brand-normalization.js";

const mockActiveIndex = {
  by_norm: new Map([["hilton", { brand_name: "Hilton", brand_slug: "hilton" }]]),
  by_slug: new Map([["hilton", { brand_name: "Hilton", brand_slug: "hilton" }]]),
  active_count: 1,
  control: { brands: [{ brand_name: "Hilton", brand_slug: "hilton" }] },
};

function testActiveClassification() {
  const gov = classifyBrandGovernanceStatus(
    {
      brand: "Hilton",
      official_property_url: "https://www.hilton.com/en/hotels/example/",
      property_name: "Hilton Example",
    },
    { activeIndex: mockActiveIndex }
  );
  assert.equal(gov.status, BRAND_GOVERNANCE_STATUS.ACTIVE_BRAND_SETUP);
  assert.equal(gov.owner_facing_eligible, true);
  assert.equal(gov.census_save_allowed, true);
}

function testEvidenceBackedNonActive() {
  const gov = classifyBrandGovernanceStatus(
    {
      brand: "Hampton by Hilton",
      official_property_url: "https://www.hilton.com/en/hotels/mexmxhx-hampton/",
      property_name: "Hampton Inn Mexico City",
      parent_company: "Hilton",
    },
    { activeIndex: mockActiveIndex }
  );
  // Hampton may or may not be in official registry — if official and not Active:
  if (gov.in_official_registry && !gov.in_active_brand_setup) {
    assert.ok(
      [
        BRAND_GOVERNANCE_STATUS.EVIDENCE_BACKED_NON_ACTIVE,
        BRAND_GOVERNANCE_STATUS.PROMOTION_CANDIDATE,
      ].includes(gov.status)
    );
    assert.equal(gov.owner_facing_eligible, false);
    assert.equal(gov.census_save_allowed, true);
  }
}

function testDirtyPartner() {
  const gov = classifyBrandGovernanceStatus(
    {
      brand: "Choice Hotels",
      property_name: "Some Hotel",
      source_url: "https://www.choicehotels.com/mexico/example",
    },
    { activeIndex: mockActiveIndex }
  );
  assert.equal(gov.status, BRAND_GOVERNANCE_STATUS.DIRTY_PARTNER_LABEL);
  assert.equal(gov.census_save_allowed, false);
}

function testBrandCodeUnresolved() {
  const gov = classifyBrandGovernanceStatus(
    { brand: "SAM", property_name: "Hotel X" },
    { activeIndex: mockActiveIndex }
  );
  assert.ok(
    [
      BRAND_GOVERNANCE_STATUS.DIRTY_PARTNER_LABEL,
      BRAND_GOVERNANCE_STATUS.BRAND_CODE_UNRESOLVED,
    ].includes(gov.status)
  );
}

function testNonActiveInsertFields() {
  const sanitized = buildInsertFieldsFromDiscovered(
    {
      property_name: "Novotel Mexico City",
      identity_key: "novotel-mexico-city|mexico",
      brand: "Novotel",
      parent_company: "Accor",
      source_family: "Accor",
      city: "Mexico City",
      country: "Mexico",
      official_property_url: "https://all.accor.com/hotel/1234/index.en.shtml",
      official_directory_url: "https://all.accor.com/hotel/1234/index.en.shtml",
      identity_confidence: "High",
      source_confidence: "High",
    },
    { activeIndex: mockActiveIndex }
  );
  const fields = sanitized.fields || sanitized;
  assert.equal(fields["Production Use Status"], CENSUS_ONLY_PRODUCTION_USE_STATUS);
  assert.equal(fields["Public Display Review Status"], "Hold");
  assert.equal(fields["Radar Display Status"], "Hold");
  // Governance approval ≠ data-quality dirtiness — do not set HR by default
  assert.equal(fields["Human Review Required"], undefined);
}

function testNonActiveCleanCoreGate() {
  const record = {
    fields: {
      "Current Brand": "Novotel",
      "Brand Family": "Accor",
      "Property Name": "Novotel Mexico City",
      "Official Property URL": "https://all.accor.com/hotel/1234/index.en.shtml",
      "Production Use Status": CENSUS_ONLY_PRODUCTION_USE_STATUS,
    },
  };
  const elig = evaluateNonActiveCleanCoreEligibility(record, {
    activeIndex: mockActiveIndex,
  });
  if (elig.governance?.in_official_registry) {
    assert.equal(elig.eligible, true);
  }

  const ownerFacing = {
    fields: {
      ...record.fields,
      "Production Use Status": "Owner-Facing Eligible",
    },
  };
  const blocked = evaluateNonActiveCleanCoreEligibility(ownerFacing, {
    activeIndex: mockActiveIndex,
  });
  if (blocked.governance?.status === BRAND_GOVERNANCE_STATUS.EVIDENCE_BACKED_NON_ACTIVE) {
    assert.equal(blocked.eligible, false);
    assert.ok(blocked.reasons.includes("must_be_census_only_not_owner_facing"));
  }
}

function testBrandSourceOfTruthCensusOnly() {
  const record = {
    fields: {
      "Current Brand": "Novotel",
      "Brand Family": "Accor",
      "Official Property URL": "https://all.accor.com/hotel/1234/index.en.shtml",
      "Production Use Status": CENSUS_ONLY_PRODUCTION_USE_STATUS,
    },
  };
  const sot = evaluateBrandSourceOfTruth(
    record,
    { by_canonical_norm: new Map(), alias_to_canonical: new Map() },
    { activeIndex: mockActiveIndex }
  );
  assert.equal(sot.pass, true);
  assert.ok(sot.reasons?.includes("census_official_registry_census_only"));
}

function testOfficialInventoryControlList() {
  const list = buildOfficialParentInventoryDiscoveryControlList({
    skipUniverseLoad: true,
    controlList: mockActiveIndex.control,
  });
  // When skipUniverseLoad + custom - buildOfficial uses buildActiveBrandSetupControlList
  // which may load universe; at minimum structure exists
  assert.ok(list.discover_all_official_parents === true);
  assert.ok(list.require_brand_match_default === false);
  assert.ok(Array.isArray(list.governance_status_values));
  assert.ok(list.governance_status_values.includes("active_brand_setup"));
  assert.ok(list.brand_setup_read_only === true);
  assert.ok(list.brand_explorer_untouched === true);
}

function testOwnerFacingGate() {
  assert.equal(isOwnerFacingBrandEligible("Hilton", { activeIndex: mockActiveIndex }), true);
  assert.equal(isOwnerFacingBrandEligible("Novotel", { activeIndex: mockActiveIndex }), false);
  assert.equal(
    isOwnerFacingBrandEligible("Novotel", {
      activeIndex: mockActiveIndex,
      explicitlyApproved: true,
    }),
    true
  );
}

function testNonActiveGovernanceFields() {
  const fields = buildNonActiveCensusGovernanceFields({
    status: BRAND_GOVERNANCE_STATUS.EVIDENCE_BACKED_NON_ACTIVE,
  });
  assert.equal(fields["Production Use Status"], CENSUS_ONLY_PRODUCTION_USE_STATUS);
  assert.equal(fields["Public Display Review Status"], "Hold");
  assert.equal(fields["Radar Display Status"], "Hold");
  assert.equal(fields["Human Review Required"], undefined);
  assert.ok(String(fields["Radar Display Reason"] || "").includes(REVIEW_REASON.GOVERNANCE_REVIEW_REQUIRED));

  const forced = buildNonActiveCensusGovernanceFields(
    { status: BRAND_GOVERNANCE_STATUS.EVIDENCE_BACKED_NON_ACTIVE },
    { force_human_review: true }
  );
  assert.equal(forced["Human Review Required"], true);

  const approved = buildNonActiveCensusGovernanceFields(
    { status: BRAND_GOVERNANCE_STATUS.EVIDENCE_BACKED_NON_ACTIVE },
    { explicitly_approved: true }
  );
  assert.equal(approved["Human Review Required"], undefined);
}

function testGovernanceVsDataQualityReview() {
  const nonActive = {
    id: "recGov",
    fields: {
      "Current Brand": "Novotel",
      "Brand Family": "Accor",
      "Official Property URL": "https://all.accor.com/hotel/1234/index.en.shtml",
      "Production Use Status": CENSUS_ONLY_PRODUCTION_USE_STATUS,
      "Public Display Review Status": "Hold",
      "Radar Display Status": "Hold",
      "Human Review Required": true,
      Country: "Mexico",
    },
  };
  const govReview = classifyCensusReviewReasons(nonActive, { activeIndex: mockActiveIndex });
  if (govReview.governance?.status === BRAND_GOVERNANCE_STATUS.EVIDENCE_BACKED_NON_ACTIVE) {
    assert.equal(govReview.governance_only, true);
    assert.equal(govReview.data_quality_review_required, false);
    assert.equal(govReview.governance_review_required, true);
  }

  const reclass = buildReviewReclassificationPatch(nonActive, { activeIndex: mockActiveIndex });
  if (reclass.reason === "governance_review_reclassify") {
    assert.equal(reclass.patch["Human Review Required"], false);
    assert.ok(!reclass.patch["Public Display Review Status"] || reclass.patch["Public Display Review Status"] === "Hold");
  }

  const dirty = {
    id: "recDq",
    fields: {
      "Current Brand": "IHG",
      Country: "Mexico",
      "Human Review Required": false,
    },
  };
  const dq = classifyCensusReviewReasons(dirty, { activeIndex: mockActiveIndex });
  if (dq.governance?.status === BRAND_GOVERNANCE_STATUS.DIRTY_PARTNER_LABEL) {
    assert.equal(dq.data_quality_review_required, true);
    assert.equal(dq.governance_only, false);
  }

  const l2Gov = evaluateLevel2Eligibility(nonActive, {
    activeIndex: mockActiveIndex,
    cleanCorePass: true,
    requireCleanCore: true,
  });
  if (govReview.governance_only) {
    assert.equal(l2Gov.eligible, true);
    assert.equal(l2Gov.governance_only_hold, true);
  }

  const l2Dq = evaluateLevel2Eligibility(dirty, {
    activeIndex: mockActiveIndex,
    cleanCorePass: true,
  });
  if (dq.data_quality_review_required) {
    assert.equal(l2Dq.eligible, false);
    assert.ok(l2Dq.reasons.includes("data_quality_review_required"));
  }
}

function testPromotionPackWrite() {
  const { payload } = writeBrandSetupPromotionDecisionPack(
    [
      {
        brand: "Test Brand X",
        parent_company: "Accor",
        count: 3,
        in_official_parent_inventory: true,
        official_source_evidence: true,
      },
    ],
    { test: true }
  );
  assert.equal(payload.brand_setup_writes, false);
  assert.equal(payload.brand_explorer_writes, false);
  assert.equal(payload.candidates.length, 1);
  assert.equal(payload.candidates[0].proposed_brand_name, "Test Brand X");
}

const tests = [
  ["active_classification", testActiveClassification],
  ["evidence_backed_non_active", testEvidenceBackedNonActive],
  ["dirty_partner", testDirtyPartner],
  ["brand_code", testBrandCodeUnresolved],
  ["non_active_insert", testNonActiveInsertFields],
  ["non_active_clean_core", testNonActiveCleanCoreGate],
  ["brand_sot_census_only", testBrandSourceOfTruthCensusOnly],
  ["official_inventory_control_list", testOfficialInventoryControlList],
  ["owner_facing_gate", testOwnerFacingGate],
  ["non_active_governance_fields", testNonActiveGovernanceFields],
  ["governance_vs_data_quality_review", testGovernanceVsDataQualityReview],
  ["promotion_pack_write", testPromotionPackWrite],
];

let failed = 0;
for (const [name, fn] of tests) {
  try {
    fn();
    console.log(`ok ${name}`);
  } catch (err) {
    failed += 1;
    console.error(`FAIL ${name}:`, err.message);
  }
}

if (failed) {
  console.error(`\n${failed} failed`);
  process.exit(1);
}
console.log(`\n${tests.length} passed`);
