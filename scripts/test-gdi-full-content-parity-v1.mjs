#!/usr/bin/env node
/**
 * Universal 100% full-content parity regressions (fixture + freeze round-trip).
 */
import assert from "node:assert/strict";
import {
  buildApprovedFullContentPresentation,
  compareApprovedFullContent,
  compareFullContentPayloads,
  assertContactMultiplicity,
  labelSubsetOnlyPass,
  FULL_CONTENT_PARITY_RESULT,
} from "../lib/group-demand-intelligence/presentation/gdi-full-content-parity-v1.js";
import { discoverCustomerFacingSchema } from "../lib/group-demand-intelligence/presentation/gdi-customer-facing-schema-discovery-v1.js";
import { freezePublishedPresentationFields } from "../lib/group-demand-intelligence/durable-publication/gdi-one-click-hotel-publish-v1.js";
import { hydrateOpportunityFromPublishedCommercialFields } from "../lib/group-demand-intelligence/durable-publication/gdi-customer-facing-published-snapshot-v1.js";
import { projectOpportunityForPresentation } from "../lib/group-demand-intelligence/presentation/gdi-universal-presentation-pipeline-v1.js";
import { GDI_PRESENTATION_SURFACES } from "../lib/group-demand-intelligence/contracts/gdi-presentation-contract-v1.js";
import { getAtPath } from "../lib/group-demand-intelligence/presentation/gdi-customer-facing-schema-discovery-v1.js";

const AYESHA = {
  id: "gdi_s4912_cb2d71ab1b15",
  hotelId: "recKRJjcPnb4tVDDS",
  title: "A'yesha Gaetán Collectives LLC",
  organizationName: "A'yesha Gaetán Collectives LLC",
  commercialInventoryClass: "ACCOUNT_DEVELOPMENT",
  commercialClassification: "ACCOUNT_DEVELOPMENT",
  demandStage: "RELATIONSHIP_DEVELOPMENT",
  priority: "WATCHLIST",
  priorityFilterKey: "WATCHLIST",
  salesActionKey: "ENGAGE_PLANNER",
  salesActionPillLabel: "Engage Planner",
  whatIsHappening:
    "Intermediary destination-wedding planner relationship development — no verified live couple brief.",
  potentialHotelBusiness:
    "Account-development pathway; no production forecast or room-block commitment.",
  whyThisHotel: "Strong qualitative fit for intimate luxury destination weddings.",
  whyThisHotelAdvantages: ["Intimate luxury fit", "All-inclusive wedding packages"],
  whyThisHotelLimitations: ["No verified live couple brief"],
  whyActNow: "Develop the intermediary relationship before a live programme exists.",
  establishedFacts: [
    "A'yesha is a certified wedding planner and travel designer.",
    "She has personally toured Spice Island Beach Resort.",
  ],
  uncertainties: [
    "No verified live couple brief or room block.",
    "Direct email and contracting authority not verified.",
  ],
  recommendedNextStep:
    "Develop the intermediary account first; ask whether an active Grenada couple brief exists.",
  qualificationQuestions: [
    "Do you currently have a Grenada couple brief?",
    "Who signs agreements and carries deposit liability?",
  ],
  sources: [
    {
      url: "https://ayeshagaetan.com/spice-island-beach-resort-grenada-review/",
      label: "Spice Island review",
    },
    {
      url: "https://ayeshagaetan.com/grenada/",
      label: "Grenada shortlist",
    },
  ],
  primaryContact: {
    name: "A'yesha Gaetán",
    role: "Certified Wedding Planner and Travel Designer",
    title: "Certified Wedding Planner and Travel Designer",
  },
  secondaryContact: {
    name: "Adrienne Brown",
    role: "Owner and Lead Travel Advisor",
    title: "Owner and Lead Travel Advisor",
    organization: "Breathless Getaways",
    whyThisContact: "Host-agency owner supporting independent advisors.",
  },
  backupContacts: [
    {
      name: "Adrienne Brown",
      role: "Owner and Lead Travel Advisor",
      organization: "Breathless Getaways",
    },
  ],
};

function testSchemaDiscoversNewFieldsAutomatically() {
  const base = discoverCustomerFacingSchema([AYESHA]);
  const withNew = discoverCustomerFacingSchema([
    { ...AYESHA, brandNewCustomerField: "must be discovered" },
  ]);
  assert.ok(withNew.topLevelKeys.includes("brandNewCustomerField"));
  assert.ok(withNew.pathCount > base.pathCount);
}

function testFreezeRoundTripFullContentParity() {
  const frozen = freezePublishedPresentationFields(AYESHA, {
    opportunityId: AYESHA.id,
    hotelId: AYESHA.hotelId,
  });
  assert.equal(frozen.publishedPresentationLocked, true);
  assert.ok(frozen.approvedCustomerFacingDetail);
  assert.ok(frozen.scalingTwoContactPresentation?.primary?.customerFacingNamed);
  assert.equal(frozen.primaryContact?.name, "A'yesha Gaetán");
  assert.equal(frozen.secondaryContact?.name, "Adrienne Brown");
  assert.equal((frozen.backupContacts || []).length, 0);
  assert.equal(frozen.demandStage, "RELATIONSHIP_DEVELOPMENT");
  assert.ok((frozen.sources || []).length >= 2);
  assert.ok((frozen.qualificationQuestions || []).length >= 2);

  const left = buildApprovedFullContentPresentation(AYESHA, {
    hotelId: AYESHA.hotelId,
  });
  const frozenPayload = frozen.approvedCustomerFacingDetail;
  const direct = compareFullContentPayloads(left.payload, frozenPayload);
  assert.equal(
    direct.result,
    FULL_CONTENT_PARITY_RESULT.FULL_CONTENT_PARITY_PASS,
    JSON.stringify(direct.diffs?.slice(0, 20), null, 2),
  );

  const hydrated = hydrateOpportunityFromPublishedCommercialFields(frozen);
  const cmp = compareApprovedFullContent(AYESHA, hydrated, {
    hotelId: AYESHA.hotelId,
  });
  assert.equal(
    cmp.result,
    FULL_CONTENT_PARITY_RESULT.FULL_CONTENT_PARITY_PASS,
    JSON.stringify(cmp.diffs?.slice(0, 20), null, 2),
  );
  assert.equal(cmp.ok, true);
  assert.equal(cmp.usedFrozenApprovedDetail, true);
}

function testContactMultiplicityAyesha() {
  const frozen = freezePublishedPresentationFields(AYESHA, {
    opportunityId: AYESHA.id,
    hotelId: AYESHA.hotelId,
  });
  const m = assertContactMultiplicity(frozen, {
    primary: "A'yesha Gaetán",
    secondary: "Adrienne Brown",
    requireTwoContact: true,
  });
  assert.equal(m.ok, true, JSON.stringify(m.fails));
}

function testDriftFails() {
  const drifted = {
    ...AYESHA,
    recommendedNextStep: "Hold room inventory and forecast production now.",
  };
  const cmp = compareApprovedFullContent(AYESHA, drifted);
  assert.equal(cmp.ok, false);
  assert.ok(
    cmp.result === FULL_CONTENT_PARITY_RESULT.FULL_CONTENT_PARITY_FAIL ||
      cmp.result === FULL_CONTENT_PARITY_RESULT.INTEGRITY_BLOCKED,
  );
}

function testSubsetLabelDoesNotUpgrade() {
  const s = labelSubsetOnlyPass("tile_drawer");
  assert.equal(s.result, FULL_CONTENT_PARITY_RESULT.TILE_DRAWER_FIELD_SUBSET_PASS);
  assert.equal(s.upgradesToFullContentParity, false);
}

function testPayloadSelfCompare() {
  const a = buildApprovedFullContentPresentation(AYESHA, { lock: true });
  const cmp = compareFullContentPayloads(a.payload, a.payload);
  assert.equal(cmp.ok, true);
  assert.equal(cmp.hashMatch, true);
}

/**
 * Universal regression: freeze → hydrate → EXTERNAL_SHARE detail DTO must
 * retain every nested approved customer-facing field. Applies to all hotels.
 */
function testApprovedDetailSurvivesExternalShareDetailDto() {
  const frozen = freezePublishedPresentationFields(AYESHA, {
    opportunityId: AYESHA.id,
    hotelId: AYESHA.hotelId,
  });
  assert.ok(frozen.approvedCustomerFacingDetail);

  const hydrated = hydrateOpportunityFromPublishedCommercialFields({
    ...frozen,
    publishedPresentationLocked: true,
    gdiPublishedInventoryRegistry: true,
  });
  assert.ok(hydrated.approvedCustomerFacingDetail);

  const projected = projectOpportunityForPresentation(hydrated, {
    hotelId: AYESHA.hotelId,
    surface: GDI_PRESENTATION_SURFACES.EXTERNAL_SHARE,
    view: "detail",
  });
  const dto = projected.dto;
  assert.ok(
    dto?.approvedCustomerFacingDetail,
    "EXTERNAL_SHARE detail DTO dropped approvedCustomerFacingDetail",
  );

  const approved = frozen.approvedCustomerFacingDetail;
  const left = buildApprovedFullContentPresentation(AYESHA, {
    hotelId: AYESHA.hotelId,
  });
  const cmp = compareApprovedFullContent(AYESHA, dto, {
    hotelId: AYESHA.hotelId,
  });
  assert.equal(cmp.usedFrozenApprovedDetail, true);
  assert.equal(
    cmp.ok,
    true,
    JSON.stringify(
      {
        diffCount: cmp.diffCount,
        diffs: (cmp.diffs || []).slice(0, 25),
        classification: cmp.classification,
      },
      null,
      2,
    ),
  );

  // Nested leaf survival — any approved path must remain addressable on DTO.
  const schema = left.schema;
  const missing = [];
  for (const path of schema.paths || []) {
    const expected = getAtPath(approved, path);
    if (expected === undefined || expected === null || expected === "") continue;
    if (Array.isArray(expected) && !expected.length) continue;
    if (typeof expected === "object" && !Array.isArray(expected)) continue;
    const onApproved = getAtPath(dto.approvedCustomerFacingDetail, path);
    const onFlat = getAtPath(dto, path);
    if (onApproved === undefined && onFlat === undefined) {
      missing.push(path);
    }
  }
  assert.equal(
    missing.length,
    0,
    `approved nested fields missing from EXTERNAL_SHARE DTO: ${missing.slice(0, 20).join(", ")}`,
  );

  assert.equal(dto.salesActionPillLabel, "Engage Planner");
  assert.equal(dto.primaryContact?.name, "A'yesha Gaetán");
  assert.equal(dto.secondaryContact?.name, "Adrienne Brown");
}

const tests = [
  ["schema_discovers_new_fields_automatically", testSchemaDiscoversNewFieldsAutomatically],
  ["freeze_round_trip_full_content_parity", testFreezeRoundTripFullContentParity],
  ["contact_multiplicity_ayesha", testContactMultiplicityAyesha],
  ["drift_fails", testDriftFails],
  ["subset_label_does_not_upgrade", testSubsetLabelDoesNotUpgrade],
  ["payload_self_compare", testPayloadSelfCompare],
  [
    "approved_detail_survives_external_share_detail_dto",
    testApprovedDetailSurvivesExternalShareDetailDto,
  ],
];

const results = [];
for (const [name, fn] of tests) {
  try {
    fn();
    results.push({ name, ok: true });
  } catch (err) {
    results.push({ name, ok: false, error: String(err?.message || err) });
  }
}

const ok = results.every((r) => r.ok);
console.log(
  JSON.stringify(
    { ok, contract: "GDI_FULL_CONTENT_PARITY_V1", results },
    null,
    2,
  ),
);
process.exit(ok ? 0 : 1);
