#!/usr/bin/env node
/**
 * Universal regression (all GDI hotels):
 * freeze → hydrate → EXTERNAL_SHARE detail DTO must retain
 * approvedCustomerFacingDetail and every nested customer-facing leaf.
 *
 * Fails closed if the share whitelist / DTO boundary drops approved content.
 */
import assert from "node:assert/strict";
import { freezePublishedPresentationFields } from "../lib/group-demand-intelligence/durable-publication/gdi-one-click-hotel-publish-v1.js";
import { hydrateOpportunityFromPublishedCommercialFields } from "../lib/group-demand-intelligence/durable-publication/gdi-customer-facing-published-snapshot-v1.js";
import { projectOpportunityForPresentation } from "../lib/group-demand-intelligence/presentation/gdi-universal-presentation-pipeline-v1.js";
import { GDI_PRESENTATION_SURFACES } from "../lib/group-demand-intelligence/contracts/gdi-presentation-contract-v1.js";
import {
  buildApprovedFullContentPresentation,
  compareApprovedFullContent,
} from "../lib/group-demand-intelligence/presentation/gdi-full-content-parity-v1.js";
import { getAtPath } from "../lib/group-demand-intelligence/presentation/gdi-customer-facing-schema-discovery-v1.js";
import { sanitizeOpportunityForShare } from "../lib/group-demand-intelligence/share/gdi-signed-share-capability-v1.js";

const FIXTURES = [
  {
    id: "gdi_fixture_ayesha",
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
    whatIsHappening: "Intermediary destination-wedding planner relationship development.",
    potentialHotelBusiness: "Account-development pathway; no production forecast.",
    whyThisHotel: "Strong qualitative fit for intimate luxury destination weddings.",
    whyActNow: "Develop the intermediary relationship before a live programme exists.",
    establishedFacts: ["A'yesha is a certified wedding planner and travel designer."],
    uncertainties: ["No verified live couple brief or room block."],
    recommendedNextStep: "Ask whether an active Grenada couple brief exists.",
    qualificationQuestions: ["Do you currently have a Grenada couple brief?"],
    sources: [{ url: "https://ayeshagaetan.com/grenada/", label: "Grenada" }],
    primaryContact: {
      name: "A'yesha Gaetán",
      role: "Certified Wedding Planner and Travel Designer",
    },
    secondaryContact: {
      name: "Adrienne Brown",
      role: "Owner and Lead Travel Advisor",
      organization: "Breathless Getaways",
    },
  },
  {
    id: "gdi_fixture_generic_hotel_b",
    hotelId: "recEXAMPLEHOTEL0001",
    title: "Generic Association Conference",
    organizationName: "Generic Association",
    commercialInventoryClass: "SALES_READY",
    commercialClassification: "SALES_READY",
    priority: "MEDIUM_PRIORITY",
    priorityFilterKey: "MEDIUM_PRIORITY",
    salesActionKey: "CONTACT_BUYER",
    salesActionPillLabel: "Contact Buyer",
    whatIsHappening: "Annual association conference sourcing hotels.",
    potentialHotelBusiness: "Peak room block opportunity for peak nights.",
    whyThisHotel: "Fits midscale association programme needs.",
    whyActNow: "RFP window is open this quarter.",
    establishedFacts: ["Event dates published on association site."],
    uncertainties: ["Exact peak-room figure not confirmed."],
    recommendedNextStep: "Contact the named buyer with hotel fit brief.",
    qualificationQuestions: ["What is the confirmed peak-room requirement?"],
    sources: [{ url: "https://example.org/event", label: "Event page" }],
    primaryContact: { name: "Jordan Lee", role: "Meetings Manager" },
    secondaryContact: { name: "Casey Ng", role: "Director of Events" },
  },
];

function assertShareSurvival(fixture) {
  const frozen = freezePublishedPresentationFields(fixture, {
    opportunityId: fixture.id,
    hotelId: fixture.hotelId,
  });
  assert.ok(frozen.approvedCustomerFacingDetail, `${fixture.id}: freeze missing approved`);

  // Prove whitelist alone drops the nested object (the production defect).
  const whitelisted = sanitizeOpportunityForShare({
    ...frozen,
    publishedPresentationLocked: true,
  });
  assert.equal(
    whitelisted?.approvedCustomerFacingDetail,
    undefined,
    `${fixture.id}: expected whitelist to omit approved (defect baseline)`,
  );

  const hydrated = hydrateOpportunityFromPublishedCommercialFields({
    ...frozen,
    publishedPresentationLocked: true,
    gdiPublishedInventoryRegistry: true,
  });

  const { dto } = projectOpportunityForPresentation(hydrated, {
    hotelId: fixture.hotelId,
    surface: GDI_PRESENTATION_SURFACES.EXTERNAL_SHARE,
    view: "detail",
  });

  assert.ok(
    dto?.approvedCustomerFacingDetail,
    `${fixture.id}: EXTERNAL_SHARE detail dropped approvedCustomerFacingDetail`,
  );

  const cmp = compareApprovedFullContent(fixture, dto, {
    hotelId: fixture.hotelId,
  });
  assert.equal(cmp.usedFrozenApprovedDetail, true, `${fixture.id}: cert must use frozen`);
  assert.equal(
    cmp.ok,
    true,
    `${fixture.id}: full-content fail ${JSON.stringify((cmp.diffs || []).slice(0, 15))}`,
  );

  const approved = frozen.approvedCustomerFacingDetail;
  const schema = buildApprovedFullContentPresentation(fixture, {
    hotelId: fixture.hotelId,
  }).schema;
  const missing = [];
  for (const path of schema.paths || []) {
    const expected = getAtPath(approved, path);
    if (expected === undefined || expected === null || expected === "") continue;
    if (Array.isArray(expected) && !expected.length) continue;
    if (typeof expected === "object" && !Array.isArray(expected)) continue;
    const onApproved = getAtPath(dto.approvedCustomerFacingDetail, path);
    const onFlat = getAtPath(dto, path);
    if (onApproved === undefined && onFlat === undefined) missing.push(path);
  }
  assert.equal(
    missing.length,
    0,
    `${fixture.id}: nested approved fields missing: ${missing.slice(0, 25).join(", ")}`,
  );
}

const results = [];
for (const fixture of FIXTURES) {
  try {
    assertShareSurvival(fixture);
    results.push({ id: fixture.id, hotelId: fixture.hotelId, ok: true });
  } catch (err) {
    results.push({
      id: fixture.id,
      hotelId: fixture.hotelId,
      ok: false,
      error: String(err?.message || err),
    });
  }
}

const ok = results.every((r) => r.ok);
console.log(
  JSON.stringify(
    {
      ok,
      contract: "GDI_EXTERNAL_SHARE_APPROVED_DETAIL_SURVIVAL_V1",
      universal: true,
      results,
    },
    null,
    2,
  ),
);
process.exit(ok ? 0 : 1);
