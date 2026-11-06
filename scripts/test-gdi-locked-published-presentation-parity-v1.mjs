#!/usr/bin/env node
/**
 * Fail-closed: locked published presentation fields must survive list DTO
 * construction, classification attach, sales-action resolve, and freeze —
 * including lean hydration without Airtable merge (production share path).
 */
import assert from "node:assert/strict";
import { attachCustomerClassificationPresentation } from "../lib/group-demand-intelligence/customer-classification-presentation-v1.js";
import { resolveCustomerSalesAction } from "../lib/group-demand-intelligence/customer-sales-action-resolver-v1.js";
import { toOpportunityListDto } from "../lib/group-demand-intelligence/opportunity-list-dto.js";
import { freezePublishedPresentationFields } from "../lib/group-demand-intelligence/durable-publication/gdi-one-click-hotel-publish-v1.js";
import { resolveOpportunityRevenueStreamIcons } from "../lib/group-demand-intelligence/opportunity-revenue-stream-icons-v1.js";

const CARPHA_LIKE = {
  id: "gdi_s4912_ba7b1cb8a3ab",
  hotelId: "recKRJjcPnb4tVDDS",
  title: "71st CARPHA Annual Health Research Conference — One Health",
  displayTitle: "71st CARPHA Annual Health Research Conference — One Health",
  publishedPresentationLocked: true,
  gdiPublishedInventoryRegistry: true,
  salesActionKey: "CONTACT_BUYER",
  salesActionPillLabel: "Contact Buyer",
  priority: "MEDIUM_PRIORITY",
  priorityFilterKey: "MEDIUM_PRIORITY",
  // Lean share hydration often loses this gate — locked action must still win.
  hasVerifiedNamedRelevantContact: false,
  tileContactName: "Roxanne Colthrust",
  tileContactRoleLabel: "Events & Workshop Coordinator",
  primaryContact: {
    name: "Roxanne Colthrust",
    role: "Events & Workshop Coordinator",
  },
  secondaryContact: {
    name: "Veron Guy-Parks",
    role: "Finance Officer / Project Accountant",
  },
  salesOpportunityQuickFilterKeys: [
    "HOTEL_SOURCING_NOT_CONFIRMED",
    "FUTURE_GROUP_BUSINESS",
  ],
  commercialInventoryClass: "SALES_READY",
  commercialClassification: "SALES_READY",
  establishedFacts: ["fact-a", "fact-b", "fact-c"],
  sources: [{ url: "https://example.com/a", label: "A" }],
};

const SAVOR_LIKE = {
  id: "gdi_s4912_03deda917778",
  hotelId: "recKRJjcPnb4tVDDS",
  title: "Savor 10 Grenada — Full Circle 10th Anniversary Gala Dinner & Dance",
  publishedPresentationLocked: true,
  gdiPublishedInventoryRegistry: true,
  salesActionKey: "BUILD_ACCOUNT",
  salesActionPillLabel: "Build Account",
  // Storage drift: raw priority blank/Watch while approved filter is Medium.
  priority: "WATCHLIST",
  priorityFilterKey: "MEDIUM_PRIORITY",
  hasVerifiedNamedRelevantContact: true,
  tileContactName: "Tasha La Touche",
  primaryContact: { name: "Tasha La Touche", role: "Founder" },
  salesOpportunityQuickFilterKeys: [
    "HOTEL_SOURCING_NOT_CONFIRMED",
    "FUTURE_GROUP_BUSINESS",
  ],
  commercialInventoryClass: "ACCOUNT_DEVELOPMENT",
};

function testLockedSalesActionSurvivesLeanHydration() {
  const sales = resolveCustomerSalesAction(CARPHA_LIKE);
  assert.equal(sales.key, "CONTACT_BUYER");
  assert.equal(sales.label, "Contact Buyer");
}

function testLockedFieldsSurviveListDtoAndAttach() {
  const listDto = toOpportunityListDto(CARPHA_LIKE, { hotelId: CARPHA_LIKE.hotelId });
  assert.equal(listDto.publishedPresentationLocked, true);
  assert.equal(listDto.salesActionKey, "CONTACT_BUYER");
  assert.equal(listDto.priorityFilterKey, "MEDIUM_PRIORITY");
  assert.deepEqual(listDto.salesOpportunityQuickFilterKeys, [
    "HOTEL_SOURCING_NOT_CONFIRMED",
    "FUTURE_GROUP_BUSINESS",
  ]);

  const attached = attachCustomerClassificationPresentation(listDto);
  assert.equal(attached.salesActionKey, "CONTACT_BUYER");
  assert.equal(attached.salesActionPillLabel, "Contact Buyer");
  assert.equal(attached.priorityFilterKey, "MEDIUM_PRIORITY");
  assert.equal(attached.priority, "MEDIUM_PRIORITY");
  assert.deepEqual(attached.salesOpportunityQuickFilterKeys, [
    "HOTEL_SOURCING_NOT_CONFIRMED",
    "FUTURE_GROUP_BUSINESS",
  ]);
}

function testSavorMediumSurvivesDespiteWatchStorage() {
  const listDto = toOpportunityListDto(SAVOR_LIKE, { hotelId: SAVOR_LIKE.hotelId });
  const attached = attachCustomerClassificationPresentation(listDto);
  assert.equal(attached.priorityFilterKey, "MEDIUM_PRIORITY");
  assert.equal(attached.priority, "MEDIUM_PRIORITY");
  assert.equal(attached.salesActionPillLabel, "Build Account");
  assert.deepEqual(attached.salesOpportunityQuickFilterKeys, [
    "HOTEL_SOURCING_NOT_CONFIRMED",
    "FUTURE_GROUP_BUSINESS",
  ]);
}

function testFreezePreservesApprovedPresentationObject() {
  const frozen = freezePublishedPresentationFields(CARPHA_LIKE, {
    opportunityId: CARPHA_LIKE.id,
    hotelId: CARPHA_LIKE.hotelId,
    establishedFacts: CARPHA_LIKE.establishedFacts,
    sources: CARPHA_LIKE.sources,
  });
  assert.equal(frozen.publishedPresentationLocked, true);
  assert.equal(frozen.salesActionKey, "CONTACT_BUYER");
  assert.equal(frozen.salesActionPillLabel, "Contact Buyer");
  assert.equal(frozen.priorityFilterKey, "MEDIUM_PRIORITY");
  assert.equal(frozen.priority, "MEDIUM_PRIORITY");
  assert.equal(frozen.hasVerifiedNamedRelevantContact, false);
  assert.equal(frozen.tileContactName, "Roxanne Colthrust");
  assert.equal(frozen.primaryContact?.name, "Roxanne Colthrust");
  assert.equal(frozen.secondaryContact?.name, "Veron Guy-Parks");
  assert.deepEqual(frozen.salesOpportunityQuickFilterKeys, [
    "HOTEL_SOURCING_NOT_CONFIRMED",
    "FUTURE_GROUP_BUSINESS",
  ]);
  assert.equal((frozen.establishedFacts || []).length, 3);
}

function testMutedRevenueStreamsOmitNotIndicatedCopy() {
  const icons = resolveOpportunityRevenueStreamIcons({
    hotelRevenueStreamClass: "REVENUE_STREAM_UNCONFIRMED",
  });
  const blob = JSON.stringify(icons.aria || {});
  assert.equal(/not indicated/i.test(blob), false);
}

const tests = [
  ["locked_sales_action_survives_lean_hydration", testLockedSalesActionSurvivesLeanHydration],
  ["locked_fields_survive_list_dto_and_attach", testLockedFieldsSurviveListDtoAndAttach],
  ["savor_medium_survives_despite_watch_storage", testSavorMediumSurvivesDespiteWatchStorage],
  ["freeze_preserves_approved_presentation_object", testFreezePreservesApprovedPresentationObject],
  ["muted_revenue_streams_omit_not_indicated_copy", testMutedRevenueStreamsOmitNotIndicatedCopy],
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
    {
      ok,
      contract: "GDI_LOCKED_PUBLISHED_PRESENTATION_PARITY_V1",
      results,
    },
    null,
    2,
  ),
);
process.exit(ok ? 0 : 1);
