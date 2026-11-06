#!/usr/bin/env node
import assert from "node:assert/strict";
import {
  buildSalesIntelligencePacket,
  buildWhyNow,
  commercialCategoryForOpportunity,
  shouldBuildLodgingSalesPacket,
  applySalesIntelligencePacketIfNeeded,
} from "../lib/group-demand-intelligence/sales-intelligence-packet-v1.js";
import { isGdiCustomerOpportunityReady } from "../lib/group-demand-intelligence/customer-readiness-gate-v1.js";
import { evaluateProspectPublicationEligibility } from "../lib/group-demand-intelligence/prospect-publication-eligibility-v1.js";

const lbs = {
  id: "gdi_e2d_test_lbs",
  organizationName: "London Business School",
  title: "London Business School — executive education open programmes 2027",
  eventName: "London Business School executive education open programmes 2027",
  eventStartDate: "2027-01-15",
  eventDemandMechanism: "SMALL_GROUP_LODGING",
  eventTerritoryClass: "TERRITORY_COMPETITIVE",
  commercialDisposition: "HOTEL_SALES_PROSPECT",
  demandEvidenceLabel: "INFERRED_DEMAND",
  eventDemandClass: "INFERRED",
  futureDemand: {
    supported: true,
    dateWindow: "2027-01-15",
    sourceUrl: "https://www.london.edu/",
  },
  localConnection:
    "Open executive education programmes draw travelling participants who often stay in central / Marylebone lodging during multi-day modules.",
  hotelFitRationale:
    "Citywide / competitive London positioning; credible only for compact SMALL_GROUP_LODGING demand, not mass blocks. Demand is inferred from verified future activity and mechanism rationale; no confirmed room block is claimed.",
  officialSource: "https://www.london.edu/",
  sources: [{ url: "https://www.london.edu/" }],
  contact: {
    businessEmail: "execed@london.edu",
    contactFormUrl: "https://www.london.edu/about/contact",
  },
  confirmedBuying: false,
};

// 1) Packet synthesizes from existing evidence without inventing confirmed demand
{
  const built = buildSalesIntelligencePacket(lbs);
  assert.equal(built.complete, true, "LBS packet should complete from existing evidence");
  assert.ok(built.packet.summaryWhat);
  assert.ok(built.packet.whyNow);
  assert.ok(built.packet.recommendedAction);
  assert.ok(built.packet.buyerRole);
  assert.match(built.packet.summaryWhat, /inferred|unconfirmed|without verified lodging/i);
  assert.doesNotMatch(built.packet.summaryWhat, /confirmed demand|open room block|actively sourcing/i);
  assert.doesNotMatch(built.packet.summaryWhat, /\bconfirmed\b.*\broom block\b/i);
  assert.equal(built.packet.evidenceStatusLabel, "Inferred");
  assert.match(built.packet.opportunityTypeLabel, /Executive Education/i);
}

// 2) No fabricated why-now without timing
{
  const noTiming = { ...lbs, eventStartDate: null, futureDemand: { supported: false } };
  const why = buildWhyNow(noTiming);
  assert.equal(why.ok, false);
  assert.equal(why.whyNow, null);
  const ongoing = buildWhyNow({
    ...lbs,
    eventStartDate: null,
    futureDemand: { supported: false },
    eventDateDisplay: "Ongoing corporate relationship",
    timing: "Ongoing",
  });
  assert.equal(ongoing.ok, false, "must not treat Ongoing relationship as why-now");
}

// 3) No fabricated named buyer
{
  const built = buildSalesIntelligencePacket(lbs);
  assert.ok(!built.patched.primaryContact?.name || built.patched.primaryContact.name === "");
  assert.match(built.packet.recommendedAction, /executive education/i);
  assert.doesNotMatch(built.packet.recommendedAction, /\b(Mr|Ms|Mrs)\.\s+[A-Z]/);
}

// 4) Ready can pass after packet (surface + packet fields)
{
  const built = buildSalesIntelligencePacket(lbs);
  const elig = evaluateProspectPublicationEligibility(built.patched);
  assert.equal(elig.hotelFacing, true);
  const ready = isGdiCustomerOpportunityReady(built.patched, { nowDate: "2026-10-19" });
  assert.equal(ready.ok, true, `Ready should pass: ${JSON.stringify(ready.failed)}`);
}

// 5) Partnership category not forced into lodging sales label
{
  const cat = commercialCategoryForOpportunity({
    organizationName: "Marylebone Village",
    commercialDisposition: "STRATEGIC_PARTNERSHIP",
    opportunityType: "VENUE_PARTNERSHIP",
  });
  assert.match(cat.commercialCategory, /partnership/i);
}

// 6) Speculative stretch spillover still does not invent confirmed buying in packet
{
  const stretch = {
    organizationName: "Popcultr Marketing Summit",
    eventName: "popcultr marketing summit",
    eventStartDate: "2026-10-23",
    eventDemandMechanism: "EXECUTIVE_DELEGATION",
    eventTerritoryClass: "TERRITORY_STRETCH",
    demandEvidenceLabel: "SIGNAL_ONLY",
    localConnection: "overnight spillover into Marylebone — not mass exhibitor room blocks",
    hotelFitRationale: "stretch vs Marylebone",
    contact: { businessEmail: "contact@popcultr.events" },
    sources: [{ url: "https://www.excel.london/whats-on/popcultr" }],
  };
  const built = buildSalesIntelligencePacket(stretch);
  assert.equal(built.packet.confirmedBuying, false);
  assert.equal(built.packet.evidenceStatusLabel, "Exploratory");
}

// 7) Partnership / market signal not routed through lodging packet apply
{
  assert.equal(
    shouldBuildLodgingSalesPacket({
      commercialDisposition: "STRATEGIC_PARTNERSHIP",
      organizationName: "Marylebone Village",
    }),
    false
  );
  assert.equal(
    shouldBuildLodgingSalesPacket({
      demandEvidenceLabel: "SIGNAL_ONLY",
      organizationName: "Popcultr Marketing Summit",
    }),
    false
  );
  const skipped = applySalesIntelligencePacketIfNeeded({
    commercialDisposition: "STRATEGIC_PARTNERSHIP",
    organizationName: "Howard de Walden Estate",
  });
  assert.equal(skipped.skipped, true);
  assert.equal(skipped.changed, false);
}

// 8) Functional department route supports packet without named buyer
{
  const built = buildSalesIntelligencePacket(lbs);
  assert.ok(built.packet.contactEmail || built.packet.contactUrl);
  assert.ok(!built.patched.primaryContact?.name);
}

// 9) Account quality not inflated — buyerRole is functional, not TRUE_BUYER invention
{
  const built = buildSalesIntelligencePacket(lbs);
  assert.match(String(built.packet.buyerRole), /executive education|meetings|operations/i);
  assert.doesNotMatch(String(built.packet.summaryWhat), /we secured|definitely booking/i);
}

// 10) Open-ended corporate account gets Why Now without a dated event
{
  const why = buildWhyNow({
    organizationName: "Deriva Energy",
    opportunityType: "CORPORATE_ACCOUNT_RELATIONSHIP",
    eventStartDate: null,
    futureDemand: { supported: false },
  });
  assert.equal(why.ok, true);
  assert.match(why.whyNow, /open-ended lodging/i);
}

// 11) SIGNAL_ONLY with lodging evidence may receive a lodging packet
{
  assert.equal(
    shouldBuildLodgingSalesPacket({
      demandEvidenceLabel: "SIGNAL_ONLY",
      proposedHotelUse: "Explore contracted attendee lodging block",
      hotelFitRationale: "Uptown meeting facilities",
    }),
    true
  );
  const applied = applySalesIntelligencePacketIfNeeded(
    {
      organizationName: "Test Org",
      demandEvidenceLabel: "SIGNAL_ONLY",
      proposedHotelUse: "Explore contracted attendee lodging block",
      hotelFitRationale: "Uptown meeting facilities",
      opportunityType: "PREFERRED_LODGING",
      segment: "sports",
      contactEmail: "events@example.org",
      contactRole: "Event operations desk",
      sources: [{ url: "https://example.org/events" }],
      officialSource: "https://example.org/events",
    },
    { force: true, forceLodging: true }
  );
  assert.equal(applied.skipped, false);
  assert.ok(applied.opportunity?.buyerRole || applied.opportunity?.whyNow);
}

console.log("test-gdi-sales-intelligence-packet-v1: PASS");
