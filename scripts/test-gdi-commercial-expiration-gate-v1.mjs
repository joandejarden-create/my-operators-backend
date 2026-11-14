#!/usr/bin/env node
import assert from "node:assert/strict";
import {
  COMMERCIAL_TEMPORAL_DISPOSITION,
  classifyCommercialTemporalStatus,
  applyCommercialExpirationGate,
  assertCustomerInventoryPassesExpirationGate,
  isCustomerPursuitEligible,
} from "../lib/group-demand-intelligence/commercial-expiration-gate-v1.js";
import { runPublicationQualityGate } from "../lib/group-demand-intelligence/publication-package/gdi-publication-quality-gate-v1.js";

const REF = "2026-11-14";

{
  const r = classifyCommercialTemporalStatus(
    {
      id: "fut1",
      title: "Expo Foo 2027",
      eventStartDate: "2027-04-20",
      whatIsHappening: "Confirmed 2027 expo at Expo Guadalajara",
    },
    { referenceDate: REF },
  );
  assert.equal(r.disposition, COMMERCIAL_TEMPORAL_DISPOSITION.ACTIVE_FUTURE_EVENT);
  assert.equal(r.customerPursuitEligible, true);
}

{
  const r = classifyCommercialTemporalStatus(
    {
      id: "exp1",
      title: "CX Forum Guadalajara 2026",
      eventStartDate: "2026-09-22",
      whatIsHappening: "Forum held September 2026",
    },
    { referenceDate: REF },
  );
  assert.equal(r.disposition, COMMERCIAL_TEMPORAL_DISPOSITION.EXPIRED_ONE_TIME_EVENT);
  assert.equal(r.customerPursuitEligible, false);
}

{
  const r = classifyCommercialTemporalStatus(
    {
      id: "ong1",
      title: "Preferred lodging partnership",
      datePillDisplayLabel: "Ongoing",
      preferredLodgingPartnershipV1: true,
      whatIsHappening: "Independent venue overnight guest referral partnership",
    },
    { referenceDate: REF },
  );
  assert.equal(r.disposition, COMMERCIAL_TEMPORAL_DISPOSITION.ACTIVE_ONGOING_DEMAND);
}

{
  const r = classifyCommercialTemporalStatus(
    {
      id: "dup1",
      title: "Duplicate congress",
      duplicateOf: "primary1",
    },
    { referenceDate: REF },
  );
  assert.equal(r.disposition, COMMERCIAL_TEMPORAL_DISPOSITION.DUPLICATE_EVENT_RECORD);
  assert.equal(isCustomerPursuitEligible(r.disposition), false);
}

{
  // Recurring past congress → future-cycle research required (do NOT invent 2027 dates)
  const r = classifyCommercialTemporalStatus(
    {
      id: "ann1",
      title: "Annual Industry Congress 2026",
      eventStartDate: "2026-03-01",
      whatIsHappening: "Annual congress completed in March 2026",
    },
    { referenceDate: REF },
  );
  assert.equal(
    r.disposition,
    COMMERCIAL_TEMPORAL_DISPOSITION.FUTURE_CYCLE_RESEARCH_REQUIRED,
  );
}
{
  // One-time past event without recurrence → expired
  const r = classifyCommercialTemporalStatus(
    {
      id: "once1",
      title: "Open House SMT Demo Day",
      eventStartDate: "2026-10-07",
      whatIsHappening: "One-time open house demonstration",
    },
    { referenceDate: REF },
  );
  assert.equal(
    r.disposition,
    COMMERCIAL_TEMPORAL_DISPOSITION.EXPIRED_ONE_TIME_EVENT,
  );
}

{
  const applied = applyCommercialExpirationGate(
    [
      {
        id: "a",
        title: "Future",
        eventStartDate: "2027-01-01",
        whatIsHappening: "Future event",
      },
      {
        id: "b",
        title: "Past Forum",
        eventStartDate: "2026-08-01",
        whatIsHappening: "Forum done",
      },
    ],
    { referenceDate: REF },
  );
  assert.equal(applied.customerVisible.length, 1);
  assert.equal(applied.researchWatch.length, 1);
  assert.equal(applied.customerVisible[0].id, "a");
}

{
  const cert = assertCustomerInventoryPassesExpirationGate(
    [
      {
        id: "bad",
        title: "Past Forum",
        eventStartDate: "2026-08-01",
        whatIsHappening: "Forum",
        inventoryPopulations: ["SALES_OPPORTUNITIES"],
      },
    ],
    { referenceDate: REF },
  );
  assert.equal(cert.ok, false);
}

{
  const q = runPublicationQualityGate({
    referenceDate: REF,
    opportunities: [
      {
        id: "exp_pub",
        title: "Past Forum",
        eventStartDate: "2026-09-01",
        whatIsHappening: "Forum concluded",
        commercialTemporalDisposition: "EXPIRED_ONE_TIME_EVENT",
      },
    ],
  });
  assert.equal(q.ok, false);
  assert.ok(
    q.blocking.some((f) => f.code === "COMMERCIAL_EXPIRATION_GATE_BLOCK"),
  );
}

console.log("test-gdi-commercial-expiration-gate-v1: PASS");
