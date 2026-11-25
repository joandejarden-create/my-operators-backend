#!/usr/bin/env node
import assert from "node:assert/strict";
import {
  filterValidatedFqoPersistenceWriteSet,
  RESEARCH_PERSISTENCE_WRITE_SET_POLICY,
  RESEARCH_PERSISTENCE_ACTIVATION,
} from "../lib/group-demand-intelligence/workflow/gdi-full-e2e-research-persistence-bridge-v1.js";

assert.equal(
  RESEARCH_PERSISTENCE_ACTIVATION.defaultProductionWriteSetPolicy,
  RESEARCH_PERSISTENCE_WRITE_SET_POLICY.VALIDATED_FQO_ONLY,
);

const filtered = filterValidatedFqoPersistenceWriteSet(
  [
    {
      opportunityId: "opp_pass",
      title: "Validated conference",
      organizationName: "Example Org",
      hotelOpportunityThesis: "Fit for Bethesda overflow and meetings.",
      hotelFit: { score: 80, rationale: "geo+meetings" },
      whyThisHotel: "Adjacent to NIH corridor demand.",
      eventStartDate: "2027-06-01",
      primaryContact: {
        name: "Jamie Example",
        email: "jamie@example.org",
        role: "Meetings Director",
      },
      recommendedNextStep: "Email Jamie to request housing list inclusion.",
      sources: [{ url: "https://example.org/event" }],
      sourceUrls: ["https://example.org/event"],
    },
    {
      opportunityId: "gdi_e2d_770e3b44ad97c1ae",
      title: "Research continuation only",
      researchContinuation: true,
    },
    {
      opportunityId: "opp_outside",
      title: "Outside certified set",
      primaryContact: { name: "A Person", email: "a@example.org" },
      sources: [{ url: "https://example.org/x" }],
    },
  ],
  {
    allowedOpportunityIds: ["opp_pass", "gdi_e2d_770e3b44ad97c1ae"],
    allowHistoricalSchemaCompat: true,
  },
);

assert.ok(filtered.keptCount >= 1);
assert.ok(
  filtered.kept.some((o) => o.opportunityId === "opp_pass"),
  "validated opportunity kept",
);
assert.ok(
  filtered.deferred.some((d) => d.opportunityId === "gdi_e2d_770e3b44ad97c1ae"),
  "research-continuation deferred",
);
assert.ok(
  filtered.deferred.some(
    (d) =>
      d.opportunityId === "opp_outside" &&
      d.reason === "OUTSIDE_CERTIFIED_VALIDATED_SET",
  ),
  "outside certified set deferred",
);

console.log(
  JSON.stringify(
    {
      ok: true,
      kept: filtered.kept.map((o) => o.opportunityId),
      deferred: filtered.deferred.map((d) => ({
        id: d.opportunityId,
        reason: d.reason,
      })),
    },
    null,
    2,
  ),
);
