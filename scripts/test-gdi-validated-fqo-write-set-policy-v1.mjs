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

// Wyndham defect: allowHistoricalSchemaCompat must NOT soft-pass undated
// opportunities that are not in the historical approved ID set.
const undatedInflation = filterValidatedFqoPersistenceWriteSet(
  [
    {
      opportunityId: "opp_undated_new",
      title: "Undated scrape",
      organizationName: "Random Org",
      hotelOpportunityThesis: "Maybe lodging fit someday.",
      whyThisHotel: "Maybe lodging fit someday.",
      eventStartDate: "",
      futureDemandPathway: false,
      primaryContact: {
        name: "Jamie Example",
        email: "jamie@example.org",
        role: "Meetings Director",
      },
      recommendedNextStep: "Email Jamie.",
      sourceUrls: ["https://example.org/x"],
    },
  ],
  { allowHistoricalSchemaCompat: true },
);
assert.equal(
  undatedInflation.keptCount,
  0,
  "allowHistoricalSchemaCompat must not grant historical pathway to new undated rows",
);
assert.ok(
  undatedInflation.deferred.some((d) => d.reason === "FQO_GATE_FAILED"),
  "undated new row deferred on FQO",
);

const historicalRetain = filterValidatedFqoPersistenceWriteSet(
  [
    {
      opportunityId: "opp_hist_open_account",
      title: "Open LMC account",
      organizationName: "Downtown Partnership",
      hotelOpportunityThesis: "Local meetings and catering lodging fit.",
      whyThisHotel: "Hotel lodging fit for recurring local meetings.",
      eventStartDate: "",
      primaryContact: {
        name: "Jamie Example",
        email: "jamie@example.org",
        role: "Meetings Director",
      },
      recommendedNextStep: "Email Jamie for catering calendar.",
      sourceUrls: [],
      _historicalApprovedInventory: true,
    },
  ],
  {
    allowHistoricalSchemaCompat: true,
    historicalApprovedOpportunityIds: ["opp_hist_open_account"],
  },
);
assert.ok(
  historicalRetain.kept.some((o) => o.opportunityId === "opp_hist_open_account"),
  "explicit historical approved open account retained",
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
