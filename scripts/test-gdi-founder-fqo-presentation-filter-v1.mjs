#!/usr/bin/env node
import assert from "node:assert/strict";
import {
  filterPackageOpportunitiesToFqo,
  resolveLatestCertifiedCandidatePackage,
  GDI_FOUNDER_FQO_PRESENTATION_FILTER_V1,
} from "../lib/group-demand-intelligence/publication-package/gdi-founder-fqo-presentation-filter-v1.js";

const HILTON = "rec35fExUxCClpOP6";

{
  const out = filterPackageOpportunitiesToFqo([
    {
      id: "bad_exhibitor",
      title: "EXHIBITOR BLOCK placeholder",
      primaryContact: { name: "" },
    },
    {
      id: "gdi_opp_5282e7a3223f2741",
      title: "SCBWI Winter Conference",
      organizationName: "SCBWI",
      eventStartDate: "2027-02-01",
      futureDemandPathway: true,
      hotelOpportunityThesis:
        "Hilton NYTS is a credible midtown overflow / housing path for SCBWI Winter Conference with dated future cycle evidence.",
      recommendedNextStep:
        "Outreach to Sarah Baker — confirm lodging decision role for SCBWI and introduce Hilton New York Times Square fit.",
      sourceUrls: ["https://www.scbwi.org/"],
      primaryContact: {
        name: "Sarah Baker",
        role: "Conference Manager",
        linkedinUrl: "https://www.linkedin.com/in/sarah-baker-example",
      },
    },
  ]);
  assert.equal(GDI_FOUNDER_FQO_PRESENTATION_FILTER_V1.includes("fqo"), true);
  assert.ok(out.afterCount <= out.beforeCount);
  assert.ok(!out.opportunities.some((o) => /EXHIBITOR/i.test(o.title || "")));
}

{
  const cand = resolveLatestCertifiedCandidatePackage({ hotelId: HILTON });
  assert.equal(cand.ok, true);
  assert.equal(cand.meta.pointerActivated, false);
  assert.equal(
    cand.meta.previewSource,
    "CANDIDATE_CERTIFIED_NOT_ACTIVATED",
  );
  assert.ok(cand.opportunities.length >= 1);
  assert.ok(cand.opportunities.length <= 6);
}

console.log("OK test-gdi-founder-fqo-presentation-filter-v1");
