#!/usr/bin/env node
/**
 * Permanent regression: Finalized Qualified Opportunity contract V1.
 * No paid providers. No package pointer mutation.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  evaluateFinalizedQualifiedOpportunity,
  evaluatePersonSpecificOutreachChannel,
  selectFinalizedQualifiedOpportunities,
  FQO_STATUS,
  GDI_FINALIZED_QUALIFIED_OPPORTUNITY_CONTRACT_V1,
} from "../lib/group-demand-intelligence/workflow/gdi-finalized-qualified-opportunity-contract-v1.js";
import { runPublicationQualityGate } from "../lib/group-demand-intelligence/publication-package/gdi-publication-quality-gate-v1.js";
import { createGdiPublicationPackageStore } from "../lib/group-demand-intelligence/publication-package/index.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

function check(name, fn) {
  try {
    fn();
    console.log(`  PASS ${name}`);
  } catch (e) {
    console.error(`  FAIL ${name}`);
    throw e;
  }
}

function baseOpp(over = {}) {
  return {
    id: "opp_test_fqo",
    title: "Annual Association Conference",
    organizationName: "Example Association",
    hotelOpportunityThesis:
      "Credible Midtown/Times Square housing fit for association attendees with evidenced future cycle demand at the target hotel.",
    recommendedNextStep:
      "Outreach to the named meetings contact to introduce Hilton New York Times Square for the next housing cycle.",
    sourceUrls: ["https://example.org/events/2027-conference"],
    eventStartDate: "2027-06-01",
    futureDemandPathway: true,
    primaryContact: {
      name: "Jordan Lee",
      role: "Director of Meetings",
      organization: "Example Association",
      email: "jordan.lee@example.org",
      emailType: "DIRECT_WORK",
      emailSource: "https://example.org/staff",
    },
    secondContactAttempted: true,
    secondaryContactResearchStatus: "ATTEMPTED_NO_SECOND_VERIFIED",
    ...over,
  };
}

check("contract_version", () => {
  assert.equal(
    GDI_FINALIZED_QUALIFIED_OPPORTUNITY_CONTRACT_V1,
    "gdi-finalized-qualified-opportunity-contract-v1",
  );
});

check("valid_named_buyer_with_verified_email", () => {
  const r = evaluateFinalizedQualifiedOpportunity(baseOpp());
  assert.equal(r.finalizedQualified, true);
  assert.equal(r.status, FQO_STATUS.FINALIZED_QUALIFIED);
  assert.equal(r.salesReadyRequired, false);
  assert.equal(r.salesReady, false);
});

check("valid_alternative_with_verified_linkedin_only", () => {
  const r = evaluateFinalizedQualifiedOpportunity(
    baseOpp({
      primaryContact: {
        name: "Alex Morgan",
        role: "Events Manager",
        organization: "Example Association",
        linkedinUrl: "https://www.linkedin.com/in/alex-morgan-meetings",
      },
      salesReady: false,
    }),
  );
  assert.equal(r.finalizedQualified, true);
  assert.ok(r.gates.PERSON_SPECIFIC_CHANNEL.ok);
});

check("named_contact_without_person_specific_channel_fails", () => {
  const r = evaluateFinalizedQualifiedOpportunity(
    baseOpp({
      primaryContact: {
        name: "Jordan Lee",
        role: "Director of Meetings",
        organization: "Example Association",
      },
    }),
  );
  assert.equal(r.finalizedQualified, false);
  assert.ok(r.reasons.includes("PERSON_SPECIFIC_CHANNEL_REQUIRED"));
});

check("generic_organizational_inbox_only_fails", () => {
  const channel = evaluatePersonSpecificOutreachChannel(
    baseOpp({
      primaryContact: {
        name: "Jordan Lee",
        role: "Director of Meetings",
        email: "info@example.org",
        emailType: "GENERIC_ORGANIZATION",
      },
    }),
  );
  assert.equal(channel.ok, false);
  const r = evaluateFinalizedQualifiedOpportunity(
    baseOpp({
      primaryContact: {
        name: "Jordan Lee",
        role: "Director of Meetings",
        email: "info@example.org",
        emailType: "GENERIC_ORGANIZATION",
      },
    }),
  );
  assert.equal(r.finalizedQualified, false);
});

check("organization_route_without_label_or_channel_fails", () => {
  const switchboardOnly = evaluateFinalizedQualifiedOpportunity(
    baseOpp({
      primaryContact: {
        name: "Jordan Lee",
        role: "Director of Meetings",
      },
      organizationPhone: "+1 212-555-0199",
    }),
  );
  assert.equal(switchboardOnly.finalizedQualified, false);
  assert.ok(switchboardOnly.reasons.includes("PERSON_SPECIFIC_CHANNEL_REQUIRED"));
});

check("named_person_plus_labeled_org_route_qualifies", () => {
  const r = evaluateFinalizedQualifiedOpportunity(
    baseOpp({
      primaryContact: {
        name: "Jordan Lee",
        role: "Director of Meetings",
        organization: "Example Association",
        email: "info@example.org",
        emailType: "GENERIC_ORGANIZATION",
        organizationContactRouteLabel:
          "Official Example Association meetings / housing desk — ask for Jordan Lee",
        askToBeDirectedTo: "Director of Meetings",
      },
    }),
  );
  assert.equal(r.finalizedQualified, true);
  assert.ok(
    r.gates.PERSON_SPECIFIC_CHANNEL.channels.some(
      (c) => c.kind === "LABELED_ORGANIZATIONAL_ROUTE",
    ),
  );
});

check("short_but_complete_recommendation_qualifies", () => {
  const r = evaluateFinalizedQualifiedOpportunity(
    baseOpp({
      recommendedNextStep: "Call Jordan to propose housing.",
      cardWhyNowLine: "",
      recommendedSalesOrMarketingAction: "",
    }),
  );
  assert.equal(r.finalizedQualified, true);
});

check("historical_provenance_without_toplevel_urls_qualifies", () => {
  const r = evaluateFinalizedQualifiedOpportunity(
    baseOpp({
      sourceUrls: [],
      evidence: [],
      sources: [],
      seedUrls: [],
      investigatedUrls: [],
      cardWhyNowLine:
        "Verified 2027 dates on the association wrap page for Bethesda lodging capture.",
      primaryContact: {
        name: "Jordan Lee",
        role: "Director of Meetings",
        organization: "Example Association",
        email: "jordan.lee@example.org",
        emailType: "DIRECT_WORK",
        // no emailSource / sourceUrl — presentation-era contact without URL arrays
      },
      _historicalApprovedInventory: true,
    }),
  );
  assert.equal(r.finalizedQualified, true);
  assert.equal(
    r.gates.EVIDENCE_BACKED_DEMAND.mode,
    "HISTORICAL_PRESENTATION_PROVENANCE",
  );
});

check("unsupported_speculation_without_urls_rejected", () => {
  const r = evaluateFinalizedQualifiedOpportunity(
    baseOpp({
      sourceUrls: [],
      evidence: [],
      sources: [],
      seedUrls: [],
      cardWhyNowLine: "Maybe a random exhibitor — speculation only, no evidence.",
      summaryWhat: "speculation only",
      whatIsHappening: "",
      hotelOpportunityThesis: "unverified scrape",
      primaryContact: {
        name: "Jordan Lee",
        role: "Director of Meetings",
        email: "jordan.lee@example.org",
        emailType: "DIRECT_WORK",
      },
    }),
  );
  assert.equal(r.finalizedQualified, false);
  assert.ok(
    r.reasons.includes("UNSUPPORTED_SPECULATION") ||
      r.reasons.includes("MISSING_EVIDENCE_URLS"),
  );
});

check("competitive_bethesda_market_incumbent_not_nyc_hardcode", () => {
  const r = evaluateFinalizedQualifiedOpportunity(
    baseOpp({
      marketScope: "COMPETITIVE",
      competitiveContext: {
        label: "CONFIRMED_COMPETITOR_BOOKING",
        incumbentHotel: "Bethesda North Marriott Hotel & Conference Center",
        pathway: "OVERFLOW_AND_FUTURE_CYCLE",
      },
      primaryContact: {
        name: "Jordan Lee",
        role: "Director of Meetings",
        email: "jordan.lee@example.org",
        relevanceEvidence: "Competitor hosted nearby; overflow pathway for Bethesda Marriott",
      },
    }),
  );
  assert.equal(r.finalizedQualified, true);
});

check("person_direct_phone_passes_channel_gate", () => {
  const r = evaluateFinalizedQualifiedOpportunity(
    baseOpp({
      primaryContact: {
        name: "Jordan Lee",
        role: "Director of Meetings",
        organization: "Example Association",
        phone: "+1 212-555-0100",
      },
    }),
  );
  assert.equal(r.finalizedQualified, true);
});

check("wrong_market_or_missing_fit_fails", () => {
  const r = evaluateFinalizedQualifiedOpportunity(
    baseOpp({
      hotelOpportunityThesis: "short unrelated blurb",
      whyThisHotel: "",
      whatIsHappening: "",
      summaryWhat: "",
      hotelMarketScopeEligible: false,
    }),
  );
  assert.equal(r.finalizedQualified, false);
  assert.ok(r.reasons.includes("MISSING_HOTEL_FIT_THESIS"));
});

check("historical_without_future_pathway_fails", () => {
  const r = evaluateFinalizedQualifiedOpportunity(
    baseOpp({
      eventStartDate: "2019-01-01",
      futureDemandPathway: false,
      futureCycleEvidenceState: "",
      pastClosed: true,
      historicalOnlyCustomerExclusion: true,
      hotelOpportunityThesis:
        "Past event only with no future cycle — must not finalize.",
    }),
  );
  assert.equal(r.finalizedQualified, false);
  assert.ok(r.reasons.includes("NO_VALID_FUTURE_SOURCING_PATHWAY"));
});

check("competitive_without_incumbent_pathway_fails", () => {
  const r = evaluateFinalizedQualifiedOpportunity(
    baseOpp({
      marketScope: "COMPETITIVE",
      competitiveContext: { label: "COMPETITOR_HOSTED_DEMAND" },
    }),
  );
  assert.equal(r.finalizedQualified, false);
  assert.ok(r.reasons.includes("COMPETITIVE_INCUMBENT_OR_PATHWAY_UNVERIFIED"));
});

check("competitive_with_incumbent_and_overflow_passes", () => {
  const r = evaluateFinalizedQualifiedOpportunity(
    baseOpp({
      marketScope: "COMPETITIVE",
      competitiveContext: {
        label: "CONFIRMED_COMPETITOR_BOOKING",
        incumbentHotel: "New York Marriott Marquis",
        pathway: "OVERFLOW_AND_FUTURE_CYCLE",
      },
    }),
  );
  assert.equal(r.finalizedQualified, true);
});

check("sales_ready_false_does_not_block_fqo", () => {
  const r = evaluateFinalizedQualifiedOpportunity(
    baseOpp({ salesReady: false, commercialDisposition: "ACCOUNT_DEVELOPMENT" }),
  );
  assert.equal(r.finalizedQualified, true);
  assert.equal(r.salesReady, false);
});

check("founder_held_is_binding", () => {
  const r = evaluateFinalizedQualifiedOpportunity(
    baseOpp({ founderHold: true, founderDisposition: "HOLD" }),
  );
  assert.equal(r.finalizedQualified, false);
  assert.equal(r.status, FQO_STATUS.FOUNDER_HELD);
  assert.equal(r.bindingFounderHold, true);
});

check("founder_rejected_is_binding", () => {
  const r = evaluateFinalizedQualifiedOpportunity(
    baseOpp({ founderRejected: true }),
  );
  assert.equal(r.status, FQO_STATUS.FOUNDER_REJECTED);
  assert.equal(r.finalizedQualified, false);
});

check("quality_gate_opt_in_fqo_blocks_incomplete", () => {
  const bad = baseOpp({
    primaryContact: { name: "Jordan Lee", role: "Director of Meetings" },
  });
  const gate = runPublicationQualityGate({
    opportunities: [bad],
    enforceFinalizedQualifiedOpportunity: true,
    enforceCommercialExpirationGate: false,
    enforceCommercialActionabilityGate: false,
  });
  assert.equal(gate.ok, false);
  assert.ok(
    gate.blocking.some((b) => b.code === "NOT_FINALIZED_QUALIFIED_OPPORTUNITY"),
  );
});

check("quality_gate_default_does_not_retroactively_break_legacy", () => {
  const thin = {
    id: "legacy",
    title: "Legacy",
    organizationName: "Org",
    primaryContact: { name: "Pat Smith", role: "Manager" },
    whatIsHappening: "A sufficiently long customer narrative body for drawer quality checks that avoids thin-body failure modes in the gate.",
  };
  const gate = runPublicationQualityGate({
    opportunities: [thin],
    enforceCommercialExpirationGate: false,
    enforceCommercialActionabilityGate: false,
  });
  assert.ok(!gate.blocking.some((b) => b.code === "NOT_FINALIZED_QUALIFIED_OPPORTUNITY"));
});

check("select_filters_to_fqo_only", () => {
  const good = baseOpp({ id: "good" });
  const bad = baseOpp({
    id: "bad",
    primaryContact: { name: "Jordan Lee", role: "Director" },
  });
  const selected = selectFinalizedQualifiedOpportunities([good, bad]);
  assert.equal(selected.count, 1);
  assert.equal(selected.opportunities[0].id, "good");
});

check("historical_staged_and_current_pointers_preserved", () => {
  const store = createGdiPublicationPackageStore();
  const peers = [
    "rece0NdV9FCnaFlVS",
    "recaokE8psIbuVeuH",
    "rec1t35e3sPerzxYu",
    "recLuxvwwxID7U2B8",
  ];
  for (const id of peers) {
    const staged = store.readPointer(id, "STAGED");
    assert.ok(staged?.publicationVersion, `STAGED missing for ${id}`);
  }
  const bethesdaCurrent = store.readPointer("recLuxvwwxID7U2B8", "CURRENT");
  assert.ok(bethesdaCurrent?.publicationVersion, "Bethesda CURRENT preserved");
  const hiltonStaged = store.readPointer("rec35fExUxCClpOP6", "STAGED");
  assert.equal(hiltonStaged, null, "Hilton STAGED must remain inactive");
});

check("no_research_tab_in_local_shell_html", () => {
  const html = fs.readFileSync(path.join(ROOT, "public/app.html"), "utf8");
  assert.ok(!/data-gdi-shell-inventory="research"/.test(html));
  assert.ok(/data-gdi-shell-inventory="staged"/.test(html));
  assert.ok(/data-gdi-shell-inventory="published"/.test(html));
});

console.log("\nOK test-gdi-finalized-qualified-opportunity-v1");
