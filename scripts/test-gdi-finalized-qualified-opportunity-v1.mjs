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

check("organization_switchboard_only_fails", () => {
  const switchboardOnly = evaluateFinalizedQualifiedOpportunity(
    baseOpp({
      primaryContact: {
        name: "Jordan Lee",
        role: "Director of Meetings",
      },
      organizationPhone: "+1 212-555-0199",
      commercialContactRoute: "https://example.org/contact",
      approvedCommercialRoute: "Official Example Association organizing / lodging channel",
    }),
  );
  assert.equal(switchboardOnly.finalizedQualified, false);
  assert.ok(switchboardOnly.reasons.includes("PERSON_SPECIFIC_CHANNEL_REQUIRED"));
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
      hotelOpportunityThesis: "short",
      whyThisHotel: "",
      whatIsHappening: "",
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
