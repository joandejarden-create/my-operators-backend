#!/usr/bin/env node
/**
 * Regression: Bethesda commercial recovery rematerialization + workflow fixes.
 * Does not spend. Does not publish.
 */

import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { classifyDemandMechanismEvidence } from "../lib/group-demand-intelligence/demand-mechanism-research-v1.js";
import { clearStaleResearchContinuationMaturity } from "../lib/group-demand-intelligence/workflow/gdi-watchlist-finalization-handoff-v1.js";
import {
  rematerializePaidEvidenceFields,
  finalizeCandidateZeroPaid,
  AUTHORIZATION_ID,
  HARD_CEILING_USD,
} from "../lib/group-demand-intelligence/research/bethesda-commercial-recovery-8usd-v1.js";
import { filterCustomerFacingOpportunities } from "../lib/group-demand-intelligence/customer-visibility.js";
import { loadHotelOpportunityDoc } from "../lib/group-demand-intelligence/research/gdi-hotel-store-integrity-guard-v1.js";
import { mergeBethesdaInternalResearchOpportunities } from "../lib/group-demand-intelligence/research/bethesda-internal-research-union-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const hotel = {
  hotelId: "recLuxvwwxID7U2B8",
  displayName: "Bethesda Marriott",
};

// 1) Negated "not … room blocks" must not classify as CONFIRMED.
{
  const classified = classifyDemandMechanismEvidence({
    whatIsHappening:
      "Montgomery County SHRM chapter meetings with breakfast and dinner catering — not guestrooms or room blocks.",
    activityLocation: "Montgomery County, Maryland",
  });
  assert.notEqual(
    classified.thesisStandard,
    "CONFIRMED_DEMAND",
    "negated room-blocks language must not confirm demand",
  );
  assert.equal(classified.thesisStandard, "RELIABLE_INFERRED_DEMAND");
}

// 2) Stale RESEARCH_CONTINUATION maturity clears when evidence elevates.
{
  const cleared = clearStaleResearchContinuationMaturity({
    commercialMaturity: "RESEARCH_CONTINUATION",
    whatIsHappening:
      "Recurring chapter meetings and holiday celebrations with breakfast and dinner catering in Montgomery County, Maryland.",
    activityLocation: "Montgomery County, Maryland",
    hotelOpportunityThesis:
      "Bethesda Marriott can host future chapter breakfast and dinner meetings in the Bethesda–Rockville corridor.",
  });
  assert.equal(cleared.cleared, true);
  assert.equal(cleared.opportunity.commercialMaturity, null);
}

// 3) Rematerialized SHRM-like row can reach publicationReviewReady without spend.
{
  const row = rematerializePaidEvidenceFields({
    id: "disp_recLuxvwwxID7U2B8_montgomery_county_shrm",
    hotelId: hotel.hotelId,
    organizationName: "Montgomery County SHRM",
    title: "Montgomery County SHRM — Chapter meetings & holiday programs",
    researchSegment: "local_meetings_catering",
    commercialMaturity: "RESEARCH_CONTINUATION",
    whatIsHappening:
      "Confirmed recurring chapter programming with substantial off-site food-and-beverage demand: holiday celebrations in 2022–2026; breakfast and dinner meetings; leadership conference. Pattern is hosted events with cocktails/networking, breakfast or buffet/plated dinner — not guestrooms or room blocks.",
    hotelOpportunityThesis:
      "Bethesda Marriott is a credible local-catering alternative for future MCSHRM meetings in the Bethesda–Rockville corridor with private dining and meeting rooms.",
    whyThisMattersForHotel:
      "Bethesda Marriott is a credible local-catering alternative for future MCSHRM meetings in the Bethesda–Rockville corridor with private dining and meeting rooms.",
    salesOutreachRecommendation:
      "Approach Neha Agarwal with a concise local-catering proposal for the 2027 chapter calendar; ask who owns venue decisions and whether Manor remains contracted.",
    primaryContact: {
      name: "Neha Agarwal",
      title: "Programs Hospitality Director",
      role: "Programs Hospitality Director",
      email: "mcshrm@gmail.com",
      namedPerson: true,
      contactRelevanceLabel: "RELEVANT_COMMERCIAL_ENTRY_POINT",
      contactClass: "RELEVANT_COMMERCIAL_ENTRY_POINT",
      commercialClass: "RELEVANT_COMMERCIAL_ENTRY_POINT",
      organizationContactRoute: "mcshrm@gmail.com",
    },
    evidenceUrls: ["https://mcshrm.com/board"],
  });
  const fin = finalizeCandidateZeroPaid(row, { hotel });
  assert.equal(fin.publicationReviewReady, true);
  assert.equal(fin.accountDevelopment, true);
  assert.equal(fin.opportunity.customerVisible, false);
  assert.equal(fin.opportunity.customerPublicationReady, false);
}

// 4) Customer feed parity lock for Bethesda remains 19 unless founder publishes.
{
  const store = loadHotelOpportunityDoc(hotel.hotelId, { root: ROOT });
  const union = mergeBethesdaInternalResearchOpportunities(
    hotel.hotelId,
    store.opportunities || [],
    { root: ROOT },
  );
  const facing = filterCustomerFacingOpportunities(union.opportunities || []);
  assert.equal(facing.length, 19);
}

// 5) Authorization constants.
assert.equal(HARD_CEILING_USD, 8);
assert.match(AUTHORIZATION_ID, /BETHESDA_COMMERCIAL_RECOVERY_8USD/);

// 6) Recovery report exists after live run (soft check).
const summaryPath = path.join(
  ROOT,
  "reports/gdi-bethesda-commercial-recovery-8usd-v1/00-summary.json",
);
if (fs.existsSync(summaryPath)) {
  const summary = JSON.parse(fs.readFileSync(summaryPath, "utf8"));
  assert.ok(Number(summary.spentUsd) <= HARD_CEILING_USD + 1e-9);
  assert.ok(summary.customerFacingParity?.unchanged === true);
  assert.ok((summary.stagedForFounderReviewCount || 0) >= 3);
}

console.log(
  JSON.stringify(
    {
      ok: true,
      test: "test-gdi-bethesda-commercial-recovery-8usd-v1",
      authorizationId: AUTHORIZATION_ID,
    },
    null,
    2,
  ),
);
