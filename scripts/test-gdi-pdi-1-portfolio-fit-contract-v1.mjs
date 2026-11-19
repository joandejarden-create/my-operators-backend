#!/usr/bin/env node
/**
 * PDI-1 — Portfolio membership + hotel opportunity fit contract ($0).
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  buildPortfolioMembershipIndex,
  rejectNotesOnlyMembershipAttempt,
  MEMBERSHIP_PROVENANCE_KIND,
} from "../lib/group-demand-intelligence/portfolio-demand-intelligence/gdi-portfolio-membership-contract-v1.js";
import {
  evaluatePortfolioMarketOpportunityAcrossHotels,
  buildSharedPublicMarketPacket,
  PORTFOLIO_HOTEL_FIT_CLASS,
} from "../lib/group-demand-intelligence/portfolio-demand-intelligence/gdi-portfolio-hotel-opportunity-fit-contract-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const FIXTURE = JSON.parse(
  fs.readFileSync(
    path.join(
      ROOT,
      "fixtures/gdi-portfolio-demand-intelligence-v1/aimbridge-cala-guadalajara.json",
    ),
    "utf8",
  ),
);
const REPORT_DIR = path.join(
  ROOT,
  "reports/gdi-portfolio-demand-intelligence-pdi-1-v1",
);

const index = buildPortfolioMembershipIndex(FIXTURE.memberships);
assert.ok(index.activeCount >= 3);

const gdlHotels = index.listHotelsForOperatorMarket(
  FIXTURE.operator.operatorId,
  "market_guadalajara_mx_v1",
);
assert.equal(gdlHotels.length, 2);
assert.ok(gdlHotels.every((h) => h.marketId === "market_guadalajara_mx_v1"));
assert.equal(
  index.isHotelAuthorized(
    FIXTURE.operator.operatorId,
    "market_guadalajara_mx_v1",
    "recTk5I8VOzsPhxDz",
  ),
  false,
  "Cancún hotel must not be authorized for Guadalajara market",
);

const notesOnly = rejectNotesOnlyMembershipAttempt({
  operatorId: FIXTURE.operator.operatorId,
  hotelId: "recXh4NgF8sDbi9cQ",
  marketId: "market_guadalajara_mx_v1",
  noteText: "Aimbridge LATAM managed (notes only)",
});
assert.equal(notesOnly.authorized, false);
assert.ok(
  notesOnly.blockers.includes("PROVENANCE_NOTES_OR_FUZZY_REJECTED") ||
    notesOnly.provenanceKind ===
      MEMBERSHIP_PROVENANCE_KIND.NOTES_OR_FUZZY_TEXT_REJECTED,
);

const shared = buildSharedPublicMarketPacket(FIXTURE.marketOpportunitySeed);
assert.ok(shared.marketOpportunityId);
assert.equal(shared.hotelSalesFeedback, undefined);
assert.equal(shared.salesOwner, undefined);
assert.ok(
  (shared.confidentialKeysStripped || []).includes("hotelSalesFeedback") ||
    (shared.confidentialKeysStripped || []).includes("salesOwner") ||
    (shared.confidentialKeysStripped || []).includes("operatorPrivateNotes"),
);

const result = evaluatePortfolioMarketOpportunityAcrossHotels({
  sourceOpportunity: FIXTURE.marketOpportunitySeed,
  memberships: gdlHotels,
  hotelConfigsById: FIXTURE.hotelConfigs,
  marketHints: FIXTURE.markets[0].marketHints,
});

assert.equal(result.airtableWrites, 0);
assert.equal(result.publicationWrites, 0);
assert.equal(result.duplicateMarketResearchObjects, false);
assert.equal(result.marketOpportunityIdentityStable, true);
assert.ok(result.marketOpportunityId);

const okRows = result.hotelAssessments.filter((a) => a.ok);
assert.equal(okRows.length, 2);

const sheraton = okRows.find((a) => a.hotelId === "recCAGqkERSXgiku4");
const unknownMeetings = okRows.find(
  (a) => a.hotelId === "fixture_gdi_pdi_gdl_select_service_unknown_meetings",
);
assert.ok(sheraton);
assert.ok(unknownMeetings);

assert.equal(sheraton.marketOpportunityId, unknownMeetings.marketOpportunityId);
assert.notEqual(sheraton.hotelOpportunityId, unknownMeetings.hotelOpportunityId);

assert.equal(
  unknownMeetings.fitClass,
  PORTFOLIO_HOTEL_FIT_CLASS.INSUFFICIENT_EVIDENCE,
  "Unknown meeting capability on meeting-led demand must be INSUFFICIENT_EVIDENCE",
);
assert.ok(
  [
    PORTFOLIO_HOTEL_FIT_CLASS.POTENTIAL_PRIMARY_HOST,
    PORTFOLIO_HOTEL_FIT_CLASS.MEETING_CATERING_OPPORTUNITY,
    PORTFOLIO_HOTEL_FIT_CLASS.OVERFLOW_ACCOMMODATION,
    PORTFOLIO_HOTEL_FIT_CLASS.OTHER_PARTNERSHIP,
  ].includes(sheraton.fitClass),
  `Sheraton fit unexpected: ${sheraton.fitClass}`,
);
assert.notEqual(sheraton.fitClass, unknownMeetings.fitClass);

assert.equal(sheraton.sharedMarketPacket?.hotelSalesFeedback, undefined);
assert.equal(sheraton.productionWeightsIntroduced, false);

// Unauthorized hotel attempt
const rogue = evaluatePortfolioMarketOpportunityAcrossHotels({
  sourceOpportunity: FIXTURE.marketOpportunitySeed,
  memberships: [
    {
      ...notesOnly,
      hotelId: "recXh4NgF8sDbi9cQ",
      authorized: false,
      blockers: notesOnly.blockers,
    },
  ],
  hotelConfigsById: {},
});
assert.equal(rogue.hotelAssessments[0].ok, false);
assert.equal(
  rogue.hotelAssessments[0].code,
  "HOTEL_NOT_AUTHORIZED_FOR_OPERATOR_MARKET",
);

fs.mkdirSync(REPORT_DIR, { recursive: true });
const report = {
  version: "gdi-portfolio-demand-intelligence-pdi-1-v1",
  at: new Date().toISOString(),
  paidCalls: 0,
  airtableWrites: 0,
  publicationWrites: 0,
  assessmentPr: 445,
  slice2aPr: 444,
  operatorId: FIXTURE.operator.operatorId,
  marketId: "market_guadalajara_mx_v1",
  marketOpportunityId: result.marketOpportunityId,
  hotelFits: okRows.map((r) => ({
    hotelId: r.hotelId,
    fitClass: r.fitClass,
    confidence: r.confidence,
    hotelOpportunityId: r.hotelOpportunityId,
    missingAttributes: r.missingAttributes,
  })),
  cancelledExcludedFromGuadalajara: ["recTk5I8VOzsPhxDz"],
  notesOnlyMembershipRejected: true,
  confidentialStripped: shared.confidentialKeysStripped || [],
  remainingAuthorizationGaps: [
    "PDI-2: authenticated portfolio API + Memberstack/operator allowlist",
    "Live census Management Company exact-label mapping for Aimbridge (not notes)",
    "Customer UI tab + drawer wiring (PDI-3)",
    "Publication remains hotel-scoped; portfolio view must not bypass publish gates",
  ],
  recommendedNextStep: "PDI-2 read-only portfolio API and authorization (after founder review)",
};
fs.writeFileSync(
  path.join(REPORT_DIR, "RESULTS.json"),
  `${JSON.stringify(report, null, 2)}\n`,
);
fs.writeFileSync(
  path.join(REPORT_DIR, "FOUNDER_RESULTS.md"),
  `# GDI PDI-1 — Portfolio fit contract results

Generated: ${report.at}

## Safety
- Paid calls: 0
- Airtable writes: 0
- Customer publication: 0
- Production scoring weights introduced: **false**

## Aimbridge CALA × Guadalajara
- Market opportunity id: \`${report.marketOpportunityId}\`
- Guadalajara hotels evaluated: ${okRows.length}
- Cancún hotel excluded from Guadalajara: **PASS**
- Notes-only membership rejected: **PASS**
- Confidential feedback stripped from shared packet: **PASS**

## Fit results
${okRows
  .map(
    (r) =>
      `- **${r.hotelId}** → \`${r.fitClass}\` (${r.confidence}) hotelOpp=\`${r.hotelOpportunityId}\``,
  )
  .join("\n")}

## Remaining authorization gaps
${report.remainingAuthorizationGaps.map((g) => `- ${g}`).join("\n")}

## Next
${report.recommendedNextStep}
`,
);

console.log(
  JSON.stringify(
    {
      ok: true,
      marketOpportunityId: result.marketOpportunityId,
      fits: report.hotelFits,
      reportDir: REPORT_DIR,
    },
    null,
    2,
  ),
);
