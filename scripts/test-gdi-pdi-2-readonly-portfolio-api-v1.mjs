#!/usr/bin/env node
/**
 * PDI-2 — read-only portfolio API authorization + fit retrieval ($0).
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createPortfolioReadonlyApi } from "../lib/group-demand-intelligence/portfolio-demand-intelligence/gdi-portfolio-readonly-api-v1.js";
import {
  PORTFOLIO_AUTH_DENY,
  rejectFuzzyOperatorAuthorizationAttempt,
} from "../lib/group-demand-intelligence/portfolio-demand-intelligence/gdi-portfolio-operator-authorization-v1.js";
import { PORTFOLIO_HOTEL_FIT_CLASS } from "../lib/group-demand-intelligence/portfolio-demand-intelligence/gdi-portfolio-hotel-opportunity-fit-contract-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const FIX = path.join(
  ROOT,
  "fixtures/gdi-portfolio-demand-intelligence-v1",
);
const REPORT = path.join(
  ROOT,
  "reports/gdi-portfolio-demand-intelligence-pdi-2-v1",
);

const fixture = JSON.parse(
  fs.readFileSync(path.join(FIX, "aimbridge-cala-guadalajara.json"), "utf8"),
);
const authRegistry = JSON.parse(
  fs.readFileSync(path.join(FIX, "pdi-2-operator-auth-registry.json"), "utf8"),
);

const api = createPortfolioReadonlyApi({
  authRegistry,
  memberships: fixture.memberships,
  hotelConfigsById: fixture.hotelConfigs,
  markets: fixture.markets,
  marketOpportunities: [
    { ...fixture.marketOpportunitySeed },
    ...(fixture.additionalMarketOpportunities || []),
  ],
});

const aimbridgeCaller = {
  authenticated: true,
  memberstackMemberId: "mem_aimbridge_cala_steward_v1",
  sessionId: "sess_aimbridge_cala_steward_v1",
};
const otherCaller = {
  authenticated: true,
  memberstackMemberId: "mem_other_operator_v1",
};

// Unauthenticated
const unauth = api.listOperatorMarkets({});
assert.equal(unauth.ok, false);
assert.equal(unauth.status, 401);
assert.equal(unauth.error.code, PORTFOLIO_AUTH_DENY.UNAUTHENTICATED);

// Fuzzy company-name rejected
const fuzzy = rejectFuzzyOperatorAuthorizationAttempt({
  companyName: "Aimbridge Hospitality",
});
assert.equal(fuzzy.ok, false);
assert.equal(fuzzy.code, PORTFOLIO_AUTH_DENY.FUZZY_NAME_MATCH_REJECTED);
const fuzzyCall = api.listOperatorMarkets({
  authenticated: true,
  companyName: "Aimbridge Hospitality",
});
assert.equal(fuzzyCall.ok, false);
assert.ok(
  [
    PORTFOLIO_AUTH_DENY.FUZZY_NAME_MATCH_REJECTED,
    PORTFOLIO_AUTH_DENY.OPERATOR_NOT_MAPPED,
  ].includes(fuzzyCall.error.code),
);

// Authorized markets
const markets = api.listOperatorMarkets(aimbridgeCaller);
assert.equal(markets.ok, true);
assert.ok(markets.markets.some((m) => m.marketId === "market_guadalajara_mx_v1"));

// Cross-operator denied
const cross = api.listPortfolioHotels(otherCaller, {
  operatorId: "op_aimbridge_cala_steward_v1",
  marketId: "market_guadalajara_mx_v1",
});
assert.equal(cross.ok, false);
assert.equal(cross.status, 403);

// Guadalajara hotels — Cancún not included
const hotels = api.listPortfolioHotels(aimbridgeCaller, {
  marketId: "market_guadalajara_mx_v1",
});
assert.equal(hotels.ok, true);
assert.equal(hotels.hotels.length, 2);
assert.ok(!hotels.hotels.some((h) => h.hotelId === "recTk5I8VOzsPhxDz"));

// Wrong-market hotel request denied
const wrongHotel = api.getPortfolioMarketOpportunity(aimbridgeCaller, {
  marketId: "market_guadalajara_mx_v1",
  hotelId: "recTk5I8VOzsPhxDz",
});
assert.equal(wrongHotel.ok, false);
assert.equal(wrongHotel.error.code, PORTFOLIO_AUTH_DENY.HOTEL_NOT_IN_PORTFOLIO);

// Missing membership market
const noMkt = api.listPortfolioHotels(aimbridgeCaller, {
  marketId: "market_does_not_exist",
});
assert.equal(noMkt.ok, false);

// Fit matrix — published corpus (internal surface includes unpublished too)
const internal = api.getPortfolioMarketOpportunity(aimbridgeCaller, {
  marketId: "market_guadalajara_mx_v1",
  customerSurface: false,
});
assert.equal(internal.ok, true);
assert.ok(internal.results.length >= 2);
const pubRow = internal.results.find((r) =>
  String(r.sharedMarketPacket?.title || "").includes("medical congress 2028"),
);
const unpubRow = internal.results.find((r) =>
  String(r.sharedMarketPacket?.title || "").includes("unpublished"),
);
assert.ok(pubRow);
assert.ok(unpubRow);
const sheraton = pubRow.hotelAssessments.find(
  (h) => h.hotelId === "recCAGqkERSXgiku4",
);
const unknown = pubRow.hotelAssessments.find(
  (h) => h.hotelId === "fixture_gdi_pdi_gdl_select_service_unknown_meetings",
);
assert.ok(sheraton);
assert.ok(unknown);
assert.equal(sheraton.marketOpportunityId, unknown.marketOpportunityId);
assert.notEqual(sheraton.hotelOpportunityId, unknown.hotelOpportunityId);
assert.ok(sheraton.drawer?.detailPath);
assert.equal(
  unknown.fitClass,
  PORTFOLIO_HOTEL_FIT_CLASS.INSUFFICIENT_EVIDENCE,
);
assert.ok(
  [
    PORTFOLIO_HOTEL_FIT_CLASS.POTENTIAL_PRIMARY_HOST,
    PORTFOLIO_HOTEL_FIT_CLASS.MEETING_CATERING_OPPORTUNITY,
    PORTFOLIO_HOTEL_FIT_CLASS.OVERFLOW_ACCOMMODATION,
    PORTFOLIO_HOTEL_FIT_CLASS.OTHER_PARTNERSHIP,
  ].includes(sheraton.fitClass),
);
const sharedBlob = JSON.stringify(pubRow.sharedMarketPacket);
assert.equal(sharedBlob.includes("CONFIDENTIAL_OPERATOR_FEEDBACK_MUST_NOT_LEAK"), false);

// Customer surface filters unpublished
const customer = api.getPortfolioMarketOpportunity(aimbridgeCaller, {
  marketId: "market_guadalajara_mx_v1",
  customerSurface: true,
});
assert.equal(customer.ok, true);
assert.equal(customer.results.length, 1);
assert.ok(
  String(customer.results[0].sharedMarketPacket.title).includes(
    "medical congress 2028",
  ),
);

// Unmapped member
const unmapped = api.listOperatorMarkets({
  authenticated: true,
  memberstackMemberId: "mem_unknown_not_in_registry",
});
assert.equal(unmapped.ok, false);
assert.equal(unmapped.error.code, PORTFOLIO_AUTH_DENY.OPERATOR_NOT_MAPPED);

fs.mkdirSync(REPORT, { recursive: true });
const summary = {
  version: "gdi-portfolio-demand-intelligence-pdi-2-v1",
  at: new Date().toISOString(),
  paidCalls: 0,
  airtableWrites: 0,
  productionWrites: 0,
  publicationWrites: 0,
  priorPrs: { pdi1: 446, slice2b: 448 },
  checks: {
    unauthenticatedDenied: true,
    fuzzyNameRejected: true,
    crossOperatorDenied: true,
    cancunExcludedFromGuadalajara: true,
    wrongMarketHotelDenied: true,
    insufficientEvidencePreserved: true,
    confidentialStripped: true,
    unpublishedFilteredOnCustomerSurface: true,
    drawerLinksPresent: true,
  },
  remainingGaps: [
    "Live Memberstack→operator steward registry (production) not yet provisioned",
    "PDI-3 portfolio tab UI deferred until founder review of this API",
    "Brief v2 narrative quality is next Track A slice after this PR",
    "Airtable production membership read-back still founder-gated",
  ],
  recommendedNext:
    "Founder review → Track A Commercial Intelligence Brief v2; then PDI-3 tab reusing Brief v2 + existing drawer",
};
fs.writeFileSync(
  path.join(REPORT, "RESULTS.json"),
  `${JSON.stringify(summary, null, 2)}\n`,
);
fs.writeFileSync(
  path.join(REPORT, "FOUNDER_RESULTS.md"),
  `# GDI PDI-2 — Read-only portfolio API results

Generated: ${summary.at}

## Safety
- Paid calls: 0 · Airtable writes: 0 · Publication writes: 0 · New tables: 0

## Authorization
- Unauthenticated → 401
- Company-name / fuzzy → rejected
- Cross-operator → 403
- Cancún hotel excluded from Guadalajara portfolio
- Wrong-market hotel request → HOTEL_NOT_IN_PORTFOLIO

## Portfolio intelligence
- Shared market id stable across hotels
- Sheraton vs unknown-meetings → different fit classes
- Confidential feedback stripped from shared packet
- Customer surface hides unpublished / RESEARCH_ONLY rows
- Drawer detail paths returned for existing GDI opportunity routes

## Remaining gaps
${summary.remainingGaps.map((g) => `- ${g}`).join("\n")}

## Next
${summary.recommendedNext}
`,
);

console.log(
  JSON.stringify(
    { ok: true, reportDir: REPORT, checks: summary.checks },
    null,
    2,
  ),
);
