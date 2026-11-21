#!/usr/bin/env node
import assert from "node:assert/strict";
import {
  monthsBetween,
  classifyTimingHorizon,
  deriveNextCommercialDecisionWindow,
  TIMING_HORIZON,
  DECISION_WINDOW_KIND,
} from "../lib/hotel-intelligence/sec-edgar/hif-v6-commercial-timing-model-v6.js";
import { classifyPhysicalGeography, GEO_CLASS } from "../lib/hotel-intelligence/sec-edgar/hif-v6-geographic-confidence-v6.js";
import {
  classifyCommercialActionability,
  COMMERCIAL_PRIORITY,
} from "../lib/hotel-intelligence/sec-edgar/hif-v6-commercial-actionability-v6.js";

const asOf = "2026-11-21";

assert.equal(monthsBetween(asOf, "2027-02-21"), 3);
assert.equal(classifyTimingHorizon(3), TIMING_HORIZON.M0_6);
assert.equal(classifyTimingHorizon(8), TIMING_HORIZON.M6_12);
assert.equal(classifyTimingHorizon(18), TIMING_HORIZON.M12_24);
assert.equal(classifyTimingHorizon(-2), TIMING_HORIZON.HISTORICAL_SUPERSEDED);
assert.equal(classifyTimingHorizon(null), TIMING_HORIZON.UNKNOWN);

const expOnly = deriveNextCommercialDecisionWindow({ expirationDate: "2027-03-01" }, asOf);
assert.equal(expOnly.decisionWindowKind, DECISION_WINDOW_KIND.EXPIRATION_ONLY_PROXY);

const notice = deriveNextCommercialDecisionWindow(
  { expirationDate: "2028-01-01", noticeDeadline: "2027-01-15" },
  asOf,
);
assert.equal(notice.decisionWindowKind, DECISION_WINDOW_KIND.CONFIRMED_DECISION_WINDOW);
assert.equal(notice.nextCommercialDecisionDate, "2027-01-15");

const geo = classifyPhysicalGeography({
  hotelCountry: "US",
  hotelName: "Hampton Inn Austin",
  sourceFamily: "SEC_CMBS",
  sourceJurisdiction: "US",
});
assert.equal(geo.physicalCountry, "US");
assert.ok([GEO_CLASS.VERIFIED, GEO_CLASS.REVIEW_REQUIRED].includes(geo.geographicClass));

const unresolvedGeo = classifyPhysicalGeography({ hotelName: "Mystery Hotel" });
assert.equal(unresolvedGeo.geographicClass, GEO_CLASS.UNRESOLVED);

const actExpOnly = classifyCommercialActionability(
  { agreementType: "UNKNOWN", agreementTypeResolved: false, commercialList: "CONTRACT_DATE_AGREEMENT_TYPE_UNRESOLVED" },
  expOnly,
  geo,
);
assert.notEqual(actExpOnly.commercialPriority, COMMERCIAL_PRIORITY.A_PURSUE_QUALIFY_NOW);

const actA = classifyCommercialActionability(
  { agreementType: "FRANCHISE", agreementTypeResolved: true, commercialList: "BRAND_CONVERSION_TARGETS" },
  notice,
  { ...geo, geographicClass: GEO_CLASS.VERIFIED },
);
assert.equal(actA.commercialPriority, COMMERCIAL_PRIORITY.A_PURSUE_QUALIFY_NOW);

console.log(JSON.stringify({ ok: true, gate: "test-hif-v6-commercial-timing-geo-v6" }, null, 2));
