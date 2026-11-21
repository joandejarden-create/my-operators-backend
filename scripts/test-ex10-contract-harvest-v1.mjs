#!/usr/bin/env node
/**
 * Unit tests for V5 EX-10 harvest classifiers + cache mining helpers.
 */
import assert from "node:assert/strict";
import {
  classifyInstrument,
  classifyScope,
  detectAgreementType,
  harvestExhibitDocument,
  enrichCandidateFromEdgarUrl,
  AGREEMENT_INSTRUMENT_CLASS,
  SCOPE_CLASS,
} from "../lib/hotel-intelligence/sec-edgar/ex10-contract-harvest-v1.js";
import { VERIFICATION_GATE } from "../lib/hotel-intelligence/sec-edgar/contract-intelligence-identity-layers-v1.js";

assert.equal(
  classifyInstrument("FORM OF HOTEL MANAGEMENT AGREEMENT This Agreement"),
  AGREEMENT_INSTRUMENT_CLASS.FORM_OF_AGREEMENT,
);
assert.equal(
  classifyInstrument("This Agreement is made and entered into as of January 1, 2020"),
  AGREEMENT_INSTRUMENT_CLASS.EXECUTED_AGREEMENT,
);
assert.equal(
  classifyInstrument("FIRST AMENDMENT TO MANAGEMENT AGREEMENT"),
  AGREEMENT_INSTRUMENT_CLASS.AMENDMENT,
);

assert.equal(
  classifyScope("Form of Hotel Management Agreement for the Hotel"),
  SCOPE_CLASS.FORM_WITHOUT_PROPERTY,
);
assert.equal(
  classifyScope("Courtyard by Marriott Miami Airport Hotel located at 1200 Airport Boulevard"),
  SCOPE_CLASS.NAMED_HOTEL,
);
assert.equal(
  classifyScope("Manager shall manage each of the hotels in the portfolio"),
  SCOPE_CLASS.PORTFOLIO_LEVEL,
);

assert.equal(detectAgreementType("Hotel Franchise Agreement between Franchisor and Franchisee"), "FRANCHISE");

const h = harvestExhibitDocument(
  {
    text: "This Agreement is made and entered into. Initial term of twenty (20) years. Renewal for two (2) successive five (5) year periods upon 180 days prior written notice. Courtyard by Marriott Austin Downtown Hotel located at 100 Congress Avenue.",
  },
  { sourceUrl: "https://www.sec.gov/Archives/edgar/data/1/0000000000-00-000001/ex10.htm", accession: "0000000000-00-000001" },
);
assert.equal(h.instrumentClass, AGREEMENT_INSTRUMENT_CLASS.EXECUTED_AGREEMENT);
assert.equal(h.scopeClass, SCOPE_CLASS.NAMED_HOTEL);
assert.equal(h.hotelAttributionAllowed, true);
assert.equal(h.gates[VERIFICATION_GATE.AGREEMENT_EXISTENCE_VERIFIED], true);
assert.equal(h.gates[VERIFICATION_GATE.CURRENT_STATUS_VERIFIED], false);
assert.equal(h.publicationState, "CUSTOMER_BLOCKED");

const enriched = enrichCandidateFromEdgarUrl({
  url: "https://www.sec.gov/Archives/edgar/data/1692412/000169241222000042/ye21-ex1033xhotelmanagemen.htm",
});
assert.equal(enriched.cik, "0001692412");
assert.equal(enriched.accession, "0001692412-22-000042");

console.log("test-ex10-contract-harvest-v1: PASS");
