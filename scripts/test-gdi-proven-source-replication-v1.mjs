#!/usr/bin/env node
import assert from "node:assert/strict";
import {
  buildProvenSourceQueries,
  classifyProvenSourceFamily,
  classifyLodgingEvidenceFromText,
  classifyCommercialStatus,
  evaluateNativeSuccessGate,
  LODGING_EVIDENCE,
  COMMERCIAL_STATUS,
  PROVEN_SOURCE_FAMILY,
  defaultProvenSourcePriority,
} from "../lib/group-demand-intelligence/proven-source/proven-source-playbook-v1.js";

const AC = "rec2PVBDavppGpenm";
const SPICE = "recKRJjcPnb4tVDDS";

assert.ok(buildProvenSourceQueries(AC, {}, { max: 10 }).length >= 8);
assert.ok(buildProvenSourceQueries(SPICE, {}, { max: 10 }).length >= 8);
assert.equal(
  classifyLodgingEvidenceFromText("Official host hotel room block now open"),
  LODGING_EVIDENCE.DIRECT
);
assert.equal(
  classifyCommercialStatus("Hotel TBD — housing information forthcoming 2027"),
  COMMERCIAL_STATUS.HOTEL_VENUE_TBD
);
assert.equal(
  classifyProvenSourceFamily({
    url: "https://example.org/events/2027-conference/housing",
    title: "Housing Information",
  }),
  PROVEN_SOURCE_FAMILY.HOUSING_PAGE
);

const prio = defaultProvenSourcePriority({ family: "SPORTS_PAGE", sourcesChecked: [] });
assert.notEqual(prio, "HOUSING_PDF");
assert.ok(
  [PROVEN_SOURCE_FAMILY.SPORTS_PAGE, PROVEN_SOURCE_FAMILY.HOUSING_PAGE].includes(prio)
);

assert.equal(
  evaluateNativeSuccessGate({ readyCount: 2 }).pass,
  true
);
assert.equal(
  evaluateNativeSuccessGate({
    readyCount: 0,
    openTbdLodgingSupported: 0,
    structuresFound: {},
  }).pass,
  false
);

assert.equal(process.env.CONTACT_INTELLIGENCE_PAID_ENRICHMENT_ENABLED !== "1", true);

console.log("test-gdi-proven-source-replication-v1: PASS");
