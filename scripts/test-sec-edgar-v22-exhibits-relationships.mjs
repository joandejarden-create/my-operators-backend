#!/usr/bin/env node
import assert from "node:assert/strict";
import {
  enumerateFilingPackageExhibits,
  dedupeExhibits,
} from "../lib/hotel-intelligence/sec-edgar/filing-package-exhibits-v2.js";
import {
  extractRelationshipsFromDocument,
  reconcileRelationshipFindings,
  RELATIONSHIP_STATUS,
} from "../lib/hotel-intelligence/sec-edgar/relationship-extractor-v2.js";
import { ENTITY_ROLE } from "../lib/hotel-intelligence/sec-edgar/entity-attribution-v2.js";
import { buildCoverageLedger } from "../lib/hotel-intelligence/sec-edgar/coverage-ledger-v2.js";
import { IDENTITY_CLASS, classifyHotelIdentity } from "../lib/hotel-intelligence/sec-edgar/hotel-identity-quality-v2.js";

let passed = 0;
function test(name, fn) {
  try {
    fn();
    passed += 1;
    console.log(`PASS ${name}`);
  } catch (err) {
    console.error(`FAIL ${name}`);
    console.error(err);
    process.exitCode = 1;
  }
}

test("issuer_named_exhibit211_matches_ex21_not_exhibit32", () => {
  const index = {
    directory: {
      item: [
        { name: "drh-20231231.htm", type: "text.gif", size: "1000" },
        { name: "drh_exhibit211x12312023.htm", type: "text.gif", size: "500" },
        { name: "drh_exhibit321x12312023.htm", type: "text.gif", size: "100" },
        { name: "exhibit10152324.htm", type: "text.gif", size: "200" },
      ],
    },
  };
  const out = enumerateFilingPackageExhibits(index, {
    cik: "0001298946",
    accessionNumber: "0001298946-24-000011",
    form: "10-K",
  });
  assert.ok(out.exhibits.some((e) => e.fileName.includes("exhibit211") && e.exhibitType === "EX-21_SUBSIDIARIES"));
  assert.ok(out.exhibits.some((e) => e.fileName.includes("exhibit101") && e.exhibitType === "EX-10_OTHER"));
  assert.ok(!out.exhibits.some((e) => e.fileName.includes("exhibit321")));
});

test("filing_package_enumerates_ex10_and_ex21_separately_from_parent", () => {
  const index = {
    directory: {
      item: [
        { name: "drh-20221231.htm", type: "10-K", size: "1000" },
        { name: "ex10-1.htm", type: "EX-10.1", size: "2000", description: "Hotel Management Agreement" },
        { name: "ex10_2.htm", type: "EX-10.2", size: "1500", description: "Franchise Agreement" },
        { name: "ex21.htm", type: "EX-21", size: "500", description: "List of Subsidiaries" },
        { name: "graphic.jpg", type: "GRAPHIC", size: "10" },
      ],
    },
  };
  const client = {
    filingDocumentUrl: (cik, acc, file) =>
      `https://www.sec.gov/Archives/edgar/data/${cik}/${acc.replace(/-/g, "")}/${file}`,
  };
  const out = enumerateFilingPackageExhibits(
    index,
    {
      cik: "0001298946",
      accessionNumber: "0001298946-23-000012",
      form: "10-K",
      filingDate: "2023-02-28",
      companyName: "DiamondRock",
    },
    client,
  );
  assert.ok(out.exhibitCount >= 3);
  assert.ok(out.exhibits.every((e) => e.parentAccession === "0001298946-23-000012"));
  assert.ok(out.exhibits.every((e) => e.sourceUrl && e.fileName));
  assert.ok(out.exhibits.some((e) => e.exhibitType === "EX-10_MANAGEMENT"));
  assert.ok(out.exhibits.some((e) => e.exhibitType === "EX-21_SUBSIDIARIES"));
  assert.ok(!out.exhibits.some((e) => e.fileName === "drh-20221231.htm"));
});

test("exhibit_dedupe_and_ibr_skip", () => {
  const exhibits = [
    {
      exhibitId: "a",
      fileName: "ex10-1.htm",
      parentAccession: "1",
      incorporatedByReference: false,
    },
    {
      exhibitId: "a",
      fileName: "ex10-1.htm",
      parentAccession: "1",
      incorporatedByReference: false,
    },
    {
      exhibitId: "b",
      fileName: "ex10-9.htm",
      parentAccession: "1",
      incorporatedByReference: true,
    },
  ];
  const d = dedupeExhibits(exhibits);
  assert.equal(d.uniqueCount, 1);
  assert.ok(d.skipped.some((s) => s.skipReason === "incorporated_by_reference"));
});

test("relationships_require_explicit_language_not_portfolio_listing", () => {
  const out = extractRelationshipsFromDocument(
    {
      text: `
        The hotel is managed by Example Hotel Management LLC under a hotel management agreement.
        The property is owned by DRH Owner SPE LLC.
        Franchise agreement with Marriott International, Inc. dated as of January 1, 2018.
        Courtyard Denver Downtown appears in our portfolio schedule.
      `,
    },
    { cik: "0001298946", companyName: "DiamondRock Hospitality Company", filingDate: "2023-02-28" },
  );
  assert.ok(out.findings.some((f) => f.role === ENTITY_ROLE.MANAGEMENT_COMPANY));
  assert.ok(out.findings.some((f) => f.role === ENTITY_ROLE.LEGAL_OWNER));
  assert.ok(out.findings.some((f) => f.role === ENTITY_ROLE.FILING_REGISTRANT));
  const owners = out.findings.filter((f) => f.role === ENTITY_ROLE.LEGAL_OWNER);
  assert.ok(owners.every((o) => o.explicitEvidence && !o.inferredFromPortfolioListing));
  // Registrant present but not as LEGAL_OWNER
  assert.ok(!owners.some((o) => /DiamondRock Hospitality Company/i.test(o.entityName)));
});

test("relationship_conflict_status", () => {
  const r = reconcileRelationshipFindings([
    {
      role: ENTITY_ROLE.MANAGEMENT_COMPANY,
      entityName: "Manager A",
      hotelNameHints: ["Courtyard Denver"],
      relationshipStatus: RELATIONSHIP_STATUS.HISTORICALLY_VERIFIED,
    },
    {
      role: ENTITY_ROLE.MANAGEMENT_COMPANY,
      entityName: "Manager B",
      hotelNameHints: ["Courtyard Denver"],
      relationshipStatus: RELATIONSHIP_STATUS.HISTORICALLY_VERIFIED,
    },
  ]);
  assert.ok(r.findings.some((f) => f.relationshipStatus === RELATIONSHIP_STATUS.CONFLICTED));
});

test("coverage_ledger_separates_core_and_extended_cohorts", () => {
  const ledger = buildCoverageLedger({ cwd: process.cwd() });
  assert.equal(ledger.version, "sec-edgar-coverage-ledger-v2");
  assert.ok(ledger.cohorts.CORE_SIX);
  assert.ok(ledger.cohorts.EXTENDED);
  assert.ok(Array.isArray(ledger.guarantees));
  assert.ok(ledger.guarantees.some((g) => /not double-counted/i.test(g)));
});

test("identity_gate_still_blocks_uncertain_as_verified", () => {
  assert.notEqual(
    classifyHotelIdentity({ hotelName: "Embassy Suites", city: "Houston", stateRegion: "TX" })
      .identityClass,
    IDENTITY_CLASS.VERIFIED_PROPERTY,
  );
});

console.log(`\n${passed} tests passed`);
