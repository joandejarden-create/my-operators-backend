#!/usr/bin/env node
/**
 * Market TI audit config regression tests (no Airtable).
 *   node scripts/test-market-ti-audit-config.mjs
 */
import { auditMarketTravelInfrastructure } from "../lib/radar-buildout/audit-market-travel-infrastructure.js";
import { getMarketTiAuditConfig } from "../lib/radar-buildout/market-travel-infrastructure-audit-configs.js";

let failed = 0;

function assert(cond, msg) {
  if (!cond) {
    console.error("FAIL:", msg);
    failed += 1;
  } else {
    console.log("ok:", msg);
  }
}

function testDominicanRepublicConfigExists() {
  const cfg = getMarketTiAuditConfig("Dominican Republic");
  assert(cfg != null, "Dominican Republic audit config exists");
  assert(Array.isArray(cfg.keywords) && cfg.keywords.length > 5, "DR keywords populated");
  assert(Array.isArray(cfg.expectedPatterns) && cfg.expectedPatterns.length > 5, "DR expected patterns populated");
}

function testDominicanRepublicDoesNotUseCancunKeywords() {
  const records = [
    {
      name: "Punta Cana International Airport",
      city: "Punta Cana",
      country: "Dominican Republic",
      submarket: "Punta Cana / Bávaro / Cap Cana",
      pointType: "Airport",
      iataCode: "PUJ",
    },
    {
      name: "Las Américas International Airport",
      city: "Santo Domingo",
      country: "Dominican Republic",
      submarket: "Santo Domingo Metro",
      pointType: "Airport",
      iataCode: "SDQ",
    },
    {
      name: "Casa De Campo International Airport",
      city: "La Romana",
      country: "Dominican Republic",
      submarket: "La Romana / Bayahibe",
      pointType: "Airport",
      iataCode: "LRM",
    },
  ];

  const report = auditMarketTravelInfrastructure(records, {
    country: "Dominican Republic",
    market: "Dominican Republic Countrywide",
  });

  assert(!report.keywords.includes("cancun"), "DR audit does not use Cancún keywords");
  assert(!report.likelyMissingRecords.includes("Cancún International Airport"), "DR audit does not expect Cancún airport");
  assert(report.summary.marketMatchedRecords === 3, "DR countrywide matches DR records");
  assert(report.summary.existingExpectedNodes >= 3, "DR expected PUJ/SDQ/LRM nodes found");
  assert(!report.likelyMissingRecords.includes("La Romana International Airport / LRM"), "Casa de Campo alias satisfies LRM expected node");
}

function testUnknownCountryDoesNotDefaultToCancun() {
  const records = [
    {
      name: "Example Airport",
      city: "Capital",
      country: "Exampleland",
      submarket: "Other",
      pointType: "Airport",
    },
  ];

  const report = auditMarketTravelInfrastructure(records, {
    country: "Exampleland",
    market: "Exampleland Countrywide",
  });

  assert(!report.keywords.includes("cancun"), "unknown country does not inherit Cancún keywords");
  assert(report.likelyMissingRecords.length === 0, "unknown country has no Cancún expected nodes");
  assert(
    report.notes.some((n) => n.includes("No dedicated TI audit config")),
    "unknown country audit notes missing-config warning"
  );
}

function testMexicoStillUsesCancunDefaults() {
  const report = auditMarketTravelInfrastructure([], {
    country: "Mexico",
    market: "Cancún / Riviera Maya",
  });
  assert(report.keywords.includes("cancun"), "Mexico market still uses Cancún keywords");
  assert(report.likelyMissingRecords.includes("Cancún International Airport"), "Mexico expects Cancún airport");
}

function main() {
  testDominicanRepublicConfigExists();
  testDominicanRepublicDoesNotUseCancunKeywords();
  testUnknownCountryDoesNotDefaultToCancun();
  testMexicoStillUsesCancunDefaults();

  if (failed) {
    console.error(`\n${failed} test(s) failed`);
    process.exit(1);
  }
  console.log("\nAll market TI audit config tests passed.");
}

main();
