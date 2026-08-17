import assert from "node:assert/strict";
import {
  operatorAlignsWithOwner,
  isJunkBuildingName,
  buildOwnerPortfolioAudit,
  MAP_PORTFOLIO_AUDIT,
} from "../lib/gtm-owner-target/owner-portfolio-audit.js";

assert.equal(operatorAlignsWithOwner("Oasis Hotels and Resorts", "Oasis Hotels and Resorts"), true);
assert.equal(operatorAlignsWithOwner("Grupo Questro", "Apple Leisure Group"), false);
assert.equal(isJunkBuildingName("Puerto Plata, Dominican Republic", "Dominican Republic"), true);
assert.equal(isJunkBuildingName("Dreams Los Cabos Suites Golf Resort & Spa", "Mexico"), false);

const audit = buildOwnerPortfolioAudit(
  {
    ownerName: "Arotesa Servicions Integrales SA de CV",
    icpSegment: "regional_operator",
    outreachReady: false,
  },
  [
    {
      buildingName: "Paraiso de la Bonita, a Luxury Collection Resort, Riviera Maya, Adult All-Inclusive",
      brandAffiliation: "Luxury Collection",
      city: "Puerto Morelos",
      country: "Mexico",
      hotelOperator: "Royalton Hotels & Resorts",
      trueOwner: "Arotesa Servicions Integrales SA de CV",
    },
  ]
);
assert.equal(audit.portfolioConfidence, "blocked");
assert.equal(audit.outreachSafe, false);
assert.ok(audit.flags.includes("entity_mismatch"));

console.log(`owner-portfolio-audit tests OK (${MAP_PORTFOLIO_AUDIT.operatorAlignHighMinRate} align threshold)`);
