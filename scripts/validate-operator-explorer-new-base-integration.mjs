#!/usr/bin/env node
/**
 * Validates Phase D — Operator Explorer new-base integration (static checks).
 */

import { readFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");

function fail(msg) {
  console.error("FAIL:", msg);
  process.exitCode = 1;
}

function pass(msg) {
  console.log("PASS:", msg);
}

function read(rel) {
  return readFileSync(join(root, rel), "utf8");
}

const goldHtml = read("public/operator-explorer-gold-mock.html");
const goldJs = read("public/js/operator-explorer-gold-mock-data.js");
const profileJs = read("public/js/operator-explorer-new-base-profile.js");
const explorerJs = read("public/js/operator-explorer.js");
const explorerApi = read("api/operator-explorer.js");
const strategyJs = read("public/js/operator-strategy-my-deals.js");
const myDealsHtml = read("public/my-deals.html");
const oasCss = read("public/css/operator-alignment-snapshot.css");
const scoring = read("lib/operator-alignment-scoring-factors.js");

const requiredProfileKeys = [
  "activeCountries",
  "serviceModelsSupported",
  "chainScalesSupported",
  "managementStructuresSupported",
  "offeredServices",
  "preOpeningSupportCapability",
  "ownerReportingLevel",
  "dataConfidenceLevel",
  "sourceType",
  "lastUpdatedDate",
  "companyName",
  "companyDescription",
];

requiredProfileKeys.forEach(function (k) {
  if (!profileJs.includes('"' + k + '"') && !profileJs.includes("'" + k + "'")) {
    fail("profile module missing key: " + k);
  } else {
    pass("profile module references " + k);
  }
});

if (!goldHtml.includes("operator-explorer-new-base-profile.js")) {
  fail("gold-mock.html must load operator-explorer-new-base-profile.js");
} else {
  pass("gold-mock loads new-base profile module");
}

if (!goldJs.includes("fetchOperatorBundle") || !goldJs.includes("/api/intake/third-party-operators/")) {
  fail("gold-mock must load live detail via intake API");
} else {
  pass("gold-mock uses intake detail API for live records");
}

if (!goldJs.includes("mountAlignmentContext") || !profileJs.includes("/api/operator-alignment-snapshot/")) {
  fail("deal-aware mode must call OAS companies API");
} else {
  pass("deal-aware alignment uses /api/operator-alignment-snapshot/:dealId/companies");
}

if (!profileJs.includes("operatorRecordId") || !profileJs.includes("matchedBy")) {
  fail("alignment context should attempt id-based match (operatorRecordId / matchedBy)");
} else {
  pass("alignment context uses id-based matching + debug metadata");
}

if (!goldHtml.includes('id="alignmentContext"')) {
  fail("alignment context container missing in gold-mock.html");
} else {
  pass("alignment context panel DOM present");
}

if (!explorerApi.includes("OPERATOR_EXPLORER_ALLOW_MOCKS")) {
  fail("api/operator-explorer.js must gate MOCK_OPERATORS behind OPERATOR_EXPLORER_ALLOW_MOCKS");
} else {
  pass("MOCK_OPERATORS gated by OPERATOR_EXPLORER_ALLOW_MOCKS");
}

if (!profileJs.includes("Sample operator profile") || !goldHtml.includes("oe-demo-banner")) {
  fail("demo/sample labeling required for non-live profiles");
} else {
  pass("demo profiles labeled as sample");
}

if (!explorerJs.includes('indexOf("rec")') || !explorerJs.includes("openGoldMockPopup")) {
  fail("operator-explorer.js must only open live rec… profiles in popup");
} else {
  pass("explorer popup restricted to rec… operator ids");
}

const strategyProfile =
  strategyJs.includes("openMyDealsOperatorProfileForDeal") &&
  strategyJs.includes("dealId") &&
  strategyJs.includes("operator-explorer-gold-mock.html");
if (!strategyProfile) {
  fail("Operator Strategy profile CTA must pass operatorId and dealId");
} else {
  pass("Operator Strategy profile CTA includes operatorId and dealId");
}

if (!myDealsHtml.includes("openMyDealsOperatorProfileForDeal") || !myDealsHtml.includes("&dealId=")) {
  fail("my-deals openProfile must support dealId query");
} else {
  pass("my-deals profile opener passes dealId");
}

const bannedInNew = [
  "Dealality recommends",
  "Preferred operator",
  "Top operator",
  "Best fit for this deal",
];
bannedInNew.forEach(function (phrase) {
  if (profileJs.includes(phrase)) fail("banned phrase in new profile module: " + phrase);
});
if (!process.exitCode) pass("no banned recommendation phrases in new profile module");

const alignmentDisclaimer =
  "does not indicate operator approval, availability, or commercial terms";
if (!profileJs.includes(alignmentDisclaimer)) {
  fail("alignment context disclaimer missing");
} else {
  pass("alignment context uses neutral disclaimer copy");
}

if (oasCss.includes("920px") && oasCss.match(/myDealsOperatorAlignmentModal[\s\S]{0,400}920px/)) {
  fail("unexpected OAS modal width regression to 920px");
} else {
  pass("OAS modal width not regressed to 920px in checked block");
}

if (!scoring.includes("scoreDealStructureFactor") || !scoring.includes("weights unchanged")) {
  fail("scoring factors module missing or unexpected change marker");
} else {
  pass("scoring factors module untouched in this phase (weights unchanged comment present)");
}

if (process.exitCode) {
  console.error("\nValidation finished with failures.");
  process.exit(process.exitCode);
}
console.log("\nAll Operator Explorer new-base integration checks passed.");
