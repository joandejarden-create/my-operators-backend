/**
 * Deal Opportunities reader — classification + CUSTOMER_BLOCKED contract smoke test.
 * Does not call Airtable. No Surfe. No schema writes.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const readerPath = path.join(
  ROOT,
  "lib/hotel-intelligence/deal-opportunities/hif-deal-opportunities-reader-v1.js"
);
const apiPath = path.join(ROOT, "api/deal-opportunities.js");
const htmlPath = path.join(ROOT, "public/deal-opportunities.html");
const cssPath = path.join(ROOT, "public/css/deal-opportunities.css");
const appPath = path.join(ROOT, "public/app.js");
const serverPath = path.join(ROOT, "server.js");

for (const p of [readerPath, apiPath, htmlPath, cssPath, appPath, serverPath]) {
  assert.ok(fs.existsSync(p), `missing ${path.relative(ROOT, p)}`);
}

const readerSrc = fs.readFileSync(readerPath, "utf8");
const apiSrc = fs.readFileSync(apiPath, "utf8");
const appSrc = fs.readFileSync(appPath, "utf8");
const serverSrc = fs.readFileSync(serverPath, "utf8");

assert.match(readerSrc, /CUSTOMER_BLOCKED/);
assert.match(apiSrc, /CUSTOMER_BLOCKED/);
assert.match(readerSrc, /publicationState:\s*"CUSTOMER_BLOCKED"/);
assert.match(readerSrc, /buildDrawerV2|drawerV2/);
assert.doesNotMatch(readerSrc, /surfe/i);
assert.match(readerSrc, /assessCommercialEligibility|PRINCIPAL_PROSPECTIVE_OPEN/);
assert.match(readerSrc, /commercial-eligibility-v1/);
assert.match(
  fs.readFileSync(
    path.join(ROOT, "lib/hotel-intelligence/deal-opportunities/hif-deal-opportunities-commercial-eligibility-v1.js"),
    "utf8",
  ),
  /PORTFOLIO_CHILD_NOT_COUNTED/,
);
assert.match(readerSrc, /verifiedCoreBctMctDecisions/);
assert.match(readerSrc, /DEAL_OPPORTUNITY_REGIONS/);
assert.match(readerSrc, /export function regionBucket/);
assert.match(readerSrc, /Middle East & Africa/);
assert.match(readerSrc, /Asia Pacific/);

const htmlSrc = fs.readFileSync(htmlPath, "utf8");
assert.match(htmlSrc, /for="region">Region</);
assert.match(htmlSrc, /All Regions/);
assert.match(htmlSrc, /Middle East &amp; Africa|Middle East & Africa/);
assert.match(htmlSrc, /opportunityRegion/);
assert.match(htmlSrc, /populateCountryFilter/);
assert.match(htmlSrc, /is-monogram/);
assert.match(htmlSrc, /monogramForOpportunity/);
assert.match(htmlSrc, /formatOpportunityIdentity/);
assert.match(htmlSrc, /identityCellHtml/);
assert.match(htmlSrc, /dop-hotel-chip/);
assert.match(htmlSrc, /lookupBrandChainScale|brandScaleMap/);
assert.match(htmlSrc, /scaleTileStyle|is-scale-unknown/);
assert.match(htmlSrc, /CHAIN_SCALE_UNKNOWN_FILL/);
assert.doesNotMatch(htmlSrc, /All Markets/);
assert.doesNotMatch(htmlSrc, /for="region">Market</);

assert.match(appSrc, /founderDealSignalPreview/);
assert.match(appSrc, /founderPreview/);
assert.match(appSrc, /['"]\/deal-opportunities['"]/);

const cssSrc = fs.readFileSync(cssPath, "utf8");
assert.match(cssSrc, /\.dop-hotel-logo\.is-monogram/);
assert.match(cssSrc, /\.dop-opp-card__logo\.is-monogram/);
assert.match(cssSrc, /\.dop-mono/);
assert.match(cssSrc, /\.dop-hotel-chip/);
assert.match(cssSrc, /\.dop-hotel-chips/);

assert.match(appSrc, /['"]\/deal-opportunities['"]/);
assert.match(appSrc, /Deal Opportunities/);
assert.match(serverSrc, /\/api\/deal-opportunities/);
assert.match(serverSrc, /getDealOpportunities/);
assert.match(serverSrc, /memberstackAuth/);

const { regionBucket, DEAL_OPPORTUNITY_REGIONS } = await import(
  pathToFileURL(readerPath).href
);
assert.deepEqual([...DEAL_OPPORTUNITY_REGIONS], [
  "Europe",
  "CALA",
  "North America",
  "Middle East & Africa",
  "Asia Pacific",
]);
assert.equal(regionBucket("US"), "North America");
assert.equal(regionBucket("CA"), "North America");
assert.equal(regionBucket("MX"), "CALA");
assert.equal(regionBucket("BR"), "CALA");
assert.equal(regionBucket("TT"), "CALA");
assert.equal(regionBucket("GB"), "Europe");
assert.equal(regionBucket("ES"), "Europe");
assert.equal(regionBucket("AE"), "Middle East & Africa");
assert.equal(regionBucket("ZA"), "Middle East & Africa");
assert.equal(regionBucket("IN"), "Asia Pacific");
assert.equal(regionBucket("MV"), "Asia Pacific");
assert.equal(regionBucket(""), "Other");

const contractPath = path.join(
  ROOT,
  "lib/hotel-intelligence/sec-edgar/discovery-inventory-airtable-contract-v23.js"
);
const foundationPath = path.join(
  ROOT,
  "lib/hotel-intelligence/sec-edgar/hif-foundation-persistence-v1.js"
);
assert.ok(fs.existsSync(contractPath), "must reuse main HIF airtable contract");
assert.ok(fs.existsSync(foundationPath), "must reuse main HIF foundation persistence");

console.log("OK deal-opportunities-reader-v1 contract smoke");
