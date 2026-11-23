/**
 * Deal Opportunities reader — classification + CUSTOMER_BLOCKED contract smoke test.
 * Does not call Airtable. No Surfe. No schema writes.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

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

assert.match(appSrc, /['"]\/deal-opportunities['"]/);
assert.match(appSrc, /Deal Opportunities/);
assert.match(serverSrc, /\/api\/deal-opportunities/);
assert.match(serverSrc, /getDealOpportunities/);
assert.match(serverSrc, /memberstackAuth/);

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
