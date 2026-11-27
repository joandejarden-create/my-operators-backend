#!/usr/bin/env node
/**
 * Organization & Portfolio UI cleanup gates:
 * - shared table padding / column models
 * - Economic Sponsor relationship labeling
 * - Key People reconciles with Contact Intelligence (no Surfe/internal copy)
 * - Strategic Context customer lede
 * - Factory PowerShell wiring
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { formatOwnershipRelationshipLabel } from "../lib/hotel-intelligence/ownership/owner-portfolio-operator-view-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const read = (rel) => fs.readFileSync(path.join(ROOT, rel), "utf8");

const bethRel = formatOwnershipRelationshipLabel({
  relationship_type: "Economic Owner",
  relationship: "Economic Owner",
  confidence: "MEDIUM",
  economic_owner_verified: true,
});
assert.equal(bethRel, "Economic Sponsor");
assert.notEqual(bethRel, "Economic Owner");

const legalRel = formatOwnershipRelationshipLabel({ relationship_type: "LEGAL_OWNER" });
assert.equal(legalRel, "Legal Owner");

const ws = read("public/js/hotel-intelligence-workspace.js");
assert.match(ws, /hiw-table--portfolio/);
assert.match(ws, /Ownership Relationship/);
assert.match(ws, /Economic Sponsor|formatOwnershipRelationshipLabel/);
assert.match(ws, /mountOrgKeyPeopleFromContactIntelligence/);
assert.match(ws, /mapContactIntelligencePeople/);
assert.match(ws, /\/api\/contact-intelligence\/hotels\//);
assert.match(ws, /Portfolio and ownership context for commercial outreach/);
assert.doesNotMatch(ws, /Executive assessment synthesized from verified and appropriately qualified proposed intelligence/);
assert.doesNotMatch(ws, /Surfe enrichment stays on Contact Intelligence/);
assert.doesNotMatch(ws, /shared contact intelligence model/);
assert.match(ws, /Ownership decision-makers not yet verified/);
assert.match(ws, /Professional contacts may still be available in Contact Intelligence/);
assert.match(ws, /sanitizeOperatingPartyName/);
assert.match(ws, /hiw-table--portfolio-manage/);
assert.match(ws, /About This Strategic Context/);

const explorer = read("public/js/hotel-explorer.js");
assert.match(explorer, /mountOrgKeyPeopleFromContactIntelligence/);

const hiwCss = read("public/css/hotel-intelligence-workspace.css");
assert.match(hiwCss, /padding:\s*14px\s+16px/);
assert.match(hiwCss, /\.hiw-table--portfolio\s*\{[\s\S]*min-width:\s*1180px/);
assert.match(hiwCss, /nth-child\(7\)/);
assert.match(hiwCss, /\.hiw-table--portfolio-manage/);

const hexCss = read("public/css/hotel-explorer.css");
assert.match(hexCss, /min-width:\s*1180px\s*!important/);
assert.doesNotMatch(
  hexCss,
  /\.hex-root \.hiw-table thead th:first-child\s*\{\s*padding-left:\s*0/
);
assert.doesNotMatch(
  hexCss,
  /\.hex-root \.hiw-table tbody td:first-child\s*\{\s*padding-left:\s*0/
);
assert.match(hexCss, /hex-owner-portfolio > \.hiw-section/);

const browserLib = read("public/js/he-owner-portfolio-operator-view-v1.js");
assert.match(browserLib, /Economic Sponsor/);
assert.match(browserLib, /ECONOMIC\\s\+\(OWNER\|SPONSOR\)/);

const ps1 = read("scripts/hotel-explorer/Invoke-HotelExplorerFactory.ps1");
assert.match(ps1, /test:he-org-portfolio-ui-cleanup-v1/);

const pkg = read("package.json");
assert.match(pkg, /"test:he-org-portfolio-ui-cleanup-v1"/);

console.log("ok - he-org-portfolio-ui-cleanup-v1");
