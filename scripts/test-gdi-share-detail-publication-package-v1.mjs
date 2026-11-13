#!/usr/bin/env node
/**
 * Share DETAIL must serve the approved publication package (not live reconstruct).
 * Regression: Bethesda Premier Cup needDateMatchV1 was on list but missing on detail,
 * so drawers skipped Hotel Need-Date Match while cards still showed package badges.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import vm from "node:vm";
import { fileURLToPath } from "node:url";
import {
  resolveOpportunityFromApprovedPackage,
  resolveShareOpportunitiesFromApprovedPackage,
} from "../lib/group-demand-intelligence/publication-package/gdi-share-approved-package-resolver-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const HOTEL = "recLuxvwwxID7U2B8";
const PREMIER = "gdi_opp_bethesda_premier_cup_2026";
const FP =
  "441773211494cebaf50c03ef8921ccac6c57f31ac5cb2398585352b425e36f7f";

const list = resolveShareOpportunitiesFromApprovedPackage({ hotelId: HOTEL });
assert.equal(list.mode, "PACKAGE");
assert.equal(list.meta?.contentFingerprintSha256, FP);

const detail = resolveOpportunityFromApprovedPackage({
  hotelId: HOTEL,
  opportunityId: PREMIER,
});
assert.equal(detail.mode, "PACKAGE");
assert.ok(detail.opportunity, "premier must resolve from package");
assert.equal(detail.opportunity.id, PREMIER);
assert.ok(
  detail.opportunity.needDateMatchV1,
  "package detail must include needDateMatchV1",
);
assert.match(
  String(detail.opportunity.needDateMatchV1.whyThisMatches || ""),
  /November 2026/i,
);

const listRow = list.opportunities.find((o) => o.id === PREMIER);
assert.deepEqual(listRow.needDateMatchV1, detail.opportunity.needDateMatchV1);

// Load presentation UI and assert rendered section HTML (not source-string only).
const uiSource = fs.readFileSync(
  path.join(ROOT, "public/js/group-demand-intelligence/dealality-gdi-ui.js"),
  "utf8",
);
const sandbox = { console, globalThis: {} };
sandbox.globalThis = sandbox;
vm.runInNewContext(uiSource, sandbox);
const UI = sandbox.DealalityGdiUi;
assert.ok(UI?.needDateMatchDrawerSectionHtml, "UI helper missing");
const html = UI.needDateMatchDrawerSectionHtml(detail.opportunity);
assert.match(html, /data-section-id="hotel_need_date_match"/i);
assert.match(html, /Hotel Need-Date Match/i);
assert.match(html, /November 2026/i);
assert.match(html, /Date-match classification/i);
assert.match(html, /What sales should verify next/i);

// Empty when fields absent — proves conditional render is data-driven.
assert.equal(UI.needDateMatchDrawerSectionHtml({}), "");

console.log(
  JSON.stringify(
    {
      ok: true,
      fingerprint: FP,
      premierNeedDateSection: true,
      htmlIncludesSection: true,
      why: detail.opportunity.needDateMatchV1.whyThisMatches,
    },
    null,
    2,
  ),
);
