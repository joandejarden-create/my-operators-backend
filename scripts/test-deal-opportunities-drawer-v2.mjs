/**
 * Deal Opportunities Drawer V2 — contract smoke (no Airtable / no Surfe).
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { buildDrawerV2 } from "../lib/hotel-intelligence/deal-opportunities/hif-deal-opportunities-drawer-v2.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

assert.ok(
  fs.existsSync(
    path.join(ROOT, "reports/dealsignal-owner-portfolio-early-warning-v2/DRAWER_HANDOFF.json")
  ),
  "DRAWER_HANDOFF.json required"
);
assert.ok(
  fs.existsSync(
    path.join(ROOT, "reports/dealsignal-owner-portfolio-early-warning-v2/RESEARCH_CANDIDATES.json")
  ),
  "RESEARCH_CANDIDATES.json required"
);

const finding = {
  findingId: "find_ds_opew_v2_bidwell_portland_bct_2026",
  fieldName: "COMMERCIAL_OPPORTUNITY_BCT",
  valueObserved: "Franchise extended to October 2026",
  evidenceSnippet: "SHO 10-K franchise disclosure",
  sourceUrl: "https://www.sec.gov/Archives/edgar/data/1295810/000110465926021440/sho-20251231x10k.htm",
  confidence: "HIGH",
  notes: "",
  effectiveDate: "2026-10-31",
  reportingDate: "2026-02-01",
};
const inventory = {
  hotelName: "The Bidwell Marriott Portland",
  city: "Portland",
  stateRegion: "OR",
  country: "US",
  currentBrand: "Marriott",
  discoveryPropertyId: "disc_ds_opew_v2_bidwell_portland_2026",
};
const opportunity = {
  name: "The Bidwell Marriott Portland",
  city: "Portland, OR, United States",
  country: "US",
  kind: "BCT",
  type: "Brand conversion",
  group: "qualified",
  window: "2027",
  timingHorizon: "6_12_MONTHS",
  status: "Private selection",
  access: "Probable",
  why: finding.valueObserved,
  sources: [],
  he: { ready: false, hpcRecordId: null },
  relatedHotels: [],
};

const v2 = buildDrawerV2({ finding, inventory, opportunity, relatedHotels: [] });
assert.equal(v2.version, "dealsignal-drawer-v2");
assert.equal(v2.publicationState, "CUSTOMER_BLOCKED");
assert.ok(v2.executiveSummary?.headline);
assert.ok(v2.whyExists?.whatIsChanging);
assert.ok(v2.whyNewPartnerRelevant?.perspective);
assert.ok(Array.isArray(v2.decisionMakers?.people));
assert.ok(v2.decisionMakers.people.length >= 1, "Bidwell should resolve named executives from overlay/handoff");
assert.equal(v2.decisionMakers.contactStatus, "Contact research required");
assert.match(v2.engagementPriority, /ENGAGE NOW|BUILD RELATIONSHIP|MONITOR/);
assert.match(v2.evidenceStrength, /Strong|Moderate|Limited/);
assert.ok(Array.isArray(v2.supportingEvidence));
assert.ok(v2.supportingEvidence.some((s) => s.url));
assert.ok(Array.isArray(v2.stillNeedToKnow));
assert.ok(v2.recommendedNextSteps?.engagementActions?.length);
assert.equal(v2.decisionMakers.contactStatus, "Contact research required");
assert.ok(v2.decisionMakers.people.every((p) => !p.contact), "no fabricated personal contacts");

const readerSrc = fs.readFileSync(
  path.join(ROOT, "lib/hotel-intelligence/deal-opportunities/hif-deal-opportunities-reader-v1.js"),
  "utf8"
);
assert.match(readerSrc, /buildDrawerV2/);
assert.match(readerSrc, /drawerV2/);

const html = fs.readFileSync(path.join(ROOT, "public/deal-opportunities.html"), "utf8");
assert.match(html, /showDrawerV2/);
assert.match(html, /Portfolio opportunity/i);
assert.match(html, /CUSTOMER_BLOCKED|DealSignal|Deal Opportunities/);

console.log("OK deal-opportunities-drawer-v2 smoke", {
  priority: v2.engagementPriority,
  evidence: v2.evidenceStrength,
  people: v2.decisionMakers.people.map((p) => p.name),
  handoff: v2.handoffUsed,
  overlay: v2.overlayUsed,
});
