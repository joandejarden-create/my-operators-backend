#!/usr/bin/env node
/**
 * GDI on-demand Surfe contact reveal — structural + sanitize + mock provider gates.
 *   node scripts/test-gdi-surfe-on-demand-contact-reveal-v1.mjs
 */
import assert from "node:assert/strict";
import { readFileSync, existsSync } from "node:fs";
import { join } from "node:path";
import { createRequire } from "node:module";
import vm from "node:vm";
import { sanitizeOpportunityForCustomerPresentation } from "../lib/group-demand-intelligence/customer-presentation-sanitize-v1.js";
import {
  revealGdiContactOnDemand,
  createMockGdiContactRevealProvider,
} from "../lib/group-demand-intelligence/gdi-contact-on-demand-reveal-v1.js";
import {
  BETHESDA_HOTEL_ID,
  selectBethesdaSalesOpportunityInventory,
} from "../lib/group-demand-intelligence/bethesda-67-sales-baseline-cmi-v1.js";
import { mergeBethesdaInternalResearchOpportunities } from "../lib/group-demand-intelligence/research/bethesda-internal-research-union-v1.js";
import { applyBethesdaCommercialEventFamilyPresentation } from "../lib/group-demand-intelligence/research/bethesda-88-commercial-event-family-v1.js";
import {
  filterOpportunitiesByCommercialSurface,
  GDI_LIST_SURFACE_VIEW,
} from "../lib/group-demand-intelligence/gdi-commercial-surface-disposition-v1.js";

const ROOT = process.cwd();
const require = createRequire(import.meta.url);

function loadUi() {
  const src = readFileSync(
    join(ROOT, "public/js/group-demand-intelligence/dealality-gdi-ui.js"),
    "utf8"
  );
  const sandbox = {
    window: {},
    globalThis: {},
    console,
    URL,
    URLSearchParams,
    document: {
      body: { classList: { contains: () => false } },
    },
  };
  sandbox.window = sandbox;
  sandbox.globalThis = sandbox;
  vm.runInNewContext(src, sandbox, { filename: "dealality-gdi-ui.js" });
  return sandbox.DealalityGdiUi;
}

const UI = loadUi();
assert.ok(UI?.whoShouldSalesContactHtml);

const server = readFileSync(join(ROOT, "server.js"), "utf8");
const app = readFileSync(
  join(ROOT, "public/js/group-demand-intelligence/app.js"),
  "utf8"
);
const sanitizeSrc = readFileSync(
  join(ROOT, "lib/group-demand-intelligence/customer-presentation-sanitize-v1.js"),
  "utf8"
);

assert.match(server, /postGdiContactReveal/);
assert.match(
  server,
  /opportunities\/:opportunityId\/contacts\/reveal/
);
assert.match(app, /data-gdi-contact-cta/);
assert.match(app, /contacts\/reveal/);
assert.match(app, /authorizeLiveProvider/);
assert.match(sanitizeSrc, /CUSTOMER_SURFE_UI_AFFORDANCE_KEYS/);
assert.equal(/delete next\.surfeEnrichmentEligible/.test(sanitizeSrc), false);

// Sanitize must preserve Find Phone affordance for named contact with email only.
const annette = sanitizeOpportunityForCustomerPresentation({
  id: "gdi_test_annette_hawks",
  title: "Cohera demand",
  organizationName: "Cohera",
  demandSegmentId: "corporate_accounts_scaling",
  segment: "Scaling Companies",
  researchSegment: "scaling_companies",
  primaryContact: {
    name: "Annette Hawks",
    title: "GM, Orlando & Washington, D.C.",
    organization: "Cohera",
    email: "annette.hawks@meetcohera.com",
    phone: null,
    whyThisContact: "Referral contact: Annette Hawks",
  },
  contactName: "Annette Hawks",
  contactEmail: "annette.hawks@meetcohera.com",
});
const primary = annette.scalingTwoContactPresentation?.primary;
assert.equal(primary?.name, "Annette Hawks");
assert.equal(primary?.email, "annette.hawks@meetcohera.com");
assert.equal(primary?.surfeEnrichmentEligible, true);
assert.equal(primary?.surfeEnrichmentChannel, "phone");

const whoHtml = UI.whoShouldSalesContactHtml(annette);
assert.match(whoHtml, /Find Phone/);
assert.match(whoHtml, /data-gdi-contact-cta="1"/);
assert.match(whoHtml, /data-gdi-contact-channel="phone"/);
assert.match(whoHtml, /annette\.hawks@meetcohera\.com/i);
assert.equal(/Why relevant:\s*—/i.test(whoHtml), false);
assert.equal(/Referral contact:\s*Annette Hawks/i.test(whoHtml), false);

// Mock provider path — no live Surfe credits.
const mockHit = await revealGdiContactOnDemand({
  channel: "phone",
  contact: { name: "Annette Hawks", role: "GM" },
  organization: { name: "Cohera" },
  provider: createMockGdiContactRevealProvider("hit"),
  authorizeLiveProvider: false,
});
assert.equal(mockHit.ok, true);
assert.equal(mockHit.persisted, false);
assert.ok(mockHit.contactDetails?.email || mockHit.contactDetails?.phone);

const mockDenied = await revealGdiContactOnDemand({
  channel: "phone",
  contact: { name: "Annette Hawks" },
  organization: { name: "Cohera" },
  authorizeLiveProvider: false,
});
assert.equal(mockDenied.ok, false);
assert.equal(mockDenied.error, "LIVE_PROVIDER_NOT_AUTHORIZED");

// Bethesda inventory audit — count named contacts that should show CTAs.
const doc = JSON.parse(
  readFileSync(
    join(
      ROOT,
      `data/group-demand-intelligence/hotels/${BETHESDA_HOTEL_ID}/opportunities.json`
    ),
    "utf8"
  )
);
const union = mergeBethesdaInternalResearchOpportunities(
  BETHESDA_HOTEL_ID,
  doc.opportunities || []
);
const family = applyBethesdaCommercialEventFamilyPresentation(
  union.opportunities,
  { hideLinkedFromList: true }
);
const surface = filterOpportunitiesByCommercialSurface(
  family.opportunities,
  GDI_LIST_SURFACE_VIEW.COMMERCIAL
);
const inventory = selectBethesdaSalesOpportunityInventory(surface, {
  salesTeamInventory: true,
  founderReview: true,
}).selected;
let named = 0;
let findEmail = 0;
let findPhone = 0;
let findBoth = 0;
let noCtaNamed = 0;
for (const row of inventory) {
  const sanitized = sanitizeOpportunityForCustomerPresentation(row);
  const slots = [
    sanitized.scalingTwoContactPresentation?.primary,
    sanitized.scalingTwoContactPresentation?.secondary,
  ].filter(Boolean);
  for (const slot of slots) {
    if (!slot.customerFacingNamed || !slot.name) continue;
    named += 1;
    const ch = slot.surfeEnrichmentChannel;
    if (ch === "email") findEmail += 1;
    else if (ch === "phone") findPhone += 1;
    else if (ch === "both") findBoth += 1;
    else noCtaNamed += 1;
  }
}

assert.ok(named > 0, "expected named contacts in Bethesda inventory");
assert.ok(
  findEmail + findPhone + findBoth > 0,
  "expected at least one Find Email/Phone/Both affordance"
);

const report = {
  suite: "gdi-surfe-on-demand-contact-reveal-v1",
  passed: true,
  rootCause:
    "Deep customer sanitize stripped surfeEnrichmentEligible/Channel; reveal route and click handlers were unwired",
  recovered: [
    "gdi-contact-on-demand-reveal-v1",
    "gdi-surfe-contact-provider-v1",
    "gdi-surfe-live-gate-v1",
    "scaling-two-contact-standard-v1",
    "DealalityGdiUi twoContactSlotHtml",
  ],
  bethesdaNamedContacts: named,
  shouldShowFindEmail: findEmail,
  shouldShowFindPhone: findPhone,
  shouldShowFindBoth: findBoth,
  namedWithBothChannelsAlready: noCtaNamed,
  annetteFindPhone: true,
  mockProviderOk: mockHit.ok === true,
  liveBlockedWithoutAuth: mockDenied.error === "LIVE_PROVIDER_NOT_AUTHORIZED",
};

console.log(JSON.stringify(report, null, 2));
assert.ok(existsSync(join(ROOT, "lib/surfe/client.js")));
