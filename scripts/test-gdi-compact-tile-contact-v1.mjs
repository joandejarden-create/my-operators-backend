#!/usr/bin/env node
/**
 * Focused regression: one compact contact block per opportunity tile.
 * Presentation-only — does not change eligibility or Airtable writes.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import vm from "node:vm";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const src = fs.readFileSync(
  path.join(ROOT, "public/js/group-demand-intelligence/dealality-gdi-ui.js"),
  "utf8"
);
const sandbox = { window: {}, globalThis: {}, console, URL, URLSearchParams };
sandbox.window = sandbox;
sandbox.globalThis = sandbox;
vm.runInNewContext(src, sandbox, { filename: "dealality-gdi-ui.js" });
const UI = sandbox.DealalityGdiUi;
assert.ok(UI?.selectCompactTileContact, "selectCompactTileContact exported");

function block(it, linked) {
  return UI.selectCompactTileContact(it, linked || it);
}

// Named person with personal email
{
  const b = block({
    organizationName: "Aero Club of Washington",
    primaryContact: {
      name: "Stephanie",
      role: "Executive Director",
      email: "stephanie@aeroclub.org",
      emailOwnership: "person_specific",
    },
  });
  assert.equal(b.kind, "person");
  assert.equal(b.name, "Stephanie");
  assert.equal(b.role, "Executive Director");
  assert.equal(b.channelKind, "email");
  assert.equal(b.channelText, "stephanie@aeroclub.org");
}

// Referral: person name + short Referral contact label + one channel
{
  const b = block({
    organizationName: "AHIMA",
    primaryContact: {
      name: "Sabrina Bracken",
      role: "SPARGO sponsorship / exhibition contact; not identified as the event buyer",
      email: "sabrina.bracken@spargoinc.com",
      emailOwnership: "person_specific",
    },
    contactResponsibilityClass: "REFERRAL_ENTRY",
  });
  assert.equal(b.kind, "referral");
  assert.equal(b.name, "Sabrina Bracken");
  assert.equal(b.role, "Referral contact");
  assert.equal(b.channelText, "sabrina.bracken@spargoinc.com");
  assert.doesNotMatch(b.role, /SPARGO|not identified/i);
}

// Organization email fallback
{
  const b = block({
    organizationName: "ACEC Metropolitan Washington",
    contactEmail: "info@acecmw.org",
    organizationContactEmail: "info@acecmw.org",
  });
  assert.equal(b.kind, "organization");
  assert.match(b.name, /ACEC|Organization contact/i);
  assert.equal(b.channelKind, "email");
  assert.equal(b.channelText, "info@acecmw.org");
}

// Named person without personal channel → org route on tile
{
  const b = block({
    organizationName: "Quorum",
    primaryContact: {
      name: "Amanda French",
      role: "VP, People",
      email: "TalentandCulture@quorum.us",
      emailOwnership: "person_specific",
    },
    organizationContactEmail: "TalentandCulture@quorum.us",
    companyPhone: "+1 202-972-9980",
    contactChannelSeparation: {
      suggestedPerson: "Amanda French",
      personSpecificEmail: "TalentandCulture@quorum.us",
      organizationRoute: "TalentandCulture@quorum.us",
      companyPhone: "+1 202-972-9980",
    },
  });
  assert.equal(b.kind, "organization");
  assert.notEqual(b.name, "Amanda French");
  assert.equal(b.channelKind, "email");
  assert.equal(b.channelText, "TalentandCulture@quorum.us");
}

// Org-named venue with email misfiled as person_specific → prefer email over page URL
{
  const b = block({
    organizationName: "Port Pavilion at Broadway Pier",
    primaryContact: {
      name: "Port Pavilion at Broadway Pier",
      role: "Port Pavilion events",
      email: "portpavilion@portofsandiego.org",
      emailOwnership: "person_specific",
    },
    publicContactPath:
      "https://www.portofsandiego.org/waterfront-development/broadway-pier",
    contactChannelSeparation: {
      suggestedPerson: "Port Pavilion at Broadway Pier",
      personSpecificEmail: "portpavilion@portofsandiego.org",
      organizationRoute:
        "https://www.portofsandiego.org/waterfront-development/broadway-pier",
    },
  });
  assert.equal(b.kind, "organization");
  assert.equal(b.channelKind, "email");
  assert.equal(b.channelText, "portpavilion@portofsandiego.org");
}

// Contact-form-only fallback uses readable link text
{
  const b = block({
    organizationName: "Example Venue",
    publicContactPath: "https://example.com/contact-us/",
  });
  assert.equal(b.kind, "organization");
  assert.equal(b.channelKind, "form");
  assert.equal(b.channelText, "Contact form");
  assert.match(b.channelHref, /^https:\/\/example\.com\/contact-us\/$/);
}

// HTML: one contact block, View Details present, no outreach instructions
{
  const html = UI.opportunityTileHtml(
    {
      opportunityId: "gdi_test_1",
      title: "Test Opportunity",
      summaryWhat: "A short summary for the tile.",
    },
    {
      id: "gdi_test_1",
      title: "Test Opportunity",
      displayTitle: "Test Opportunity",
      summaryWhat: "A short summary for the tile.",
      organizationName: "Test Org",
      primaryContact: {
        name: "Jordan Lee",
        role: "Director of Meetings",
        email: "jordan@test.org",
        emailOwnership: "person_specific",
      },
      priority: "WATCHLIST",
    },
    {}
  );
  assert.match(html, /brand-card__contact-name/);
  assert.match(html, /jordan@test\.org/);
  assert.match(html, /View Details/);
  assert.doesNotMatch(html, /Ask to be directed|Outreach|Office address/i);
  assert.equal((html.match(/brand-card__contact-name/g) || []).length, 1);
}

// Property bar: primary row + history footer contract
{
  assert.match(src, /gdi-property-bar__primary/);
  assert.match(src, /gdi-property-bar__history/);
  assert.match(src, /Last research:/);
  assert.match(src, /Last published:/);
  const bar = UI.propertyBarHtml({
    mode: "auth",
    hotel: { hotelName: "Test Hotel", city: "Washington", state: "DC" },
    hotelId: "recTEST",
    hotels: [{ hotelId: "recTEST", hotelName: "Test Hotel", locationLine: "Washington, DC" }],
    lastResearch: "Oct 15, 2026, 12:00 PM EDT",
    runStatusLabel: "Completed",
    lastPublishedUpdate: "Oct 15, 2026, 9:24 AM EDT",
    actionHtml:
      '<button type="button" class="aiv-btn-apply" id="gdiRunBtn">Run Research</button>' +
      '<button type="button" class="btn-clear" id="gdiResetViewBtn">Reset View</button>',
  });
  assert.match(bar, /gdi-property-bar__primary/);
  assert.match(bar, /gdiRunBtn/);
  assert.match(bar, /gdiResetViewBtn/);
  assert.match(bar, /Last research:/);
  assert.match(bar, /Run:/);
  assert.match(bar, /Last published:/);
}

// Hotel-fit tiles stay static (no Details expansion)
assert.doesNotMatch(src, /gdi-score-expand-hint/);
assert.doesNotMatch(src, /gdi-score-cell--expandable/);

console.log("test-gdi-compact-tile-contact-v1: PASS");
