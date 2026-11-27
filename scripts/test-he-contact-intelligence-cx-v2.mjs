/**
 * HE Contact Intelligence CX V2 — purpose sections, uniform cards, customer language.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import vm from "node:vm";
import {
  assignPurposeSections,
  buildPurposeGroupedContactSections,
  CONTACT_PURPOSE_SECTION,
} from "../lib/hotel-intelligence/contact-intelligence/contact-purpose-sections-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
function read(rel) {
  return fs.readFileSync(path.join(ROOT, rel), "utf8");
}

let passed = 0;
function run(name, fn) {
  fn();
  passed += 1;
  console.log(`ok - ${name}`);
}

run("purpose_assignment_multi_section_without_canonical_dup", () => {
  const gm = {
    name: "Meinrad Lang III",
    title: "General Manager",
    organization: "Marriott International",
    whyRelevant: "Property Leadership — General Manager",
    propertyResponsibility: "CONFIRMED",
    contactClass: "HOTEL_LEADERSHIP",
  };
  const sections = assignPurposeSections(gm);
  assert.ok(sections.includes(CONTACT_PURPOSE_SECTION.HOTEL_LEADERSHIP));
  assert.ok(sections.includes(CONTACT_PURPOSE_SECTION.MANAGEMENT_COMPANY));
});

run("ownership_am_not_forced_into_hotel_leadership", () => {
  const am = {
    name: "Katherine Button",
    title: "VP, Asset Management and Investments",
    organization: "Mullen Real Estate Capital",
    whyRelevant: "Asset Management",
    propertyResponsibility: "INFERRED",
    contactClass: "RELEVANT_COMMERCIAL_ENTRY_POINT",
  };
  const sections = assignPurposeSections(am);
  assert.ok(sections.includes(CONTACT_PURPOSE_SECTION.OWNERSHIP_ASSET));
  assert.ok(!sections.includes(CONTACT_PURPOSE_SECTION.HOTEL_LEADERSHIP));
});

run("build_purpose_groups_no_global_recommendation", () => {
  const people = [
    {
      personId: "1",
      name: "Jeff Mullen",
      title: "President and CEO",
      organization: "Mullen Real Estate Capital",
      propertyResponsibility: "INFERRED",
      contactClass: "OWNER_DECISION_AUTHORITY",
      whyRelevant: "Ownership / Decision Authority",
    },
    {
      personId: "2",
      name: "Meinrad Lang III",
      title: "General Manager",
      organization: "Marriott International",
      propertyResponsibility: "CONFIRMED",
      contactClass: "HOTEL_LEADERSHIP",
      whyRelevant: "Property Leadership — General Manager",
    },
  ];
  const built = buildPurposeGroupedContactSections(people, { hotelId: "recLuxvwwxID7U2B8" });
  assert.equal(built.recommendation, null);
  assert.equal(built.recommendedContacts.length, 0);
  const hotelLead = built.sections.find((s) => s.id === CONTACT_PURPOSE_SECTION.HOTEL_LEADERSHIP);
  assert.ok(hotelLead.people.some((p) => p.name === "Meinrad Lang III"));
  const ownership = built.sections.find((s) => s.id === CONTACT_PURPOSE_SECTION.OWNERSHIP_ASSET);
  assert.ok(ownership.people.some((p) => p.name === "Jeff Mullen"));
});

run("renderer_source_customer_language_and_uniform_cards", () => {
  const hci = read("public/js/hotel-contact-intelligence.js");
  const css = read("public/css/hotel-contact-intelligence.css");
  assert.ok(hci.includes("Find Contact Details"));
  assert.ok(hci.includes("Hotel Leadership"));
  assert.ok(hci.includes("Sales & Commercial"));
  assert.ok(hci.includes("Ownership & Asset Management"));
  assert.ok(hci.includes("data-hci-purpose-filter"));
  assert.ok(hci.includes("Direct Business Email"));
  assert.ok(hci.includes("Direct Business Phone"));
  assert.ok(hci.includes("Company website") || hci.includes("companyWebsite"));
  assert.ok(hci.includes("Company contact routes"));
  assert.ok(!hci.includes("Recommended First Contact"));
  assert.ok(!hci.includes("Alternative Decision-Makers"));
  assert.ok(!hci.includes("Public research first"));
  assert.ok(!hci.includes("Surfe on demand"));
  assert.ok(!hci.includes("Inferred · not property-confirmed"));
  assert.ok(!hci.includes("Organizational Contact Route"));
  assert.ok(css.includes("hci-person--featured") && css.includes("width: 100%"));
  assert.ok(css.includes("hci-purpose-filter"));
});

run("screenshot_and_api_evidence_artifacts_present", () => {
  const shot = path.join(ROOT, "reports/hotel-explorer/HE-CONTACT-CX-V2-BETHESDA-SCREENSHOT.png");
  const api = path.join(ROOT, "reports/hotel-explorer/HE-CONTACT-CX-V2-BETHESDA-API-EVIDENCE.json");
  assert.ok(fs.existsSync(shot), "Bethesda CX V2 screenshot required");
  assert.ok(fs.statSync(shot).size > 1000, "screenshot must be non-trivial");
  assert.ok(fs.existsSync(api), "Bethesda CX V2 API evidence required");
  const evidence = JSON.parse(fs.readFileSync(api, "utf8"));
  assert.equal(evidence.hasRecommendedFirstContact, false);
  assert.ok((evidence.purposeSectionLabels || []).includes("Ownership & Asset Management"));
  assert.ok((evidence.purposeSectionLabels || []).includes("Hotel Leadership"));
});

{
  const src = read("public/js/hotel-contact-intelligence.js");
  const sandbox = {
    console,
    setTimeout,
    clearTimeout,
    encodeURIComponent,
    Promise,
    JSON,
    Math,
    Object,
    Array,
    String,
    Boolean,
    Number,
    Error,
    Date,
  };
  sandbox.window = sandbox;
  sandbox.globalThis = sandbox;
  sandbox.fetch = () =>
    Promise.resolve({
      ok: true,
      json: async () => ({ success: true, surfeEnabled: true, mockEnabled: true }),
    });
  vm.runInNewContext(src, sandbox);
  const HCI = sandbox.HotelContactIntelligence;
  const people = [
    {
      personId: "gm",
      name: "Meinrad Lang III",
      title: "General Manager",
      organization: "Marriott International",
      whyRelevant: "Property Leadership — General Manager",
      propertyResponsibility: "CONFIRMED",
      contactClass: "HOTEL_LEADERSHIP",
      emailContacts: [],
      phoneContacts: [],
      linkedinUrl: "https://www.linkedin.com/in/meinradlang",
      enrichAction: { label: "Find Contact Details", enabled: true },
    },
    {
      personId: "kb",
      name: "Katherine Button",
      title: "VP, Asset Management and Investments",
      organization: "Mullen Real Estate Capital",
      whyRelevant: "Asset Management",
      propertyResponsibility: "INFERRED",
      contactClass: "RELEVANT_COMMERCIAL_ENTRY_POINT",
      emailContacts: [],
      phoneContacts: [],
      linkedinUrl: "https://www.linkedin.com/in/katherine-button",
      enrichAction: { label: "Find Contact Details", enabled: true },
    },
    {
      personId: "jm",
      name: "Jeff Mullen",
      title: "President and CEO",
      organization: "Mullen Real Estate Capital",
      whyRelevant: "Ownership / Decision Authority",
      propertyResponsibility: "INFERRED",
      contactClass: "OWNER_DECISION_AUTHORITY",
      emailContacts: [],
      phoneContacts: [],
      linkedinUrl: "https://www.linkedin.com/in/jeffmmullen",
      enrichAction: { label: "Find Contact Details", enabled: true },
    },
  ];
  const vmModel = {
    version: "contact-intelligence-viewmodel-v3",
    hotelId: "recLuxvwwxID7U2B8",
    hotelName: "Bethesda Marriott",
    ownershipSummary: {
      hotel: { name: "Bethesda Marriott" },
      sponsor: {
        name: "Mullen Real Estate Capital",
        economicOwnershipCertified: false,
      },
      operator: { name: "Marriott International" },
    },
    people,
    purposeSections: buildPurposeGroupedContactSections(people).sections,
    recommendation: null,
    contactableOwnerOrg: {
      name: "Mullen Real Estate Capital",
      website: "https://mullencap.com/",
      emails: [{ value: "info@mullencap.com", type: "ORG_GENERAL_EMAIL" }],
      phones: [],
    },
    unresolvedItems: [],
  };
  const html = HCI.renderViewModel(vmModel);
  run("vm_render_purpose_sections_and_hotel_name", () => {
    const subtitle = (html.match(/<p class="hci-subtitle">([^<]*)<\/p>/) || [])[1] || "";
    assert.equal(subtitle.trim(), "Bethesda Marriott");
    assert.ok(html.includes("Hotel Leadership"));
    assert.ok(/Ownership &amp; Asset Management|Ownership & Asset Management/.test(html));
    assert.ok(html.includes("Meinrad Lang III"));
    assert.ok(html.includes("Katherine Button"));
    assert.ok(html.includes("Jeff Mullen"));
    assert.ok(html.includes("Ownership organization (pending certification)"));
    assert.ok(!html.includes("Recommended First Contact"));
    assert.ok(!html.includes("Owner: Mullen"));
    assert.ok(!html.includes("Inferred · not property-confirmed"));
    assert.ok(html.includes("Hotel responsibility not confirmed"));
    assert.ok(html.includes("Company contact routes") || html.includes("Company website"));
    const articles = html.match(/<article class="hci-person"/g) || [];
    assert.ok(articles.length >= 3, "uniform hci-person cards only");
    assert.ok(!html.includes("hci-person--featured"));
    assert.ok(!html.includes("hci-person--compact"));
  });
}

console.log(`\n${passed} checks passed (he-contact-intelligence-cx-v2)`);
