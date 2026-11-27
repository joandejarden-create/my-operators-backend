#!/usr/bin/env node
/**
 * Permanent regression: GDI List View contact columns + actions refinements.
 *   npm run test:gdi-list-view-contact-actions-v1
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import vm from "node:vm";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const CACHE = "gdi-contact-hydration-linkedin-v1";

let failed = 0;
function check(name, fn) {
  try {
    fn();
    console.log(`PASS ${name}`);
  } catch (err) {
    failed += 1;
    console.error(`FAIL ${name}: ${err && err.message ? err.message : err}`);
  }
}

function read(rel) {
  return fs.readFileSync(path.join(ROOT, rel), "utf8");
}

function loadModules() {
  const sandbox = {
    console,
    sessionStorage: {
      _m: Object.create(null),
      getItem(k) {
        return Object.prototype.hasOwnProperty.call(this._m, k) ? this._m[k] : null;
      },
      setItem(k, v) {
        this._m[k] = String(v);
      },
      removeItem(k) {
        delete this._m[k];
      },
    },
  };
  sandbox.window = sandbox;
  sandbox.globalThis = sandbox;
  sandbox.document = {
    createElement() {
      return {
        style: {},
        setAttribute() {},
        addEventListener() {},
        querySelector() {
          return null;
        },
      };
    },
    body: { appendChild() {} },
    addEventListener() {},
  };
  vm.runInNewContext(read("public/js/group-demand-intelligence/dealality-gdi-ui.js"), sandbox);
  vm.runInNewContext(
    read("public/js/group-demand-intelligence/gdi-commercial-list-table-v1.js"),
    sandbox,
  );
  return sandbox;
}

check("cache_bust_and_columns_contract", () => {
  const auth = read("public/group-demand-intelligence.html");
  assert.match(auth, new RegExp(`gdi-commercial-list-table-v1\\.js\\?v=${CACHE}`));
  assert.match(auth, /group-demand-intelligence\.css\?v=/);
  const { DealalityGdiCommercialListTable: Table } = loadModules();
  const ids = Table.DEFAULT_COLUMNS.map((c) => c.id);
  assert.equal(
    ids.join(","),
    [
      "opportunity",
      "priority",
      "demandType",
      "demandWindow",
      "salesAction",
      "whoToContact",
      "contactDetails",
      "actions",
    ].join(","),
  );
  assert.ok(Table.OPTIONAL_COLUMNS.some((c) => c.id === "contactReadiness"));
  assert.ok(Table.DEFAULT_COLUMNS.every((c) => !/^[A-Z]{3,}$/.test(c.label) || c.label === "Actions"));
});

check("who_to_contact_title_rules", () => {
  const { DealalityGdiCommercialListTable: Table } = loadModules();
  assert.equal(Table.resolveProfessionalTitle("events").title, "Title Not Verified");
  assert.equal(
    Table.resolveProfessionalTitle("gala / membership events").title,
    "Title Not Verified",
  );
  assert.equal(
    Table.resolveProfessionalTitle(
      "Published meeting-logistics and membership contact; exact job title not stated",
    ).title,
    "Title Not Verified",
  );
  assert.match(
    Table.resolveProfessionalTitle("Corporate Marketing Specialist").title,
    /Corporate Marketing Specialist/i,
  );
  assert.match(
    Table.resolveProfessionalTitle(
      "Manager, Meeting Operations, per professional profile; official directory lists Sr. Lead",
    ).title,
    /Manager, Meeting Operations/i,
  );
  const who = Table.whoContactMeta(
    {
      id: "1",
      primaryContact: { name: "Kathryn Newstead", role: "events", title: "events" },
    },
    null,
  );
  assert.equal(who.name, "Kathryn Newstead");
  assert.equal(who.role, "Title Not Verified");
});

check("contact_details_email_phone_and_switchboard_guard", () => {
  const { DealalityGdiCommercialListTable: Table } = loadModules();
  const both = Table.contactDetailsMeta({
    id: "a",
    primaryContact: {
      name: "Ann Marie Levra",
      title: "Senior Director Education & Meetings Strategy",
      email: "alevra@aans.org",
      phone: "847.378.0548",
      phoneTypeLabel: "direct_phone",
    },
  });
  assert.equal(both.email, "alevra@aans.org");
  assert.equal(both.phone, "847.378.0548");
  assert.match(both.emailHref, /^mailto:/);
  assert.match(both.telHref, /^tel:/);

  const switchboard = Table.contactDetailsMeta({
    id: "b",
    primaryContact: {
      name: "Madeline Foresman",
      title: "Director - Events & Programs",
      email: "asnehq@navalengineers.org",
      phone: "+7038366729",
      phoneTypeLabel: "general_switchboard",
    },
  });
  assert.equal(switchboard.email, "asnehq@navalengineers.org");
  assert.equal(switchboard.phone, "");
  assert.equal(switchboard.phoneDisplay, "Phone Not Available");

  // Compact-tile contact may omit phoneType; still honor primaryContact classification.
  const deptViaPrimary = Table.contactDetailsMeta(
    {
      id: "b2",
      primaryContact: {
        name: "Kristine Coan",
        title: "Published meeting-logistics and membership contact; exact job title not stated",
        email: "kcoan@ascrs.org",
        phone: "(703) 591-2220",
        phoneTypeLabel: "department_phone",
      },
    },
    {
      selectCompactTileContact: function () {
        return {
          name: "Kristine Coan",
          email: "kcoan@ascrs.org",
          phone: "(703) 591-2220",
        };
      },
    },
  );
  assert.equal(deptViaPrimary.phone, "");
  assert.equal(deptViaPrimary.phoneDisplay, "Phone Not Available");

  const none = Table.contactDetailsMeta({
    id: "c",
    primaryContact: { name: "David Aguirre", title: "Project Manager" },
  });
  assert.equal(none.emailDisplay, "Email Not Available");
  assert.equal(none.phoneDisplay, "Phone Not Available");

  const linkedinOnly = Table.contactDetailsMeta({
    id: "d",
    primaryContact: {
      name: "David Aguirre",
      title: "Project Manager",
      sourceUrl: "https://linkedin.com/in/david-aguirre-27020411",
      linkedinUrl: "https://linkedin.com/in/david-aguirre-27020411",
    },
  });
  assert.equal(linkedinOnly.emailDisplay, "Email Not Available");
  assert.equal(linkedinOnly.phoneDisplay, "Phone Not Available");
  assert.match(linkedinOnly.linkedinUrl, /linkedin\.com\/in\/david-aguirre/);
  const linkedinHtml = Table.contactDetailsCellHtml({
    id: "d",
    primaryContact: {
      name: "David Aguirre",
      title: "Project Manager",
      sourceUrl: "https://linkedin.com/in/david-aguirre-27020411",
    },
  });
  assert.match(linkedinHtml, /gdi-list-linkedin/);
  assert.match(linkedinHtml, /linkedin\.com\/in\/david-aguirre/);
});

check("sales_action_prefers_short_pill_not_outreach_prose", () => {
  const { DealalityGdiCommercialListTable: Table } = loadModules();
  const sa = Table.salesActionMeta({
    id: "x",
    tileBadgePlan: {
      salesActionKey: "PURSUE_CATERING",
      salesActionPillLabel: "Pursue Catering",
    },
    recommendedNextStep:
      "Email Kathryn Newstead (knewstead@downtownsandiego.org) to introduce Wyndham",
  });
  assert.equal(sa.label, "Pursue Catering");
});

check("table_html_contact_details_actions_and_no_readiness_pill_default", () => {
  const { DealalityGdiCommercialListTable: Table } = loadModules();
  const html = Table.tableHtml(
    [
      {
        id: "opp-1",
        title: "Alpha",
        priority: "MEDIUM_PRIORITY",
        priorityFilterKey: "MEDIUM_PRIORITY",
        segmentLabel: "association meeting",
        salesActionPillLabel: "build account",
        primaryContact: {
          name: "Brittany Jolly",
          title: "Conference Team Lead",
          email: "brittanyjolly@esri.nl",
          emailOwnership: "person_specific",
          phoneTypeLabel: "none",
          buyerVsEntry: "DIRECT_BUYER",
        },
        contactActionabilityTier: "SALES_READY",
        contactActionabilityLabel: "Sales-ready contact",
      },
    ],
    { sortKey: "priority", sortDir: 1 },
  );
  assert.match(html, />Contact Details</);
  assert.match(html, /Who to Contact/);
  assert.doesNotMatch(html, />Contact Readiness</);
  assert.match(html, /mailto:brittanyjolly@esri\.nl/);
  assert.match(html, /Phone Not Available/);
  assert.match(html, /Conference Team Lead/);
  assert.match(html, /gdi-list-more-btn/);
  assert.match(html, /action-icon--more/);
  assert.match(html, /gdi-list-view-btn/);
  assert.match(html, /gdi-list-plain-text/);
  assert.match(html, /Build Account/);
  assert.match(html, /association meeting/i);
  assert.doesNotMatch(html, /border-radius:\s*999px/);
});

check("optional_contact_readiness_still_available", () => {
  const { DealalityGdiCommercialListTable: Table } = loadModules();
  const html = Table.tableHtml(
    [
      {
        id: "opp-2",
        title: "Beta",
        priority: "WATCHLIST",
        contactActionabilityTier: "CONTACT_RESEARCH_REQUIRED",
        contactActionabilityLabel: "Contact research needed",
        primaryContact: { name: "Sam", title: "Procurement Lead" },
      },
    ],
    {
      sortKey: "title",
      sortDir: 1,
      optionalColumns: { contactReadiness: true },
    },
  );
  assert.match(html, /Contact Readiness/);
  assert.match(html, /Contact Research Needed/);
  assert.match(html, /gdi-list-plain-text/);
});

check("css_proper_case_headers_and_menu", () => {
  const css = read("public/css/group-demand-intelligence.css");
  assert.match(
    css,
    /\.gdi-page \.gdi-commercial-table th \{[\s\S]*?text-transform:\s*none;/,
  );
  assert.match(css, /\.gdi-list-more-menu/);
  assert.match(css, /\.gdi-list-contact-details/);
  assert.match(css, /\.gdi-list-plain-text/);
});

check("surfe_not_auto_called_from_list_module", () => {
  const src = read("public/js/group-demand-intelligence/gdi-commercial-list-table-v1.js");
  assert.doesNotMatch(src, /surfe|Surfe|findymail|Findymail/i);
  assert.doesNotMatch(src, /fetch\(.*enrich/i);
});

if (failed) {
  console.error(`\n${failed} failure(s)`);
  process.exit(1);
}
console.log("\nOK test:gdi-list-view-contact-actions-v1");
