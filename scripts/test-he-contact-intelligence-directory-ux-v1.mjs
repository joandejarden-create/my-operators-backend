/**
 * HE-QL-027 — Contact Intelligence compact directory + Surfe reveal gates.
 * No live Surfe / Airtable writes.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import vm from "node:vm";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

function read(rel) {
  return fs.readFileSync(path.join(ROOT, rel), "utf8");
}

function mustInclude(hay, needle) {
  assert.ok(hay.includes(needle), `expected to include: ${needle}`);
}

let passed = 0;
function run(name, fn) {
  fn();
  passed += 1;
  console.log(`ok - ${name}`);
}

const bethResearch = JSON.parse(
  read("reports/hotel-explorer/phase4-quality/HE-BETH-OWNER-DECISION-MAKER-RESEARCH-STAGED-V1.json")
);

run("bethesda_ranking_katherine_first_mullen_reece_alts", () => {
  assert.equal(bethResearch.recommendation.primaryPersonId, "proposed:katherine_button");
  assert.deepEqual(bethResearch.recommendation.alternativePersonIds, [
    "proposed:jeff_mullen",
    "proposed:patrick_reece",
  ]);
  const k = bethResearch.people.find((p) => p.personId === "proposed:katherine_button");
  assert.ok(k);
  assert.equal(k.propertyResponsibility, "INFERRED");
  assert.ok(k.linkedinUrl.includes("linkedin.com/in/katherine-button"));
});

run("shared_renderer_is_purpose_directory_v3_table", () => {
  const hci = read("public/js/hotel-contact-intelligence.js");
  mustInclude(hci, "Find Contact Details");
  mustInclude(hci, "Hotel Leadership");
  mustInclude(hci, "Ownership & Asset Management");
  mustInclude(hci, "Company contact routes");
  mustInclude(hci, "data-hci-purpose-filter");
  mustInclude(hci, "data-hci-cx=\"v3\"");
  mustInclude(hci, "hci-directory-table");
  mustInclude(hci, "DealalitySortableTable");
  assert.ok(!hci.includes("Recommended First Contact"));
  assert.ok(!hci.includes("Alternative Decision-Makers"));
  assert.ok(!hci.includes("Ownership contact summary"));
  assert.ok(!hci.includes("hci-quality__item"));
});

run("surfe_not_on_get_and_mock_path_exists", () => {
  const api = read("api/contact-intelligence.js");
  mustInclude(api, "buildMockSurfePerson");
  mustInclude(api, "CONTACT_ENRICHMENT_MOCK");
  mustInclude(api, "autoEnrichOnLoad: false");
  mustInclude(api, "persist_forbidden");
  mustInclude(api, "reveal_in_flight");
  mustInclude(api, "PROVIDER_RETURNED_UNREVIEWED");
  mustInclude(api, "No additional professional contact details found");
  mustInclude(api, 'provider: "surfe_mock"');
});

run("renderer_mount_does_not_call_provider", () => {
  const hci = read("public/js/hotel-contact-intelligence.js");
  const mountIdx = hci.indexOf("function mount(");
  const slice = hci.slice(mountIdx, mountIdx + 2000);
  assert.ok(!/enrichPeople|createSurfeContactProvider/.test(slice));
});

run("founder_preview_does_not_rewrite_surfe_reveal", () => {
  const fp = read("public/js/hotel-explorer-founder-preview.js");
  assert.ok(/people/.test(fp) && /reveal/.test(fp) && /enrichment-capability/.test(fp));
  assert.ok(/method !== "GET"/.test(fp));
  assert.ok(/never Surfe reveal/i.test(fp));
});

run("quality_registry_he_ql_027", () => {
  const reg = JSON.parse(
    read("docs/ai-build-system/HOTEL_EXPLORER_FOUNDER_QUALITY_LESSONS_REGISTRY.json")
  );
  assert.ok(
    String(reg.version).startsWith("hotel-explorer-founder-quality-lessons-v1."),
    "registry version present"
  );
  const lesson = reg.lessons.find((l) => l.id === "HE-QL-027");
  assert.ok(lesson);
  mustInclude(lesson.approvedStandard, "Find Email");
  assert.ok(reg.additionalGates.includes("test:he-contact-intelligence-directory-ux-v1"));
});

{
  const src = read("public/js/hotel-contact-intelligence.js");
  const sandbox = {
    console,
    setTimeout,
    clearTimeout,
    encodeURIComponent,
    decodeURIComponent,
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
  sandbox.fetch = (url) => {
    const u = String(url);
    if (u.includes("enrichment-capability")) {
      return Promise.resolve({
        ok: true,
        json: async () => ({ success: true, surfeEnabled: true, mockEnabled: true }),
      });
    }
    return Promise.resolve({ ok: true, json: async () => null });
  };
  sandbox.DealalitySortableTable = {
    renderMarkup(opts) {
      const rows = opts.rows || [];
      return (
        '<div class="dl-table-wrap"><table class="dl-table hci-directory-table"><tbody>' +
        rows
          .map((r) => {
            const p = r._person || {};
            return (
              '<tr class="hci-dir-row" data-person-id="' +
              String(p.personId || "") +
              '"><td>' +
              (r._cells?.name || r.name) +
              "</td><td>" +
              (r._cells?.organization || r.organization) +
              "</td><td>" +
              (r._cells?.actions || "") +
              "</td></tr>" +
              (r._detailsHtml || "")
            );
          })
          .join("") +
        "</tbody></table></div>"
      );
    },
    bind() {},
  };
  vm.runInNewContext(src, sandbox);
  const HCI = sandbox.HotelContactIntelligence;
  const people = bethResearch.people.map((p) => ({
    ...p,
    organizationName: p.organization,
    // Only Mullen employees may carry mullencap domain (regression for Marriott leak).
    organizationDomain: /mullen/i.test(String(p.organization || ""))
      ? "mullencap.com"
      : null,
    propertyResponsibilityLabel:
      p.propertyResponsibility === "INFERRED"
        ? "Inferred from portfolio / role"
        : "Property responsibility not confirmed",
    emailContacts: [],
    phoneContacts: [],
    profileUrls: p.linkedinUrl
      ? [{ type: "LinkedIn", url: p.linkedinUrl, verified: true }]
      : [],
    enrichAction: { label: "Find Contact Details", enabled: true },
  }));
  const vmModel = {
    version: "contact-intelligence-viewmodel-v3",
    hotelId: bethResearch.hotelId,
    hotelName: bethResearch.hotelName,
    researchWorkflow: false,
    ownershipSummary: {
      hotel: { name: bethResearch.hotelName },
      propco: { name: "Marbeth", shortName: "Marbeth" },
      sponsor: { name: "Mullen Real Estate Capital", economicOwnershipCertified: false },
      operator: { name: "Marriott International" },
      brand: { name: "Marriott" },
    },
    recommendation: null,
    recommendedContacts: [],
    otherContacts: people,
    people,
    contactableOwnerOrg: {
      name: "Mullen Real Estate Capital",
      website: "https://mullencap.com/",
      emails: [
        {
          type: "ORG_GENERAL_EMAIL",
          value: "info@mullencap.com",
          classification: "official_org",
        },
      ],
      phones: [],
    },
    bestContactPaths: [{ label: "MREC organization email", value: "info@mullencap.com" }],
    unresolvedItems: bethResearch.unresolved,
  };
  const hostAttrs = Object.create(null);
  const host = {
    innerHTML: "",
    addEventListener() {},
    getAttribute(name) {
      return hostAttrs[name] == null ? null : hostAttrs[name];
    },
    setAttribute(name, value) {
      hostAttrs[name] = String(value);
    },
    querySelector() {
      return null;
    },
    querySelectorAll() {
      return [];
    },
    contains() {
      return true;
    },
  };
  sandbox.fetch = (url) => {
    const u = String(url);
    if (u.includes("enrichment-capability")) {
      return Promise.resolve({
        ok: true,
        json: async () => ({ success: true, surfeEnabled: true, mockEnabled: true }),
      });
    }
    return Promise.resolve({ ok: true, json: async () => vmModel });
  };
  await HCI.mount(host, bethResearch.hotelId);
  const html = host.innerHTML;
  run("vm_render_bethesda_directory_table_without_enums", () => {
    mustInclude(html, "Katherine Button");
    mustInclude(html, "Jeff Mullen");
    mustInclude(html, "Patrick Reece");
    mustInclude(html, "Bethesda Marriott");
    assert.ok(html.includes("data-hci-cx=\"v3\"") || html.includes("hci-directory-table"));
    assert.ok(
      html.includes("Find Contact Details") || html.includes("data-hci-enrich"),
      "Find Contact Details action present"
    );
    mustInclude(html, "linkedin.com/in/katherine-button");
    mustInclude(html, "Company contact routes");
    mustInclude(html, "info@mullencap.com");
    assert.ok(html.includes("hci-purpose-filter") || html.includes("All Contacts"));
    assert.ok(!/\bVERIFIED_PUBLIC\b|\bPROVIDER_RETURNED_UNREVIEWED\b/.test(html));
    assert.ok(!html.includes("Recommended First Contact"));
    assert.ok(!html.includes("Inferred · not property-confirmed"));
    assert.ok((html.match(/data-hci-enrich/g) || []).length >= 3);
    assert.ok(html.includes("hci-section--org-channels"));
    assert.ok((html.match(/hci-dir-row/g) || []).length >= 3, "directory table rows present");
    assert.ok(
      !/Direct Business Email[\s\S]{0,80}info@mullencap\.com/.test(html),
      "info@ must not be labeled as personal business email"
    );
  });
}

console.log(`\n${passed} checks passed (he-contact-intelligence-directory-ux-v1)`);
