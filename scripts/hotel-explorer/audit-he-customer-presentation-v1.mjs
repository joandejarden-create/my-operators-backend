#!/usr/bin/env node
/**
 * HE customer presentation gates (static + optional live/browser).
 *
 *   node scripts/hotel-explorer/audit-he-customer-presentation-v1.mjs
 *   node scripts/hotel-explorer/audit-he-customer-presentation-v1.mjs --browser
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const browserEnabled = process.argv.includes("--browser");
const read = (rel) => fs.readFileSync(path.join(ROOT, rel), "utf8");

function gate(ok) {
  return ok ? "YES" : "NO";
}

function browserGate(valueWhenTested) {
  if (!browserEnabled) return "NOT_TESTED";
  return valueWhenTested;
}

const hdp = read("public/js/hotel-detail-panel.js");
const hdpCss = read("public/css/hotel-detail-panel.css");
const hexCss = read("public/css/hotel-explorer.css");
const hci = read("public/js/hotel-contact-intelligence.js");
const hciCss = read("public/css/hotel-contact-intelligence.css");
const rms = read("public/js/hotel-explorer-rooms-meeting-space.js");
const fmt = read("public/js/he-customer-presentation-format-v1.js");
const about = read("public/js/he-about-this-analysis-v1.js");
const presCss = read("public/css/he-customer-presentation-v1.css");
const radar = read("public/deal-capture-radar-with-ranked-list.html");
const preview = read("public/hotel-explorer-preview.html");

const HE_CUSTOMER_COPY_VALIDATED = gate(
  /he-customer-presentation-format-v1/.test(radar) &&
    /he-about-this-analysis-v1/.test(radar) &&
    /HeAboutThisAnalysis/.test(hdp) &&
    /Market Coverage/.test(hdp) &&
    !/never fabricated/.test(hdp) &&
    !/legacy Hotel Census prohibited/i.test(hdp) &&
    !/HPC persisted/.test(rms)
);

const HE_PROPERTY_FACTS_FORMAT_VALIDATED = gate(
  /text-transform:\s*none/.test(hdpCss) &&
    /text-transform:\s*none/.test(hexCss) &&
    /Brand Parent Company|Parent Company/.test(hdp) &&
    /Guestrooms/.test(hdp) &&
    /Year Built/.test(hdp) &&
    /Last Renovated/.test(hdp) &&
    /Major Renovation/.test(hdp) &&
    /Sq\. Ft\./.test(hdp) &&
    !/label: "Data confidence"/.test(hdp) &&
    /formatLastRenovated|formatSqFt/.test(fmt)
);

const HE_CONTACT_TABLE_LAYOUT_VALIDATED = gate(
  /data-hci-columns="5"/.test(hci) &&
    /Function \/ Relationship/.test(hci) &&
    /hci-dir-contact-stack/.test(hci) &&
    /hci-dir-channel-summary/.test(hci) &&
    /hci-directory-table--5col/.test(hci) &&
    !/label: "Email"/.test(hci) &&
    !/label: "Telephone"/.test(hci) &&
    !/label: "LinkedIn"/.test(hci) &&
    !/Recommended First Contact/.test(hci)
);

const HE_CONTACT_DETAILS_DRAWER_VALIDATED = gate(
  /hci-dir-details-panel/.test(hci) &&
    /hci-dir-details-host/.test(hci) &&
    /Find Contact Details/.test(hci) &&
    /inline:\s*false/.test(hci) &&
    /Property-level authority/.test(hci) &&
    /personBelongsToOrganization|isCorporateMislabel|companyUrl/.test(hci)
);

const HE_SUBMARKET_METHODOLOGY_VALIDATED = gate(
  /he-market-coverage/.test(hdp) &&
    /Verified Hotels/.test(hdp) &&
    /Verified Guestrooms/.test(hdp) &&
    /defaultSubmarketNote/.test(about) &&
    !/Coverage cohorts \(HPC only/.test(hdp)
);

const HE_NO_INTERNAL_TERMINOLOGY_LEAKAGE = gate(
  /scrubInternalTerminology/.test(fmt) &&
    !/HPC persisted/.test(rms) &&
    !/never fabricated/.test(hdp) &&
    !/legacy Hotel Census prohibited/i.test(hdp) &&
    /customerPersistenceLabel/.test(fmt) &&
    /he-customer-presentation-v1\.css/.test(radar) &&
    /he-customer-presentation-v1\.css/.test(preview)
);

let loadingBrowser = { status: "NOT_TESTED", ok: true };
if (browserEnabled) {
  const r = spawnSync(
    "npm",
    ["run", "verify:he-loading-component-browser-v1", "--", "--require-server"],
    { cwd: ROOT, encoding: "utf8", shell: true }
  );
  let report = null;
  try {
    report = JSON.parse(
      fs.readFileSync(
        path.join(ROOT, "reports/hotel-explorer/HE-LOADING-COMPONENT-BROWSER-V1.json"),
        "utf8"
      )
    );
  } catch {
    report = null;
  }
  loadingBrowser = {
    status: report?.status || (r.status === 0 ? "PASS" : "FAIL"),
    ok: r.status === 0 && report?.status === "PASS",
    metrics: report?.metrics?.best || null,
  };
}

const gates = {
  HE_CUSTOMER_COPY_VALIDATED,
  HE_PROPERTY_FACTS_FORMAT_VALIDATED,
  HE_CONTACT_TABLE_LAYOUT_VALIDATED,
  HE_CONTACT_DETAILS_DRAWER_VALIDATED,
  HE_SUBMARKET_METHODOLOGY_VALIDATED,
  HE_NO_INTERNAL_TERMINOLOGY_LEAKAGE,
  HE_LOADING_COMPONENT_CONSISTENT: gate(
    /HeLoadingUx/.test(hci) &&
      /he-async-loading/.test(hci) &&
      !/class="hci-loading"/.test(hci) &&
      (!browserEnabled || loadingBrowser.ok)
  ),
  HE_CONTACT_TABLE_OVERFLOW_BROWSER: browserGate(
    loadingBrowser.ok ? "PASS" : "FAIL"
  ),
  FULL_HE_E2E_VALIDATED: "NO",
};

const required = [
  "HE_CUSTOMER_COPY_VALIDATED",
  "HE_PROPERTY_FACTS_FORMAT_VALIDATED",
  "HE_CONTACT_TABLE_LAYOUT_VALIDATED",
  "HE_CONTACT_DETAILS_DRAWER_VALIDATED",
  "HE_SUBMARKET_METHODOLOGY_VALIDATED",
  "HE_NO_INTERNAL_TERMINOLOGY_LEAKAGE",
  "HE_LOADING_COMPONENT_CONSISTENT",
];

const ok = required.every((k) => gates[k] === "YES") && gates.FULL_HE_E2E_VALIDATED === "NO";

const outDir = path.join(ROOT, "reports/hotel-explorer");
fs.mkdirSync(outDir, { recursive: true });
const outPath = path.join(outDir, "HE-CUSTOMER-PRESENTATION-AUDIT-V1.json");
const report = {
  version: "he-customer-presentation-audit-v1",
  generatedAt: new Date().toISOString(),
  browserEnabled,
  ok,
  gates,
  loadingBrowser,
  sharedModules: {
    format: "public/js/he-customer-presentation-format-v1.js",
    about: "public/js/he-about-this-analysis-v1.js",
    css: "public/css/he-customer-presentation-v1.css",
  },
};
fs.writeFileSync(outPath, JSON.stringify(report, null, 2));
console.log(JSON.stringify(report, null, 2));

assert.equal(gates.FULL_HE_E2E_VALIDATED, "NO");
if (!ok) {
  const failed = required.filter((k) => gates[k] !== "YES");
  assert.fail(`customer presentation gates failed: ${failed.join(", ")}`);
}
