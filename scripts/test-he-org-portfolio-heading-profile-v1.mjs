#!/usr/bin/env node
/**
 * Organization & Portfolio — heading inset + Key People PROFILE LinkedIn gates.
 * Wired into Invoke-HotelExplorerFactory.ps1 ConsistencyModes.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import vm from "node:vm";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const read = (rel) => fs.readFileSync(path.join(ROOT, rel), "utf8");

const hexCss = read("public/css/hotel-explorer.css");
assert.match(
  hexCss,
  /\.hex-root \.hex-owner-portfolio > \.hiw-section > \.hiw-section__h,\s*\n\.hex-root \.hex-owner-portfolio > \.hiw-section > \.hiw-section__head \{[\s\S]*?padding-left:\s*16px;[\s\S]*?padding-right:\s*16px;/
);

const ws = read("public/js/hotel-intelligence-workspace.js");
assert.match(ws, /function personLinkedInFromRaw\(/);
assert.match(ws, /function enrichOrgKeyPeopleRowProfile\(/);
assert.match(ws, /Seed rows often lack LinkedIn/);
assert.match(ws, /PERSON_LINKEDIN channel value/);
assert.match(ws, /linkedin\\\.com\\\/in\\\//);

const start = ws.indexOf("function personProfessionalProfile(");
const mid = ws.indexOf("function collectOrgKeyPeople(");
const mapStart = ws.indexOf("function renderPeopleProfileCell(");
const mapEnd = ws.indexOf("function renderPeopleGrouped(");
assert.ok(start > 0 && mid > start, "normalize helper slice bounds");
assert.ok(mapStart > mid && mapEnd > mapStart, "map/render helper slice bounds");
const helpers =
  ws.slice(start, mid) +
  "\nfunction esc(s){ return String(s == null ? '' : s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/\"/g,'&quot;'); }\n" +
  ws.slice(mapStart, mapEnd);

const sandbox = { console };
vm.createContext(sandbox);
vm.runInContext(
  helpers +
    "\nthis.personProfessionalProfile = personProfessionalProfile;" +
    "\nthis.personLinkedInFromRaw = personLinkedInFromRaw;" +
    "\nthis.normalizeOrgKeyPeopleRows = normalizeOrgKeyPeopleRows;" +
    "\nthis.mapContactIntelligencePeople = mapContactIntelligencePeople;" +
    "\nthis.renderPeopleProfileCell = renderPeopleProfileCell;",
  sandbox
);

const jeffCi = {
  display_name: "Jeff Mullen",
  title: "President and CEO",
  organization_name: "Mullen Real Estate Capital",
  property_relevance: "OWNER_ORG",
  channels: [
    {
      kind: "PERSON_LINKEDIN",
      value: "https://www.linkedin.com/in/jeffmmullen/",
      verified: false,
    },
  ],
};
assert.equal(
  sandbox.personLinkedInFromRaw(jeffCi),
  "https://www.linkedin.com/in/jeffmmullen/"
);

const mapped = sandbox.mapContactIntelligencePeople([jeffCi]);
assert.equal(mapped.length, 1);
assert.equal(mapped[0].linkedinUrl, "https://www.linkedin.com/in/jeffmmullen/");
assert.equal(mapped[0].professional_profile_verified, true);

const seed = [
  {
    name: "Jeff Mullen",
    title: "President and CEO",
    organization: "Mullen Real Estate Capital",
  },
];
const merged = sandbox.normalizeOrgKeyPeopleRows(seed.concat(mapped));
assert.equal(merged.length, 1);
assert.equal(merged[0].linkedinUrl, "https://www.linkedin.com/in/jeffmmullen/");
const profile = sandbox.personProfessionalProfile(merged[0]);
assert.equal(profile.verified, true);
assert.match(profile.url, /linkedin\.com\/in\/jeffmmullen/i);

const cell = sandbox.renderPeopleProfileCell(merged[0]);
assert.match(cell, /href="https:\/\/www\.linkedin\.com\/in\/jeffmmullen\/"/);
assert.match(cell, /target="_blank"/);
assert.doesNotMatch(cell, /hiw-profile-empty/);

const noProfile = sandbox.normalizeOrgKeyPeopleRows([
  { name: "No Link Person", title: "Analyst", organization: "Example" },
]);
assert.equal(sandbox.personProfessionalProfile(noProfile[0]).verified, false);
assert.match(sandbox.renderPeopleProfileCell(noProfile[0]), /hiw-profile-empty/);

const companyOnly = sandbox.mapContactIntelligencePeople([
  {
    display_name: "Corp Only",
    channels: [{ kind: "ORG_LINKEDIN", value: "https://www.linkedin.com/company/mullen/" }],
  },
]);
assert.equal(companyOnly[0].linkedinUrl, null);

const katherine = {
  display_name: "Katherine Button",
  channels: [
    { kind: "PERSON_LINKEDIN", value: "https://www.linkedin.com/in/katherine-button/" },
  ],
};
const five = sandbox.normalizeOrgKeyPeopleRows(
  [
    { name: "Jeff Mullen" },
    { name: "Katherine Button" },
    { name: "Patrick Reece" },
    { name: "Meinrad Lang III" },
    { name: "Jenny Pyne" },
  ].concat(
    sandbox.mapContactIntelligencePeople([
      jeffCi,
      katherine,
      {
        display_name: "Patrick Reece",
        channels: [
          {
            kind: "PERSON_LINKEDIN",
            value: "https://www.linkedin.com/in/patrick-reece-375b8937/",
          },
        ],
      },
      {
        display_name: "Meinrad Lang III",
        channels: [
          { kind: "PERSON_LINKEDIN", value: "https://www.linkedin.com/in/meinradlang/" },
        ],
      },
      {
        display_name: "Jenny Pyne",
        channels: [
          {
            kind: "PERSON_LINKEDIN",
            value: "https://www.linkedin.com/in/jenny-pyne-9b50976/",
          },
        ],
      },
    ])
  )
);
assert.equal(five.length, 5);
five.forEach((row) => {
  const p = sandbox.personProfessionalProfile(row);
  assert.equal(p.verified, true, row.name + " should have verified LinkedIn");
  assert.match(p.url, /linkedin\.com\/in\//i);
});

const ps1 = read("scripts/hotel-explorer/Invoke-HotelExplorerFactory.ps1");
assert.match(ps1, /test:he-org-portfolio-heading-profile-v1/);

const pkg = read("package.json");
assert.match(pkg, /"test:he-org-portfolio-heading-profile-v1"/);

console.log("ok - he-org-portfolio-heading-profile-v1");
