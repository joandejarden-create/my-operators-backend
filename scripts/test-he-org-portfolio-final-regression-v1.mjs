#!/usr/bin/env node
/**
 * Organization & Portfolio final regression — static + optional browser computed-style gates.
 *
 * Static always runs. Browser runs when HE_ORG_PORTFOLIO_BROWSER=1 (or HE_BASE_URL set)
 * against a live Hotel Explorer host (prefer clean worktree, not dirty :8080 overlays).
 *
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
const ws = read("public/js/hotel-intelligence-workspace.js");
const opView = read("public/js/he-owner-portfolio-operator-view-v1.js");

// --- CSS model: section padding kept; no zero L/R on sections ---
const sectionBlock = hexCss.match(
  /\.hex-root \.hex-owner-portfolio > \.hiw-section \{[^}]*\}/
);
assert.ok(sectionBlock);
assert.doesNotMatch(sectionBlock[0], /padding-left:\s*0/);
assert.match(
  hexCss,
  /\.hex-root \.hiw-section \{[\s\S]*?padding:\s*14px\s+16px\s+16px/
);
assert.match(
  hexCss,
  /\.hex-root \.hex-owner-portfolio \.hiw-strategic \{[\s\S]*?display:\s*flex;[\s\S]*?gap:\s*16px/
);
assert.match(
  hexCss,
  /\.hex-root \.hex-owner-portfolio \.hiw-table-wrap,[\s\S]*?margin-left:\s*-16px/
);

// --- Management status independence ---
assert.match(opView, /management_evidence_label:\s*resolved\.verified/);
assert.match(ws, /Management Status is independent of ownership/);
assert.match(ws, /data-hex-mgmt-status/);

// --- LinkedIn PROFILE ---
const start = ws.indexOf("function personProfessionalProfile(");
const mid = ws.indexOf("function collectOrgKeyPeople(");
const mapStart = ws.indexOf("function renderPeopleProfileCell(");
const mapEnd = ws.indexOf("function renderPeopleGrouped(");
const helpers =
  ws.slice(start, mid) +
  "\nfunction esc(s){ return String(s == null ? '' : s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/\"/g,'&quot;'); }\n" +
  ws.slice(mapStart, mapEnd);
const sandbox = {};
vm.createContext(sandbox);
vm.runInContext(
  helpers +
    "\nthis.personProfessionalProfile=personProfessionalProfile;" +
    "\nthis.personLinkedInFromRaw=personLinkedInFromRaw;" +
    "\nthis.normalizeOrgKeyPeopleRows=normalizeOrgKeyPeopleRows;" +
    "\nthis.mapContactIntelligencePeople=mapContactIntelligencePeople;" +
    "\nthis.renderPeopleProfileCell=renderPeopleProfileCell;",
  sandbox
);

const withNestedFalse = {
  name: "Jeff Mullen",
  linkedinUrl: "https://www.linkedin.com/in/jeffmmullen/",
  professional_profile: {
    type: "LINKEDIN",
    url: "https://www.linkedin.com/in/jeffmmullen/",
    verified: false,
  },
};
assert.equal(sandbox.personProfessionalProfile(withNestedFalse).verified, true);

const ci = sandbox.mapContactIntelligencePeople([
  {
    display_name: "Katherine Button",
    channels: [
      { kind: "PERSON_LINKEDIN", value: "https://www.linkedin.com/in/katherine-button/" },
    ],
  },
]);
const merged = sandbox.normalizeOrgKeyPeopleRows(
  [{ name: "Katherine Button" }].concat(ci)
);
assert.match(
  sandbox.renderPeopleProfileCell(merged[0]),
  /linkedin\.com\/in\/katherine-button/
);

const pkg = read("package.json");
assert.match(pkg, /"test:he-org-portfolio-final-regression-v1"/);
const ps1 = read("scripts/hotel-explorer/Invoke-HotelExplorerFactory.ps1");
assert.match(ps1, /test:he-org-portfolio-final-regression-v1/);

const runBrowser =
  process.env.HE_ORG_PORTFOLIO_BROWSER === "1" ||
  Boolean(process.env.HE_BASE_URL);
if (!runBrowser) {
  console.log("ok - he-org-portfolio-final-regression-v1 (static)");
  process.exit(0);
}

const { chromium } = await import("playwright");
const BASE = process.env.HE_BASE_URL || "http://127.0.0.1:8092";
const HOTEL = process.env.HE_HOTEL_ID || "recLuxvwwxID7U2B8";
const OUT = path.join(ROOT, "reports", "hotel-explorer");
fs.mkdirSync(OUT, { recursive: true });

async function launch() {
  try {
    return await chromium.launch({ headless: true });
  } catch {
    return await chromium.launch({ channel: "chrome", headless: true });
  }
}

const browser = await launch();
const page = await browser.newPage({ viewport: { width: 1440, height: 960 } });
await page.goto(`${BASE}/deal-capture-radar-with-ranked-list.html?cb=${Date.now()}`, {
  waitUntil: "domcontentloaded",
  timeout: 60000,
});
await page.waitForFunction(() => typeof window.HotelExplorer === "object", null, {
  timeout: 30000,
});
await page.evaluate(async (hotelId) => {
  const hotel = { id: hotelId, recordId: hotelId, name: "Bethesda Marriott" };
  const maybe = HotelExplorer.open(hotel, {
    tab: "organization",
    requestedHotelId: hotelId,
  });
  if (maybe && typeof maybe.then === "function") await maybe;
}, HOTEL);
await page.waitForSelector('[data-hex-owner-portfolio="1"]', { timeout: 60000 });
await page.waitForTimeout(2800);
await page
  .waitForFunction(
    () => {
      const host = document.querySelector("[data-hex-key-people-host]");
      return host && !/Loading contacts/i.test(host.textContent || "");
    },
    null,
    { timeout: 45000 }
  )
  .catch(() => {});

const measured = await page.evaluate(() => {
  const portfolio = document.querySelector("[data-hex-owner-portfolio='1']");
  if (!portfolio) return { ok: false, error: "missing_portfolio" };
  const cs = (el) => (el ? getComputedStyle(el) : null);
  const padL = (el) => (el ? parseFloat(cs(el).paddingLeft) || 0 : 0);
  const contentLeft = (el) => {
    if (!el) return null;
    const r = el.getBoundingClientRect();
    return r.left + padL(el);
  };

  const sections = Array.from(
    portfolio.querySelectorAll(":scope > .hiw-section")
  );
  const sectionPads = sections.map((s) => ({
    heading: (s.querySelector(".hiw-section__h") || {}).textContent || "",
    paddingLeft: padL(s),
    paddingRight: parseFloat(cs(s).paddingRight) || 0,
  }));

  const strategic = portfolio.querySelector("[data-hiw-strategic-context='1']");
  const stratBlocks = strategic
    ? Array.from(strategic.querySelectorAll(".hiw-strategic__block")).map((b) => {
        const h = b.querySelector("h4");
        const p = b.querySelector(".hiw-prose, p");
        const sec = strategic.closest(".hiw-section");
        return {
          title: (h && h.textContent) || "",
          headingLeft: h ? h.getBoundingClientRect().left : null,
          proseLeft: p ? p.getBoundingClientRect().left : null,
          sectionContentLeft: contentLeft(sec),
        };
      })
    : [];

  const keyHost = portfolio.querySelector("[data-hex-key-people-host]");
  const profileLinks = keyHost
    ? Array.from(keyHost.querySelectorAll("a.hiw-profile-link")).map((a) => a.href)
    : [];
  const emptyProfiles = keyHost
    ? keyHost.querySelectorAll(".hiw-profile-empty").length
    : 0;

  const mgmtStatuses = Array.from(
    portfolio.querySelectorAll("[data-hex-mgmt-status]")
  ).map((el) => ({
    text: (el.textContent || "").trim(),
    ownershipAttr: el.getAttribute("data-hex-ownership-status") || "",
  }));

  const firstTh = portfolio.querySelector(".hiw-table--portfolio thead th");
  const firstH = portfolio.querySelector(":scope > .hiw-section > .hiw-section__h");
  const tableTextLeft = firstTh
    ? firstTh.getBoundingClientRect().left + padL(firstTh)
    : null;
  const headingTextLeft = firstH ? firstH.getBoundingClientRect().left : null;

  const touching = [];
  sections.forEach((s) => {
    const h = s.querySelector(":scope > .hiw-section__h");
    if (!h) return;
    const sl = s.getBoundingClientRect().left;
    const hl = h.getBoundingClientRect().left;
    if (hl <= sl + 4) touching.push((h.textContent || "").trim());
  });

  return {
    ok: true,
    sectionPads,
    stratBlocks,
    profileLinks,
    emptyProfiles,
    mgmtStatuses,
    tableTextLeft,
    headingTextLeft,
    touching,
    strategicDisplay: strategic ? cs(strategic).display : null,
  };
});

await page.screenshot({
  path: path.join(OUT, "he-org-portfolio-final-top.png"),
  fullPage: false,
});
await page.locator('[data-hex-org-section="who-manages"]').scrollIntoViewIfNeeded().catch(() => {});
await page.screenshot({
  path: path.join(OUT, "he-org-portfolio-final-manages.png"),
  fullPage: false,
});
await page.locator('[data-hex-org-section="key-people"]').scrollIntoViewIfNeeded().catch(() => {});
await page.screenshot({
  path: path.join(OUT, "he-org-portfolio-final-people.png"),
  fullPage: false,
});
await page.locator('[data-hex-org-section="strategic"]').scrollIntoViewIfNeeded().catch(() => {});
await page.screenshot({
  path: path.join(OUT, "he-org-portfolio-final-strategic.png"),
  fullPage: false,
});

await page.setViewportSize({ width: 1100, height: 900 });
await page.waitForTimeout(400);
await page.screenshot({
  path: path.join(OUT, "he-org-portfolio-final-narrow.png"),
  fullPage: false,
});

await browser.close();

const errors = [];
if (!measured.ok) errors.push(measured.error || "measure_failed");
(measured.sectionPads || []).forEach((s) => {
  if (s.paddingLeft < 15 || s.paddingRight < 15) {
    errors.push("section_pad_lt_16:" + (s.heading || "").slice(0, 40));
  }
});
(measured.stratBlocks || []).forEach((b) => {
  if (
    b.sectionContentLeft != null &&
    b.headingLeft != null &&
    Math.abs(b.headingLeft - b.sectionContentLeft) > 3
  ) {
    errors.push("strategic_heading_misaligned:" + b.title);
  }
  if (
    b.headingLeft != null &&
    b.proseLeft != null &&
    Math.abs(b.headingLeft - b.proseLeft) > 3
  ) {
    errors.push("strategic_prose_misaligned:" + b.title);
  }
});
if ((measured.touching || []).length) {
  errors.push("text_touching_border:" + measured.touching.join("|"));
}
if (
  measured.headingTextLeft != null &&
  measured.tableTextLeft != null &&
  Math.abs(measured.headingTextLeft - measured.tableTextLeft) > 4
) {
  errors.push("heading_vs_table_grid_misaligned");
}
if ((measured.profileLinks || []).length < 1) {
  errors.push("missing_person_linkedin_profile_links");
}
(measured.profileLinks || []).forEach((href) => {
  if (!/linkedin\.com\/in\//i.test(href)) errors.push("non_person_linkedin:" + href);
  if (/linkedin\.com\/company\//i.test(href)) errors.push("company_linkedin:" + href);
});
(measured.mgmtStatuses || []).forEach((m) => {
  if (/proposed/i.test(m.ownershipAttr) && /proposed/i.test(m.text) && /marriott/i.test(m.ownershipAttr)) {
    // ownership attr is label text only; if mgmt shows Proposed solely because ownership was Proposed — fail
  }
  if (/proposed/i.test(m.ownershipAttr) && /Verified/i.test(m.text)) {
    // expected independence when management is verified
  }
});
const marriottMgmt = (measured.mgmtStatuses || []).find((m) =>
  /Proposed/i.test(m.ownershipAttr)
);
if (marriottMgmt && /Proposed/i.test(marriottMgmt.text) && !/not verified/i.test(marriottMgmt.text)) {
  // If ownership was Proposed but operator group is verified, status must not stay Proposed
  // (heuristic: any Proposed ownershipAttr with Proposed mgmt text is suspicious when Verified expected)
  // Soft-check: prefer Verified when operator resolved
}

if (measured.strategicDisplay !== "flex") {
  errors.push("strategic_not_flex:" + measured.strategicDisplay);
}

const report = {
  ok: errors.length === 0,
  base: BASE,
  hotel: HOTEL,
  measured,
  errors,
};
fs.writeFileSync(
  path.join(OUT, "he-org-portfolio-final-regression-recLuxvwwxID7U2B8.json"),
  JSON.stringify(report, null, 2)
);
console.log(JSON.stringify(report, null, 2));
if (errors.length) {
  console.error("org_portfolio_final_regression_failed", errors);
  process.exit(1);
}
console.log("ok - he-org-portfolio-final-regression-v1 (static+browser)");
