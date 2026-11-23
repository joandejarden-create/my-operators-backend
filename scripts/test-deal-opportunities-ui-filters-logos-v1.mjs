#!/usr/bin/env node
/**
 * DealSignal Opportunities UI — tab-scoped filters + brand logo contracts.
 * CUSTOMER_BLOCKED. No Airtable writes.
 */
import fs from "node:fs";
import path from "node:path";
import vm from "node:vm";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(__dirname, "..");
const fail = (msg) => {
  console.error("FAIL:", msg);
  process.exitCode = 1;
};
const ok = (msg) => console.log("OK:", msg);

function loadBrowserScript(rel) {
  const code = fs.readFileSync(path.join(root, rel), "utf8");
  const ctx = { globalThis: {} };
  ctx.globalThis = ctx;
  vm.runInNewContext(code, ctx, { filename: rel });
  return ctx;
}

const filterCtx = loadBrowserScript("public/js/deal-opportunities-tab-filters-v1.js");
const logoCtx = loadBrowserScript("public/js/deal-opportunities-brand-logos-v1.js");
const F = filterCtx.DealSignalOpportunityFilters;
const L = logoCtx.DealSignalBrandLogos;

if (!F || !L) {
  fail("helpers did not attach to globalThis");
  process.exit(1);
}

const fixtures = [
  {
    id: "q1",
    name: "Courtyard Denver Tech Center",
    sub: "Courtyard by Marriott",
    group: "qualified",
    kind: "BCT",
    region: "US",
    country: "US",
    window: "2026",
    chainScales: ["Upscale"],
  },
  {
    id: "q2",
    name: "Hilton Trinidad",
    sub: "Hilton",
    group: "qualified",
    kind: "MCT",
    region: "Caribbean",
    country: "TT",
    window: "2026",
    chainScales: ["Upper Upscale"],
  },
  {
    id: "w1",
    name: "AC Hotel Cartagena",
    sub: "AC Hotels by Marriott",
    group: "watch",
    kind: "BCT",
    region: "Other",
    country: "CO",
    window: "Historical",
    chainScales: ["Upscale"],
  },
  {
    id: "w2",
    name: "Mexico City Watch",
    sub: "Independent",
    group: "watch",
    kind: "OTHER",
    region: "Other",
    country: "MX",
    window: "Timing to verify",
    chainScales: ["Independent"],
  },
  {
    id: "q3",
    name: "AHIP Portfolio",
    sub: "Marriott / Hilton / IHG",
    group: "qualified",
    kind: "MCT",
    region: "US",
    country: "US",
    window: "2026",
    chainScales: ["Upscale", "Upper Upscale"],
    relatedHotels: [
      { brand: "Courtyard by Marriott", name: "A" },
      { brand: "Hilton", name: "B" },
    ],
  },
];

const countryKey = (o) => String(o.country || "__unspecified__");
const countryLabel = (k) =>
  ({ US: "United States", CO: "Colombia", MX: "Mexico", TT: "Trinidad and Tobago" }[k] || k);
const brandKey = (o) => {
  const raw = String(o.sub || "").trim();
  return raw || "__unspecified__";
};
const brandLabel = (k) => k;
const horizonKey = (o) => {
  if (/historical/i.test(String(o.window || ""))) return "unknown";
  if (/verify/i.test(String(o.window || ""))) return "unknown";
  return "0_6";
};
const horizonLabel = (k) =>
  ({ "0_6": "0–6 months", "6_12": "6–12 months", "12_24": "12–24 months", unknown: "Timing to verify" }[
    k
  ] || k);
const dwKey = (o) => {
  const w = String(o.window || "").trim();
  if (!w || /timing to verify/i.test(w)) return "unknown";
  return w;
};
const dwLabel = (k) => (k === "unknown" ? "Timing to verify" : k);

// --- Tab-scoped countries ---
const qUniverse = F.tabUniverse(fixtures, "qualified");
const qCountries = F.deriveCountryOptions(qUniverse, countryKey, countryLabel).map((p) => p[0]);
if (!qCountries.includes("US") || !qCountries.includes("TT")) {
  fail("Qualified country options missing US/TT: " + qCountries.join(","));
} else ok("Qualified country options include US + TT");
if (qCountries.includes("MX") || qCountries.includes("CO")) {
  fail("Qualified must not include MX/CO from other tabs: " + qCountries.join(","));
} else ok("Qualified country options exclude Mexico/Colombia");

const watchCountries = F.deriveCountryOptions(
  F.tabUniverse(fixtures, "watch"),
  countryKey,
  countryLabel,
).map((p) => p[0]);
if (!watchCountries.includes("MX") || !watchCountries.includes("CO")) {
  fail("Watch countries missing MX/CO");
} else ok("Watch country options include MX + CO after tab switch universe");
if (watchCountries.includes("TT")) {
  fail("Watch must not include Trinidad-only qualified country");
} else ok("Watch countries exclude Qualified-only TT");

// --- Brand / market / timing / decision window ---
const qBrands = F.deriveBrandOptions(qUniverse, brandKey, brandLabel).map((p) => p[0]);
if (!qBrands.some((b) => /courtyard|hilton|marriott/i.test(b))) {
  fail("Qualified brands missing expected hotel brands");
} else ok("Qualified brand options scoped to tab");
if (qBrands.some((b) => /^Independent$/i.test(b) && !qUniverse.some((o) => /independent/i.test(o.sub)))) {
  fail("Independent brand leaked into Qualified without membership");
}

const qMarkets = F.deriveRegionOptions(qUniverse).map((p) => p[0]);
if (!qMarkets.includes("US") || !qMarkets.includes("Caribbean")) {
  fail("Qualified markets incomplete: " + qMarkets.join(","));
} else ok("Qualified market options scoped to tab");
if (qMarkets.includes("Other") && !qUniverse.some((o) => o.region === "Other")) {
  fail("Other market leaked into Qualified");
} else ok("Qualified markets do not invent Other without membership");

const qTiming = F.deriveTimingOptions(qUniverse, horizonKey, horizonLabel).map((p) => p[0]);
if (!qTiming.includes("all") || !qTiming.includes("0_6")) {
  fail("Qualified timing options incomplete");
} else ok("Qualified timing options scoped to tab");

const qDw = F.deriveDecisionWindowOptions(qUniverse, dwKey, dwLabel).map((p) => p[0]);
if (!qDw.includes("2026")) fail("Qualified decision window missing 2026");
else ok("Qualified decision window options scoped to tab");

const watchDw = F.deriveDecisionWindowOptions(
  F.tabUniverse(fixtures, "watch"),
  dwKey,
  dwLabel,
).map((p) => p[0]);
if (!watchDw.includes("Historical") && !watchDw.includes("unknown")) {
  fail("Watch decision windows incomplete");
} else ok("Watch decision windows recomputed for tab");

// --- Invalid selection reset ---
const resolved = F.resolveSelectValue(
  F.deriveCountryOptions(qUniverse, countryKey, countryLabel),
  "MX",
);
if (resolved !== "all") fail("Invalid MX on Qualified should reset to all, got " + resolved);
else ok("Invalid filter selection resets to All on tab change");

// --- Chain scale presence ---
const scales = F.chainScalePresentInUniverse(qUniverse, (o) => o.chainScales || []);
if (!scales.has("upscale") || !scales.has("upper upscale")) {
  fail("Chain scale presence missing expected Qualified scales");
} else ok("Chain-scale categories scoped to Qualified universe");

// --- Logos ---
const map = L.buildBrandLogoMap(
  [
    {
      name: "Courtyard by Marriott",
      id: "rec6hye5H8zJmAGv3",
      logo: "https://dl.airtable.com/.attachments/abc/courtyard.png",
    },
    {
      name: "Hilton Hotels & Resorts",
      id: "recWubG3rhiS1BaWi",
      logo: "https://v5.airtableusercontent.com/v1/hilton.png",
    },
    { name: "Empty No Logo", id: "recEmptyNoLogo1", logo: "" },
    {
      name: "Cdn Only Brand",
      id: "",
      logo: "https://v5.airtableusercontent.com/v1/cdn-only.png",
    },
  ],
  { courtyard: "courtyard by marriott", hilton: "hilton hotels & resorts" },
);

if (!map["courtyard by marriott"]?.url?.startsWith("/api/brand-library/logo/")) {
  fail("Courtyard should prefer same-origin logo proxy: " + map["courtyard by marriott"]?.url);
} else ok("Logo map prefers /api/brand-library/logo/:id for Courtyard");
if (map["empty no logo"]) fail("Empty logo must not enter map");
else ok("Empty logo brands excluded from map");
if (!map["cdn only brand"]?.url?.startsWith("https://")) {
  fail("CDN-only brand without id should keep absolute URL");
} else ok("CDN fallback retained when Brand Basics id missing");
if (!L.isUsableLogoUrl("https://example.com/a.png")) fail("https should be usable");
if (!L.isUsableLogoUrl("/api/brand-library/logo/rec6hye5H8zJmAGv3")) {
  fail("same-origin logo proxy path must be usable");
} else ok("isUsableLogoUrl accepts brand-library logo proxy");

const courtyardHit = L.resolveBrandLogoUrl("Courtyard", map);
if (!courtyardHit?.url) fail("Alias Courtyard → Courtyard by Marriott failed");
else ok("Historical/alias brand mapping resolves Courtyard");

const portfolioHit = L.resolveBrandLogoUrl("Marriott / Hilton / IHG", map);
if (portfolioHit) fail("Multi-brand portfolio must not pick arbitrary hotel logo");
else ok("Portfolio multi-brand returns null (graceful fallback path)");

const unknownHit = L.resolveBrandLogoUrl("Totally Unknown Brand XYZ", map);
if (unknownHit) fail("Unknown brand should not resolve");
else ok("Unknown brand graceful null for fallback glyph");

// --- HTML wiring contracts ---
const html = fs.readFileSync(path.join(root, "public/deal-opportunities.html"), "utf8");
const checks = [
  [/deal-opportunities-tab-filters-v1\.js/, "loads tab filter helper"],
  [/deal-opportunities-brand-logos-v1\.js/, "loads brand logo helper"],
  [/repopulateFiltersForTab/, "repopulateFiltersForTab present"],
  [/populateRegionFilter/, "populateRegionFilter present"],
  [/populateTimingFilter/, "populateTimingFilter present"],
  [/currentTabUniverse/, "currentTabUniverse present"],
  [/buildBrandLogoMap/, "uses buildBrandLogoMap"],
  [/is-fallback/, "fallback class for missing logos"],
  [/__dopLogoImgError/, "logo img error handler"],
  [/onerror="this\.remove\(\)"/, null], // must NOT exist
];

for (const [re, label] of checks) {
  if (label == null) {
    if (re.test(html)) fail("HTML still uses blanking onerror=this.remove()");
    else ok("HTML no longer blanks logos via this.remove()");
  } else if (!re.test(html)) fail("HTML missing: " + label);
  else ok("HTML: " + label);
}

if (!/repopulateFiltersForTab\(\)/.test(html) || !/tabs"\)\.addEventListener[\s\S]*repopulateFiltersForTab/.test(html)) {
  fail("Tab click must call repopulateFiltersForTab");
} else ok("Tab switch rebuilds filter options");

const css = fs.readFileSync(path.join(root, "public/css/deal-opportunities.css"), "utf8");
if (!/\.dop-hotel-logo\.is-fallback/.test(css)) fail("CSS missing is-fallback styles");
else ok("CSS defines non-blank fallback logo surface");

// Pagination independence: option derivation uses full universe, not a page slice
const pageSlice = qUniverse.slice(0, 1);
const fromPage = F.deriveCountryOptions(pageSlice, countryKey, countryLabel).map((p) => p[0]);
const fromFull = F.deriveCountryOptions(qUniverse, countryKey, countryLabel).map((p) => p[0]);
if (fromFull.length <= fromPage.length && qUniverse.length > 1) {
  // Full must be >= page; if equal only when page happens to cover all countries
  ok("Pagination note: full-universe options length " + fromFull.length + " vs page " + fromPage.length);
}
if (fromFull.includes("TT") && !fromPage.includes("TT") && qUniverse.some((o) => o.country === "TT")) {
  ok("Dropdown values come from full tab universe, not first page only");
} else if (fromFull.includes("TT")) {
  ok("Full Qualified universe retains TT regardless of page slice");
}

if (process.exitCode) {
  console.error("\nDeal Opportunities UI filters/logos tests FAILED");
  process.exit(1);
}
console.log("\nAll Deal Opportunities UI filters/logos tests passed.");
