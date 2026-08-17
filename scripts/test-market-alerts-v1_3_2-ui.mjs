#!/usr/bin/env node
/**
 * Market Alerts V1.3.2 UI simplification checks (presentation-only).
 * Cache-bust asset version is 1.3.4.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");
const UI_VERSION = "1.3.4";

let failed = 0;
function assert(cond, msg) {
  if (!cond) {
    failed += 1;
    console.error("FAIL:", msg);
  } else {
    console.log("OK:", msg);
  }
}

const html = fs.readFileSync(path.join(ROOT, "public", "market-alerts.html"), "utf8");
const js = fs.readFileSync(path.join(ROOT, "public", "market-alerts.js"), "utf8");
const appJs = fs.readFileSync(path.join(ROOT, "public", "app.js"), "utf8");
const appHtml = fs.readFileSync(path.join(ROOT, "public", "app.html"), "utf8");
const htmlOnly = html.replace(/<script[\s\S]*?<\/script>/gi, "");

console.log("\n--- DEFAULT ---");
assert(/data-window="7d"[^>]*\bactive\b|class="btn-time active"[^>]*data-window="7d"/.test(html), "7d selected in markup");
assert(/class="btn-feed-mode active"[^>]*id="feedModeAll"|id="feedModeAll"[^>]*class="[^"]*\bactive\b/.test(html), "All intelligence selected in markup");
assert(/let feedMode = 'all'/.test(js), "JS default feedMode = all");
assert(/selectedTimeWindow = '7d'/.test(js), "JS default timeframe = 7d");

console.log("\n--- RESET ---");
assert(/feedMode = 'all'/.test(js) && /selectedTimeWindow = '7d'/.test(js), "Reset restores 7d + All");
assert(/showToast\('View reset'\)/.test(js), "Reset toast copy is View reset");
assert(/searchInputEl\.value = ''/.test(js), "Reset clears search");
assert(/savedFilterOn = false/.test(js), "Reset clears Saved-only");
assert(!/loadFeed\(\{\s*autoFallback:\s*true/.test(js), "Reset/init do not auto-fallback modes");

console.log("\n--- FILTER LABELS ---");
assert(/id="feedModeActionable"[^>]*>\s*Act Now\s*</.test(html), "Act Now label");
assert(/id="feedModeWorth"[^>]*>\s*Watch\s*</.test(html), "Watch label");
assert(/id="feedModeAll"[^>]*>\s*All\s*</.test(html), "All label");
assert(!/>\s*Actionable\s*</.test(htmlOnly), "no Actionable user label in markup");
assert(!/>\s*Worth Reviewing\s*</.test(htmlOnly), "no Worth Reviewing user label in markup");
assert(!htmlOnly.includes("All Market Activity"), "no All Market Activity label");

console.log("\n--- LAYOUT ---");
assert(html.includes("news-filter-row--single"), "single horizontal filter row");
assert((html.match(/news-filter-divider/g) || []).length >= 3, "dividers between search / time / mode / actions");
assert(html.includes("news-filter-actions"), "actions grouped after intelligence filters");
assert(html.includes("intel-summary-row"), "two-column intel summary class");
assert(/grid-template-columns:\s*1fr\s*1fr/.test(html), "desktop two columns");
assert(/@media \(max-width:\s*900px\)[\s\S]*intel-summary-row[\s\S]*grid-template-columns:\s*1fr/.test(html), "mobile stacks columns");
{
  const headlinesIdx = html.indexOf('<h2>Headlines</h2>');
  const actNowIdx = html.indexOf('<h2>Act Now</h2>');
  const watchIdx = html.indexOf('<h2>Watch</h2>');
  assert(headlinesIdx > -1 && actNowIdx > headlinesIdx, "Headlines appear above Act Now");
  assert(watchIdx > actNowIdx, "Watch follows Act Now");
}
{
  const searchIdx = html.indexOf('id="searchInput"');
  const timeIdx = html.indexOf('data-window="7d"');
  const actIdx = html.indexOf('id="feedModeActionable"');
  const savedIdx = html.indexOf('id="savedToggle"');
  assert(searchIdx > -1 && searchIdx < timeIdx && timeIdx < actIdx && actIdx < savedIdx, "filter order: search → time → mode → actions");
}

console.log("\n--- SECTIONS ---");
assert(/<h2>Act Now<\/h2>/.test(html), "Act Now section exists");
assert(/<h2>Watch<\/h2>/.test(html), "Watch section exists");
assert(!htmlOnly.includes("Top Read"), "Top Read absent");
assert(!htmlOnly.includes("Latest Market Activity"), "Latest Market Activity summary absent");
assert(!/id="topReadList"|id="liveFeedList"/.test(html), "Top Read / Latest list nodes absent");
assert(/id="actionableNowList"/.test(html) && /id="worthReviewingList"/.test(html), "Act Now / Watch lists remain");
assert(/id="heroGrid"/.test(html), "main Headlines feed preserved");

console.log("\n--- TOAST ---");
assert(/id="successMessage"/.test(html) && /class="success-message"/.test(html), "shared successMessage toast markup");
assert(/getElementById\('successMessage'\)/.test(js), "JS uses shared toast");
assert(!/market-alerts-toast/.test(js), "no Market Alerts-specific toast class");
assert(!/Filters cleared/.test(js), "old Filters cleared copy removed");

console.log("\n--- CARD METADATA ---");
assert(/function compactIntelMeta/.test(js), "compactIntelMeta helper");
assert(/function noteworthyProjectDirection/.test(js), "project direction gate");
assert(/Challenged\|Delayed\|Blocked\|Advancing/.test(js), "noteworthy directions only");

console.log("\n--- STAKEHOLDER ---");
assert(/actionableBtn\.disabled = false/.test(js), "Act Now stays enabled");
assert(/worthBtn\.disabled = false/.test(js), "Watch stays enabled");
assert(!/actionableBtn\.disabled\s*=\s*true/.test(js), "no role-based disable");

console.log("\n--- INTERNAL METADATA ---");
assert(!/EARLY_SIGNAL/.test(htmlOnly), "EARLY_SIGNAL visible count = 0 in markup");
assert(/function getUserFacingSourceName/.test(js), "source sanitizer preserved");

console.log("\n--- CACHE BUST ---");
assert(html.includes(`content="${UI_VERSION}"`), `HTML meta ${UI_VERSION}`);
assert(html.includes(`market-alerts.js?v=${UI_VERSION}`), `JS query ${UI_VERSION}`);
assert(appJs.includes(`MARKET_ALERTS_EMBED_VERSION = '${UI_VERSION}'`), `app embed ${UI_VERSION}`);
assert(appHtml.includes(`/app.js?v=ma-${UI_VERSION}`), `app.html ${UI_VERSION}`);

console.log("\n--- DATA PATHS PRESERVED ---");
assert(/topRead: data\.topRead \|\| \[\]/.test(js), "rail still receives topRead from API");
assert(/function renderTopRead/.test(js), "Top Read renderer kept (not destroyed)");
assert(/params\.set\('actionable', '1'\)/.test(js), "internal actionable filter param preserved");
assert(/params\.set\('worthReviewing', '1'\)/.test(js), "internal worthReviewing filter param preserved");

if (failed) {
  console.error(`\n${failed} Market Alerts V1.3.2 UI test(s) failed`);
  process.exit(1);
}
console.log("\nAll Market Alerts V1.3.2 UI tests passed");
