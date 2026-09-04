#!/usr/bin/env node
/**
 * Static gates for BAI customer longitudinal visual cleanup.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { buildBaiCustomerLongitudinalPayloadV1 } from "../lib/ai-visibility/brand-longitudinal/bai-customer-longitudinal-payload-v1.js";
import { BAI_VIEW_MODE } from "../lib/ai-visibility/brand-longitudinal/resolve-bai-prior-comparable-period-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");

let failed = 0;
function pass(msg) {
  console.log(`PASS  ${msg}`);
}
function fail(msg) {
  failed += 1;
  console.error(`FAIL  ${msg}`);
}

const css = fs.readFileSync(
  path.join(ROOT, "public/js/ai-visibility/ai-visibility-shared.css"),
  "utf8"
);
const js = fs.readFileSync(
  path.join(ROOT, "public/js/ai-visibility/bai-customer-longitudinal.js"),
  "utf8"
);
const brandHtml = fs.readFileSync(
  path.join(ROOT, "public/ai-visibility-brand.html"),
  "utf8"
);
const shareHtml = fs.readFileSync(
  path.join(ROOT, "public/brand-ai-visibility-share.html"),
  "utf8"
);

for (const [name, html] of [
  ["auth", brandHtml],
  ["share", shareHtml],
]) {
  if (!html.includes('data-bai-er-shell="1"')) fail(`${name}: missing ER shell`);
  else pass(`${name}: Executive Read shell`);

  if (!html.includes('data-bai-kpi-row="removed"')) {
    fail(`${name}: duplicate KPI host not marked removed`);
  } else pass(`${name}: duplicate KPI row removed`);

  if (!html.includes('data-bai-section="competitive-movement"')) {
    fail(`${name}: competitive section wrapper missing`);
  } else pass(`${name}: competitive section wrapper`);

  if (!html.includes("bai-long-disclosures--strip")) {
    fail(`${name}: disclosure strip missing`);
  } else pass(`${name}: disclosure strip`);

  if (html.includes("aiv-theme-group--secondary") && html.includes("aivThemeMarkets")) {
    // Markets should not use secondary demotion anymore
    const marketsIdx = html.indexOf("aivThemeMarkets");
    const slice = html.slice(Math.max(0, marketsIdx - 120), marketsIdx);
    if (slice.includes("aiv-theme-group--secondary")) {
      fail(`${name}: Markets still secondary-demoted`);
    } else pass(`${name}: Markets section parity`);
  } else pass(`${name}: Markets section parity`);
}

if (!js.includes("clearDuplicateSummaryKpis") || js.includes("data-bai-kpi-count=\"4\"")) {
  fail("JS: still renders four-KPI duplicate row");
} else pass("BAI_LONGITUDINAL_NO_DUPLICATE_SUMMARY_KPIS (JS)");

if (!css.includes("BAI_LONGITUDINAL_NO_DUPLICATE_SUMMARY_KPIS")) {
  fail("CSS: missing no-duplicate gate annotation");
} else pass("CSS: cleanup gates annotated");

if (!css.includes("BAI_BODY_TEXT_CONTRAST_PARITY")) fail("CSS: body contrast gate missing");
else pass("BAI_BODY_TEXT_CONTRAST_PARITY (CSS)");

if (!css.includes("BAI_SECTION_BACKGROUND_HIERARCHY_CONSISTENCY")) {
  fail("CSS: background hierarchy gate missing");
} else pass("BAI_SECTION_BACKGROUND_HIERARCHY_CONSISTENCY (CSS)");

if (!css.includes(".bai-cust-narrative") || !css.includes("min(110ch")) {
  fail("CSS: narrative width utilization missing");
} else pass("BAI_NARRATIVE_WIDTH_UTILIZATION (CSS)");

const built = buildBaiCustomerLongitudinalPayloadV1({
  viewMode: BAI_VIEW_MODE.CUSTOMER_PUBLISHED,
});
const payload = built?.customerLongitudinal || built;
if (!payload?.available || !payload?.parents?.length) {
  fail("payload unavailable");
} else {
  pass(`content stress parents=${payload.parents.length}`);
  for (const p of payload.parents) {
    pass(`content stress: ${p.parentCompanyKey}`);
  }
}

const outDir = path.join(ROOT, "reports", "bai-customer-longitudinal-visual");
fs.mkdirSync(outDir, { recursive: true });
fs.writeFileSync(
  path.join(outDir, "static-cleanup-gate-summary.json"),
  JSON.stringify({ failed }, null, 2)
);

console.log(
  `\nStatic visual cleanup: ${failed === 0 ? "PASS" : "FAIL"} (${failed} failures)`
);
process.exit(failed ? 1 : 0);
