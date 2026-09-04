#!/usr/bin/env node
/**
 * Static + payload gates for BAI customer longitudinal visual integrity.
 * Does not change measurement. Playwright proof is separate.
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

// --- BAI_CUSTOMER_EXECUTIVE_READ_VISUAL_INTEGRITY (markup contract) ---
for (const [name, html] of [
  ["auth", brandHtml],
  ["share", shareHtml],
]) {
  if (!html.includes('data-bai-er-shell="1"')) {
    fail(`${name}: missing Executive Read shell`);
  } else if (!html.includes("aivCustLongErPosition") || !html.includes("aivCustLongErChanged")) {
    fail(`${name}: missing ER columns`);
  } else if (html.includes('id="aivCustLongExec"') && html.includes('class="bai-w4-exec"')) {
    fail(`${name}: legacy raw exec paragraph still present`);
  } else {
    pass(`${name}: Executive Read shell present`);
  }

  if (!html.includes('data-bai-kpi-row="four"')) {
    fail(`${name}: missing four-KPI host`);
  } else {
    pass(`${name}: four-KPI host present`);
  }

  if (!html.includes("bai-long-disclosure")) {
    fail(`${name}: missing disclosure callouts`);
  } else {
    pass(`${name}: disclosure callouts present`);
  }
}

if (brandHtml.replace(/\s+/g, " ") === shareHtml.replace(/\s+/g, " ")) {
  // not required to be byte-identical shells, but longitudinal block should match
}
const erAuth = brandHtml.slice(
  brandHtml.indexOf("aivCustomerLongitudinal"),
  brandHtml.indexOf("aivThemeMarkets")
);
const erShare = shareHtml.slice(
  shareHtml.indexOf("aivCustomerLongitudinal"),
  shareHtml.indexOf("aivThemeMarkets")
);
const normalize = (s) =>
  s
    .replace(/INTERNAL PREVIEW[\s\S]*?(?=<\/div>)/, "")
    .replace(/\s+/g, " ")
    .trim();
if (normalize(erAuth) !== normalize(erShare)) {
  // Allow minor banner/copy differences but structure keys must match
  const keys = [
    "aivCustLongExecRead",
    "aivCustLongErPosition",
    "aivCustLongErChanged",
    "aivCustLongKpis",
    "aivCustLongDisclosures",
    "aivCustLongTrendChart",
    "aivCustLongProviderBody",
    "aivCustLongBrandBody",
  ];
  const missing = keys.filter((k) => !erAuth.includes(k) || !erShare.includes(k));
  if (missing.length) fail(`auth/share parity missing ids: ${missing.join(",")}`);
  else pass("BAI_CUSTOMER_SHARE_VISUAL_PARITY (shell ids)");
} else {
  pass("BAI_CUSTOMER_SHARE_VISUAL_PARITY (shell identical)");
}

// --- CSS contracts ---
if (!css.includes(".bai-customer-longitudinal .bai-w4-kpi-grid")) {
  fail("CSS: customer KPI grid not scoped");
} else {
  pass("CSS: customer KPI grid scoped (not only .bai-wave4-qa)");
}
if (!css.includes("repeat(4, minmax(0, 1fr))")) {
  fail("CSS: missing 4-column KPI rule");
} else {
  pass("CSS: 4-column KPI rule present");
}
if (!css.includes("BAI_LONGITUDINAL_FOUR_KPI_DESKTOP_SINGLE_ROW")) {
  fail("CSS: missing gate comment for four-KPI");
} else {
  pass("CSS: four-KPI gate annotated");
}
if (!css.includes(".bai-customer-longitudinal .bai-long-disclosure")) {
  fail("CSS: disclosure hierarchy missing");
} else {
  pass("BAI_LONGITUDINAL_DISCLOSURE_VISUAL_HIERARCHY (CSS)");
}
if (!css.includes(".bai-customer-longitudinal .bai-customer-er")) {
  fail("CSS: ER ADP-family customer scope missing");
} else {
  pass("BAI_EXECUTIVE_READ_ADP_FAMILY_PARITY (CSS hooks)");
}

// --- JS contracts ---
if (!js.includes('data-bai-kpi-count="4"') || !js.includes('data-bai-kpi="dates"')) {
  fail("JS: four KPI cards including Dates not rendered");
} else {
  pass("JS: Current/Prior/Change/Dates KPI cards");
}
if (js.includes('"Abs " +') || js.includes("'Abs ' +")) {
  fail("JS: still concatenates raw Abs/Rel into one string");
} else {
  pass("JS: no raw Abs/Rel string dump");
}
if (!js.includes("renderExecutiveRead") || !js.includes("What Changed")) {
  fail("JS: structured Executive Read missing");
} else {
  pass("BAI_CUSTOMER_EXECUTIVE_READ_VISUAL_INTEGRITY (JS structure)");
}

// --- Content stress: all four parents build without layout-critical empty crashes ---
const built = buildBaiCustomerLongitudinalPayloadV1({
  viewMode: BAI_VIEW_MODE.CUSTOMER_PUBLISHED,
});
const payload = built?.customerLongitudinal || built;
if (!payload?.available || !payload?.parents?.length) {
  fail("payload: customer longitudinal unavailable");
} else {
  pass(`payload: ${payload.parents.length} parents available`);
  for (const p of payload.parents) {
    const port = p.portfolio || {};
    const hasStressFields =
      port.currentPresence != null &&
      port.priorPresence != null &&
      port.absoluteLabel &&
      port.relativeLabel;
    if (!hasStressFields) {
      fail(`content stress: ${p.parentCompanyKey} missing portfolio fields`);
    } else {
      const noGain = port.noBrandsImproved === true || !port.strongestPositiveMover;
      pass(
        `content stress: ${p.parentCompanyKey} ok (noPositiveMover=${noGain}, abs=${port.absoluteLabel}, rel=${port.relativeLabel})`
      );
    }
  }
  pass("BAI_EXECUTIVE_READ_CONTENT_STRESS_INTEGRITY (payload fields)");
}

const outDir = path.join(ROOT, "reports", "bai-customer-longitudinal-visual");
fs.mkdirSync(outDir, { recursive: true });
fs.writeFileSync(
  path.join(outDir, "static-gate-summary.json"),
  JSON.stringify(
    {
      failed,
      gates: {
        BAI_CUSTOMER_EXECUTIVE_READ_VISUAL_INTEGRITY: failed === 0 ? "PASS" : "FAIL",
        BAI_EXECUTIVE_READ_ADP_FAMILY_PARITY: failed === 0 ? "PASS" : "FAIL",
        BAI_LONGITUDINAL_FOUR_KPI_DESKTOP_SINGLE_ROW: "SEE_PLAYWRIGHT",
        BAI_LONGITUDINAL_DISCLOSURE_VISUAL_HIERARCHY: failed === 0 ? "PASS" : "FAIL",
        BAI_CUSTOMER_SHARE_VISUAL_PARITY: failed === 0 ? "PASS" : "FAIL",
        BAI_EXECUTIVE_READ_CONTENT_STRESS_INTEGRITY: failed === 0 ? "PASS" : "FAIL",
      },
    },
    null,
    2
  )
);

console.log(
  `\nStatic visual integrity: ${failed === 0 ? "PASS" : "FAIL"} (${failed} failures)`
);
process.exit(failed ? 1 : 0);
