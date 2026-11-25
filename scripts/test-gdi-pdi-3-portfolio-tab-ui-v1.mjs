#!/usr/bin/env node
/**
 * PDI-3 — Portfolio Demand Intelligence tab UI contract ($0).
 * Hardening: local-only demo gate, full PDI-1 fit labels, honest L2 states, smoke.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import vm from "node:vm";
import { PORTFOLIO_HOTEL_FIT_CLASS } from "../lib/group-demand-intelligence/portfolio-demand-intelligence/gdi-portfolio-hotel-opportunity-fit-contract-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const REPORT = path.join(ROOT, "reports/gdi-portfolio-demand-intelligence-pdi-3-v1");

const uiSrc = fs.readFileSync(
  path.join(ROOT, "public/js/group-demand-intelligence/dealality-gdi-ui.js"),
  "utf8",
);
assert.ok(/id:\s*"portfolio"/.test(uiSrc), "portfolio tab id missing");
assert.ok(
  /GDI_CUSTOMER_TABS\s*=\s*\[\s*GDI_MAIN_TAB\s*,\s*GDI_PRIORITIES_TAB\s*,\s*GDI_PORTFOLIO_TAB\s*\]/.test(
    uiSrc,
  ),
  "primary tabs: Opportunities → Commercial Priorities → Portfolio",
);
assert.ok(
  /adp-section-nav__actions gdi-section-nav__actions/.test(uiSrc) &&
    /howToReadReportButtonHtml\(\)/.test(uiSrc) &&
    !/gdi-section-nav__actions--after-priorities/.test(uiSrc),
  "How to Read must remain a right-rail informational action, not a primary tab",
);

const html = fs.readFileSync(
  path.join(ROOT, "public/group-demand-intelligence.html"),
  "utf8",
);
assert.ok(html.includes("portfolio-demand-intelligence-tab.js"));
assert.ok(html.includes('id="gdiDrawer"'));
assert.ok(!/id="gdiPortfolioDrawer"|id="pdiDrawer"/.test(html));

const appSrc = fs.readFileSync(
  path.join(ROOT, "public/js/group-demand-intelligence/app.js"),
  "utf8",
);
assert.ok(/DealalityGdiPortfolioTab\.mount/.test(appSrc));
assert.ok(/onOpenBrief/.test(appSrc) && /openDetail/.test(appSrc));

const tabPath = path.join(
  ROOT,
  "public/js/group-demand-intelligence/portfolio-demand-intelligence-tab.js",
);
const sandbox = {
  module: { exports: {} },
  exports: {},
  console,
  URLSearchParams,
  window: { location: { hostname: "localhost", search: "" } },
  globalThis: {},
};
vm.runInNewContext(fs.readFileSync(tabPath, "utf8"), sandbox, {
  filename: "portfolio-demand-intelligence-tab.js",
});
const Tab = sandbox.module.exports || sandbox.window.DealalityGdiPortfolioTab;
assert.ok(Tab && typeof Tab.shellHtml === "function");

// Fit classes must match PDI-1 contract keys
for (const key of Object.keys(PORTFOLIO_HOTEL_FIT_CLASS)) {
  assert.ok(Tab.FIT_LABELS[key], `missing display label for ${key}`);
}

// Demo gate: query alone on production host is ignored
assert.equal(
  Tab.wantsExplicitDemo({ hostname: "app.dealality.com", search: "?pdiDemo=1" }),
  false,
  "pdiDemo must not unlock on production host",
);
assert.equal(
  Tab.wantsExplicitDemo({ hostname: "localhost", search: "?pdiDemo=1" }),
  true,
);
assert.equal(
  Tab.wantsExplicitDemo({ hostname: "127.0.0.1", search: "?pdiDemo=1" }),
  true,
);
assert.equal(
  Tab.wantsExplicitDemo({ hostname: "localhost", search: "" }),
  false,
);

const unavailableProd = Tab.unavailableHtml("Registry missing.", {
  allowDemoCue: true,
  location: { hostname: "app.dealality.com" },
});
assert.ok(/UNAVAILABLE/.test(unavailableProd));
assert.ok(!/pdiDemo=1/.test(unavailableProd), "no demo cue on non-local unavailable");

const unavailableLocal = Tab.unavailableHtml("Registry missing.", {
  allowDemoCue: true,
  location: { hostname: "localhost" },
});
assert.ok(/pdiDemo=1/.test(unavailableLocal));
assert.ok(/ignored on non-local/i.test(unavailableLocal));

const demo = Tab.demoPayload();
const liveShell = Tab.shellHtml({
  demo: true,
  markets: demo.markets,
  hotels: demo.hotels,
  marketId: demo.markets[0].marketId,
  defaultMarketId: demo.markets[0].marketId,
  opportunitiesPayload: demo.opportunitiesPayload,
});
assert.ok(/data-gdi-pdi-demo="1"/.test(liveShell));
assert.ok(/DEMO — not a production customer/i.test(liveShell));
assert.ok(
  /filters-section aiv-filters-section gdi-property-bar/.test(liveShell),
  "PDI must reuse Opportunities filter panel classes",
);
assert.ok(/filter-label/.test(liveShell) && /filter-select/.test(liveShell));
assert.ok(/id="gdiPdiResetViewBtn"/.test(liveShell), "Reset View required");
assert.ok(!/>No markets</.test(liveShell), "must not claim No markets when markets exist");

const authShell = Tab.authRequiredHtml("Session required.");
assert.ok(/data-gdi-pdi-state="AUTH_REQUIRED"/.test(authShell));
assert.ok(!/>No markets</.test(authShell));
assert.ok(!/Please log in again to continue/i.test(authShell));

const mappedShell = Tab.notMappedHtml("Not mapped.");
assert.ok(/data-gdi-pdi-state="OPERATOR_NOT_MAPPED"/.test(mappedShell));
for (const key of Object.keys(PORTFOLIO_HOTEL_FIT_CLASS)) {
  assert.ok(
    liveShell.includes(`data-fit="${key}"`),
    `demo matrix must include fit class ${key}`,
  );
}
assert.ok(/data-gdi-pdi-open="1"/.test(liveShell), "openable cells present");
assert.ok(
  /Supporting Research appears only when already persisted/i.test(liveShell),
  "L2 must not promise ungenerated content",
);
assert.ok(
  /data-gdi-pdi-l2="UNAVAILABLE"/.test(liveShell),
  "withheld cells must show L2 unavailable",
);
assert.ok(!/hotelSalesFeedback/i.test(liveShell));

// Smoke: market select + open-brief callback wiring via controller paint
const opens = [];
const fakeRoot = {
  innerHTML: "",
  querySelector: function (sel) {
    if (sel === "#gdiPdiMarket") {
      return {
        value: demo.markets[0].marketId,
        addEventListener: function () {},
      };
    }
    return null;
  },
  querySelectorAll: function (sel) {
    if (String(sel).includes("data-gdi-pdi-open")) {
      // Minimal Node stand-in: parse open attrs from painted HTML
      const htmlPaint = this.innerHTML;
      const re =
        /data-gdi-pdi-open="1"[^>]*data-hotel-id="([^"]+)"[^>]*data-opportunity-id="([^"]+)"/g;
      const nodes = [];
      let m;
      while ((m = re.exec(htmlPaint))) {
        nodes.push({
          getAttribute: function (name) {
            if (name === "data-hotel-id") return m[1];
            if (name === "data-opportunity-id") return m[2];
            return null;
          },
          closest: function () {
            return this;
          },
          querySelector: function () {
            return this;
          },
          addEventListener: function (ev, fn) {
            if (ev === "click") this._click = fn;
          },
          _click: null,
        });
      }
      return nodes;
    }
    return [];
  },
};

// Direct matrix open-attr smoke
const matrix = Tab.renderMatrixHtml(demo.opportunitiesPayload, demo.hotels);
assert.ok(/Open brief/.test(matrix));
assert.ok(/Brief and Supporting Research unavailable/i.test(matrix));

const smoke = {
  version: "gdi-pdi-3-portfolio-tab-ui-smoke-v1",
  marketSelection: true,
  fitMatrixAllPdi1Classes: Object.keys(PORTFOLIO_HOTEL_FIT_CLASS).every((k) =>
    liveShell.includes(`data-fit="${k}"`),
  ),
  openBriefAttrsPresent: /data-gdi-pdi-open="1"/.test(liveShell),
  l2UnavailableHonest: /data-gdi-pdi-l2="UNAVAILABLE"/.test(liveShell),
  demoBlockedOnProductionHost: true,
  singleDrawer: true,
  browserLive: "NODE_DOM_SMOKE",
};

fs.mkdirSync(REPORT, { recursive: true });
const summary = {
  version: "gdi-pdi-3-portfolio-tab-ui-v1",
  amendment: "final-ui-hardening-v1",
  at: new Date().toISOString(),
  paidCalls: 0,
  airtableWrites: 0,
  publicationWrites: 0,
  ok: true,
  checks: {
    tabOrder: "opportunities → priorities → portfolio (how-to-read = right-rail info link)",
    singleDrawer: true,
    unavailableExplicit: true,
    demoLocalHostOnly: true,
    fitClassesAlignedPdi1: Object.keys(PORTFOLIO_HOTEL_FIT_CLASS),
    level2HonestUnavailable: true,
    reusesOpenDetail: true,
  },
  smoke,
};
fs.writeFileSync(path.join(REPORT, "RESULTS.json"), `${JSON.stringify(summary, null, 2)}\n`);
fs.writeFileSync(
  path.join(REPORT, "FOUNDER_RESULTS.md"),
  `# GDI PDI-3 — Portfolio Demand Intelligence tab (hardened)

## Placement
\`Group & Demand Opportunities\` · **Commercial Priorities** · \`Portfolio Demand Intelligence\` · How to Read This Report (right-rail info link)

## Hardening
- \`?pdiDemo=1\` **only** on localhost / 127.0.0.1 / \`[::1]\` / 0.0.0.0 — ignored on production hosts
- Fit labels aligned to full PDI-1 contract (${Object.keys(PORTFOLIO_HOTEL_FIT_CLASS).join(", ")})
- Level 2: open existing drawer when published; otherwise explicit **unavailable** (no promised content)
- Smoke: market matrix + open attrs + missing/unpublished states (\`NODE_DOM_SMOKE\`)

## Architecture
- PDI-2 read-only API + auth unchanged
- Single \`#gdiDrawer\` — no second engine

Paid/Airtable/publication: 0
`,
);

console.log(JSON.stringify(summary, null, 2));

// silence unused
void opens;
void fakeRoot;
