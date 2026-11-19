#!/usr/bin/env node
/**
 * PDI-3 — Portfolio Demand Intelligence tab UI contract ($0).
 * No live Memberstack / Airtable. Verifies tab order, unavailable/demo honesty,
 * matrix rendering, and single-drawer open attrs (no second engine).
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { createRequire } from "node:module";
import { fileURLToPath } from "node:url";
import vm from "node:vm";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const REPORT = path.join(ROOT, "reports/gdi-portfolio-demand-intelligence-pdi-3-v1");
const require = createRequire(import.meta.url);

const uiSrc = fs.readFileSync(
  path.join(ROOT, "public/js/group-demand-intelligence/dealality-gdi-ui.js"),
  "utf8",
);
assert.ok(/id:\s*"portfolio"/.test(uiSrc), "portfolio tab id missing");
assert.ok(
  /GDI_CUSTOMER_TABS\s*=\s*\[\s*GDI_MAIN_TAB\s*,\s*GDI_PORTFOLIO_TAB\s*,\s*GDI_PRIORITIES_TAB\s*\]/.test(
    uiSrc,
  ),
  "portfolio must sit immediately left of Commercial Priorities",
);

const html = fs.readFileSync(
  path.join(ROOT, "public/group-demand-intelligence.html"),
  "utf8",
);
assert.ok(
  html.includes("portfolio-demand-intelligence-tab.js"),
  "HTML must load PDI-3 module",
);
assert.ok(html.includes('id="gdiDrawer"'), "must keep single gdiDrawer");
assert.ok(
  !/id="gdiPortfolioDrawer"|id="pdiDrawer"/.test(html),
  "must not add a second portfolio drawer",
);

const appSrc = fs.readFileSync(
  path.join(ROOT, "public/js/group-demand-intelligence/app.js"),
  "utf8",
);
assert.ok(/gdiPortfolioRoot/.test(appSrc), "app must mount #gdiPortfolioRoot");
assert.ok(
  /DealalityGdiPortfolioTab\.mount/.test(appSrc),
  "app must mount portfolio tab",
);
assert.ok(
  /onOpenBrief/.test(appSrc) && /openDetail/.test(appSrc),
  "portfolio open must reuse openDetail / existing drawer",
);

const tabPath = path.join(
  ROOT,
  "public/js/group-demand-intelligence/portfolio-demand-intelligence-tab.js",
);
const sandbox = { module: { exports: {} }, exports: {}, console, window: {} };
vm.runInNewContext(fs.readFileSync(tabPath, "utf8"), sandbox, {
  filename: "portfolio-demand-intelligence-tab.js",
});
const Tab = sandbox.module.exports || sandbox.window.DealalityGdiPortfolioTab;
assert.ok(Tab && typeof Tab.shellHtml === "function");

const unavailable = Tab.unavailableHtml("Registry missing.", { allowDemoCue: true });
assert.ok(/UNAVAILABLE/.test(unavailable));
assert.ok(/pdiDemo=1/.test(unavailable));
assert.ok(/never a production customer/i.test(unavailable));

const demo = Tab.demoPayload();
const liveShell = Tab.shellHtml({
  demo: true,
  markets: demo.markets,
  hotels: demo.hotels,
  marketId: demo.markets[0].marketId,
  opportunitiesPayload: demo.opportunitiesPayload,
});
assert.ok(/data-gdi-pdi-demo="1"/.test(liveShell), "demo banner required");
assert.ok(/DEMO — not a production customer/i.test(liveShell));
assert.ok(/data-gdi-pdi-matrix="1"/.test(liveShell));
assert.ok(/POTENTIAL_PRIMARY_HOST/.test(liveShell));
assert.ok(/INSUFFICIENT_EVIDENCE/.test(liveShell));
assert.ok(/UNPUBLISHED_HOTEL_DRAWER|Drawer withheld/i.test(liveShell));
assert.ok(/Supporting Research \(Level 2\)/.test(liveShell));

// Confidential keys must not appear in shared demo packet rendering path
assert.ok(!/hotelSalesFeedback/i.test(liveShell));

const matrix = Tab.renderMatrixHtml(
  {
    results: [
      {
        marketOpportunityId: "mog_1",
        sharedMarketPacket: {
          title: "Test Expo",
          summaryWhat: "Shared public packet only",
        },
        hotelAssessments: [
          {
            hotelId: "h1",
            fitClass: "OVERFLOW_ACCOMMODATION",
            confidence: "MEDIUM",
            recommendedPursuit: "Qualify overflow only.",
            drawer: {
              hotelId: "h1",
              hotelOpportunityId: "opp_h1",
              detailPath: "/api/group-demand-intelligence/hotels/h1/opportunities/opp_h1",
            },
          },
        ],
      },
    ],
  },
  [{ hotelId: "h1", hotelDisplayName: "Hotel One" }],
);
assert.ok(/data-gdi-pdi-open="1"/.test(matrix));
assert.ok(/data-opportunity-id="opp_h1"/.test(matrix));
assert.ok(/Open brief/.test(matrix));

fs.mkdirSync(REPORT, { recursive: true });
const summary = {
  version: "gdi-pdi-3-portfolio-tab-ui-v1",
  at: new Date().toISOString(),
  paidCalls: 0,
  airtableWrites: 0,
  publicationWrites: 0,
  ok: true,
  checks: {
    tabOrder: "opportunities → portfolio → priorities",
    singleDrawer: true,
    unavailableExplicit: true,
    demoLabeledNotSilent: true,
    reusesOpenDetail: true,
    level2NotePresent: true,
  },
};
fs.writeFileSync(path.join(REPORT, "RESULTS.json"), `${JSON.stringify(summary, null, 2)}\n`);
fs.writeFileSync(
  path.join(REPORT, "FOUNDER_RESULTS.md"),
  `# GDI PDI-3 — Portfolio Demand Intelligence tab

## Placement
\`Group & Demand Opportunities\` · **Portfolio Demand Intelligence** · \`Commercial Priorities\`

## Architecture
- Data: PDI-2 read-only API (\`/portfolio/markets|hotels|market-opportunities\`)
- Auth: verified Memberstack (same as PDI-2)
- Drawer: existing \`#gdiDrawer\` + Brief v2 / Supporting Research (Level 2) — **no second engine**
- Live registry missing → explicit **UNAVAILABLE** (optional labeled \`?pdiDemo=1\` DEMO only)

## Tests
\`npm run test:gdi-pdi-3-portfolio-tab-ui-v1\` — PASS

Paid/Airtable/publication: 0
`,
);

console.log(JSON.stringify(summary, null, 2));
