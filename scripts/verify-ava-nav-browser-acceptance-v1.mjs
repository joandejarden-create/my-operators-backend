#!/usr/bin/env node
/**
 * One-shot browser acceptance for AI Visibility Audit navigation restore.
 * Writes screenshots under reports/ava-nav-regression-v1/.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { chromium } from "playwright";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const BASE = (
  process.env.DEALALITY_LOCAL_ADMIN_BASE ||
  "http://127.0.0.1:8080"
).replace(/\/$/, "");
const OUT = path.join(ROOT, "reports/ava-nav-regression-v1");
fs.mkdirSync(OUT, { recursive: true });

const browser = await chromium.launch({ headless: true });
const page = await browser.newPage({ viewport: { width: 1440, height: 900 } });

await page.addInitScript(() => {
  const plain = {
    authFetch(url, opts) {
      return fetch(
        url,
        Object.assign({ credentials: "same-origin" }, opts || {})
      );
    },
  };
  Object.defineProperty(window, "DealalityMemberstackAuth", {
    configurable: true,
    get() {
      return plain;
    },
    set() {},
  });
});

await page.route("**/api/me**", async (route) => {
  await route.fulfill({
    status: 200,
    contentType: "application/json",
    body: JSON.stringify({
      success: true,
      ok: true,
      user: {
        firstName: "Demo",
        lastName: "Dealality",
        email: "dealalitydemo@dealality.com",
      },
      dealality: {
        email: "dealalitydemo@dealality.com",
        role: "admin",
        isAdmin: true,
        isOwner: true,
        isDemo: true,
        adpMonthlyReviewAdmin: true,
        localDealalityAdminAccess: true,
        flags: { isAdmin: true, isDemo: true },
      },
    }),
  });
});

await page.goto(`${BASE}/app/admin/ai-demand-admin.html?tab=reviews`, {
  waitUntil: "domcontentloaded",
  timeout: 60000,
});
await page.waitForTimeout(1000);

await page.evaluate(async () => {
  const allowed = await window.SupportAdminGate.requireAdpMonthlyReviewAdmin({
    contentId: "supportGateContent",
    loadingMessage: "Verifying…",
  });
  if (!allowed) throw new Error("admin_gate_denied");
  const loadingEl = document.getElementById("supportGateLoading");
  if (window.SupportAdminGate.hidePageLoading) {
    window.SupportAdminGate.hidePageLoading(loadingEl);
  }
  const denied = document.getElementById("supportGateDenied");
  if (denied) denied.hidden = true;
  const content = document.getElementById("supportGateContent");
  if (content) content.hidden = false;
  window.__ADP_ADMIN_WORKSPACE_AUTH__ = true;
  window.dispatchEvent(new CustomEvent("dealality-adp-admin-ready"));
});

await page.click("#adaTabVisibilityAudit");
await page.waitForSelector("#adaPanelVisibilityAudit:not([hidden])", {
  timeout: 10000,
});
await page.waitForFunction(
  () => document.querySelectorAll("#avaTableBody tr").length > 5,
  null,
  { timeout: 20000 }
);
await page.screenshot({
  path: path.join(OUT, "01-ava-tab-open-desktop.png"),
  fullPage: false,
});

await page.fill("#avaSearch", "Waterstone");
await page.click("#avaApplyFilters");
await page.waitForTimeout(400);
await page
  .locator("#avaTableBody tr", { hasText: "Waterstone" })
  .locator('button[aria-label="Open Audit"]')
  .click();
await page.waitForFunction(
  () => {
    const d = document.getElementById("avaDrawer");
    return d && d.hidden === false;
  },
  null,
  { timeout: 15000 }
);
await page.waitForTimeout(800);
await page.screenshot({
  path: path.join(OUT, "02-ava-waterstone-audit-drawer-desktop.png"),
  fullPage: false,
});

// Close drawer so it does not intercept later tab clicks.
await page.evaluate(() => {
  const close = document.getElementById("avaDrawerClose");
  if (close) close.click();
  const drawer = document.getElementById("avaDrawer");
  if (drawer) drawer.hidden = true;
});

await page.setViewportSize({ width: 390, height: 844 });
await page.evaluate(() => {
  if (typeof window.__ADP_ADMIN_SET_TAB__ === "function") {
    window.__ADP_ADMIN_SET_TAB__("visibility-audit");
  }
});
await page.waitForTimeout(300);
await page.screenshot({
  path: path.join(OUT, "03-ava-tab-open-mobile.png"),
  fullPage: false,
});

await page.evaluate(() => window.__ADP_ADMIN_SET_TAB__("reviews"));
await page.waitForSelector("#adaPanelReviews:not([hidden])");
await page.evaluate(() => window.__ADP_ADMIN_SET_TAB__("visibility-audit"));
await page.waitForSelector("#adaPanelVisibilityAudit:not([hidden])");

const state = await page.evaluate(() => ({
  tab: document
    .getElementById("adaTabVisibilityAudit")
    ?.classList.contains("active"),
  panel: !document.getElementById("adaPanelVisibilityAudit")?.hidden,
  rows: document.querySelectorAll("#avaTableBody tr").length,
  drawer: !document.getElementById("avaDrawer")?.hidden,
  drawerTitle: document.getElementById("avaDrawerTitle")?.textContent || null,
  drawerHasValidated: /AUDIT_VALIDATED|Validated/i.test(
    document.getElementById("avaDrawerBody")?.textContent || ""
  ),
}));

fs.writeFileSync(
  path.join(OUT, "browser-acceptance.json"),
  JSON.stringify({ ok: true, ...state }, null, 2)
);
console.log(JSON.stringify({ ok: true, ...state }, null, 2));
await browser.close();
