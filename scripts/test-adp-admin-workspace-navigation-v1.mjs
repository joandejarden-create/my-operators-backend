#!/usr/bin/env node
/**
 * AI Demand Admin workspace navigation regression.
 *
 * Every visible tab control must:
 *  - be listed in ALLOWED
 *  - toggle its destination panel
 *  - update parent hash ?tab=
 *
 * AI Visibility Audit must open #adaPanelVisibilityAudit, mount #avaTableBody,
 * and the audit API must respond (auth may return 401/403 on locked envs; never 404).
 *
 *   node scripts/test-adp-admin-workspace-navigation-v1.mjs
 *   npm run test:adp-admin-workspace-navigation-v1
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { chromium } from "playwright";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const BASE = (
  process.env.DEALALITY_LOCAL_ADMIN_BASE ||
  "http://127.0.0.1:8080"
).replace(/\/$/, "");

const EXPECTED_TABS = Object.freeze([
  { tab: "reviews", btn: "adaTabReviews", panel: "adaPanelReviews" },
  { tab: "action-plan", btn: "adaTabActionPlan", panel: "adaPanelActionPlan" },
  { tab: "gdi-reports", btn: "adaTabGdiReports", panel: "adaPanelGdiReports" },
  {
    tab: "report-archive",
    btn: "adaTabReportArchive",
    panel: "adaPanelReportArchive",
  },
  {
    tab: "visibility-audit",
    btn: "adaTabVisibilityAudit",
    panel: "adaPanelVisibilityAudit",
  },
  {
    tab: "gdi-research-audit",
    btn: "adaTabGdiResearchAudit",
    panel: "adaPanelGdiResearchAudit",
  },
]);

function staticContracts() {
  const shellJs = fs.readFileSync(
    path.join(ROOT, "public/js/admin-ai-demand-admin.js"),
    "utf8"
  );
  const html = fs.readFileSync(
    path.join(ROOT, "public/app/admin/ai-demand-admin.html"),
    "utf8"
  );
  const avaJs = fs.readFileSync(
    path.join(ROOT, "public/js/admin-ai-visibility-audit.js"),
    "utf8"
  );

  assert.match(
    shellJs,
    /"visibility-audit"\s*:\s*true/,
    "ALLOWED must include visibility-audit (silent click no-op regression)"
  );
  assert.match(
    shellJs,
    /"gdi-research-audit"\s*:\s*true/,
    "ALLOWED must include gdi-research-audit"
  );
  assert.match(
    shellJs,
    /adaTabVisibilityAudit/,
    "setActiveTab must wire adaTabVisibilityAudit"
  );
  assert.match(
    shellJs,
    /adaPanelVisibilityAudit/,
    "setActiveTab must show/hide adaPanelVisibilityAudit"
  );
  assert.match(
    shellJs,
    /adaTabGdiResearchAudit/,
    "setActiveTab must wire adaTabGdiResearchAudit"
  );
  assert.match(
    shellJs,
    /adaPanelGdiResearchAudit/,
    "setActiveTab must show/hide adaPanelGdiResearchAudit"
  );

  for (const t of EXPECTED_TABS) {
    assert.match(
      html,
      new RegExp(`data-tab="${t.tab}"`),
      `HTML missing data-tab=${t.tab}`
    );
    assert.match(html, new RegExp(`id="${t.btn}"`), `HTML missing #${t.btn}`);
    assert.match(
      html,
      new RegExp(`id="${t.panel}"`),
      `HTML missing #${t.panel}`
    );
  }

  assert.match(
    html,
    /id="avaTableBody"/,
    "AVA hotel table body must exist (truncated panel regression)"
  );
  assert.match(
    html,
    /id="avaDrawer"/,
    "AVA audit drawer must exist (truncated panel regression)"
  );
  assert.match(
    html,
    /admin-ai-visibility-audit\.js/,
    "AI Visibility Audit script must be loaded"
  );
  assert.match(
    html,
    /admin-gdi-research-audit\.js/,
    "GDI Research Audit script must be loaded"
  );
  assert.match(
    html,
    /id="graTableBody"/,
    "GDI Research Audit table body must exist"
  );
  // Panel must remain inside the gated workspace shell.
  const shellOpen = html.indexOf('id="supportGateContent"');
  const avaPanel = html.indexOf('id="adaPanelVisibilityAudit"');
  const shellCloseAfterAva = html.indexOf("</div>", html.indexOf('id="avaDrawer"'));
  assert.ok(shellOpen >= 0 && avaPanel > shellOpen, "AVA panel must be inside supportGateContent");
  assert.ok(
    shellCloseAfterAva > avaPanel,
    "supportGateContent must close after AVA panel"
  );

  assert.match(avaJs, /dealality-adp-admin-tab/);
  assert.match(avaJs, /visibility-audit/);
  assert.match(avaJs, /avaTableBody/);
}

async function browserContracts() {
  const shellRes = await fetch(
    `${BASE}/js/admin-ai-demand-admin.js?v=ada-gdi-research-audit-nav-v1`
  );
  assert.equal(shellRes.status, 200, "admin shell JS must be served");
  const shellText = await shellRes.text();
  assert.match(shellText, /"visibility-audit"\s*:\s*true/);
  assert.match(shellText, /"gdi-research-audit"\s*:\s*true/);

  const htmlRes = await fetch(`${BASE}/app/admin/ai-demand-admin.html`);
  assert.equal(htmlRes.status, 200, "AI Demand Admin page must be served");
  const htmlText = await htmlRes.text();
  assert.match(htmlText, /id="avaTableBody"/);
  assert.match(htmlText, /id="avaDrawer"/);

  // Canonical list endpoint (there is no bare /api/admin/ai-visibility-audit root).
  const apiRes = await fetch(`${BASE}/api/admin/ai-visibility-audit/hotels`);
  assert.notEqual(
    apiRes.status,
    404,
    "AI Visibility Audit /hotels API route must be registered"
  );
  assert.ok(
    [200, 401, 403, 500].includes(apiRes.status),
    `unexpected AVA hotels API status ${apiRes.status}`
  );

  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage();
  const pageErrors = [];
  page.on("pageerror", (err) =>
    pageErrors.push(String(err && err.message ? err.message : err))
  );

  // Local only: plain authFetch (no fake Bearer) — matches PDF download browser gate.
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
  await page.waitForTimeout(1200);

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
    const content = document.getElementById("supportGateContent");
    if (content) content.hidden = false;
    window.__ADP_ADMIN_WORKSPACE_AUTH__ = true;
    window.dispatchEvent(new CustomEvent("dealality-adp-admin-ready"));
  });

  await page.waitForSelector(".ada-tabs [data-tab]", {
    state: "visible",
    timeout: 15000,
  });

  const results = [];
  for (const t of EXPECTED_TABS) {
    await page.evaluate((id) => {
      const el = document.getElementById(id);
      if (!el) throw new Error("missing " + id);
      el.click();
    }, t.btn);
    await page.waitForTimeout(250);
    const state = await page.evaluate((spec) => {
      const btn = document.getElementById(spec.btn);
      const panel = document.getElementById(spec.panel);
      return {
        btnExists: Boolean(btn),
        panelExists: Boolean(panel),
        btnActive: btn ? btn.classList.contains("active") : false,
        ariaSelected: btn ? btn.getAttribute("aria-selected") : null,
        panelVisible: panel ? panel.hidden === false : false,
      };
    }, t);
    assert.equal(state.btnExists, true, `${t.tab}: button missing`);
    assert.equal(state.panelExists, true, `${t.tab}: panel missing`);
    assert.equal(state.btnActive, true, `${t.tab}: button not active after click`);
    assert.equal(state.ariaSelected, "true", `${t.tab}: aria-selected`);
    assert.equal(
      state.panelVisible,
      true,
      `${t.tab}: panel still hidden after click`
    );
    results.push({ tab: t.tab, ok: true, ...state });
  }

  // Deep-check GDI Research Audit destination mounts.
  await page.click("#adaTabGdiResearchAudit");
  await page.waitForSelector("#graTableBody", { state: "attached", timeout: 10000 });
  await page.waitForTimeout(400);
  const graUi = await page.evaluate(() => {
    const panel = document.getElementById("adaPanelGdiResearchAudit");
    const body = document.getElementById("graTableBody");
    const text = panel ? String(panel.textContent || "") : "";
    return {
      panelVisible: panel ? panel.hidden === false : false,
      hasCopy: /GDI Research Audit|research run/i.test(text),
      hasTableBody: Boolean(body),
      urlTab: new URLSearchParams(window.location.search).get("tab"),
    };
  });
  assert.equal(graUi.panelVisible, true, "GDI Research Audit panel must be visible");
  assert.equal(graUi.hasTableBody, true, "GDI Research Audit #graTableBody must mount");
  assert.ok(graUi.hasCopy, "GDI Research Audit panel must render audit copy");
  assert.equal(graUi.urlTab, "gdi-research-audit");

  // Deep-check AI Visibility Audit destination content mounts + loads.
  await page.click("#adaTabVisibilityAudit");
  await page.waitForSelector("#avaTableBody", { state: "attached", timeout: 10000 });
  await page.waitForTimeout(800);
  const avaUi = await page.evaluate(() => {
    const panel = document.getElementById("adaPanelVisibilityAudit");
    const body = document.getElementById("avaTableBody");
    const drawer = document.getElementById("avaDrawer");
    const text = panel ? String(panel.textContent || "") : "";
    const rowCount = body
      ? body.querySelectorAll("tr").length
      : 0;
    return {
      panelVisible: panel ? panel.hidden === false : false,
      hasAuditCopy:
        /AI Visibility Audit/i.test(text) ||
        /Visibility Audit/i.test(text) ||
        /Eligible Hotels/i.test(text),
      hasTableBody: Boolean(body),
      hasDrawer: Boolean(drawer),
      rowCount,
      childCount: panel ? panel.children.length : 0,
      urlTab: new URLSearchParams(window.location.search).get("tab"),
    };
  });
  assert.equal(avaUi.panelVisible, true, "AVA panel must be visible");
  assert.equal(avaUi.hasTableBody, true, "AVA #avaTableBody must mount");
  assert.equal(avaUi.hasDrawer, true, "AVA #avaDrawer must mount");
  assert.ok(
    avaUi.hasAuditCopy || avaUi.childCount > 0,
    "AVA panel must render audit UI content"
  );
  assert.ok(avaUi.rowCount >= 1, "AVA table must render at least one row");
  if (apiRes.status === 200) {
    await page.waitForFunction(
      () => document.querySelectorAll("#avaTableBody tr").length > 1,
      null,
      { timeout: 15000 }
    );
    const loaded = await page.evaluate(
      () => document.querySelectorAll("#avaTableBody tr").length
    );
    assert.ok(
      loaded > 1,
      "AVA hotels API 200 must populate more than a loading placeholder"
    );
    avaUi.rowCount = loaded;
  }

  // Ignore transient Memberstack noise; fail on Dealality shell/module errors.
  const dealalityErrors = pageErrors.filter(
    (m) =>
      !/ResizeObserver|Script error|Memberstack|Please log in again/i.test(m)
  );
  assert.equal(
    dealalityErrors.length,
    0,
    `unexpected page errors: ${dealalityErrors.join(" | ")}`
  );

  await browser.close();
  return { results, avaUi, graUi, apiStatus: apiRes.status };
}

staticContracts();
const browser = await browserContracts();
console.log(
  JSON.stringify(
    {
      ok: true,
      contract: "adp-admin-workspace-navigation-v1",
      tabs: EXPECTED_TABS.map((t) => t.tab),
      visibilityAuditRestored: true,
      gdiResearchAuditPresent: true,
      apiStatus: browser.apiStatus,
      avaUi: browser.avaUi,
      graUi: browser.graUi,
    },
    null,
    2
  )
);
