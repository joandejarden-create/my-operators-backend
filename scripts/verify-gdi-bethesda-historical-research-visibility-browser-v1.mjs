#!/usr/bin/env node
/**
 * Browser evidence: Bethesda Historical Research Recovery panel in AI Demand Admin.
 *
 *   DEALALITY_LOCAL_ADMIN_BASE=http://127.0.0.1:8080 \
 *     node scripts/verify-gdi-bethesda-historical-research-visibility-browser-v1.mjs
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { chromium } from "playwright";
import { buildBethesdaHistoricalResearchVisibility } from "../lib/group-demand-intelligence/research/bethesda-historical-research-visibility-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const BASE = (
  process.env.DEALALITY_LOCAL_ADMIN_BASE ||
  "http://127.0.0.1:8080"
).replace(/\/$/, "");
const OUT = path.join(
  ROOT,
  "reports",
  "gdi-bethesda-historical-research-visibility-v1",
  "screenshots"
);
fs.mkdirSync(OUT, { recursive: true });

const recoveryPayload = {
  ok: true,
  success: true,
  ...buildBethesdaHistoricalResearchVisibility({ root: ROOT }),
  hotelStoreUntouched: true,
  customerPublication: false,
};

async function injectAdminAuth(page) {
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
  await page.route(
    "**/api/admin/group-demand-intelligence/hotels/*/historical-research-recovery**",
    async (route) => {
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify(recoveryPayload),
      });
    }
  );
  await page.route(
    "**/api/admin/group-demand-intelligence/reports**",
    async (route) => {
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          ok: true,
          hotels: [
            {
              hotelId: "recLuxvwwxID7U2B8",
              displayName: "Bethesda Marriott",
              runCount: 1,
            },
          ],
        }),
      });
    }
  );
  await page.route(
    "**/api/group-demand-intelligence/hotels/*/research-runs**",
    async (route) => {
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({ ok: true, runs: [] }),
      });
    }
  );
}

const browser = await chromium.launch({ headless: true });
const page = await browser.newPage({ viewport: { width: 1440, height: 1100 } });
await injectAdminAuth(page);

await page.goto(`${BASE}/app/admin/ai-demand-admin.html?tab=gdi-research-audit`, {
  waitUntil: "domcontentloaded",
  timeout: 60000,
});
await page.evaluate(async () => {
  if (!window.SupportAdminGate) return;
  await window.SupportAdminGate.requireAdpMonthlyReviewAdmin({
    contentId: "supportGateContent",
    loadingMessage: "Verifying…",
  });
  const loadingEl = document.getElementById("supportGateLoading");
  if (window.SupportAdminGate.hidePageLoading) {
    window.SupportAdminGate.hidePageLoading(loadingEl);
  }
  const content = document.getElementById("supportGateContent");
  if (content) content.hidden = false;
  window.__ADP_ADMIN_WORKSPACE_AUTH__ = true;
  if (window.__ADP_ADMIN_SET_TAB__) {
    window.__ADP_ADMIN_SET_TAB__("gdi-research-audit");
  }
  window.dispatchEvent(
    new CustomEvent("dealality-adp-admin-ready", { detail: { ok: true } })
  );
  window.dispatchEvent(
    new CustomEvent("dealality-adp-admin-tab", {
      detail: { tab: "gdi-research-audit" },
    })
  );
});

await page.waitForTimeout(800);
await page.evaluate(() => {
  const denied = document.getElementById("supportGateDenied");
  if (denied) denied.hidden = true;
  const content = document.getElementById("supportGateContent");
  if (content) content.hidden = false;
  const loading = document.getElementById("supportGateLoading");
  if (loading) loading.hidden = true;
  if (window.__ADP_ADMIN_SET_TAB__) {
    window.__ADP_ADMIN_SET_TAB__("gdi-research-audit");
  }
});
await page.selectOption("#graHotelSelect", "recLuxvwwxID7U2B8");
await page.waitForFunction(() => {
  const el = document.getElementById("graHistAttested");
  const panel = document.getElementById("graHistoricalRecovery");
  return (
    panel &&
    !panel.hidden &&
    el &&
    el.textContent &&
    el.textContent.trim() === "29"
  );
}, null, { timeout: 15000 });

await page.locator("#graHistoricalRecovery").scrollIntoViewIfNeeded();
await page.waitForTimeout(300);

const rendered = await page.evaluate(() => {
  const text = (id) =>
    (document.getElementById(id) && document.getElementById(id).textContent) ||
    "";
  const panel = document.getElementById("graHistoricalRecovery");
  const body = document.getElementById("graHistoricalTableBody");
  const rows = body ? [...body.querySelectorAll("tr")] : [];
  return {
    panelHidden: panel ? panel.hidden : true,
    deniedHidden: document.getElementById("supportGateDenied")?.hidden !== false,
    contentHidden: document.getElementById("supportGateContent")?.hidden === true,
    title:
      (document.querySelector(".gra-historical-recovery__title") || {})
        .textContent || "",
    canonical: text("graHistCanonical").trim(),
    attested: text("graHistAttested").trim(),
    continuation: text("graHistContinuation").trim(),
    catering: text("graHistCatering").trim(),
    partnerships: text("graHistPartnerships").trim(),
    other: text("graHistOther").trim(),
    rowCount: rows.length,
    firstOrg: rows[0] ? rows[0].querySelector("td")?.textContent || "" : "",
    hasHistOnly: /HISTORICAL_ATTESTATION_ONLY/.test(body ? body.innerText : ""),
    hasOverflow: /OVERFLOW_WATCH/.test(body ? body.innerText : ""),
    hasPartnershipWatch: /PARTNERSHIP_WATCH/.test(body ? body.innerText : ""),
    visibleSnippet: (panel && panel.innerText ? panel.innerText : "").slice(
      0,
      600
    ),
  };
});

assert.equal(rendered.panelHidden, false);
assert.equal(rendered.contentHidden, false);
assert.equal(rendered.attested, "29");
assert.equal(rendered.canonical, "59");
assert.equal(rendered.catering, "10");
assert.equal(rendered.partnerships, "8");
assert.equal(rendered.other, "11");
assert.equal(rendered.rowCount, 29);
assert.equal(rendered.hasHistOnly, true);
assert.equal(rendered.hasOverflow, true);
assert.equal(rendered.hasPartnershipWatch, true);
assert.match(rendered.title, /Historical Research Recovery/i);
assert.match(rendered.visibleSnippet, /Greater Bethesda Chamber/);

const shotPath = path.join(OUT, "bethesda-historical-research-recovery-desktop.png");
await page.locator("#graHistoricalRecovery").screenshot({ path: shotPath });

await browser.close();

console.log(
  JSON.stringify(
    {
      ok: true,
      contract: "gdi-bethesda-historical-research-visibility-browser-v1",
      base: BASE,
      route:
        "/app/admin/ai-demand-admin.html?tab=gdi-research-audit",
      clickPath: [
        "Open AI Demand Admin",
        "GDI Research Audit tab",
        "Select Bethesda Marriott",
        "View Historical Research Recovery panel",
      ],
      rendered: {
        canonical: rendered.canonical,
        attested: rendered.attested,
        continuation: rendered.continuation,
        catering: rendered.catering,
        partnerships: rendered.partnerships,
        other: rendered.other,
        rowCount: rendered.rowCount,
        firstOrg: rendered.firstOrg,
      },
      screenshot: path.relative(ROOT, shotPath).replace(/\\/g, "/"),
      authNote:
        "Browser used mocked /api/me + recovery payload (admin gate). Unauthenticated live API remains 403.",
    },
    null,
    2
  )
);
