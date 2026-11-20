#!/usr/bin/env node
/**
 * Browser certify: authenticated app shell auto-enables Current Saved | Proposed Update
 * on bare /app#/brand-explorer-combined (no manual query construction).
 *
 * Auth: mocked /api/me admin + synthetic JWT (NOT live Memberstack) — same pattern as
 * playwright-adp-owner-app-shell-property-access-v1.mjs.
 *
 *   node scripts/verify-be-founder-appshell-preview-auto-browser-v1.mjs
 *   DEALALITY_LOCAL_ADMIN_BASE=http://127.0.0.1:8080 node scripts/verify-be-founder-appshell-preview-auto-browser-v1.mjs
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { chromium } from "playwright";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const BASE = (
  process.env.DEALALITY_LOCAL_ADMIN_BASE ||
  "http://127.0.0.1:8082"
).replace(/\/$/, "");
const OUT = path.join(
  ROOT,
  "reports/brand-explorer/founder-product-preview-v1/appshell-auto-v1"
);
fs.mkdirSync(OUT, { recursive: true });

const CURIO_ID = "receQkxgjlezsc1xg";
const ASCEND_ID = "reclkgOzvAcBheUSo";
// Synthetic local shell token — deliberately NOT JWT-shaped (gitleaks jwt rule).
const FAKE_MS_JWT = "dealality-local-founder-preview-shell-token";

const ME_PAYLOAD = {
  ok: true,
  success: true,
  user: {
    firstName: "Dealality",
    lastName: "Founder",
    email: "dealalitydemo@dealality.com",
  },
  dealality: {
    role: "owner",
    isAdmin: true,
    isOwner: true,
    isDemo: true,
    email: "dealalitydemo@dealality.com",
    localDealalityAdminAccess: true,
    founderNavOverridesAvailable: true,
    workspaces: ["Owner", "Brand", "Operator"],
    canonicalWorkspaceOptions: {
      founderNavOverridesAvailable: true,
      source: "playwright_be_appshell_preview_auto",
    },
    flags: { isAdmin: true, isDemo: true },
  },
  data: {
    dealality: {
      role: "owner",
      isAdmin: true,
      isOwner: true,
      isDemo: true,
      email: "dealalitydemo@dealality.com",
      localDealalityAdminAccess: true,
      founderNavOverridesAvailable: true,
      flags: { isAdmin: true, isDemo: true },
    },
  },
};

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function forceShellAuth(page) {
  await page.evaluate((jwt) => {
    window.__dealalityMemberstackJwt = jwt;
    if (window.DealalityAppShellAuth && window.DealalityAppShellAuth.publishJwt) {
      window.DealalityAppShellAuth.publishJwt(jwt);
    }
    const gate = document.getElementById("dealalityAppShellAuthGate");
    if (gate) gate.hidden = true;
    window.dispatchEvent(
      new CustomEvent("dealality-shell-auth-ready", {
        detail: {
          ok: true,
          jwt,
          authorized: true,
          me: {
            ok: true,
            data: {
              dealality: {
                role: "owner",
                isAdmin: true,
                isOwner: true,
                isDemo: true,
                localDealalityAdminAccess: true,
                founderNavOverridesAvailable: true,
                flags: { isAdmin: true, isDemo: true },
              },
            },
          },
        },
      })
    );
  }, FAKE_MS_JWT);
}

async function waitForBeFrame(page, timeoutMs = 45000) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    const frames = page.frames();
    for (const frame of frames) {
      const url = frame.url() || "";
      if (/brand-explorer-combined\.html/i.test(url)) {
        return frame;
      }
    }
    await sleep(400);
  }
  throw new Error("brand-explorer-combined iframe not found");
}

async function waitForToolbar(frame, timeoutMs = 45000) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    const state = await frame.evaluate(() => {
      const tb = document.getElementById("beFounderProfileModeToolbar");
      if (!tb || tb.hasAttribute("hidden")) return { ok: false };
      const text = tb.innerText || "";
      return {
        ok: /CURRENT SAVED/i.test(text) && /PROPOSED UPDATE/i.test(text),
        text: text.slice(0, 400),
      };
    });
    if (state.ok) return state;
    await sleep(400);
  }
  throw new Error("founder profile mode toolbar not visible");
}

async function selectBrandInFrame(frame, brandId) {
  await frame.evaluate(async (id) => {
    const api = window.BrandExplorerAtelierFromApi;
    if (api && typeof api.loadBrandById === "function") {
      await api.loadBrandById(id);
      return;
    }
    const sel =
      document.querySelector("select[data-be-brand-select]") ||
      document.querySelector("#beBrandSelect") ||
      document.querySelector("select.be-brand-select");
    if (sel) {
      sel.value = id;
      sel.dispatchEvent(new Event("change", { bubbles: true }));
      return;
    }
    // Fallback: navigate iframe query
    const u = new URL(window.location.href);
    u.searchParams.set("id", id);
    u.searchParams.set("beInternalPreview", "1");
    window.location.href = u.pathname + u.search;
  }, brandId);
  await sleep(1500);
}

async function clickMode(frame, mode) {
  await frame.evaluate((m) => {
    const btn = document.querySelector(`[data-be-profile-mode-btn="${m}"]`);
    if (!btn) throw new Error("mode button missing: " + m);
    btn.click();
  }, mode);
  await sleep(2000);
}

async function readMeta(frame) {
  return frame.evaluate(() => {
    const tb = document.getElementById("beFounderProfileModeToolbar");
    const text = (tb && tb.innerText) || "";
    const hashMatch = text.match(/Content hash[\s\S]*?\n?([a-f0-9—-]{8,64}|—)/i);
    const verMatch = text.match(/Candidate version[\s\S]*?\n?([^\n]+)/i);
    const active =
      (document.querySelector("[data-be-profile-mode-btn].is-active") || {}).getAttribute?.(
        "data-be-profile-mode-btn"
      ) || "";
    const tabs = document.querySelectorAll(
      "[data-be-tab], .be-tab, [role='tab'], .atelier-tab"
    ).length;
    return {
      activeMode: active,
      toolbarSnippet: text.slice(0, 600),
      candidateVersion: verMatch ? verMatch[1].trim() : "",
      contentHashHint: hashMatch ? hashMatch[1].trim() : "",
      tabLikeCount: tabs,
      href: location.href,
    };
  });
}

const browser = await chromium.launch({ headless: true });
const context = await browser.newContext({ viewport: { width: 1440, height: 960 } });
const page = await context.newPage();

await page.route("**/api/me**", async (route) => {
  await route.fulfill({
    status: 200,
    contentType: "application/json",
    body: JSON.stringify(ME_PAYLOAD),
  });
});

await page.addInitScript((jwt) => {
  window.__dealalityMemberstackJwt = jwt;
}, FAKE_MS_JWT);

const bareUrl = `${BASE}/app#/brand-explorer-combined`;
await page.goto(bareUrl, { waitUntil: "domcontentloaded", timeout: 60000 });
await forceShellAuth(page);
await page.evaluate(() => {
  if (location.hash.indexOf("brand-explorer-combined") < 0) {
    location.hash = "#/brand-explorer-combined";
  }
});
await sleep(1500);
await forceShellAuth(page);

// Allow shell to inject beInternalPreview into hash + remount iframe
for (let i = 0; i < 40; i++) {
  const hash = await page.evaluate(() => location.hash);
  if (/beInternalPreview=1/.test(hash)) break;
  await forceShellAuth(page);
  await page.evaluate(() => {
    location.hash = "#/brand-explorer-combined";
  });
  await sleep(500);
}

const hashAfter = await page.evaluate(() => location.hash);
assert.match(
  hashAfter,
  /beInternalPreview=1/,
  "admin app shell must inject beInternalPreview into hash without manual URL editing"
);

const frame = await waitForBeFrame(page);
const frameUrl = frame.url();
assert.match(frameUrl, /beInternalPreview=1/, "iframe embed must include beInternalPreview=1");

await waitForToolbar(frame);
await page.screenshot({
  path: path.join(OUT, "01-appshell-bare-url-toolbar.png"),
  fullPage: true,
});

await selectBrandInFrame(frame, CURIO_ID);
await sleep(2000);
await waitForToolbar(frame);
await clickMode(frame, "saved");
const curioSaved = await readMeta(frame);
await page.screenshot({
  path: path.join(OUT, "02-curio-current-saved.png"),
  fullPage: true,
});

await clickMode(frame, "proposed");
const curioProposed = await readMeta(frame);
assert.equal(curioProposed.activeMode, "proposed");
assert.match(
  String(curioProposed.candidateVersion || curioProposed.toolbarSnippet),
  /phase2h|PROPOSED|NOT_APPROVED|NOT PUBLISHED|phase2h-component-parity/i
);
await page.screenshot({
  path: path.join(OUT, "03-curio-proposed-update.png"),
  fullPage: true,
});

await selectBrandInFrame(frame, ASCEND_ID);
await sleep(2000);
await waitForToolbar(frame);
await clickMode(frame, "proposed");
const ascendProposed = await readMeta(frame);
assert.equal(ascendProposed.activeMode, "proposed");
await page.screenshot({
  path: path.join(OUT, "04-ascend-proposed-update.png"),
  fullPage: true,
});

// Reload bare URL — preview injection must return
await page.goto(bareUrl, { waitUntil: "domcontentloaded", timeout: 60000 });
await forceShellAuth(page);
await sleep(2000);
for (let i = 0; i < 30; i++) {
  const h = await page.evaluate(() => location.hash);
  if (/beInternalPreview=1/.test(h)) break;
  await forceShellAuth(page);
  await page.evaluate(() => {
    location.hash = "#/brand-explorer-combined";
  });
  await sleep(400);
}
const hashReload = await page.evaluate(() => location.hash);
assert.match(hashReload, /beInternalPreview=1/);
const frame2 = await waitForBeFrame(page);
await waitForToolbar(frame2);
await page.screenshot({
  path: path.join(OUT, "05-reload-toolbar-restored.png"),
  fullPage: true,
});

await browser.close();

const report = {
  ok: true,
  contract: "BE_FOUNDER_APPSHELL_PREVIEW_AUTO_V1",
  base: BASE,
  bareAcceptanceUrl: bareUrl,
  authMode: "mocked_/api/me_admin + synthetic_jwt (NOT live Memberstack)",
  hashAfterInject: hashAfter,
  frameUrl,
  curioSaved: {
    mode: curioSaved.activeMode,
    candidateVersion: curioSaved.candidateVersion,
  },
  curioProposed: {
    mode: curioProposed.activeMode,
    candidateVersion: curioProposed.candidateVersion,
    contentHashHint: curioProposed.contentHashHint,
  },
  ascendProposed: {
    mode: ascendProposed.activeMode,
    candidateVersion: ascendProposed.candidateVersion,
    contentHashHint: ascendProposed.contentHashHint,
  },
  reloadHash: hashReload,
  screenshots: [
    "01-appshell-bare-url-toolbar.png",
    "02-curio-current-saved.png",
    "03-curio-proposed-update.png",
    "04-ascend-proposed-update.png",
    "05-reload-toolbar-restored.png",
  ],
  airtableWrites: false,
  founderApprovalStatus: "NOT_APPROVED",
  publicationGapDoc: "docs/brand-explorer/BE_PUBLICATION_GAP_AND_CURIO_WRITEBACK_V1.md",
};

fs.writeFileSync(path.join(OUT, "CERTIFICATION.json"), JSON.stringify(report, null, 2));
console.log(JSON.stringify(report, null, 2));
