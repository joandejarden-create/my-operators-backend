#!/usr/bin/env node
/**
 * Full GDI client application parity — local share preview vs Railway share.
 *
 *   npm run test:gdi-full-client-app-parity
 *
 * Requires:
 *   GDI_BETHESDA_PRODUCTION_SHARE_TOKEN (or GDI_PARITY_SHARE_TOKEN)
 *   Local server at GDI_PARITY_LOCAL_URL (default http://127.0.0.1:8080)
 *   Production at GDI_PARITY_PRODUCTION_URL
 */
import assert from "node:assert/strict";
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { chromium } from "playwright";
import { spawn } from "node:child_process";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const outDir = path.join(
  ROOT,
  "reports/gdi-complete-application-release-parity-v1",
);
fs.mkdirSync(path.join(outDir, "screenshots"), { recursive: true });

const share =
  process.env.GDI_PARITY_SHARE_TOKEN ||
  (fs.readFileSync(path.join(ROOT, ".env"), "utf8").match(
    /^GDI_BETHESDA_PRODUCTION_SHARE_TOKEN=(.+)$/m,
  ) || [])[1]?.trim();
assert.ok(share, "share token required");

const localBase = (
  process.env.GDI_PARITY_LOCAL_URL || "http://127.0.0.1:8080"
).replace(/\/$/, "");
const prodBase = (
  process.env.GDI_PARITY_PRODUCTION_URL ||
  "https://my-operators-backend-production.up.railway.app"
).replace(/\/$/, "");

function sha256(s) {
  return crypto.createHash("sha256").update(s).digest("hex");
}

async function runAssetIdentity() {
  return new Promise((resolve, reject) => {
    const child = spawn(
      process.execPath,
      [path.join(ROOT, "scripts/gdi-frontend-asset-identity-v1.mjs")],
      {
        cwd: ROOT,
        env: { ...process.env, GDI_PARITY_PRODUCTION_URL: prodBase },
        stdio: ["ignore", "pipe", "pipe"],
      },
    );
    let out = "";
    child.stdout.on("data", (d) => (out += d));
    child.stderr.on("data", (d) => (out += d));
    child.on("close", (code) => {
      try {
        const parsed = JSON.parse(out.trim().split("\n").pop());
        resolve({ code, ...parsed });
      } catch {
        resolve({ code, raw: out });
      }
    });
    child.on("error", reject);
  });
}

async function extractShareUi(page, label) {
  await page.goto(
    `${label === "local" ? localBase : prodBase}/group-demand-intelligence-share.html?share=${encodeURIComponent(share)}`,
    { waitUntil: "domcontentloaded", timeout: 120000 },
  );
  await page.waitForSelector("[data-open], .gdi-card, .brand-card", {
    timeout: 120000,
  });
  // Reset filters if control present
  const reset = page.locator("#gdiResetViewBtn");
  if (await reset.count()) {
    await reset.click().catch(() => {});
    await page.waitForTimeout(500);
  }
  return page.evaluate(() => {
    const text = (sel) => {
      const el = document.querySelector(sel);
      return el ? (el.innerText || el.textContent || "").replace(/\s+/g, " ").trim() : null;
    };
    const typeSelect = document.querySelector(
      "#gdiOpportunityTypeFilter, select[name='opportunityType'], [data-filter='opportunityType']",
    );
    const typeLabel =
      typeSelect?.selectedOptions?.[0]?.textContent?.replace(/\s+/g, " ").trim() ||
      text(".gdi-opportunity-type-filter") ||
      null;
    const cards = [
      ...new Set(
        [...document.querySelectorAll("[data-open]")]
          .map((el) => el.getAttribute("data-open"))
          .filter(Boolean),
      ),
    ];
    const needStatus = document
      .querySelector("[data-hotel-need-date-status]")
      ?.getAttribute("data-hotel-need-date-status");
    const needText = text("[data-hotel-need-date-status]");
    const resetBadge = document.querySelector("#gdiFilterCountBadge");
    const toolbar = text(".aiv-filter-actions, .gdi-browse-filters, .gdi-toolbar");
    const sections = {
      resetPresent: Boolean(document.querySelector("#gdiResetViewBtn")),
      resetBadgeVisible: resetBadge
        ? resetBadge.style.display !== "none" && resetBadge.offsetParent !== null
        : false,
      resetBadgeText: resetBadge
        ? (resetBadge.textContent || "").trim()
        : null,
      opportunityTypeLabel: typeLabel,
      needDateStatus: needStatus || null,
      needDateText: needText,
      cardCount: cards.length,
      cardIds: cards.slice(0, 80),
      consoleProbe: null,
    };
    return sections;
  });
}

const report = {
  generatedAt: new Date().toISOString(),
  localBase,
  prodBase,
  fingerprintExpected:
    "441773211494cebaf50c03ef8921ccac6c57f31ac5cb2398585352b425e36f7f",
  gates: {},
  ok: false,
};

report.gates.assetIdentity = await runAssetIdentity();

const browser = await chromium.launch({ headless: true });
const context = await browser.newContext({
  viewport: { width: 1440, height: 900 },
});
const localPage = await context.newPage();
const prodPage = await context.newPage();
const localErrors = [];
const prodErrors = [];
localPage.on("pageerror", (e) => localErrors.push(String(e.message || e)));
prodPage.on("pageerror", (e) => prodErrors.push(String(e.message || e)));

let localUi;
let prodUi;
try {
  localUi = await extractShareUi(localPage, "local");
  await localPage.screenshot({
    path: path.join(outDir, "screenshots/local-share-reset.png"),
    fullPage: false,
  });
} catch (e) {
  localUi = { error: String(e?.message || e) };
}
try {
  prodUi = await extractShareUi(prodPage, "prod");
  await prodPage.screenshot({
    path: path.join(outDir, "screenshots/prod-share-reset.png"),
    fullPage: false,
  });
} catch (e) {
  prodUi = { error: String(e?.message || e) };
}

await browser.close();

report.gates.browser = {
  local: localUi,
  production: prodUi,
  localErrors: localErrors.slice(0, 10),
  prodErrors: prodErrors.slice(0, 10),
};

const failures = [];
if (report.gates.assetIdentity?.mismatchCount) {
  failures.push("asset_identity_mismatch");
}
if (localUi?.error) failures.push("local_share_extract_failed");
if (prodUi?.error) failures.push("prod_share_extract_failed");
if (
  localUi?.cardCount != null &&
  prodUi?.cardCount != null &&
  localUi.cardCount !== prodUi.cardCount
) {
  failures.push(
    `card_count_local_${localUi.cardCount}_prod_${prodUi.cardCount}`,
  );
}
if (
  localUi?.needDateStatus &&
  prodUi?.needDateStatus &&
  localUi.needDateStatus !== prodUi.needDateStatus
) {
  failures.push(
    `need_date_status_local_${localUi.needDateStatus}_prod_${prodUi.needDateStatus}`,
  );
}
if (
  localUi?.opportunityTypeLabel &&
  prodUi?.opportunityTypeLabel &&
  localUi.opportunityTypeLabel !== prodUi.opportunityTypeLabel
) {
  failures.push(
    `opportunity_type_label_local_${localUi.opportunityTypeLabel}_prod_${prodUi.opportunityTypeLabel}`,
  );
}
if (
  sha256(JSON.stringify(localUi?.cardIds || [])) !==
  sha256(JSON.stringify(prodUi?.cardIds || []))
) {
  failures.push("card_id_set_mismatch_at_reset");
}

report.failures = failures;
report.ok = failures.length === 0;

fs.writeFileSync(
  path.join(outDir, "09-VISUAL-REGRESSION-RESULTS.json"),
  JSON.stringify(report, null, 2),
);
fs.writeFileSync(
  path.join(outDir, "04-TOOLBAR-AND-FILTER-PARITY.json"),
  JSON.stringify(
    {
      ok:
        !failures.some((f) => f.includes("opportunity_type") || f.includes("card_")),
      local: {
        opportunityTypeLabel: localUi?.opportunityTypeLabel,
        resetBadgeText: localUi?.resetBadgeText,
        resetBadgeVisible: localUi?.resetBadgeVisible,
      },
      production: {
        opportunityTypeLabel: prodUi?.opportunityTypeLabel,
        resetBadgeText: prodUi?.resetBadgeText,
        resetBadgeVisible: prodUi?.resetBadgeVisible,
      },
      note: "All types (N) = opportunity count in current facet subset (other filters applied, type filter omitted). 61 typically = Hotel Market Scope; lower N means additional filters are active — not a distinct-type count.",
    },
    null,
    2,
  ),
);
fs.writeFileSync(
  path.join(outDir, "06-SVG-ICON-PARITY.json"),
  JSON.stringify(
    {
      needDateStatusLocal: localUi?.needDateStatus,
      needDateStatusProduction: prodUi?.needDateStatus,
      needDateTextLocal: localUi?.needDateText,
      needDateTextProduction: prodUi?.needDateText,
      ok: localUi?.needDateStatus === prodUi?.needDateStatus,
    },
    null,
    2,
  ),
);

console.log(
  JSON.stringify(
    {
      ok: report.ok,
      failures,
      assetOk: report.gates.assetIdentity?.ok,
      localCards: localUi?.cardCount,
      prodCards: prodUi?.cardCount,
      localNeed: localUi?.needDateStatus,
      prodNeed: prodUi?.needDateStatus,
      localType: localUi?.opportunityTypeLabel,
      prodType: prodUi?.opportunityTypeLabel,
    },
    null,
    2,
  ),
);
process.exit(report.ok ? 0 : 2);
