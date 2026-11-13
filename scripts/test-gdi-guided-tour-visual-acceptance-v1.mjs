#!/usr/bin/env node
/**
 * GDI guided tour visual acceptance — production (or local) share URL.
 *
 * Fails if drawer steps leave a blank dark field / mispositioned drawer even
 * when Next remains clickable.
 *
 *   GDI_PARITY_SHARE_TOKEN=... node scripts/test-gdi-guided-tour-visual-acceptance-v1.mjs
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { chromium } from "playwright";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const outDir = path.join(
  ROOT,
  "reports/gdi-guided-tour-production-visual-regression-v1",
);
fs.mkdirSync(path.join(outDir, "screenshots"), { recursive: true });

const share =
  process.env.GDI_PARITY_SHARE_TOKEN ||
  (fs.readFileSync(path.join(ROOT, ".env"), "utf8").match(
    /^GDI_BETHESDA_PRODUCTION_SHARE_TOKEN=(.+)$/m,
  ) || [])[1]?.trim();
assert.ok(share, "share token required");

const base = (
  process.env.GDI_PARITY_PRODUCTION_URL ||
  process.env.GDI_PARITY_LOCAL_URL ||
  "https://my-operators-backend-production.up.railway.app"
).replace(/\/$/, "");

const EXPECTED_TITLES = [
  /Start with the Opportunities overview/i,
  /Select your hotel and filters/i,
  /Understand Market Scope/i,
  /Read the opportunity cards/i,
  /Understand opportunity readiness/i,
  /Open Opportunity Details/i,
  /Evidence and recommended action/i,
  /Hotel Validation/i,
  /Export and continue/i,
  /How to use GDI/i,
];

const browser = await chromium.launch({ headless: true });
const page = await browser.newPage({ viewport: { width: 1440, height: 900 } });
const consoleErrors = [];
page.on("pageerror", (e) => consoleErrors.push(String(e.message || e)));

await page.goto(
  `${base}/group-demand-intelligence-share.html?share=${encodeURIComponent(share)}`,
  { waitUntil: "domcontentloaded", timeout: 120000 },
);
await page.waitForSelector("#gdiHowToReadReport, .adp-howto-read-btn", {
  timeout: 120000,
});
await page.waitForTimeout(1200);
await page.locator("#gdiHowToReadReport, .adp-howto-read-btn").first().click();
await page.waitForSelector(".adp-gt__callout:not([hidden])", { timeout: 30000 });

const steps = [];
const failures = [];

for (let i = 0; i < 10; i++) {
  await page.waitForTimeout(700);
  const title = await page.locator("#dgtTitle, .adp-gt__title").first().innerText();
  assert.match(title, EXPECTED_TITLES[i], `step ${i + 1} title`);

  const geo = await page.evaluate(() => {
    const drawer = document.getElementById("gdiShareDrawer");
    const callout = document.querySelector(".adp-gt__callout");
    const highlight = document.querySelector(".adp-gt__highlight");
    const next = document.querySelector('[data-dgt="next"]');
    const r = (el) => {
      if (!el) return null;
      const b = el.getBoundingClientRect();
      return {
        x: Math.round(b.x),
        y: Math.round(b.y),
        w: Math.round(b.width),
        h: Math.round(b.height),
      };
    };
    const nextHit = (() => {
      if (!next) return null;
      const b = next.getBoundingClientRect();
      const el = document.elementFromPoint(
        b.left + b.width / 2,
        b.top + b.height / 2,
      );
      return {
        tag: el?.tagName || null,
        inTour: !!(el && el.closest && el.closest(".adp-gt")),
      };
    })();
    const center = document.elementFromPoint(
      window.innerWidth / 2,
      window.innerHeight / 2,
    );
    return {
      drawer: r(drawer),
      callout: r(callout),
      highlight: r(highlight),
      drawerOpen: !!(drawer && drawer.open),
      drawerModal: (() => {
        try {
          return !!(drawer && drawer.matches(":modal"));
        } catch {
          return null;
        }
      })(),
      tourNonModalClass: !!(
        drawer && drawer.classList.contains("gdi-drawer--tour-nonmodal")
      ),
      nextHit,
      centerTag: center?.tagName || null,
      centerClass: String(center?.className || "").slice(0, 80),
      title: document.querySelector("#dgtTitle")?.textContent || "",
    };
  });

  const shot = path.join(
    outDir,
    "screenshots",
    `accept-step-${i + 1}of10.png`,
  );
  await page.screenshot({ path: shot, fullPage: false });

  const stepFail = [];
  if (!geo.callout || geo.callout.w < 80 || geo.callout.h < 80) {
    stepFail.push("callout_not_visible");
  }
  if (geo.callout && (geo.callout.x < -20 || geo.callout.y < -20)) {
    stepFail.push("callout_offscreen");
  }
  if (!geo.nextHit?.inTour) stepFail.push("next_not_hittable");

  // Drawer steps 6–8 (0-indexed 5–7)
  if (i >= 5 && i <= 7) {
    if (!geo.drawerOpen) stepFail.push("drawer_closed");
    if (geo.drawerModal) stepFail.push("drawer_still_modal");
    if (!geo.tourNonModalClass) stepFail.push("missing_tour_nonmodal_class");
    if (!geo.drawer || geo.drawer.y > 48) {
      stepFail.push(`drawer_not_top_rail_y=${geo.drawer?.y}`);
    }
    if (!geo.drawer || geo.drawer.w < 280) stepFail.push("drawer_too_narrow");
    if (!geo.highlight || geo.highlight.w < 40 || geo.highlight.h < 40) {
      stepFail.push("highlight_missing");
    }
    // Step 6 targets the full drawer rail — cutout must be near full height.
    if (i === 5 && (!geo.highlight || geo.highlight.h < Math.floor(900 * 0.55))) {
      stepFail.push(`drawer_highlight_too_short_h=${geo.highlight?.h}`);
    }
    // Coachmark must not cover the drawer rail (leave ≥24px clearance).
    if (
      i === 5 &&
      geo.callout &&
      geo.drawer &&
      geo.callout.x + geo.callout.w > geo.drawer.x - 24
    ) {
      stepFail.push("callout_overlaps_drawer");
    }
    // Center of viewport should not be bare HTML through an empty scrim hole.
    if (geo.centerTag === "HTML") {
      stepFail.push("center_hits_blank_html");
    }
  }

  steps.push({ index: i + 1, title, geo, shot, stepFail });
  for (const f of stepFail) failures.push(`step${i + 1}:${f}`);

  if (i < 9) {
    const prevTitle = title;
    await page.locator('[data-dgt="next"]').click({ timeout: 15000 });
    await page.waitForFunction(
      (prev) => {
        const t = document.querySelector("#dgtTitle")?.textContent || "";
        return t && t !== prev;
      },
      prevTitle,
      { timeout: 20000 },
    );
  }
}

await browser.close();

const report = {
  ok: failures.length === 0,
  base,
  tourVersionExpected: "GDI_GUIDED_REPORT_TOUR_V1_1g",
  failures,
  consoleErrors: consoleErrors.slice(0, 20),
  steps,
};
fs.writeFileSync(
  path.join(outDir, "05-GUIDED-TOUR-STEP-RESULTS.json"),
  JSON.stringify(report, null, 2),
);
console.log(
  JSON.stringify(
    { ok: report.ok, failures, consoleErrors: consoleErrors.length },
    null,
    2,
  ),
);
process.exit(report.ok ? 0 : 2);
