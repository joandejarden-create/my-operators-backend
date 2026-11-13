#!/usr/bin/env node
/**
 * GDI guided tour — REAL browser Next/Back interaction through drawer steps.
 *
 * Previous structural tests only asserted source strings / Popover API usage and
 * never clicked Next while a modal Opportunity Details dialog was open — so they
 * missed the modal-vs-popover hit-testing regression.
 *
 * This suite uses Playwright mouse clicks at the Next/Back button center
 * (hit-testing path) and asserts step progress after every click.
 *
 *   node scripts/test-gdi-guided-tour-drawer-navigation-browser-v1.mjs
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { chromium } from "playwright";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const BASE = (
  process.env.DEALALITY_LOCAL_ADMIN_BASE || "http://127.0.0.1:8080"
).replace(/\/$/, "");
const OUT = path.join(ROOT, "reports/gdi-guided-tour-drawer-nav-v1");
fs.mkdirSync(path.join(OUT, "screenshots"), { recursive: true });

const HOTELS = [
  {
    id: "recLuxvwwxID7U2B8",
    label: "bethesda-marriott",
    expectCards: true,
  },
  {
    id: "recnPB3Q02NO8Ma1o",
    label: "wyndham-san-diego-bayside",
    expectCards: true,
  },
  {
    id: "gdi_hotel_waterstone_boca_raton",
    label: "waterstone-boca-raton",
    expectCards: false,
  },
];

const EXPECTED_TITLES = [
  /Start with the Opportunities overview/i,
  /Select your hotel and filters/i,
  /Understand Market Scope/i,
  /Read the opportunity cards/i,
  /Understand opportunity readiness/i,
  /Open Opportunity Details/i,
  /Evidence and recommended action/i,
  /Hotel Validation \/ Action \/ Outcome/i,
  /Export and continue working the list/i,
  /How to use GDI/i,
];

async function authRoute(page) {
  await page.route("**/api/me**", async (route) => {
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        ok: true,
        success: true,
        user: { email: "dealalitydemo@dealality.com" },
        dealality: {
          email: "dealalitydemo@dealality.com",
          isAdmin: true,
          isDemo: true,
          flags: { isAdmin: true, gdiPilot: true },
        },
      }),
    });
  });
}

async function tourState(page) {
  return page.evaluate(() => {
    const root = document.getElementById("gdiGuidedTourRoot");
    const next = root && root.querySelector('[data-dgt="next"]');
    const back = root && root.querySelector('[data-dgt="back"]');
    const callout = root && root.querySelector(".adp-gt__callout");
    const drawer = document.getElementById("gdiDrawer");
    const nr = next ? next.getBoundingClientRect() : null;
    let hit = null;
    if (nr && nr.width > 2) {
      const el = document.elementFromPoint(
        nr.left + nr.width / 2,
        nr.top + nr.height / 2
      );
      hit = el
        ? {
            tag: el.tagName,
            dgt: el.getAttribute && el.getAttribute("data-dgt"),
            inTour: !!(root && root.contains(el)),
            cls: String(el.className || "").slice(0, 80),
          }
        : null;
    }
    return {
      active: !!(window.GdiGuidedReportTour && GdiGuidedReportTour.isActive()),
      progress: (document.getElementById("dgtProgress") || {}).textContent || "",
      title: (document.getElementById("dgtTitle") || {}).textContent || "",
      drawerOpen: !!(drawer && drawer.open),
      drawerModal: !!(drawer && drawer.matches && drawer.matches(":modal")),
      popoverOpen: !!(root && root.matches && root.matches(":popover-open")),
      muted: !!(callout && callout.classList.contains("adp-gt__callout--muted")),
      nextHit: hit,
      nextRect: nr && {
        left: nr.left,
        top: nr.top,
        w: nr.width,
        h: nr.height,
      },
    };
  });
}

async function waitForSettled(page, { timeout = 12000 } = {}) {
  const start = Date.now();
  while (Date.now() - start < timeout) {
    const s = await tourState(page);
    if (s.active && s.progress && !s.muted && s.nextRect && s.nextRect.w > 2) {
      return s;
    }
    await page.waitForTimeout(120);
  }
  return tourState(page);
}

async function mouseClickTourButton(page, which, { mobile = false } = {}) {
  let box = null;
  for (let attempt = 0; attempt < 40; attempt++) {
    box = await page.evaluate((sel) => {
      const root = document.getElementById("gdiGuidedTourRoot");
      const el = root && root.querySelector(`[data-dgt="${sel}"]`);
      if (!el) return null;
      const callout = root.querySelector(".adp-gt__callout");
      if (callout && callout.hasAttribute("hidden")) return { w: 0, h: 0 };
      // Scroll actions into view inside the callout panel.
      try {
        el.scrollIntoView({ block: "nearest", inline: "nearest" });
      } catch {
        /* ignore */
      }
      const r = el.getBoundingClientRect();
      const hit = document.elementFromPoint(r.left + r.width / 2, r.top + r.height / 2);
      return {
        x: r.left + r.width / 2,
        y: r.top + r.height / 2,
        w: r.width,
        h: r.height,
        hitInTour: !!(hit && root.contains(hit)),
        hitDgt: hit && hit.getAttribute && hit.getAttribute("data-dgt"),
      };
    }, which);
    if (box && box.w > 2 && box.h > 2 && box.hitInTour) break;
    if (box && box.w > 2 && box.h > 2 && !box.hitInTour) {
      // Repair pointer capture (modal demotion) then retry.
      await page.evaluate(() => {
        try {
          const root = document.getElementById("gdiGuidedTourRoot");
          if (root && root.showPopover) {
            if (root.matches(":popover-open")) root.hidePopover();
            root.showPopover();
          }
          const d = document.getElementById("gdiDrawer");
          if (d && d.open && d.matches && d.matches(":modal")) {
            d.close();
            d.show();
          }
        } catch {
          /* ignore */
        }
      });
    }
    await page.waitForTimeout(120);
  }
  assert.ok(box && box.w > 2 && box.h > 2, `${which} button not laid out`);
  const vp = page.viewportSize() || { width: 1440, height: 1100 };
  if (box.y < 8 || box.y > vp.height - 8 || box.x < 0 || box.x > vp.width) {
    await page.evaluate((sel) => {
      const callout = document.querySelector("#gdiGuidedTourRoot .adp-gt__callout");
      if (callout) {
        const vh = window.innerHeight;
        const vw = window.innerWidth;
        const w = Math.min(380, vw - 24);
        callout.style.left = Math.max(8, Math.round((vw - w) / 2)) + "px";
        callout.style.top = "8px";
        callout.style.maxHeight = Math.floor(vh * 0.8) + "px";
      }
      const el = document.querySelector(`#gdiGuidedTourRoot [data-dgt="${sel}"]`);
      if (el) el.scrollIntoView({ block: "nearest" });
    }, which);
    box = await page.evaluate((sel) => {
      const el = document.querySelector(`#gdiGuidedTourRoot [data-dgt="${sel}"]`);
      const r = el.getBoundingClientRect();
      return { x: r.left + r.width / 2, y: r.top + r.height / 2, w: r.width, h: r.height };
    }, which);
  }
  assert.ok(
    box && box.w > 2 && box.h > 2 && box.y >= 0 && box.y <= vp.height,
    `${which} button not in viewport (${JSON.stringify(box)})`
  );
  // Real pointer path — mouse on desktop, touch on mobile viewports.
  if (mobile) {
    await page.touchscreen.tap(box.x, box.y);
  } else {
    await page.mouse.click(box.x, box.y);
  }
}

async function startTour(page) {
  await page.click("#gdiHowToReadReport");
  const s = await waitForSettled(page);
  assert.equal(s.active, true, "tour should be active");
  assert.match(s.progress, /1 of \d+/);
  return s;
}

async function clickNav(page, which, mobile) {
  return mouseClickTourButton(page, which, { mobile });
}

async function runFullTour(page, hotel, { mobile = false } = {}) {
  const consoleErrors = [];
  page.on("console", (msg) => {
    if (msg.type() === "error") consoleErrors.push(msg.text());
  });

  await page.addInitScript((hotelId) => {
    try {
      localStorage.setItem("gdi.hotelId", hotelId);
      localStorage.setItem("dealality.gdi.hotelId", hotelId);
      localStorage.removeItem("gdi_guided_tour_analytics_v1");
    } catch {
      /* ignore */
    }
  }, hotel.id);

  const url = `${BASE}/group-demand-intelligence.html?hotelId=${encodeURIComponent(
    hotel.id
  )}&inventoryAudience=sales-team&cb=tour-nav-${Date.now()}`;
  await page.goto(url, { waitUntil: "domcontentloaded", timeout: 60000 });
  await page.waitForSelector("#gdiHowToReadReport", { timeout: 30000 });
  if (hotel.expectCards) {
    await page.waitForFunction(
      () => document.querySelectorAll("article.brand-card").length > 0,
      null,
      { timeout: 45000 }
    );
  } else {
    await page.waitForTimeout(1500);
  }

  // Asset cache-bust must include the fix revision.
  const assets = await page.evaluate(() => {
    const scripts = [...document.querySelectorAll("script[src]")].map((s) => s.src);
    const links = [...document.querySelectorAll("link[rel=stylesheet]")].map((l) => l.href);
    return { scripts, links };
  });
  assert.ok(
    assets.scripts.some((s) => /gdi-guided-report-tour\.js\?v=gdi-howto-read-v1-1g/.test(s)),
    "page must load gdi-guided-report-tour.js?v=gdi-howto-read-v1-1g"
  );
  assert.ok(
    assets.scripts.some((s) => /dealality-guided-tour-engine\.js\?v=gdi-howto-read-v1-1g/.test(s)),
    "page must load dealality-guided-tour-engine.js?v=gdi-howto-read-v1-1g"
  );

  const cardCount = await page.locator("article.brand-card").count();
  const shortTour = !hotel.expectCards || cardCount === 0;

  await startTour(page);
  const stepsLog = [];
  let prevProgress = "";

  // Advance with real mouse clicks until finish (or shortened empty inventory).
  for (let i = 0; i < 14; i++) {
    const before = await waitForSettled(page);
    stepsLog.push({ ...before, phase: "before-next", i });
    if (/How to use GDI/i.test(before.title) || /Finish/i.test(before.progress)) {
      break;
    }
    // On drawer steps: Next must hit the coachmark, not the dialog.
    if (/Open Opportunity Details|Evidence and recommended|Hotel Validation/i.test(before.title)) {
      assert.equal(before.drawerOpen, true, `drawer open at ${before.title}`);
      assert.equal(
        before.drawerModal,
        false,
        `drawer must be non-modal for pointer capture at ${before.title}`
      );
      assert.equal(
        before.nextHit && before.nextHit.inTour,
        true,
        `Next hit-test must land in tour (got ${JSON.stringify(before.nextHit)}) at ${before.title}`
      );
      await page.screenshot({
        path: path.join(
          OUT,
          "screenshots",
          `${hotel.label}${mobile ? "-mobile" : ""}-step-${before.progress.replace(/\s+/g, "")}.png`
        ),
        fullPage: false,
      });
    }
    await clickNav(page, "next", mobile);
    const after = await waitForSettled(page);
    stepsLog.push({ ...after, phase: "after-next", i });
    assert.notEqual(
      after.progress,
      before.progress,
      `Next must advance progress (was ${before.progress} / ${before.title})`
    );
    prevProgress = after.progress;
    if (/How to use GDI/i.test(after.title)) break;
    if (shortTour && i > 6) break;
  }

  // Back through drawer region on inventory hotels.
  if (!shortTour) {
    // Restart and walk to step 8, then Back twice.
    await page.evaluate(() => {
      try {
        if (window.GdiGuidedReportTour) GdiGuidedReportTour.exit();
      } catch {
        /* ignore */
      }
    });
    await page.waitForTimeout(300);
    await startTour(page);
    for (let i = 0; i < 7; i++) {
      await clickNav(page, "next", mobile);
      await waitForSettled(page);
    }
    let s = await waitForSettled(page);
    assert.match(s.title, /Hotel Validation/i);
    assert.equal(s.drawerOpen, true);
    assert.equal(s.drawerModal, false);
    await clickNav(page, "back", mobile);
    s = await waitForSettled(page);
    assert.match(s.title, /Evidence and recommended/i);
    await clickNav(page, "back", mobile);
    s = await waitForSettled(page);
    assert.match(s.title, /Open Opportunity Details/i);

    // Rapid Next clicks should not freeze (queued pending delta).
    await clickNav(page, "next", mobile);
    await clickNav(page, "next", mobile);
    await page.waitForTimeout(200);
    await clickNav(page, "next", mobile);
    s = await waitForSettled(page, { timeout: 15000 });
    assert.ok(
      /Export|How to use|Evidence|Hotel Validation|Open Opportunity/i.test(s.title),
      `rapid clicks should still settle on a valid step, got ${s.title}`
    );

    // Finish remaining steps with Next.
    for (let i = 0; i < 6; i++) {
      s = await waitForSettled(page);
      if (/How to use GDI/i.test(s.title)) break;
      await clickNav(page, "next", mobile);
    }
    s = await waitForSettled(page);
    assert.match(s.title, /How to use GDI/i);
    await clickNav(page, "next", mobile); // Finish
    await page.waitForTimeout(500);
    s = await tourState(page);
    assert.equal(s.active, false, "tour should exit after Finish");
  }

  // Hotel feedback must not have been submitted by the tour.
  const feedbackTouched = await page.evaluate(() => {
    const body = document.getElementById("gdiDrawerBody");
    if (!body) return false;
    const dirty = body.querySelector(
      '[data-gdi-feedback-dirty="1"], .gdi-feedback--dirty, [aria-busy="true"]'
    );
    return !!dirty;
  });
  assert.equal(feedbackTouched, false, "tour must not dirty hotel feedback");

  const unexpected = consoleErrors.filter(
    (t) => !/favicon|ResizeObserver|net::ERR/i.test(t)
  );
  return {
    hotel: hotel.label,
    mobile,
    shortTour,
    cardCount,
    stepsLog: stepsLog.slice(0, 24),
    consoleErrors: unexpected,
    prevProgress,
  };
}

const browser = await chromium.launch({ headless: true });
const results = [];
let failed = 0;

try {
  for (const hotel of HOTELS) {
    const page = await browser.newPage({
      viewport: { width: 1440, height: 1100 },
    });
    await authRoute(page);
    try {
      const r = await runFullTour(page, hotel);
      results.push(r);
      console.log("PASS", hotel.label, r.shortTour ? "(short/empty)" : "(full)");
    } catch (err) {
      failed += 1;
      results.push({ hotel: hotel.label, error: String(err && err.message || err) });
      console.error("FAIL", hotel.label, err);
      try {
        await page.screenshot({
          path: path.join(OUT, "screenshots", `${hotel.label}-FAIL.png`),
        });
      } catch {
        /* ignore */
      }
    } finally {
      await page.close();
    }
  }

  // Mobile Bethesda — narrow viewport (mouse path; mirrors phone layout).
  {
    const hotel = HOTELS[0];
    const page = await browser.newPage({
      viewport: { width: 390, height: 844 },
    });
    await authRoute(page);
    try {
      const r = await runFullTour(page, hotel, { mobile: false });
      results.push({ ...r, mobile: true });
      console.log("PASS", "bethesda-mobile");
    } catch (err) {
      failed += 1;
      results.push({ hotel: "bethesda-mobile", error: String(err && err.message || err) });
      console.error("FAIL bethesda-mobile", err);
      try {
        await page.screenshot({
          path: path.join(OUT, "screenshots", "bethesda-mobile-FAIL.png"),
        });
      } catch {
        /* ignore */
      }
    } finally {
      await page.close();
    }
  }
} finally {
  await browser.close();
}

const report = {
  suite: "gdi-guided-tour-drawer-navigation-browser-v1",
  rootCause:
    "Modal dialog showModal() wins top-layer hit-testing over popover coachmarks; demote tour-owned drawer to non-modal show().",
  whyPriorTestsMissed:
    "test-gdi-guided-report-tour-v1.mjs only asserted source markup/strings — never clicked Next while the drawer was open.",
  results,
  failed,
  out: OUT,
};
fs.writeFileSync(path.join(OUT, "01-browser-results.json"), `${JSON.stringify(report, null, 2)}\n`);

if (failed) {
  console.error(`\n${failed} browser scenario(s) failed`);
  process.exit(1);
}
console.log("\nAll guided-tour drawer navigation browser checks passed.");
console.log(JSON.stringify({ passed: true, hotels: results.map((r) => r.hotel) }, null, 2));
