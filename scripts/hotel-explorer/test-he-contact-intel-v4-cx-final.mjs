#!/usr/bin/env node
/**
 * Contact Intelligence V4 CX final gates.
 * node scripts/hotel-explorer/test-he-contact-intel-v4-cx-final.mjs [--browser]
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { createRequire } from "node:module";
import { fileURLToPath } from "node:url";
import {
  computeMissingContactChannels,
  surfeIncludeFromMissingState,
} from "../../lib/shared-contact-intelligence/missing-channel-enrichment-v3.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const OUT = path.join(ROOT, "reports/hotel-explorer/he-contact-intel-v4-cx-final");
const SHOTS = path.join(OUT, "SCREENSHOTS");
fs.mkdirSync(SHOTS, { recursive: true });
const BASE = (process.env.DEALALITY_LOCAL_ADMIN_BASE || "http://127.0.0.1:8080").replace(/\/$/, "");
const HOTEL = "recLuxvwwxID7U2B8";
const BROWSER = process.argv.includes("--browser");
const LIVE = process.argv.includes("--live-surfe") && process.argv.includes("--confirm-live-spend");

const results = [];
function gate(id, fn) {
  try {
    fn();
    results.push({ id, ok: true });
  } catch (err) {
    results.push({ id, ok: false, error: String(err?.message || err) });
  }
}

const js = fs.readFileSync(path.join(ROOT, "public/js/hotel-contact-intelligence.js"), "utf8");
const css = fs.readFileSync(path.join(ROOT, "public/css/hotel-contact-intelligence.css"), "utf8");

gate("CONTACT_ONE_CLICK_SURFE_DIRECT", () => {
  assert.match(js, /data-hci-one-click-surfe/);
  assert.doesNotMatch(js, /data-hci-hybrid-enrich="1"/);
  assert.match(js, /"\/reveal"/);
  assert.doesNotMatch(js, /useHybrid/);
});

gate("CONTACT_NO_FINDYMAIL_ON_USER_CLICK", () => {
  assert.match(js, /allowPaidFindymail:\s*false/);
  assert.doesNotMatch(js, /\/enrich-hybrid"/);
});

gate("CONTACT_NO_SECOND_PROVIDER_BUTTON", () => {
  assert.doesNotMatch(js, /Try Surfe \(session only\)/);
  assert.match(js, /paintSurfeFallbackOffer\(\) \{\s*\n\s*\/\/ CONTACT INTELLIGENCE V4/);
});

gate("CONTACT_DYNAMIC_MISSING_CHANNELS", () => {
  for (const c of [
    { email: "a@x.test", phone: "+1", expect: "none" },
    { email: "a@x.test", phone: null, expect: "phone" },
    { email: null, phone: "+1", expect: "email" },
    { email: null, phone: null, expect: "both" },
  ]) {
    const s = computeMissingContactChannels(c);
    const inc = surfeIncludeFromMissingState(s);
    assert.equal(s.requestChannel, c.expect);
    assert.equal(inc.email, c.expect === "email" || c.expect === "both");
    assert.equal(inc.mobile, c.expect === "phone" || c.expect === "both");
  }
});

gate("CONTACT_NO_INTERNAL_CUSTOMER_COPY", () => {
  assert.doesNotMatch(js, /Surfe · Session Only/);
  assert.doesNotMatch(js, /not persisted to Airtable/i);
  assert.doesNotMatch(js, /Try Surfe \(session only\)/);
  assert.doesNotMatch(js, /Lookup complete/);
  assert.doesNotMatch(js, /Hybrid Enrichment/i);
  assert.doesNotMatch(js, /Findymail durable results save to Airtable/);
  assert.match(js, /Results stay in this session only and are not saved to Dealality records/);
  assert.match(js, /gdi-contact-cta-note/);
  assert.doesNotMatch(js, /Available for this session/);
  assert.match(js, /Contact Details Found/);
  assert.match(js, /No Additional Contact Details Available/);
  assert.match(js, /Unable to Complete Lookup\. Please Try Again\./);
});

gate("CONTACT_NO_SURFE_PII_PERSISTENCE", () => {
  assert.match(js, /persist:\s*false/);
  assert.match(js, /_sessionSurfeByPerson/);
});

gate("CONTACT_CONTACT_DETAILS_LAYOUT_STABLE", () => {
  assert.match(css, /gdi-list-contact-link[\s\S]{0,220}white-space:\s*nowrap/);
  assert.doesNotMatch(css, /gdi-list-contact-link[\s\S]{0,160}overflow-wrap:\s*anywhere/);
  assert.match(css, /gdi-contact-cta-note/);
});

gate("CONTACT_TOP_RIGHT_TOAST_PARITY", () => {
  assert.match(css, /hci-enrich-loading-toast[\s\S]{0,200}top:\s*20px[\s\S]{0,80}right:\s*20px/);
  assert.match(js, /document\.body\.appendChild\(host\)/);
});

gate("CONTACT_DUPLICATE_CREDIT_PROTECTION", () => {
  assert.match(js, /data-session-lookup/);
  assert.match(js, /_inflightReveal/);
});

results.push({
  id: "CONTACT_LIVE_PROVIDER_NOT_TESTED",
  ok: false,
  blocked: !LIVE,
  note: LIVE
    ? "Live flag set"
    : "LIVE_PROVIDER_NOT_TESTED — no --live-surfe --confirm-live-spend",
});

async function launch() {
  const require = createRequire(
    path.join(process.env.DEALALITY_NODE_MODULES_ROOT || "C:/Dev/dealality/ws-gdi", "package.json"),
  );
  const { chromium } = require("playwright");
  try {
    return await chromium.launch({ headless: true });
  } catch {
    return chromium.launch({ channel: "chrome", headless: true });
  }
}

async function runBrowser() {
  const browser = await launch();
  const page = await browser.newPage({ viewport: { width: 1440, height: 900 } });
  const network = [];
  let release = () => {};
  const hold = LIVE
    ? Promise.resolve()
    : new Promise((r) => {
        release = r;
      });

  await page.route("**/api/contact-intelligence/hotels/*/people/*/reveal**", async (route) => {
    const req = route.request();
    if (req.method() !== "POST") return route.continue();
    let body = {};
    try {
      body = JSON.parse(req.postData() || "{}");
    } catch {
      body = {};
    }
    network.push({ type: "reveal", body, url: req.url() });
    if (LIVE) return route.continue();
    await hold;
    return route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        ok: true,
        mock: true,
        persistence: "NOT_WRITTEN",
        person: { email: "cx.v4@example.hotel", phone: "+13015550111" },
      }),
    });
  });
  page.on("request", (req) => {
    if (req.method() === "POST" && /enrich-hybrid|findymail/i.test(req.url())) {
      network.push({ type: "forbidden", url: req.url() });
    }
  });

  await page.goto(
    `${BASE}/deal-capture-radar-with-ranked-list.html?hotelExplorer=${HOTEL}&hexTab=contacts&cb=${Date.now()}`,
    { waitUntil: "domcontentloaded", timeout: 90000 },
  );
  await page.waitForFunction(() => typeof window.HotelExplorer === "object", null, { timeout: 45000 });
  await page.evaluate(
    (id) => HotelExplorer.open({ id, recordId: id }, { tab: "contacts", requestedHotelId: id }),
    HOTEL,
  );
  await page.waitForFunction(() => {
    const tr = [...document.querySelectorAll("tr")].find((r) => /Meinrad/i.test(r.innerText || ""));
    return !!(tr && tr.querySelector("[data-hci-enrich][data-hci-one-click-surfe]"));
  }, null, { timeout: 90000 });

  assert.equal(await page.locator("[data-hci-surfe-fallback]").count(), 0);
  assert.equal(await page.locator("[data-hci-hybrid-enrich]").count(), 0);

  const forbiddenCopy = await page.evaluate(() => {
    const text = document.body.innerText || "";
    return {
      surfeLabel: /Surfe · Session Only/i.test(text),
      airtable: /not persisted to Airtable|save to Airtable/i.test(text),
      trySurfe: /Try Surfe/i.test(text),
      hybrid: /Hybrid Enrichment/i.test(text),
    };
  });
  assert.equal(forbiddenCopy.surfeLabel, false);
  assert.equal(forbiddenCopy.trySurfe, false);
  assert.equal(forbiddenCopy.hybrid, false);

  const meinrad = page.locator("tr").filter({ hasText: /Meinrad/i }).first();
  const enrichBtn = meinrad.locator("[data-hci-enrich][data-hci-one-click-surfe]");
  await page.waitForFunction(() => {
    const tr = [...document.querySelectorAll("tr")].find((r) => /Meinrad/i.test(r.innerText || ""));
    const btn = tr && tr.querySelector("[data-hci-enrich][data-hci-one-click-surfe]");
    return !!(btn && !btn.disabled);
  }, null, { timeout: 15000 });
  await enrichBtn.click();
  await page.waitForTimeout(280);
  const toast = await page.evaluate(() => {
    const el = document.getElementById("hci-enrich-loading-toast");
    if (!el) return { visible: false };
    const r = el.getBoundingClientRect();
    const style = getComputedStyle(el);
    const shown =
      style.display !== "none" && Number(style.opacity || "0") > 0.05 && r.width > 0;
    return {
      visible: shown,
      upperRight: shown && r.top <= 120 && window.innerWidth - r.right <= 120,
      outsideTable: !el.closest("table"),
      text: (el.innerText || "").slice(0, 120),
    };
  });
  await page.screenshot({ path: path.join(SHOTS, "01_toast.png"), fullPage: false });
  release();

  if (!LIVE) {
    await page.waitForFunction(() => {
      const tr = [...document.querySelectorAll("tr")].find((r) => /Meinrad/i.test(r.innerText || ""));
      return !!(
        tr &&
        /cx\.v4@example\.hotel/i.test(tr.innerText || "") &&
        /Results stay in this session only/i.test(tr.innerText || "") &&
        tr.querySelector("[data-hci-session-note]")
      );
    }, null, { timeout: 15000 });
    // Soft remounts must keep session results for the same hotel — wait through one remount cycle.
    await page.waitForTimeout(1800);
    await page.waitForFunction(() => {
      const tr = [...document.querySelectorAll("tr")].find((r) => /Meinrad/i.test(r.innerText || ""));
      return !!(
        tr &&
        /cx\.v4@example\.hotel/i.test(tr.innerText || "") &&
        tr.querySelector("[data-hci-session-note]") &&
        tr.querySelector("[data-hci-enrich]")
      );
    }, null, { timeout: 20000 });
  } else {
    await page.waitForTimeout(90000);
  }
  await page.screenshot({ path: path.join(SHOTS, "02_after_one_click.png"), fullPage: false });

  // Re-query after outerHTML channel refresh — prior locator may be detached.
  const meinradLive = page.locator("tr").filter({ hasText: /Meinrad/i }).first();
  const after = await meinradLive.evaluate((tr) => {
    const text = (tr.innerText || "").replace(/\s+/g, " ");
    const stack = tr.querySelector("[data-hci-channel-stack]");
    const emailLink = stack && stack.querySelector('a[href^="mailto:"]');
    const phoneLink = stack && stack.querySelector('a[href^="tel:"]');
    const note = tr.querySelector("[data-hci-session-note]");
    const action = tr.querySelector("[data-hci-enrich-slot]");
    const sessionTagInStack = stack
      ? [...stack.querySelectorAll(".gdi-list-session-tag")].some((el) => {
          const s = getComputedStyle(el);
          return s.display !== "none" && s.visibility !== "hidden";
        })
      : false;
    function singleLine(el) {
      if (!el) return null;
      const rects = el.getClientRects();
      return { whiteSpace: getComputedStyle(el).whiteSpace, lines: rects.length };
    }
    return {
      text: text.slice(0, 480),
      hasSession: /Results stay in this session only/i.test(text),
      noteUnderButton: !!(note && action && action.contains(note)),
      noteInStack: !!(stack && (stack.querySelector("[data-hci-session-note]") || sessionTagInStack)),
      hasTrySurfe: !!tr.querySelector("[data-hci-surfe-fallback]"),
      hasTech: /Surfe|Airtable|Findymail|Hybrid/i.test(text),
      rowHeight: Math.round(tr.getBoundingClientRect().height),
      emailLine: singleLine(emailLink),
      phoneLine: singleLine(phoneLink),
      emailVisible: /cx\.v4@example\.hotel|@/.test(text),
      phoneFormatted: /\+1\s*\(\d{3}\)/.test(text),
      sessionLocked: !!tr.querySelector('[data-hci-enrich][data-session-lookup="1"]'),
    };
  });

  assert.equal(after.hasTrySurfe, false);
  assert.equal(after.hasTech, false);
  assert.ok(after.rowHeight < 260, "row too tall " + after.rowHeight);
  assert.equal(network.filter((n) => n.type === "forbidden").length, 0);
  assert.ok(network.some((n) => n.type === "reveal"));
  if (!LIVE) {
    assert.ok(after.hasSession, after.text);
    assert.ok(after.noteUnderButton, "session note must sit under the button");
    assert.equal(after.noteInStack, false);
    assert.ok(after.emailVisible, after.text);
    assert.ok(
      after.sessionLocked || after.noteUnderButton,
      "session must lock button or keep GDI note under dimmed control",
    );
    assert.ok(after.emailLine && after.emailLine.lines <= 1, JSON.stringify(after.emailLine));
    assert.ok(after.phoneLine && after.phoneLine.lines <= 1, JSON.stringify(after.phoneLine));
    assert.ok(after.phoneFormatted, after.text);
  }

  // Duplicate click should not fire another reveal
  const beforeDup = network.filter((n) => n.type === "reveal").length;
  await meinradLive.locator("[data-hci-enrich]").first().click({ force: true }).catch(() => {});
  await page.waitForTimeout(400);
  assert.equal(network.filter((n) => n.type === "reveal").length, beforeDup);

  const vpResults = [];
  for (const [w, h] of [
    [1920, 1080],
    [1440, 900],
    [1366, 768],
    [768, 1024],
    [390, 844],
  ]) {
    await page.setViewportSize({ width: w, height: h });
    await page.waitForTimeout(300);
    const rowNow = page.locator("tr").filter({ hasText: /Meinrad/i }).first();
    const hRow = await rowNow.evaluate((tr) => Math.round(tr.getBoundingClientRect().height));
    vpResults.push({ viewport: `${w}x${h}`, rowHeight: hRow, ok: hRow < 320 });
    await page.screenshot({ path: path.join(SHOTS, `vp-${w}x${h}.png`), fullPage: false });
  }
  await browser.close();
  return { toast, after, network, vpResults };
}

if (BROWSER) {
  try {
    const br = await runBrowser();
    results.push({ id: "CONTACT_SURFE_SESSION_RESULT_VISIBLE", ok: Boolean(br.after.hasSession || LIVE) });
    results.push({ id: "CONTACT_NO_SECOND_PROVIDER_BUTTON", ok: !br.after.hasTrySurfe });
    results.push({
      id: "CONTACT_DUPLICATE_CREDIT_PROTECTION",
      ok: br.network.filter((n) => n.type === "reveal").length === 1,
    });
    results.push({
      id: "CONTACT_TOP_RIGHT_TOAST_PARITY",
      ok: br.toast.visible && br.toast.upperRight && br.toast.outsideTable,
      detail: br.toast,
    });
    results.push({
      id: "CONTACT_CONTACT_DETAILS_LAYOUT_STABLE",
      ok: br.after.rowHeight < 240 && br.vpResults.every((v) => v.ok),
      detail: br.vpResults,
    });
    results.push({
      id: "CONTACT_NO_FINDYMAIL_ON_USER_CLICK",
      ok: !br.network.some((n) => n.type === "forbidden"),
    });
    results.push({
      id: "CONTACT_NO_INTERNAL_CUSTOMER_COPY",
      ok: !br.after.hasTech,
    });
    fs.writeFileSync(path.join(OUT, "BROWSER_RESULT.json"), JSON.stringify(br, null, 2));
  } catch (err) {
    results.push({ id: "BROWSER_CERT", ok: false, error: String(err?.stack || err).slice(0, 900) });
  }
}

const failed = results.filter((r) => !r.ok && !r.blocked);
const summary = {
  at: new Date().toISOString(),
  classification: failed.length
    ? "HE_CONTACT_INTEL_V4_CX_NOT_CERTIFIED"
    : BROWSER
      ? "HE_CONTACT_INTEL_V4_CX_CERTIFIED_MOCKED"
      : "HE_CONTACT_INTEL_V4_CX_STATIC_PASS",
  liveSurfe: LIVE,
  passed: results.filter((r) => r.ok).length,
  failed: failed.length,
  blocked: results.filter((r) => r.blocked).length,
  results,
};
fs.writeFileSync(path.join(OUT, "CERTIFICATION.json"), JSON.stringify(summary, null, 2));
console.log(
  JSON.stringify(
    {
      ok: !failed.length,
      classification: summary.classification,
      passed: summary.passed,
      failed: summary.failed,
      blocked: summary.blocked,
    },
    null,
    2,
  ),
);
if (failed.length) {
  console.error(failed);
  process.exit(1);
}
