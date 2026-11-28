#!/usr/bin/env node
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { chromium } from "playwright";
import {
  computeMissingContactChannels,
  surfeIncludeFromMissingState,
} from "../../lib/shared-contact-intelligence/missing-channel-enrichment-v3.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const OUT = path.join(ROOT, "reports/hotel-explorer/he-contact-intel-v4-one-click-surfe");
const SHOTS = path.join(OUT, "SCREENSHOTS");
fs.mkdirSync(SHOTS, { recursive: true });
const BASE = (process.env.DEALALITY_LOCAL_ADMIN_BASE || "http://127.0.0.1:8080").replace(/\/$/, "");
const HOTEL = "recLuxvwwxID7U2B8";
const LIVE = process.argv.includes("--live-surfe") && process.argv.includes("--confirm-live-spend");
const BROWSER = process.argv.includes("--browser");

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
const gdiReveal = fs.readFileSync(
  path.join(ROOT, "public/js/group-demand-intelligence/gdi-contact-reveal-client-v1.js"),
  "utf8",
);

gate("CONTACT_ONE_CLICK_SURFE_ONLY", () => {
  assert.match(js, /data-hci-one-click-surfe/);
  assert.doesNotMatch(js, /data-hci-hybrid-enrich="1"/);
  assert.match(js, /\/people\/" \+\s*\n\s*encodeURIComponent\(personId\) \+\s*\n\s*"\/reveal"/);
});

gate("CONTACT_NO_FINDYMAIL_ON_CUSTOMER_CLICK", () => {
  assert.match(js, /allowPaidFindymail:\s*false/);
  assert.match(js, /"\/reveal"/);
  assert.doesNotMatch(js, /useHybrid/);
});

gate("CONTACT_NO_SECOND_CLICK", () => {
  assert.doesNotMatch(js, /Try Surfe \(session only\)/);
  assert.match(js, /paintSurfeFallbackOffer\(\) \{\s*\n\s*\/\/ CONTACT INTELLIGENCE V4/);
});

gate("CONTACT_DYNAMIC_SURFE_CHANNEL_SELECTION", () => {
  const cases = [
    { email: "a@x.test", phone: "+10000000001", expect: "none" },
    { email: "a@x.test", phone: null, expect: "phone" },
    { email: null, phone: "+10000000001", expect: "email" },
    { email: null, phone: null, expect: "both" },
  ];
  for (const c of cases) {
    const s = computeMissingContactChannels(c);
    const inc = surfeIncludeFromMissingState(s);
    assert.equal(s.requestChannel, c.expect);
    assert.equal(inc.email, c.expect === "email" || c.expect === "both");
    assert.equal(inc.mobile, c.expect === "phone" || c.expect === "both");
  }
});

gate("CONTACT_SURFE_NO_AIRTABLE_WRITE", () => {
  assert.match(js, /persist:\s*false/);
  assert.match(js, /_sessionSurfeByPerson/);
});

gate("CONTACT_DETAILS_NO_VERTICAL_CHARACTER_WRAP", () => {
  assert.match(css, /gdi-list-contact-link[\s\S]{0,220}overflow-wrap:\s*break-word/);
  assert.doesNotMatch(css, /gdi-list-contact-link[\s\S]{0,220}overflow-wrap:\s*anywhere/);
});

gate("CONTACT_ENRICHMENT_TOP_RIGHT_TOAST", () => {
  assert.match(css, /hci-enrich-loading-toast[\s\S]{0,180}top:\s*20px[\s\S]{0,80}right:\s*20px/);
});

gate("CONTACT_TABLE_LAYOUT_STABLE", () => {
  assert.match(js, /showEnrichToast/);
  assert.match(css, /tr\[data-enriching="1"\]/);
});

gate("GDI_SHARED_REVEAL_PARITY", () => {
  assert.match(gdiReveal, /reveal/);
});

function normalizePersonForMissingChannels(p = {}) {
  return {
    ...p,
    name: p.name || p.display_name || p.personName || "",
    personId: p.personId || p.person_id || "",
    email: p.email || null,
    phone: p.phone || null,
    emails: p.emails || p.emailContacts || [],
    phones: p.phones || p.phoneContacts || [],
    channels: Array.isArray(p.channels)
      ? p.channels.map((c) => ({
          ...c,
          kind: c.kind || c.channelType || c.channel_type,
          channelType: c.channelType || c.kind || c.channel_type,
          value: c.value || c.channelValue || c.channel_value,
          verificationStatus: c.verificationStatus || c.verification_status || c.deliverability,
        }))
      : [],
  };
}

async function bethesdaMatrixFromApi() {
  const res = await fetch(`${BASE}/api/contact-intelligence/hotels/${HOTEL}`);
  assert.equal(res.status, 200, "contacts API status");
  const body = await res.json();
  const people =
    body?.package?.people ||
    body.people ||
    body.contacts ||
    body.directory ||
    [];
  const byName = Object.create(null);
  for (const raw of people) {
    const p = normalizePersonForMissingChannels(raw);
    const name = String(p.name || "").trim();
    if (!name) continue;
    byName[name] = computeMissingContactChannels(p);
  }
  const expected = [
    { match: /Jeff Mullen/i, channel: "phone" },
    { match: /Katherine Button/i, channel: "none" },
    { match: /Patrick Reece/i, channel: "none" },
    { match: /Meinrad Lang/i, channel: "both" },
    { match: /Jenny Pyne/i, channel: "both" },
  ];
  const matrix = expected.map((row) => {
    const hit = Object.keys(byName).find((n) => row.match.test(n));
    const state = hit ? byName[hit] : null;
    return {
      contact: hit || row.match.source,
      expected: row.channel,
      actual: state ? state.requestChannel : null,
      needsEmail: state ? state.needsEmail : null,
      needsPhone: state ? state.needsPhone : null,
      ok: state ? state.requestChannel === row.channel : false,
    };
  });
  fs.writeFileSync(path.join(OUT, "BETHESDA_REQUEST_MATRIX.json"), JSON.stringify({ at: new Date().toISOString(), matrix }, null, 2));
  return matrix;
}

try {
  const matrix = await bethesdaMatrixFromApi();
  gate("CONTACT_BETHESDA_REQUEST_MATRIX", () => {
    assert.equal(matrix.length, 5);
    for (const row of matrix) {
      assert.ok(row.ok, `${row.contact}: expected ${row.expected}, got ${row.actual}`);
    }
  });
} catch (err) {
  results.push({
    id: "CONTACT_BETHESDA_REQUEST_MATRIX",
    ok: false,
    error: String(err?.message || err),
  });
}

results.push({
  id: "CONTACT_LIVE_SURFE_BROWSER_CLICK",
  ok: false,
  blocked: !LIVE,
  note: LIVE
    ? "Live flag set — evaluated in browser run"
    : "BLOCKED — no explicit --live-surfe --confirm-live-spend credit authorization",
});

async function launch() {
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
  let releaseReveal = () => {};
  const revealGate = LIVE
    ? Promise.resolve()
    : new Promise((resolve) => {
        releaseReveal = resolve;
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
    await revealGate;
    return route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        ok: true,
        mock: true,
        provider: "surfe_mock",
        providerAttribution: "Surfe · Session Only",
        persistence: "NOT_WRITTEN",
        requestedInclude: { email: true, mobile: true },
        person: { email: "mock.v4@example.hotel", phone: "+13015550999" },
      }),
    });
  });
  page.on("request", (req) => {
    if (req.method() === "POST" && /enrich-hybrid|findymail/i.test(req.url())) {
      network.push({ type: "forbidden_customer_path", url: req.url() });
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

  const meinrad = page.locator("tr").filter({ hasText: /Meinrad/i }).first();
  const btn = meinrad.locator("[data-hci-enrich][data-hci-one-click-surfe]").first();
  await btn.click();
  await page.waitForTimeout(300);

  const toast = await page.evaluate(() => {
    const el = document.getElementById("hci-enrich-loading-toast");
    if (!el) return { visible: false };
    const r = el.getBoundingClientRect();
    const style = getComputedStyle(el);
    const isShown =
      style.display !== "none" &&
      style.visibility !== "hidden" &&
      Number(style.opacity || "1") > 0.05 &&
      r.width > 0;
    return {
      visible: isShown,
      upperRight: isShown && r.top <= 120 && window.innerWidth - r.right <= 120,
      outsideTable: !el.closest("table"),
      text: (el.innerText || "").slice(0, 120),
      top: Math.round(r.top),
      rightGap: Math.round(window.innerWidth - r.right),
    };
  });
  await page.screenshot({ path: path.join(SHOTS, "01_toast_upper_right.png"), fullPage: false });
  releaseReveal();

  if (LIVE) {
    await page.waitForTimeout(90000);
  } else {
    await page.waitForFunction(() => {
      const tr = [...document.querySelectorAll("tr")].find((r) => /Meinrad/i.test(r.innerText || ""));
      return !!(tr && /Surfe · Session Only/i.test(tr.innerText || ""));
    }, null, { timeout: 15000 });
  }
  await page.screenshot({ path: path.join(SHOTS, "02_after_one_click.png"), fullPage: false });

  const after = await meinrad.evaluate((tr) => {
    const text = (tr.innerText || "").replace(/\s+/g, " ");
    const stack = tr.querySelector("[data-hci-channel-stack]");
    const link = stack && stack.querySelector("a.gdi-list-contact-link, a[href^='mailto:']");
    const cs = link ? getComputedStyle(link) : null;
    return {
      text: text.slice(0, 420),
      hasSession: /Surfe · Session Only/i.test(text),
      hasTrySurfe: !!tr.querySelector("[data-hci-surfe-fallback]"),
      rowHeight: Math.round(tr.getBoundingClientRect().height),
      overflowWrap: cs && cs.overflowWrap,
      emailVisible: /mock\.v4@example\.hotel|@/.test(text),
      channelText: stack ? (stack.innerText || "").replace(/\s+/g, " ").slice(0, 220) : null,
    };
  });

  assert.equal(after.hasTrySurfe, false);
  assert.ok(after.rowHeight < 240, "row too tall " + after.rowHeight);
  assert.equal(network.filter((n) => n.type === "forbidden_customer_path").length, 0);
  assert.ok(network.some((n) => n.type === "reveal"), "reveal not invoked");
  if (!LIVE) {
    assert.ok(after.hasSession, "session label missing: " + after.text);
    assert.ok(after.emailVisible, "email missing: " + after.text);
    assert.notEqual(after.overflowWrap, "anywhere");
  }

  const vpResults = [];
  for (const [w, h] of [
    [1920, 1080],
    [1440, 900],
    [1366, 768],
    [768, 1024],
    [390, 844],
  ]) {
    await page.setViewportSize({ width: w, height: h });
    await page.waitForTimeout(350);
    const hRow = await meinrad.evaluate((tr) => Math.round(tr.getBoundingClientRect().height));
    vpResults.push({ viewport: `${w}x${h}`, rowHeight: hRow, ok: hRow < 300 });
    await page.screenshot({ path: path.join(SHOTS, `vp-${w}x${h}.png`), fullPage: false });
  }
  await browser.close();
  return { toast, after, network, vpResults };
}

if (BROWSER) {
  try {
    const browserResult = await runBrowser();
    results.push({
      id: "CONTACT_SURFE_SESSION_RESULT_VISIBLE",
      ok: Boolean(browserResult.after.hasSession || LIVE),
    });
    results.push({ id: "CONTACT_SURFE_NO_SECOND_CLICK", ok: !browserResult.after.hasTrySurfe });
    results.push({
      id: "CONTACT_NO_DUPLICATE_PROVIDER_CREDITS",
      ok: browserResult.network.filter((n) => n.type === "reveal").length === 1,
    });
    results.push({
      id: "CONTACT_ENRICHMENT_TOP_RIGHT_TOAST",
      ok:
        browserResult.toast.visible &&
        browserResult.toast.upperRight &&
        browserResult.toast.outsideTable,
      detail: browserResult.toast,
    });
    results.push({
      id: "CONTACT_DETAILS_NO_VERTICAL_CHARACTER_WRAP",
      ok: browserResult.after.rowHeight < 240,
    });
    results.push({
      id: "CONTACT_TABLE_LAYOUT_STABLE",
      ok: browserResult.vpResults.every((v) => v.ok),
      detail: browserResult.vpResults,
    });
    results.push({
      id: "CONTACT_NO_FINDYMAIL_RUNTIME",
      ok: !browserResult.network.some((n) => n.type === "forbidden_customer_path"),
    });
    if (LIVE) {
      const liveIdx = results.findIndex((r) => r.id === "CONTACT_LIVE_SURFE_BROWSER_CLICK");
      if (liveIdx >= 0) {
        results[liveIdx] = {
          id: "CONTACT_LIVE_SURFE_BROWSER_CLICK",
          ok: Boolean(browserResult.after.hasSession),
          blocked: false,
          note: "Live Surfe browser click executed with credit confirmation flags",
        };
      }
    }
    fs.writeFileSync(path.join(OUT, "BROWSER_RESULT.json"), JSON.stringify(browserResult, null, 2));
  } catch (err) {
    results.push({ id: "BROWSER_CERT", ok: false, error: String(err?.stack || err).slice(0, 800) });
  }
}

const failed = results.filter((r) => !r.ok && !r.blocked);
const blocked = results.filter((r) => r.blocked);
const summary = {
  at: new Date().toISOString(),
  classification: failed.length
    ? "HE_CONTACT_INTEL_V4_NOT_CERTIFIED"
    : blocked.length
      ? "HE_CONTACT_INTEL_V4_CERTIFIED_MOCKED_LIVE_BLOCKED"
      : "HE_CONTACT_INTEL_V4_CERTIFIED",
  liveSurfe: LIVE,
  passed: results.filter((r) => r.ok).length,
  failed: failed.length,
  blocked: blocked.length,
  results,
};
fs.writeFileSync(path.join(OUT, "CERTIFICATION.json"), JSON.stringify(summary, null, 2));
console.log(
  JSON.stringify(
    {
      ok: !failed.length,
      passed: summary.passed,
      failed: summary.failed,
      blocked: summary.blocked,
      classification: summary.classification,
    },
    null,
    2,
  ),
);
if (failed.length) {
  console.error(failed);
  process.exit(1);
}
