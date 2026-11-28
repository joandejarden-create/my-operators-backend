#!/usr/bin/env node
/**
 * Live UI diagnostic — Meinrad Find Email & Phone click sequence on :8080.
 * Default: mock Surfe /reveal success after hybrid (no Surfe credit).
 * Pass --live-surfe --confirm-live-spend to allow real /reveal.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { chromium } from "playwright";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const BASE = (process.env.DEALALITY_LOCAL_ADMIN_BASE || "http://127.0.0.1:8080").replace(
  /\/$/,
  "",
);
const HOTEL = "recLuxvwwxID7U2B8";
const LIVE_SURFE =
  process.argv.includes("--live-surfe") && process.argv.includes("--confirm-live-spend");
const OUT = path.join(
  ROOT,
  "reports/hotel-explorer/he-contact-intel-v31-recovery/DIAGNOSE_CLICK",
);
fs.mkdirSync(OUT, { recursive: true });

const stages = [];
function stage(id, status, detail = {}) {
  stages.push({ id, status, at: new Date().toISOString(), ...detail });
  console.log(`[stage] ${id} → ${status}`, detail.error || detail.note || "");
}

async function launch() {
  try {
    return await chromium.launch({ headless: true });
  } catch {
    return chromium.launch({ channel: "chrome", headless: true });
  }
}

const network = [];

async function main() {
  const browser = await launch();
  const page = await browser.newPage({ viewport: { width: 1440, height: 900 } });
  page.on("console", (msg) => {
    if (/hci|enrich|surfe|hybrid|error/i.test(msg.text())) {
      network.push({ type: "console", text: msg.text().slice(0, 300) });
    }
  });
  page.on("pageerror", (err) => {
    network.push({ type: "pageerror", text: String(err.message || err).slice(0, 300) });
    stage("frontend_exception", "FAIL", { error: String(err.message || err).slice(0, 200) });
  });

  // Only mock Surfe /reveal (credit guard). Never intercept hybrid — continue() can
  // drop long Findymail responses and surface as a frontend network_error.
  await page.route("**/api/contact-intelligence/hotels/*/people/*/reveal**", async (route) => {
    const req = route.request();
    if (req.method() !== "POST") return route.continue();
    const postData = req.postData() || "";
    let body = {};
    try {
      body = JSON.parse(postData);
    } catch {
      body = {};
    }
    network.push({
      type: "request",
      url: req.url().slice(0, 220),
      isHybrid: false,
      isReveal: true,
      linkedinUrl: body.linkedinUrl || null,
      channel: body.channel || null,
      personName: body.personName || null,
    });
    stage("surfe_api_request", "HIT", {
      linkedinUrl: body.linkedinUrl || null,
      channel: body.channel || null,
    });
    if (LIVE_SURFE) return route.continue();
    const mock = {
      ok: true,
      cached: false,
      pending: false,
      surfeCalls: 0,
      mock: true,
      provider: "surfe_mock",
      providerAttribution: "Surfe · Session Only",
      verificationStatus: "PROVIDER_RETURNED_UNREVIEWED",
      person: {
        personId: body.personId || null,
        email: "mock.meinrad@example.hotel",
        phone: "+13015550111",
        emails: [{ email: "mock.meinrad@example.hotel", value: "mock.meinrad@example.hotel" }],
        phones: [{ phone: "+13015550111", value: "+13015550111" }],
      },
      missingChannelState: {
        needsEmail: true,
        needsPhone: true,
        requestChannel: "both",
      },
      requestedInclude: { email: true, mobile: true },
      persistence: "NOT_WRITTEN",
      note: "Diagnostic mock Surfe success — no credits.",
    };
    network.push({ type: "mocked_reveal", body: mock });
    stage("surfe_api_response", "MOCK_SUCCESS", { email: true, phone: true });
    return route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify(mock),
    });
  });

  page.on("request", (req) => {
    if (req.method() !== "POST") return;
    const url = req.url();
    if (!/\/enrich-hybrid(?:\?|$)/.test(url)) return;
    let body = {};
    try {
      body = JSON.parse(req.postData() || "{}");
    } catch {
      body = {};
    }
    network.push({
      type: "request",
      url: url.slice(0, 220),
      isHybrid: true,
      isReveal: false,
      linkedinUrl: body.linkedinUrl || null,
      channel: body.channel || null,
      personName: body.personName || null,
    });
    stage("hybrid_api_request", "HIT", {
      linkedinUrl: body.linkedinUrl || null,
      channel: body.channel || null,
    });
  });

  page.on("response", async (res) => {
    const url = res.url();
    if (!/\/api\/contact-intelligence\/hotels\/.*\/people\//.test(url)) return;
    if (res.request().method() !== "POST") return;
    let json = null;
    try {
      json = await res.json();
    } catch {
      json = null;
    }
    const isHybrid = /enrich-hybrid/.test(url);
    network.push({
      type: "response",
      status: res.status(),
      isHybrid,
      ok: json?.ok,
      error: json?.error || null,
      email: Boolean(json?.email || json?.person?.email),
      phone: Boolean(json?.phone || json?.person?.phone),
      surfeFallbackAvailable: json?.surfeFallbackAvailable,
      pending: json?.pending,
      persistence: json?.persistence,
      message: (json?.message || json?.userMessage || "").slice(0, 160),
      missingChannelState: json?.missingChannelState || null,
      requestedInclude: json?.requestedInclude || null,
    });
    if (isHybrid) {
      stage("hybrid_api_response", json?.ok === false ? "FAIL" : "OK", {
        email: Boolean(json?.email),
        phone: Boolean(json?.phone),
        surfeFallbackAvailable: json?.surfeFallbackAvailable,
        message: (json?.message || "").slice(0, 120),
      });
      if (json?.surfeFallbackAvailable) stage("surfe_fallback_offer", "AVAILABLE");
      else stage("surfe_fallback_offer", "NOT_OFFERED");
    } else if (LIVE_SURFE) {
      stage(
        "surfe_api_response",
        json?.ok && (json?.person?.email || json?.person?.phone) ? "SUCCESS" : "NO_CHANNEL",
        {
          pending: json?.pending,
          error: json?.error,
          email: Boolean(json?.person?.email),
          phone: Boolean(json?.person?.phone),
        },
      );
    }
  });

  stage("navigate", "START");
  await page.goto(
    `${BASE}/deal-capture-radar-with-ranked-list.html?hotelExplorer=${HOTEL}&hexTab=contacts&cb=${Date.now()}`,
    { waitUntil: "domcontentloaded", timeout: 90000 },
  );
  await page.waitForFunction(() => typeof window.HotelExplorer === "object", null, {
    timeout: 45000,
  });
  await page.evaluate((id) => {
    HotelExplorer.open({ id, recordId: id }, { tab: "contacts" });
  }, HOTEL);
  await page.waitForSelector("[data-hci-hybrid-enrich]", { timeout: 60000 });
  stage("contacts_loaded", "OK");

  const meinrad = page
    .locator("tr")
    .filter({ hasText: /Meinrad/i })
    .first();
  await meinrad.waitFor({ timeout: 20000 });
  const btn = meinrad.locator("[data-hci-hybrid-enrich]").first();
  const btnMeta = await btn.evaluate((el) => ({
    text: (el.textContent || "").trim(),
    disabled: el.disabled,
    linkedinUrl: el.getAttribute("data-linkedin-url"),
    channel: el.getAttribute("data-enrich-channel"),
    personId: el.getAttribute("data-person-id"),
    personName: el.getAttribute("data-person-name"),
  }));
  stage("button_click_ready", btnMeta.disabled ? "BLOCKED" : "OK", btnMeta);
  await page.screenshot({ path: path.join(OUT, "01_before_click.png"), fullPage: false });

  // Observe toast vs in-row loader
  await btn.click();
  stage("button_click", "OK");
  await page.waitForTimeout(800);
  const loadingDom = await page.evaluate(() => {
    const inRow = !!document.querySelector(
      "[data-hci-enrich-slot] [data-hci-wave-loading], .hci-enrich__result[data-hci-wave-loading], [data-hci-enrich-slot] .dealality-wave-loader, [data-hci-enrich-slot] .he-loading-toast-host",
    );
    const toastHosts = [...document.querySelectorAll(".he-loading-toast-host, .dealality-loading-toast-host, [data-he-loading-toast]")].map(
      (el) => ({
        text: (el.textContent || "").slice(0, 80),
        inTable: !!el.closest("table, .hci-directory-table, [data-hci-contact-details-cell]"),
      }),
    );
    return { inRow, toastHosts };
  });
  stage(
    "loading_ui",
    loadingDom.inRow ? "IN_ROW_FAIL" : toastHostsOk(loadingDom) ? "TOAST_OK" : "AMBIGUOUS",
    loadingDom,
  );
  await page.screenshot({ path: path.join(OUT, "02_loading.png"), fullPage: false });

  // Wait hybrid to finish (Findymail path can exceed 20s)
  await page
    .waitForFunction(
      () => {
        const tr = [...document.querySelectorAll("tr")].find((row) =>
          /Meinrad/i.test(row.innerText || ""),
        );
        if (!tr) return false;
        return (
          !!tr.querySelector("[data-hci-surfe-fallback]") ||
          !!tr.querySelector(".hci-note") ||
          !!tr.querySelector("[data-surfe-session='1']") ||
          /Surfe|Findymail|not available on this server|Lookup could not/i.test(
            tr.innerText || "",
          )
        );
      },
      null,
      { timeout: 90000 },
    )
    .catch(() => {});
  await page.waitForTimeout(500);
  await page.screenshot({ path: path.join(OUT, "03_after_hybrid.png"), fullPage: false });

  const afterHybrid = await meinrad.evaluate((tr) => {
    const text = (tr.innerText || "").replace(/\s+/g, " ");
    const surfeBtn = tr.querySelector("[data-hci-surfe-fallback]");
    const result = tr.querySelector(".hci-enrich__result");
    return {
      text: text.slice(0, 400),
      hasTrySurfe: !!surfeBtn,
      trySurfeText: surfeBtn ? (surfeBtn.textContent || "").trim() : null,
      resultHidden: result ? result.hidden : null,
      resultHtml: result ? (result.innerHTML || "").slice(0, 400) : null,
      emailVisible: /@/.test(text) && !/Email Not Available/i.test(text),
      phoneVisible: /(\+\d{8,}|\d{3}[\s).-]*\d{3}[\s.-]*\d{4})/.test(text),
    };
  });
  stage(
    "after_hybrid_ui",
    afterHybrid.hasTrySurfe ? "SURFE_OFFERED" : afterHybrid.emailVisible || afterHybrid.phoneVisible ? "CHANNELS_VISIBLE" : "NO_FALLBACK_UI",
    afterHybrid,
  );

  if (afterHybrid.hasTrySurfe) {
    await meinrad.locator("[data-hci-surfe-fallback]").first().click();
    stage("surfe_fallback_click", "OK");
    await page.waitForTimeout(LIVE_SURFE ? 90000 : 2500);
    await page.screenshot({ path: path.join(OUT, "04_after_surfe.png"), fullPage: false });
    const afterSurfe = await meinrad.evaluate((tr) => {
      const text = (tr.innerText || "").replace(/\s+/g, " ");
      const result = tr.querySelector(".hci-enrich__result, .hci-enrich-result");
      const session = tr.querySelector("[data-surfe-session='1']");
      const channelStack = tr.querySelector("[data-hci-channel-stack]");
      return {
        text: text.slice(0, 500),
        hasSessionLabel: /Surfe · Session Only/i.test(text),
        sessionNode: !!session,
        resultHtml: result ? (result.innerHTML || "").slice(0, 500) : null,
        resultHidden: result ? result.hidden : null,
        channelStackText: channelStack ? (channelStack.innerText || "").replace(/\s+/g, " ").slice(0, 240) : null,
        mockEmailVisible: /mock\.meinrad@example\.hotel/i.test(text),
        emailNotAvailable: /Email Not Available/i.test(text),
        phoneNotAvailable: /Phone Not Available/i.test(text),
      };
    });
    const displayOk =
      afterSurfe.hasSessionLabel &&
      (afterSurfe.mockEmailVisible || afterSurfe.sessionNode) &&
      (!afterSurfe.resultHidden || afterSurfe.mockEmailVisible);
    stage("frontend_rendering", displayOk ? "PASS" : "FAIL", afterSurfe);
  } else {
    stage("surfe_fallback_click", "SKIPPED", { reason: "no Try Surfe button" });
    stage("frontend_rendering", "FAIL", { reason: "could not reach Surfe fallback UI" });
  }

  const report = {
    at: new Date().toISOString(),
    base: BASE,
    hotelId: HOTEL,
    liveSurfe: LIVE_SURFE,
    stages,
    network,
    firstFail: stages.find((s) => /FAIL|IN_ROW_FAIL|NO_FALLBACK|BLOCKED/.test(s.status)) || null,
  };
  fs.writeFileSync(path.join(OUT, "DIAGNOSE.json"), JSON.stringify(report, null, 2));
  console.log(JSON.stringify(report, null, 2));
  await browser.close();
  if (report.firstFail) process.exitCode = 2;
}

function toastHostsOk(loadingDom) {
  return (
    Array.isArray(loadingDom.toastHosts) &&
    loadingDom.toastHosts.some((t) => /Finding Contact/i.test(t.text) && !t.inTable)
  );
}

main().catch((err) => {
  console.error(String(err?.stack || err).slice(0, 800));
  process.exit(1);
});
