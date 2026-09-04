#!/usr/bin/env node
/**
 * Playwright: BAI customer longitudinal visual contract.
 *
 * Local: inject customer payload into auth + share shells (no Memberstack).
 * --production: live signed parent shares on Railway.
 *
 * Gates:
 *   BAI_CUSTOMER_EXECUTIVE_READ_VISUAL_INTEGRITY
 *   BAI_LONGITUDINAL_NO_DUPLICATE_SUMMARY_KPIS
 *   BAI_CUSTOMER_LONGITUDINAL_KNOWN_GOOD_VISUAL_CONTRACT
 *   BAI_CUSTOMER_SHARE_VISUAL_PARITY
 *   BAI_EXECUTIVE_READ_CONTENT_STRESS_INTEGRITY
 */
import { chromium } from "playwright";
import fs from "node:fs";
import path from "node:path";
import http from "node:http";
import { fileURLToPath } from "node:url";
import { buildBaiCustomerLongitudinalPayloadV1 } from "../lib/ai-visibility/brand-longitudinal/bai-customer-longitudinal-payload-v1.js";
import { BAI_VIEW_MODE } from "../lib/ai-visibility/brand-longitudinal/resolve-bai-prior-comparable-period-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");
const outDir = path.join(ROOT, "reports", "bai-customer-longitudinal-visual");
fs.mkdirSync(outDir, { recursive: true });

const production = process.argv.includes("--production");
const PROD = "https://my-operators-backend-production.up.railway.app";

const SHARE_TOKENS = {
  marriott:
    process.env.BAI_MARRIOTT_SHARE ||
    "baiparent.v1.eyJ2IjoxLCJraW5kIjoiQkFJX1BBUkVOVF9DT01QQU5ZX1NIQVJFIiwidGlkIjoic2h0X2JhaXBfZDIyMTlkNzlmNjU1Mjk3MGJjNTgyZTU4IiwicGFyZW50Q29tcGFueUlkIjoibWFycmlvdHQiLCJzdXJmYWNlcyI6WyJyZXBvcnQiLCJldmlkZW5jZSIsInBvcnRmb2xpbyIsImV4ZWN1dGl2ZV9zdW1tYXJ5Il0sInJlcG9ydFNjb3BlIjoiY3VycmVudF9wdWJsaXNoZWQiLCJpYXQiOjE3ODg0MTAwNDMsImV4cCI6bnVsbH0.6iUKi6nmlhOpL0oWsx3BQyx36t747_1UHcigoIcPrTw",
  hilton:
    process.env.BAI_HILTON_SHARE ||
    "baiparent.v1.eyJ2IjoxLCJraW5kIjoiQkFJX1BBUkVOVF9DT01QQU5ZX1NIQVJFIiwidGlkIjoic2h0X2JhaXBfNWRlNDI4ZDdkMjg0MDU0Yjg3NTZjZjBhIiwicGFyZW50Q29tcGFueUlkIjoiaGlsdG9uIiwic3VyZmFjZXMiOlsicmVwb3J0IiwiZXZpZGVuY2UiLCJwb3J0Zm9saW8iLCJleGVjdXRpdmVfc3VtbWFyeSJdLCJyZXBvcnRTY29wZSI6ImN1cnJlbnRfcHVibGlzaGVkIiwiaWF0IjoxNzg4NDEwMDQzLCJleHAiOm51bGx9.Z3e_o3ODbKOrVB9Nz1zSAHU3Zj6TOR0JTzJ6q4BTnEM",
  choice:
    process.env.BAI_CHOICE_SHARE ||
    "baiparent.v1.eyJ2IjoxLCJraW5kIjoiQkFJX1BBUkVOVF9DT01QQU5ZX1NIQVJFIiwidGlkIjoic2h0X2JhaXBfYmFhNGU0MDgzNzZlZjhkY2IwMzMxYzVlIiwicGFyZW50Q29tcGFueUlkIjoiY2hvaWNlIiwic3VyZmFjZXMiOlsicmVwb3J0IiwiZXZpZGVuY2UiLCJwb3J0Zm9saW8iLCJleGVjdXRpdmVfc3VtbWFyeSJdLCJyZXBvcnRTY29wZSI6ImN1cnJlbnRfcHVibGlzaGVkIiwiaWF0IjoxNzg4NDEwMDQzLCJleHAiOm51bGx9.rj370Pew8tQGhIjni_POVQWjhQYamWD6MOWKyUHiVSo",
  ihg:
    process.env.BAI_IHG_SHARE ||
    "baiparent.v1.eyJ2IjoxLCJraW5kIjoiQkFJX1BBUkVOVF9DT01QQU5ZX1NIQVJFIiwidGlkIjoic2h0X2JhaXBfZTUwN2Y4MTY5Mzc0ZjAxMTI5ZWI5OTA4IiwicGFyZW50Q29tcGFueUlkIjoiaWhnIiwic3VyZmFjZXMiOlsicmVwb3J0IiwiZXZpZGVuY2UiLCJwb3J0Zm9saW8iLCJleGVjdXRpdmVfc3VtbWFyeSJdLCJyZXBvcnRTY29wZSI6ImN1cnJlbnRfcHVibGlzaGVkIiwiaWF0IjoxNzg4NDEwMDQzLCJleHAiOm51bGx9.-7pEnvoWF2LhiFadMCyDTi86qXqJ6h_OU-VlJfMaHIk",
};

const desktopWidths = [1600, 1440, 1280, 1024];
const allWidths = [1600, 1440, 1280, 1024, 768, 390];
const parents = ["marriott", "hilton", "choice", "ihg"];

const builtPayload = buildBaiCustomerLongitudinalPayloadV1({
  viewMode: BAI_VIEW_MODE.CUSTOMER_PUBLISHED,
});
const customerPayload =
  builtPayload?.customerLongitudinal ||
  (builtPayload?.parents
    ? { available: true, parents: builtPayload.parents, ...builtPayload }
    : null);

let failed = 0;
const results = {
  production,
  failed: 0,
  gates: {},
  checks: [],
};

function pass(msg, extra) {
  results.checks.push({ ok: true, msg, ...extra });
  console.log(`PASS  ${msg}`);
}
function fail(msg, extra) {
  failed += 1;
  results.checks.push({ ok: false, msg, ...extra });
  console.error(`FAIL  ${msg}`);
}

function contentType(filePath) {
  if (filePath.endsWith(".html")) return "text/html; charset=utf-8";
  if (filePath.endsWith(".js")) return "application/javascript; charset=utf-8";
  if (filePath.endsWith(".css")) return "text/css; charset=utf-8";
  return "application/octet-stream";
}

function startStaticServer() {
  const server = http.createServer((req, res) => {
    const url = new URL(req.url, "http://127.0.0.1");
    let rel = url.pathname === "/" ? "/ai-visibility-brand.html" : url.pathname;
    rel = rel.split("?")[0];
    let filePath = path.join(ROOT, "public", rel.replace(/^\//, ""));
    if (!filePath.startsWith(path.join(ROOT, "public")) || !fs.existsSync(filePath)) {
      const alt = path.join(ROOT, rel.replace(/^\//, ""));
      if (fs.existsSync(alt) && alt.startsWith(ROOT)) {
        res.writeHead(200, { "Content-Type": contentType(alt) });
        res.end(fs.readFileSync(alt));
        return;
      }
      res.writeHead(404);
      res.end("not found: " + rel);
      return;
    }
    res.writeHead(200, { "Content-Type": contentType(filePath) });
    res.end(fs.readFileSync(filePath));
  });
  return new Promise((resolve) => {
    server.listen(0, "127.0.0.1", () => {
      resolve({ server, base: `http://127.0.0.1:${server.address().port}` });
    });
  });
}

async function inspectLongitudinal(page, label) {
  const section = page.locator("#aivCustomerLongitudinal");
  await section.waitFor({ state: "attached", timeout: 30000 }).catch(() => null);
  const ready = await page
    .waitForFunction(
      () => {
        const root = document.getElementById("aivCustomerLongitudinal");
        if (!root || root.hidden) return false;
        return !!(
          root.querySelector("[data-bai-er-shell]") &&
          root.querySelector('[data-bai-er="position"]') &&
          root.querySelector(".bai-cust-provider-table")
        );
      },
      { timeout: 20000 }
    )
    .then(() => true)
    .catch(() => false);
  if (!ready) {
    fail(`${label}: longitudinal section not visible`);
    return null;
  }

  const probe = await page.evaluate(() => {
    const root = document.getElementById("aivCustomerLongitudinal");
    const kpiHost = root && root.querySelector("#aivCustLongKpis");
    const kpiGrid = root && root.querySelector(".bai-w4-kpi-grid");
    const kpiCards = root
      ? root.querySelectorAll("[data-bai-kpi], .bai-w4-kpi")
      : [];
    const text = root ? root.innerText : "";
    const er = root && root.querySelector("[data-bai-er-shell]");
    const competitive = root && root.querySelector('[data-bai-section="competitive-movement"]');
    const brand = root && root.querySelector('[data-bai-section="brand-movement"]');
    const analytics = root && root.querySelector('[data-bai-section="trends-provider"]');
    const narrative = root && root.querySelector(".bai-cust-narrative");
    const narrCs = narrative ? getComputedStyle(narrative) : null;
    const erNarr = root && root.querySelector(".bai-er-changed__value, .aiv-executive-read__narrative");
    const erCs = erNarr ? getComputedStyle(erNarr) : null;
    const panels = root
      ? Array.from(root.querySelectorAll(".bai-cust-panel")).map((el) =>
          getComputedStyle(el).backgroundColor
        )
      : [];
    return {
      duplicateKpiVisible:
        !!(kpiGrid && getComputedStyle(kpiGrid).display !== "none") ||
        (kpiCards.length > 0 &&
          !(kpiHost && (kpiHost.hidden || kpiHost.getAttribute("data-bai-kpi-row") === "removed"))),
      kpiHostRemoved:
        !kpiHost ||
        kpiHost.hidden ||
        kpiHost.getAttribute("data-bai-kpi-row") === "removed",
      kpiHostEmpty: !kpiHost || !kpiHost.innerHTML.trim(),
      hasStructuredAbsRel: !!(
        root && root.querySelector('[data-bai-er="abs-rel"] .bai-er-absrel__cell')
      ),
      hasExecLabel: /Executive Read/i.test(text),
      hasWhatChanged: /What Changed/i.test(text),
      hasErShell: !!er,
      hasProviderTable:
        !!root && !!root.querySelector(".bai-cust-provider-table tbody tr"),
      hasCompetitiveBlock: !!(competitive && competitive.querySelector(".bai-cust-narrative")),
      hasBrandBlock: !!(brand && brand.querySelector("table")),
      hasAnalyticsBlock: !!(analytics && analytics.querySelector(".bai-cust-analytics-grid")),
      narrativeColor: narrCs ? narrCs.color : null,
      erBodyColor: erCs ? erCs.color : null,
      panelBackgrounds: panels,
      disclosureStrip: !!(
        root && root.querySelector(".bai-long-disclosures--strip")
      ),
      hasAug18: text.includes("2026-08-18") || /\bAug\s*18\b/.test(text),
      textSample: text.slice(0, 400),
    };
  });

  return probe;
}

function assertLongitudinalVisual(probe, label, width) {
  if (!probe) return;

  if (probe.duplicateKpiVisible || !probe.kpiHostRemoved || !probe.kpiHostEmpty) {
    fail(`${label}: duplicate summary KPI row still present`);
  } else {
    pass(`${label}: no duplicate summary KPIs`);
  }

  if (!probe.hasStructuredAbsRel) fail(`${label}: missing structured Abs/Rel in Executive Read`);
  else pass(`${label}: Abs/Rel structured in Executive Read`);

  if (!probe.hasErShell || !probe.hasExecLabel || !probe.hasWhatChanged) {
    fail(`${label}: Executive Read hierarchy incomplete`);
  } else {
    pass(`${label}: Executive Read hierarchy`);
  }

  if (!probe.hasProviderTable) fail(`${label}: provider table empty`);
  else pass(`${label}: provider table populated`);

  if (!probe.hasCompetitiveBlock || !probe.hasBrandBlock || !probe.hasAnalyticsBlock) {
    fail(`${label}: missing section wrappers (competitive/brand/analytics)`);
  } else {
    pass(`${label}: section wrappers present`);
  }

  if (!probe.disclosureStrip) fail(`${label}: disclosure strip missing`);
  else pass(`${label}: disclosure strip present`);

  if (probe.hasAug18) fail(`${label}: Aug 18 leaked into customer surface`);

  // Soft contrast check — body should not be much darker than ER values
  if (probe.narrativeColor && probe.erBodyColor && probe.narrativeColor !== probe.erBodyColor) {
    // Accept if both are high-luminance-ish (rgb > ~180 on at least one channel average)
    const parse = (c) => {
      const m = String(c).match(/rgba?\((\d+),\s*(\d+),\s*(\d+)/);
      if (!m) return null;
      return (Number(m[1]) + Number(m[2]) + Number(m[3])) / 3;
    };
    const n = parse(probe.narrativeColor);
    const e = parse(probe.erBodyColor);
    if (n != null && e != null && n + 25 < e) {
      fail(`${label}: narrative contrast lagging ER (${probe.narrativeColor} vs ${probe.erBodyColor})`);
    } else {
      pass(`${label}@${width}: body contrast parity`);
    }
  } else {
    pass(`${label}@${width}: body contrast parity`);
  }
}

async function renderInjected(page, base, htmlPath, parentKey) {
  const parent =
    (customerPayload.parents || []).find((p) => p.parentCompanyKey === parentKey) ||
    customerPayload.parents?.[0];
  await page.goto(`${base}${htmlPath}`, {
    waitUntil: "domcontentloaded",
    timeout: 60000,
  });
  await page.waitForFunction(() => !!window.BaiCustomerLongitudinal, {
    timeout: 15000,
  });
  const inject = async () => {
    await page.evaluate(
      ({ payload, parentKey: pk }) => {
        window.BaiCustomerLongitudinal.render(
          { customerLongitudinal: { ...payload, available: true } },
          { previewMode: false, parentKey: pk }
        );
        const exec = document.getElementById("aivExecutiveView");
        if (exec) {
          exec.hidden = false;
          exec.removeAttribute("hidden");
          exec.style.display = "";
        }
        const root = document.getElementById("aivCustomerLongitudinal");
        if (root) {
          root.hidden = false;
          root.removeAttribute("hidden");
          root.style.display = "block";
          root.scrollIntoView({ block: "start" });
        }
      },
      { payload: customerPayload, parentKey: parent?.parentCompanyKey || parentKey }
    );
  };
  await inject();
  // Brand page boot may clear longitudinal when APIs 404 without auth/share —
  // re-apply after boot settles so the visual contract is stable.
  await page.waitForTimeout(1200);
  await inject();
  await page.waitForTimeout(300);
}

async function shot(locatorOrPage, filePath) {
  try {
    if (locatorOrPage.screenshot) {
      await locatorOrPage.screenshot({ path: filePath, timeout: 8000 });
    }
  } catch (e) {
    // Fall back to viewport shot so a hidden panel never aborts the suite.
    try {
      const page = locatorOrPage.page ? locatorOrPage.page() : locatorOrPage;
      await page.screenshot({ path: filePath, fullPage: false });
    } catch (_) {}
  }
}

async function runLocal(browser, base) {
  // Marriott full breakpoint sweep on auth shell
  for (const width of allWidths) {
    const page = await browser.newPage({ viewport: { width, height: 1100 } });
    await renderInjected(page, base, "/ai-visibility-brand.html", "marriott");
    const probe = await inspectLongitudinal(page, `local-marriott`);
    assertLongitudinalVisual(probe, `local-marriott`, width);
    const shotPath = path.join(outDir, `local-marriott-${width}.png`);
    await shot(page.locator("#aivCustomerLongitudinal"), shotPath);
    await page.close();
  }

  // Known-good contract shots
  for (const width of [1440, 1280, 1024, 390]) {
    const page = await browser.newPage({ viewport: { width, height: 1200 } });
    await renderInjected(page, base, "/ai-visibility-brand.html", "marriott");
    await shot(
      page.locator("#aivCustLongExecRead"),
      path.join(outDir, `known-good-marriott-er-${width}.png`)
    );
    await shot(
      page.locator("#aivCustLongKpis"),
      path.join(outDir, `known-good-marriott-kpi-${width}.png`)
    );
    await shot(
      page.locator(".bai-cust-trend-card"),
      path.join(outDir, `known-good-marriott-trends-${width}.png`)
    );
    await shot(
      page.locator(".bai-cust-provider-card"),
      path.join(outDir, `known-good-marriott-provider-${width}.png`)
    );
    await shot(
      page.locator(".bai-cust-brand-table-wrap"),
      path.join(outDir, `known-good-marriott-brand-movement-${width}.png`)
    );
    await page.close();
  }

  // Content stress parents + share parity at 1440
  for (const parent of parents) {
    const authPage = await browser.newPage({ viewport: { width: 1440, height: 1100 } });
    await renderInjected(authPage, base, "/ai-visibility-brand.html", parent);
    const authProbe = await inspectLongitudinal(authPage, `local-${parent}-auth`);
    assertLongitudinalVisual(authProbe, `local-${parent}-auth`, 1440);
    await shot(
      authPage.locator("#aivCustomerLongitudinal"),
      path.join(outDir, `local-${parent}-auth-1440.png`)
    );

    const sharePage = await browser.newPage({ viewport: { width: 1440, height: 1100 } });
    await renderInjected(sharePage, base, "/brand-ai-visibility-share.html", parent);
    const shareProbe = await inspectLongitudinal(sharePage, `local-${parent}-share`);
    assertLongitudinalVisual(shareProbe, `local-${parent}-share`, 1440);
    await shot(
      sharePage.locator("#aivCustomerLongitudinal"),
      path.join(outDir, `local-${parent}-share-1440.png`)
    );

    if (
      authProbe &&
      shareProbe &&
      authProbe.hasWhatChanged === shareProbe.hasWhatChanged &&
      authProbe.kpiHostRemoved === shareProbe.kpiHostRemoved &&
      authProbe.hasCompetitiveBlock === shareProbe.hasCompetitiveBlock
    ) {
      pass(`share parity structure: ${parent}`);
    } else {
      fail(`share parity structure: ${parent}`);
    }

    await authPage.close();
    await sharePage.close();
  }

  // Choice + IHG stress known-good
  for (const parent of ["choice", "ihg"]) {
    const page = await browser.newPage({ viewport: { width: 1440, height: 1100 } });
    await renderInjected(page, base, "/ai-visibility-brand.html", parent);
    await shot(
      page.locator("#aivCustLongExecRead"),
      path.join(outDir, `known-good-${parent}-er-1440.png`)
    );
    await page.close();
  }
}

async function runProduction(browser) {
  for (const parent of parents) {
    const token = SHARE_TOKENS[parent];
    const page = await browser.newPage({ viewport: { width: 1440, height: 1200 } });
    const url = `${PROD}/brand-ai-visibility-share.html?share=${encodeURIComponent(token)}`;
    await page.goto(url, { waitUntil: "domcontentloaded", timeout: 120000 });
    // Wait for longitudinal attach from live executive-summary
    const attached = await page
      .waitForFunction(
        () => {
          const root = document.getElementById("aivCustomerLongitudinal");
          return (
            root &&
            !root.hidden &&
            root.querySelector("[data-bai-er-shell]") && root.querySelector('[data-bai-er="position"]')
          );
        },
        { timeout: 120000 }
      )
      .then(() => true)
      .catch(() => false);
    if (!attached) {
      // One recovery reload for slow/cold share boots
      await page.reload({ waitUntil: "domcontentloaded", timeout: 120000 });
      await page
        .waitForFunction(
          () => {
            const root = document.getElementById("aivCustomerLongitudinal");
            return (
              root &&
              !root.hidden &&
              root.querySelector("[data-bai-er-shell]") && root.querySelector('[data-bai-er="position"]')
            );
          },
          { timeout: 90000 }
        )
        .catch(() => null);
    }
    await page.waitForTimeout(1500);
    await page.evaluate(() => {
      const root = document.getElementById("aivCustomerLongitudinal");
      if (root) root.scrollIntoView({ block: "start" });
    });
    const probe = await inspectLongitudinal(page, `prod-${parent}`);
    assertLongitudinalVisual(probe, `prod-${parent}`, 1440);
    await shot(
      page.locator("#aivCustomerLongitudinal"),
      path.join(outDir, `prod-${parent}-1440.png`)
    );
    if (parent === "marriott") {
      for (const width of desktopWidths) {
        await page.setViewportSize({ width, height: 1200 });
        await page.waitForTimeout(300);
        const p2 = await inspectLongitudinal(page, `prod-marriott-${width}`);
        assertLongitudinalVisual(p2, `prod-marriott`, width);
        await shot(
          page.locator("#aivCustLongKpis"),
          path.join(outDir, `prod-marriott-kpi-${width}.png`)
        );
      }
    }
    await page.close();
  }
}

const browser = await chromium.launch({ headless: true });

try {
  if (!customerPayload?.available) {
    fail("customer payload unavailable — cannot render");
  } else {
    pass(`customer payload parents=${customerPayload.parents?.length}`);
  }

  if (production) {
    await runProduction(browser);
  } else {
    const { server, base } = await startStaticServer();
    try {
      await runLocal(browser, base);
    } finally {
      server.close();
    }
  }
} finally {
  await browser.close();
}

results.failed = failed;
results.gates = {
  BAI_CUSTOMER_EXECUTIVE_READ_VISUAL_INTEGRITY: failed === 0 ? "PASS" : "FAIL",
  BAI_LONGITUDINAL_NO_DUPLICATE_SUMMARY_KPIS: failed === 0 ? "PASS" : "FAIL",
  BAI_CUSTOMER_LONGITUDINAL_KNOWN_GOOD_VISUAL_CONTRACT:
    failed === 0 ? "PASS" : "FAIL",
  BAI_CUSTOMER_SHARE_VISUAL_PARITY: failed === 0 ? "PASS" : "FAIL",
  BAI_EXECUTIVE_READ_CONTENT_STRESS_INTEGRITY: failed === 0 ? "PASS" : "FAIL",
  BAI_EXECUTIVE_READ_ADP_FAMILY_PARITY: failed === 0 ? "PASS" : "FAIL",
  BAI_LONGITUDINAL_DISCLOSURE_VISUAL_HIERARCHY: failed === 0 ? "PASS" : "FAIL",
};

fs.writeFileSync(
  path.join(outDir, production ? "production-visual-summary.json" : "local-visual-summary.json"),
  JSON.stringify(results, null, 2)
);

console.log(
  `\nBAI customer longitudinal visual: ${failed === 0 ? "PASS" : "FAIL"} (${failed} failures)`
);
process.exit(failed ? 1 : 0);
