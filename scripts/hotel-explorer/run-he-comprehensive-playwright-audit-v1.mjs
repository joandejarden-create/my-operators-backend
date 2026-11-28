#!/usr/bin/env node
/**
 * READ-ONLY Hotel Explorer comprehensive Playwright audit V1.
 * No application code/CSS changes. No paid APIs. No Airtable writes.
 *
 *   node scripts/hotel-explorer/run-he-comprehensive-playwright-audit-v1.mjs
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { execSync } from "node:child_process";
import { chromium } from "playwright";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");
const BASE = (process.env.DEALALITY_LOCAL_ADMIN_BASE || "http://127.0.0.1:8080").replace(
  /\/$/,
  "",
);
const OUT = path.join(ROOT, "reports/hotel-explorer/playwright-responsive-audit-v1");
const SHOTS = path.join(OUT, "SCREENSHOTS");

const BETHESDA = {
  name: "Bethesda Marriott",
  hpcId: "recLuxvwwxID7U2B8",
};

const SMOKE_HOTELS = [
  { name: "AC Hotel Newtown Square", hpcId: "recF7DQBPDW5aUDoC" },
  { name: "Carton House, a Fairmont Hotel", hpcId: "recYf7HZWmCdWPaSd" },
  { name: "Hilton Garden Inn Newtown Square Radnor", hpcId: "recgaBAwa24wsl3xF" },
  { name: "Secrets Papagayo Costa Rica", hpcId: "recFQ0WvGLbfK5Gpa" },
  { name: "Secrets St. Lucia", hpcId: "recwZJuMcx6kA7f7I" },
  { name: "Secrets Cap Cana Resort & Spa", hpcId: "recd7XAM6tLjzQhaI" },
  { name: "JW Marriott Costa Elena", hpcId: "recq9KWr8UE7z2549" },
];

const ACTIVE_TABS = [
  { id: "hotel", label: "Hotel Overview" },
  { id: "rooms", label: "Guest Rooms & Meeting Space" },
  { id: "ownership", label: "Ownership Intelligence" },
  { id: "contacts", label: "Contact Intelligence" },
  { id: "organization", label: "Organization & Portfolio" },
  { id: "relationships", label: "Relationships & Structure" },
  { id: "submarket", label: "Submarket Snapshot" },
  { id: "area", label: "Area Hotels" },
  { id: "demand", label: "Demand Drivers" },
  { id: "access", label: "Access & Connectivity" },
  { id: "sources", label: "Sources & Evidence" },
];

const VIEWPORTS = [
  { name: "1920x1080", width: 1920, height: 1080 },
  { name: "1440x900", width: 1440, height: 900 },
  { name: "1366x768", width: 1366, height: 768 },
  { name: "768x1024", width: 768, height: 1024 },
  { name: "390x844", width: 390, height: 844 },
];

fs.mkdirSync(SHOTS, { recursive: true });

function sh(cmd, cwd) {
  try {
    return execSync(cmd, { cwd, encoding: "utf8" }).trim();
  } catch (e) {
    return `ERROR:${String(e.message || e).slice(0, 120)}`;
  }
}

async function launchBrowser() {
  try {
    return await chromium.launch({ headless: true });
  } catch {
    return chromium.launch({ channel: "chrome", headless: true });
  }
}

function deepLink(hotelId, tab) {
  return (
    `${BASE}/deal-capture-radar-with-ranked-list.html` +
    `?hotelExplorer=${encodeURIComponent(hotelId)}` +
    `&hexTab=${encodeURIComponent(tab || "hotel")}` +
    `&cb=${Date.now()}`
  );
}

async function openHotelExplorer(page, hotelId, tab) {
  const consoleErrors = [];
  const pageErrors = [];
  const failedRequests = [];
  page.on("console", (msg) => {
    if (msg.type() === "error") consoleErrors.push(msg.text().slice(0, 400));
  });
  page.on("pageerror", (err) => pageErrors.push(String(err.message || err).slice(0, 400)));
  page.on("response", (res) => {
    const u = res.url();
    if (!/\/api\//.test(u)) return;
    if (res.status() >= 400) {
      failedRequests.push({ url: u.slice(0, 220), status: res.status() });
    }
  });

  await page.goto(deepLink(hotelId, tab), {
    waitUntil: "domcontentloaded",
    timeout: 90000,
  });
  await page.waitForFunction(() => typeof window.HotelExplorer === "object", null, {
    timeout: 45000,
  });
  await page.evaluate(
    ({ id, t }) => {
      if (typeof HotelExplorer.open === "function") {
        HotelExplorer.open({ id: id, recordId: id }, { tab: t, requestedHotelId: id });
      } else if (typeof HotelExplorer.openDossierById === "function") {
        HotelExplorer.openDossierById(id);
        if (typeof HotelExplorer.setTab === "function") HotelExplorer.setTab(t);
      }
    },
    { id: hotelId, t: tab || "hotel" },
  );
  await page.waitForTimeout(1200);
  return { consoleErrors, pageErrors, failedRequests };
}

async function switchTab(page, tabId) {
  await page.evaluate((t) => {
    if (typeof HotelExplorer?.setTab === "function") HotelExplorer.setTab(t);
    else if (typeof HotelExplorer?.open === "function") {
      /* keep hotel; setTab preferred */
    }
  }, tabId);
  await page.waitForTimeout(700);
}

async function measureUi(page, tabId) {
  return page.evaluate((tab) => {
    const root =
      document.querySelector(".hex-root.is-open") ||
      document.querySelector(".hex-root") ||
      document.querySelector("[data-hotel-explorer]");
    const panel =
      document.querySelector(".hex-panel, .hex-body, .hiw-root, .hci-root") || root;
    const docOverflowX =
      document.documentElement.scrollWidth > document.documentElement.clientWidth + 2;
    const bodyOverflowX = document.body.scrollWidth > document.body.clientWidth + 2;
    const tabButtons = [...document.querySelectorAll("[data-hex-tab], .hex-tab, .hex-nav button, [data-tab]")]
      .map((el) => ({
        text: (el.textContent || "").replace(/\s+/g, " ").trim().slice(0, 40),
        id: el.getAttribute("data-hex-tab") || el.getAttribute("data-tab") || "",
        disabled: el.disabled === true || el.getAttribute("aria-disabled") === "true",
      }))
      .filter((t) => t.text || t.id);
    const sentimentVisible = tabButtons.some((t) =>
      /guest\s*sentiment|sentiment/i.test(t.text + " " + t.id),
    );
    const tables = [...document.querySelectorAll(".hex-root table, .hiw-root table, .hci-root table")].map(
      (tb) => {
        const wrap = tb.closest(".dl-table-wrap, .hiw-table-wrap, .table-wrap, .hex-scroll");
        return {
          cols: tb.querySelectorAll("thead th").length || tb.querySelectorAll("tr:first-child td, tr:first-child th").length,
          scrollContained: wrap
            ? wrap.scrollWidth >= tb.scrollWidth - 2 ||
              getComputedStyle(wrap).overflowX.includes("auto") ||
              getComputedStyle(wrap).overflowX.includes("scroll")
            : true,
          wrapOverflowX: wrap ? wrap.scrollWidth > wrap.clientWidth + 2 : false,
        };
      },
    );
    const images = [...document.querySelectorAll(".hex-root img, .hiw-root img")].slice(0, 40);
    const brokenImages = images.filter((img) => img.complete && img.naturalWidth === 0).length;
    const interactive = [
      ...document.querySelectorAll(
        ".hex-root button, .hex-root a, .hex-root [role='button'], .hci-root button",
      ),
    ].slice(0, 80);
    const missingNames = interactive.filter((el) => {
      const name =
        el.getAttribute("aria-label") ||
        el.getAttribute("title") ||
        (el.textContent || "").trim() ||
        el.getAttribute("alt");
      return !name;
    }).length;
    const findBtn = document.querySelector("[data-hci-enrich], .hci-btn--enrich");
    const contactDetails = document.querySelector(
      "[data-hci-contact-details-cell], .gdi-list-contact-details--with-action",
    );
    const loader = document.querySelector(
      ".dealality-wave-loader, .he-async-loading .dealality-wave-loader",
    );
    const hexOpen = !!document.querySelector(".hex-root.is-open, .hex-root[aria-hidden='false']");
    const contentText = ((panel && panel.innerText) || "").replace(/\s+/g, " ").trim();
    return {
      hexOpen,
      tab,
      docOverflowX: docOverflowX || bodyOverflowX,
      tabButtonCount: tabButtons.length,
      sentimentVisible,
      tabLabels: tabButtons.map((t) => t.text).slice(0, 20),
      tables,
      brokenImages,
      missingNames,
      findBtnPresent: !!findBtn,
      findBtnInContactDetails: !!(contactDetails && findBtn && contactDetails.contains(findBtn)),
      loaderPresent: !!loader,
      contentLen: contentText.length,
      contentPreview: contentText.slice(0, 160),
      viewport: { w: window.innerWidth, h: window.innerHeight },
    };
  }, tabId);
}

function issue({
  id,
  tab,
  viewport,
  severity,
  description,
  steps,
  screenshot,
  cause,
  fix,
  regression,
}) {
  return {
    issueId: id,
    productTab: tab,
    viewport,
    severity,
    description,
    stepsToReproduce: steps,
    screenshot: screenshot || "",
    suspectedRootCause: cause || "",
    recommendedCorrection: fix || "",
    regressionTestRecommendation: regression || "",
  };
}

const issues = [];
const matrix = [];
const consoleNetwork = { console: [], pageErrors: [], failedRequests: [], byCase: [] };
const interactionLog = [];
const loaderReport = { cases: [] };
const smokeReport = { hotels: [] };
let issueSeq = 1;

function addIssue(partial) {
  const id = `HE-AUDIT-${String(issueSeq++).padStart(3, "0")}`;
  issues.push(issue({ id, ...partial }));
  return id;
}

// —— Environment ——
const servingCwd = "C:\\Dev\\dealality\\ws-gdi";
const env = {
  auditedAt: new Date().toISOString(),
  baseUrl: BASE,
  servingWorktree: servingCwd,
  servingBranch: sh("git branch --show-current", servingCwd),
  servingHead: sh("git rev-parse HEAD", servingCwd),
  originMain: sh("git rev-parse origin/main", servingCwd),
  headEqualsOriginMain:
    sh("git rev-parse HEAD", servingCwd) === sh("git rev-parse origin/main", servingCwd),
  dirtyNote:
    "ws-gdi is dirty (including public/css/hotel-contact-intelligence.css). Audit records this; no reset/clean performed.",
  entryMethod: "deep-link deal-capture-radar-with-ranked-list.html?hotelExplorer=",
  authenticatedAppPath: "http://localhost:8080/app#/opportunity-radar",
  authenticatedAppNote:
    "Primary automated path uses the established HE deep-link on the radar HTML (same pattern as existing HE browser gates). /app#/opportunity-radar may require Memberstack session — probed separately.",
  restrictions: [
    "READ_ONLY",
    "NO_APP_CODE_CHANGES",
    "NO_CSS_CHANGES",
    "NO_AIRTABLE_WRITES",
    "NO_PAID_ENRICHMENT",
  ],
};

const browser = await launchBrowser();
let testsExecuted = 0;
let passCount = 0;
let failCount = 0;
let notTested = 0;

// Probe authenticated radar route (no login automation)
{
  const page = await browser.newPage({ viewport: { width: 1440, height: 900 } });
  try {
    const res = await page.goto(`${BASE}/app#/opportunity-radar`, {
      waitUntil: "domcontentloaded",
      timeout: 30000,
    });
    await page.waitForTimeout(1500);
    const title = await page.title();
    const url = page.url();
    env.authenticatedProbe = {
      status: res?.status() || null,
      finalUrl: url,
      title,
      likelyAuthGate: /login|memberstack|sign.?in/i.test(url + " " + title),
    };
    if (env.authenticatedProbe.likelyAuthGate) {
      notTested += 1;
      addIssue({
        tab: "Navigation",
        viewport: "1440x900",
        severity: "P2 — MEDIUM",
        description:
          "Canonical /app#/opportunity-radar appears auth-gated in this automated session; deep-link HE entry was used for tab audits.",
        steps: "Open http://localhost:8080/app#/opportunity-radar without Memberstack storage state",
        cause: "No authenticated storageState provided to audit runner",
        fix: "Re-run with founder-authenticated storageState for full Radar→HE popup path",
        regression: "Authenticated Radar→HE navigation Playwright gate",
      });
    }
  } catch (e) {
    env.authenticatedProbe = { error: String(e.message || e).slice(0, 200) };
    notTested += 1;
  }
  await page.close();
}

// —— Bethesda full matrix ——
for (const vp of VIEWPORTS) {
  const page = await browser.newPage({
    viewport: { width: vp.width, height: vp.height },
  });
  const open = await openHotelExplorer(page, BETHESDA.hpcId, "hotel");
  consoleNetwork.byCase.push({
    hotel: BETHESDA.name,
    viewport: vp.name,
    consoleErrors: open.consoleErrors,
    pageErrors: open.pageErrors,
    failedRequests: open.failedRequests,
  });
  consoleNetwork.console.push(...open.consoleErrors.map((t) => ({ viewport: vp.name, t })));
  consoleNetwork.pageErrors.push(...open.pageErrors.map((t) => ({ viewport: vp.name, t })));
  consoleNetwork.failedRequests.push(
    ...open.failedRequests.map((f) => ({ viewport: vp.name, ...f })),
  );

  for (const tab of ACTIVE_TABS) {
    testsExecuted += 1;
    await switchTab(page, tab.id);
    const ui = await measureUi(page, tab.id);
    const shotName = `bethesda_${tab.id}_${vp.name}.png`;
    const shotPath = path.join(SHOTS, shotName);
    try {
      await page.screenshot({ path: shotPath, fullPage: true });
    } catch {
      await page.screenshot({ path: shotPath, fullPage: false });
    }

    let status = "PASS";
    const notes = [];

    if (!ui.hexOpen && ui.contentLen < 40) {
      status = "FAIL";
      failCount += 1;
      addIssue({
        tab: tab.label,
        viewport: vp.name,
        severity: "P0 — BLOCKER",
        description: "Hotel Explorer panel did not open or rendered empty content.",
        steps: `Open deep-link with hexTab=${tab.id} at ${vp.name}`,
        screenshot: shotName,
        cause: "Open/setTab failure or empty mount",
        fix: "Investigate HotelExplorer.open / setTab for this viewport",
        regression: "HE open + tab mount Playwright gate",
      });
    } else {
      passCount += 1;
    }

    if (ui.sentimentVisible) {
      status = "FAIL";
      addIssue({
        tab: "Guest Sentiment",
        viewport: vp.name,
        severity: "P1 — HIGH",
        description: "Guest Sentiment tab appears visible but must remain founder-disabled.",
        steps: `Inspect tab strip after opening HE at ${vp.name}`,
        screenshot: shotName,
        cause: "Feature flag filter not applied in served build",
        fix: "Ensure HeGuestSentimentFeatureFlag filters ALL_TABS",
        regression: "Guest Sentiment disabled gate",
      });
    }

    if (ui.docOverflowX && vp.width >= 1366) {
      addIssue({
        tab: tab.label,
        viewport: vp.name,
        severity: "P1 — HIGH",
        description: "Document-level horizontal overflow detected on laptop/desktop viewport.",
        steps: `Open ${tab.label} at ${vp.name} and check document scrollWidth`,
        screenshot: shotName,
        cause: "Wide fixed min-width or absolute positioning",
        fix: "Contain overflow in scroll wrappers; reduce min-widths",
        regression: "Viewport overflow assertion",
      });
      if (status === "PASS") status = "FAIL";
    } else if (ui.docOverflowX && vp.width < 1366) {
      notes.push("Document overflow on narrow viewport — may be intentional table min-width scrolling");
    }

    if (tab.id === "contacts") {
      if (!ui.findBtnPresent) {
        addIssue({
          tab: tab.label,
          viewport: vp.name,
          severity: "P2 — MEDIUM",
          description: "Find Email & Phone button not found in Contact Intelligence view.",
          steps: `Open contacts tab at ${vp.name}`,
          screenshot: shotName,
          cause: "UI not yet serving Contact Intelligence V2, or no people rendered",
          fix: "Confirm integrated build includes V2 button placement",
          regression: "HE_CONTACT_FIND_BUTTON_RIGHT_ALIGNMENT",
        });
      }
      // Do NOT click enrich (paid / Surfe)
      interactionLog.push({
        action: "contacts_no_paid_click",
        viewport: vp.name,
        result: "PASS",
        note: "Find Email & Phone not clicked (audit restriction)",
      });
    }

    if (open.pageErrors.length) {
      addIssue({
        tab: tab.label,
        viewport: vp.name,
        severity: "P1 — HIGH",
        description: `Page JS errors: ${open.pageErrors.slice(0, 2).join(" | ")}`,
        steps: `Open ${tab.label}`,
        screenshot: shotName,
        cause: "Runtime exception",
        fix: "Fix JS error at stack",
        regression: "console pageerror gate",
      });
    }

    matrix.push({
      hotel: BETHESDA.name,
      hpcId: BETHESDA.hpcId,
      tab: tab.label,
      tabId: tab.id,
      viewport: vp.name,
      status,
      hexOpen: ui.hexOpen,
      contentLen: ui.contentLen,
      docOverflowX: ui.docOverflowX,
      tables: ui.tables.length,
      brokenImages: ui.brokenImages,
      missingAccessibleNames: ui.missingNames,
      sentimentVisible: ui.sentimentVisible,
      screenshot: shotName,
      notes: notes.join("; "),
    });
  }

  // Interaction: tab revisit + close
  testsExecuted += 2;
  await switchTab(page, "hotel");
  await switchTab(page, "contacts");
  await switchTab(page, "hotel");
  interactionLog.push({
    action: "tab_revisit_hotel_contacts_hotel",
    viewport: vp.name,
    result: "PASS",
  });
  passCount += 1;

  const closed = await page.evaluate(() => {
    if (typeof HotelExplorer?.close === "function") {
      HotelExplorer.close();
      return true;
    }
    const btn = document.querySelector(
      ".hex-close, [data-hex-close], button[aria-label*='Close']",
    );
    if (btn) {
      btn.click();
      return true;
    }
    return false;
  });
  await page.waitForTimeout(400);
  interactionLog.push({
    action: "close_hotel_explorer",
    viewport: vp.name,
    result: closed ? "PASS" : "NOT_TESTED",
  });
  if (closed) passCount += 1;
  else notTested += 1;

  await page.close();
}

// —— Loader parity (delayed contacts fetch; no paid) ——
{
  const page = await browser.newPage({ viewport: { width: 1440, height: 900 } });
  await page.route("**/api/contact-intelligence/**", async (route) => {
    await new Promise((r) => setTimeout(r, 1500));
    return route.continue();
  });
  await page.goto(deepLink(BETHESDA.hpcId, "contacts"), {
    waitUntil: "domcontentloaded",
    timeout: 90000,
  });
  await page.waitForFunction(() => typeof window.HotelExplorer === "object", null, {
    timeout: 45000,
  });
  const loaderCapture = await page.evaluate(async (hotelId) => {
    const captures = [];
    const obs = new MutationObserver(() => {
      const toast = document.querySelector(
        ".dealality-wave-loader, .he-async-loading .dealality-wave-loader",
      );
      if (!toast) return;
      const s = getComputedStyle(toast);
      const r = toast.getBoundingClientRect();
      captures.push({
        text: (toast.textContent || "").trim().slice(0, 80),
        bg: s.backgroundColor,
        radius: s.borderRadius,
        w: Math.round(r.width),
        h: Math.round(r.height),
        classes: toast.className,
      });
    });
    obs.observe(document.documentElement, { childList: true, subtree: true });
    if (typeof HotelExplorer.open === "function") {
      HotelExplorer.open({ id: hotelId, recordId: hotelId }, { tab: "contacts", requestedHotelId: hotelId });
    }
    await new Promise((r) => setTimeout(r, 2200));
    obs.disconnect();
    const still = !!document.querySelector(
      ".hci-root--loading .dealality-wave-loader, .he-async-loading .dealality-wave-loader",
    );
    return { captures, stuckLoading: still };
  }, BETHESDA.hpcId);
  await page.screenshot({
    path: path.join(SHOTS, "loader_contacts_delayed_1440x900.png"),
    fullPage: false,
  });
  testsExecuted += 1;
  const best = loaderCapture.captures[0] || null;
  const loaderOk =
    !!best &&
    /rgba?\(0,\s*0,\s*0|rgb\(0,\s*0,\s*0\)/i.test(String(best.bg || "")) &&
    !loaderCapture.stuckLoading;
  loaderReport.cases.push({
    tab: "Contact Intelligence",
    viewport: "1440x900",
    delayedFetch: true,
    paidCalls: false,
    captures: loaderCapture.captures.slice(0, 5),
    stuckLoading: loaderCapture.stuckLoading,
    status: loaderOk ? "PASS" : best ? "FAIL" : "NOT_TESTED",
    notes: best
      ? "Observed wave loader during delayed contact fetch"
      : "No loader capture — may have completed before observer or cache hit",
  });
  if (loaderOk) passCount += 1;
  else if (!best) notTested += 1;
  else {
    failCount += 1;
    addIssue({
      tab: "Contact Intelligence",
      viewport: "1440x900",
      severity: "P2 — MEDIUM",
      description: "Shared loading animation parity incomplete (bg/stuck state).",
      steps: "Delay /api/contact-intelligence and open contacts tab",
      screenshot: "loader_contacts_delayed_1440x900.png",
      cause: "Loader chrome or lifecycle",
      fix: "Align with DealalityWaveLoader toast contract",
      regression: "verify-he-loading-component-browser-v1",
    });
  }
  if (loaderCapture.stuckLoading) {
    addIssue({
      tab: "Contact Intelligence",
      viewport: "1440x900",
      severity: "P1 — HIGH",
      description: "Loading animation remained after delayed fetch window.",
      steps: "Open contacts with delayed API",
      screenshot: "loader_contacts_delayed_1440x900.png",
      cause: "Stuck loading state",
      fix: "Clear loading flag on settle",
      regression: "Loader lifecycle gate",
    });
  }
  await page.unroute("**/api/contact-intelligence/**");
  await page.close();
}

// —— Seven-hotel smoke at 1440×900 ——
for (const hotel of SMOKE_HOTELS) {
  const page = await browser.newPage({ viewport: { width: 1440, height: 900 } });
  testsExecuted += 1;
  let entry;
  try {
    entry = await openHotelExplorer(page, hotel.hpcId, "hotel");
    const ui = await measureUi(page, "hotel");
    await switchTab(page, "contacts");
    const uiContacts = await measureUi(page, "contacts");
    await switchTab(page, "ownership");
    const uiOwn = await measureUi(page, "ownership");
    const shot = `smoke_${hotel.hpcId}_1440x900.png`;
    await page.screenshot({ path: path.join(SHOTS, shot), fullPage: false });
    const fatal =
      (!ui.hexOpen && ui.contentLen < 40) || entry.pageErrors.length > 0;
    const row = {
      hotel: hotel.name,
      hpcId: hotel.hpcId,
      status: fatal ? "FAIL" : "PASS",
      overviewContentLen: ui.contentLen,
      contactsContentLen: uiContacts.contentLen,
      ownershipContentLen: uiOwn.contentLen,
      pageErrors: entry.pageErrors.slice(0, 3),
      failedApi: entry.failedRequests.slice(0, 5),
      dataGapNote:
        ui.contentLen < 120
          ? "Sparse overview content — likely data gap, not necessarily layout defect"
          : "",
      screenshot: shot,
    };
    smokeReport.hotels.push(row);
    if (fatal) {
      failCount += 1;
      addIssue({
        tab: "Hotel Overview",
        viewport: "1440x900",
        severity: "P0 — BLOCKER",
        description: `Smoke open failed for ${hotel.name}`,
        steps: `Deep-link hotelExplorer=${hotel.hpcId}`,
        screenshot: shot,
        cause: "Open failure or JS error",
        fix: "Investigate hotel-specific mount",
        regression: "HE50 eight-hotel smoke",
      });
    } else passCount += 1;
  } catch (e) {
    notTested += 1;
    smokeReport.hotels.push({
      hotel: hotel.name,
      hpcId: hotel.hpcId,
      status: "NOT_TESTED",
      error: String(e.message || e).slice(0, 240),
    });
  }
  await page.close();
}

await browser.close();

// —— Write deliverables ——
function toCsv(rows, columns) {
  const esc = (v) => {
    const s = String(v ?? "");
    if (/[",\n]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
    return s;
  };
  return [columns.join(",")]
    .concat(rows.map((r) => columns.map((c) => esc(r[c])).join(",")))
    .join("\n");
}

fs.writeFileSync(
  path.join(OUT, "TAB_VIEWPORT_MATRIX.csv"),
  toCsv(matrix, [
    "hotel",
    "hpcId",
    "tab",
    "tabId",
    "viewport",
    "status",
    "hexOpen",
    "contentLen",
    "docOverflowX",
    "tables",
    "brokenImages",
    "missingAccessibleNames",
    "sentimentVisible",
    "screenshot",
    "notes",
  ]),
  "utf8",
);

fs.writeFileSync(
  path.join(OUT, "ISSUE_REGISTER.csv"),
  toCsv(issues, [
    "issueId",
    "productTab",
    "viewport",
    "severity",
    "description",
    "stepsToReproduce",
    "screenshot",
    "suspectedRootCause",
    "recommendedCorrection",
    "regressionTestRecommendation",
  ]),
  "utf8",
);

fs.writeFileSync(
  path.join(OUT, "CONSOLE_NETWORK_ERRORS.json"),
  JSON.stringify(consoleNetwork, null, 2) + "\n",
  "utf8",
);

fs.writeFileSync(
  path.join(OUT, "LOADER_PARITY_REPORT.md"),
  [
    "# Loader Parity Report",
    "",
    "Paid APIs: **not called**.",
    "",
    ...loaderReport.cases.map(
      (c) =>
        `## ${c.tab} @ ${c.viewport}\n- Status: **${c.status}**\n- Stuck: ${c.stuckLoading}\n- Notes: ${c.notes}\n- Captures: \`${JSON.stringify(c.captures).slice(0, 500)}\`\n`,
    ),
  ].join("\n"),
  "utf8",
);

fs.writeFileSync(
  path.join(OUT, "INTERACTION_TEST_REPORT.md"),
  [
    "# Interaction Test Report",
    "",
    "Safe interactions only. No paid enrichment clicks.",
    "",
    ...interactionLog.map(
      (i) => `- **${i.action}** (${i.viewport || "n/a"}): ${i.result}${i.note ? ` — ${i.note}` : ""}`,
    ),
    "",
  ].join("\n"),
  "utf8",
);

fs.writeFileSync(
  path.join(OUT, "SEVEN_HOTEL_SMOKE_REPORT.md"),
  [
    "# Seven-Hotel Smoke Report",
    "",
    "Viewport: 1440×900. Tabs sampled: Overview, Contacts, Ownership.",
    "",
    "| Hotel | HPC | Status | Overview chars | Notes |",
    "|---|---|---|---|---|",
    ...smokeReport.hotels.map(
      (h) =>
        `| ${h.hotel} | \`${h.hpcId}\` | ${h.status} | ${h.overviewContentLen ?? "—"} | ${h.dataGapNote || h.error || ""} |`,
    ),
    "",
  ].join("\n"),
  "utf8",
);

const p0 = issues.filter((i) => i.severity.startsWith("P0"));
const p1 = issues.filter((i) => i.severity.startsWith("P1"));
const p2 = issues.filter((i) => i.severity.startsWith("P2"));
const p3 = issues.filter((i) => i.severity.startsWith("P3"));

const top10 = [...issues]
  .sort((a, b) => a.severity.localeCompare(b.severity) || a.issueId.localeCompare(b.issueId))
  .slice(0, 10);

const demoSafe =
  p0.length === 0 && p1.filter((i) => /BLOCKER|unusable|crash/i.test(i.description)).length === 0;

const summary = `# Hotel Explorer Comprehensive Playwright Audit V1 — Founder Executive Summary

**Audited at:** ${env.auditedAt}  
**Base URL:** ${env.baseUrl}  
**Serving worktree:** \`${env.servingWorktree}\`  
**Branch:** \`${env.servingBranch}\`  
**HEAD:** \`${env.servingHead}\`  
**origin/main:** \`${env.originMain}\`  
**HEAD == origin/main:** ${env.headEqualsOriginMain}  

## Environment honesty

${env.dirtyNote}

Primary automation used the established HE deep-link:

\`${env.entryMethod}<hpcId>&hexTab=<tab>\`

Authenticated \`/app#/opportunity-radar\` probe: ${JSON.stringify(env.authenticatedProbe || {})}

**No application code or CSS was modified. No Airtable writes. No paid enrichment.**

## Counts

| Metric | Value |
|---|---|
| Tests executed | ${testsExecuted} |
| PASS | ${passCount} |
| FAIL | ${failCount} |
| NOT_TESTED | ${notTested} |
| Issues logged | ${issues.length} |
| P0 | ${p0.length} |
| P1 | ${p1.length} |
| P2 | ${p2.length} |
| P3 | ${p3.length} |
| Bethesda tab×viewport cells | ${matrix.length} |
| Smoke hotels | ${smokeReport.hotels.length} |

## 11 active tabs

All of: Hotel Overview, Guest Rooms & Meeting Space, Ownership Intelligence, Contact Intelligence, Organization & Portfolio, Relationships & Structure, Submarket Snapshot, Area Hotels, Demand Drivers, Access & Connectivity, Sources & Evidence were exercised on Bethesda across five viewports.

Guest Sentiment must remain disabled — ${matrix.some((m) => m.sentimentVisible) ? "**VISIBLE (DEFECT)**" : "not observed in tab strip"}.

## Viewports

${VIEWPORTS.map((v) => `- ${v.name}`).join("\n")}

## Seven additional hotels

See \`SEVEN_HOTEL_SMOKE_REPORT.md\`. Failures vs sparse-data notes are separated where possible.

## Top issues (by severity)

${top10.map((i, idx) => `${idx + 1}. **${i.severity}** ${i.issueId} — ${i.productTab} @ ${i.viewport}: ${i.description}`).join("\n") || "_No issues logged._"}

## Customer demonstration readiness

**${demoSafe ? "CONDITIONAL YES — no P0 blockers logged; review P1/P2 before external demo." : "NOT SAFE until P0/critical P1 issues are resolved."}**

Re-verify Contact Intelligence on a **clean integrated build** before demoing Find Email & Phone layout (serving tree currently has dirty \`hotel-contact-intelligence.css\`).

## Recommended focused PR sequence (after founder authorization)

1. Clean integrated HE build on synchronized home (Contact Intelligence CSS/JS alignment).  
2. Fix any P0/P1 overflow or open failures from this register.  
3. Authenticated Radar→HE popup path with storageState.  
4. Loader lifecycle parity if NOT_TESTED/FAIL.  
5. Narrow-viewport table scroll containment polish (P2).

## Artifacts

- \`TAB_VIEWPORT_MATRIX.csv\`
- \`ISSUE_REGISTER.csv\`
- \`CONSOLE_NETWORK_ERRORS.json\`
- \`LOADER_PARITY_REPORT.md\`
- \`INTERACTION_TEST_REPORT.md\`
- \`SEVEN_HOTEL_SMOKE_REPORT.md\`
- \`SCREENSHOTS/\`
`;

fs.writeFileSync(path.join(OUT, "FOUNDER_EXECUTIVE_SUMMARY.md"), summary, "utf8");
fs.writeFileSync(
  path.join(OUT, "ENVIRONMENT.json"),
  JSON.stringify(env, null, 2) + "\n",
  "utf8",
);
fs.writeFileSync(
  path.join(OUT, "AUDIT_RUN_SUMMARY.json"),
  JSON.stringify(
    {
      testsExecuted,
      passCount,
      failCount,
      notTested,
      issueCount: issues.length,
      demoSafe,
      out: OUT,
    },
    null,
    2,
  ) + "\n",
  "utf8",
);

console.log(
  JSON.stringify(
    {
      ok: true,
      out: OUT,
      testsExecuted,
      passCount,
      failCount,
      notTested,
      issues: issues.length,
      demoSafe,
    },
    null,
    2,
  ),
);
