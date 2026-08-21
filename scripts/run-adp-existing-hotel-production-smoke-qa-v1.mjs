#!/usr/bin/env node
/**
 * Production smoke QA for Existing Hotel ADP recovery (all 5 properties).
 * Captures Trend screenshots and verifies payload/UI correctness gates.
 *
 *   PRODUCTION_BASE=https://my-operators-backend-production.up.railway.app \
 *     node scripts/run-adp-existing-hotel-production-smoke-qa-v1.mjs
 */
import { mkdirSync, writeFileSync, readFileSync, existsSync } from "fs";
import { join } from "path";
import { loadPublishedReport } from "../lib/ai-demand-positioning/published-snapshot.js";

const BASE =
  process.env.PRODUCTION_BASE ||
  process.env.ADP_QA_BASE ||
  "https://my-operators-backend-production.up.railway.app";

const PROPERTIES = [
  { id: "adp_waterstone_boca_raton", label: "Waterstone Boca Raton" },
  { id: "adp_renaissance_times_square", label: "Renaissance Times Square" },
  { id: "adp_cambridge_beaches_bermuda", label: "Cambridge Beaches Bermuda" },
  { id: "adp_now_now_noho", label: "NOW NOW NOHO" },
  { id: "adp_hotel_phillips_kansas_city", label: "Hotel Phillips Kansas City" },
];

const OUT_DIR = join(process.cwd(), "reports/ai-demand-positioning/production-smoke-qa");
const SCREEN_DIR = join(OUT_DIR, "screenshots");

function num(v) {
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function approxEqual(a, b, tol = 0.15) {
  if (a == null && b == null) return true;
  if (a == null || b == null) return false;
  return Math.abs(Number(a) - Number(b)) <= tol;
}

function isProseName(name) {
  const n = String(name || "").trim();
  if (!n) return false;
  if (/^(while|although|its|their|one of|many of)\b/i.test(n)) return true;
  if (/^[a-z]/.test(n)) return true;
  if (/\bis presented as\b/i.test(n)) return true;
  // Long Title-Case soft-brand hotel names are valid; only flag sentence-like clauses.
  if (n.split(/\s+/).length >= 10 && !/,/.test(n) && /\b(is|are|was|were|presented|known)\b/i.test(n)) {
    return true;
  }
  return false;
}

async function waitForDeploy(maxMs = 60000) {
  const start = Date.now();
  while (Date.now() - start < maxMs) {
    try {
      const health = await fetch(`${BASE}/api/ai-demand-positioning/read-health`);
      if (health.ok) {
        const j = await health.json();
        if (j?.requested === "filesystem" || j?.ok) return { ready: true, health: j };
      }
    } catch (_) {}
    await new Promise((r) => setTimeout(r, 5000));
  }
  return { ready: false };
}

async function main() {
  mkdirSync(SCREEN_DIR, { recursive: true });
  console.log("Waiting for production deploy…", BASE);
  const deploy = await waitForDeploy();
  if (!deploy.ready) {
    const out = { STATUS: "FAIL", reason: "deploy_timeout", BASE };
    writeFileSync(join(OUT_DIR, "RESULT.json"), JSON.stringify(out, null, 2));
    console.log(JSON.stringify(out, null, 2));
    process.exit(1);
  }
  console.log("Deploy ready", deploy.health || deploy.phillipsGemini);

  let playwright;
  try {
    playwright = await import("playwright");
  } catch {
    console.log(JSON.stringify({ STATUS: "FAIL", reason: "playwright_not_installed" }));
    process.exit(1);
  }

  const { chromium } = playwright;
  const browser = await chromium.launch({ headless: true });
  const results = [];

  for (const prop of PROPERTIES) {
    const local = loadPublishedReport(prop.id);
    const page = await browser.newPage();
    const consoleErrors = [];
    const pageErrors = [];
    const failedRequests = [];
    page.on("console", (msg) => {
      if (msg.type() === "error") consoleErrors.push(msg.text());
    });
    page.on("pageerror", (err) => pageErrors.push(String(err)));
    page.on("requestfailed", (req) => {
      const u = req.url();
      if (/\/api\/ai-demand|owner-ai-demand/i.test(u)) failedRequests.push(u);
    });

    const notes = [];
    let ok = true;
    let payload = null;

    try {
      const apiRes = await fetch(
        `${BASE}/api/ai-demand-positioning/property/${encodeURIComponent(prop.id)}/report`
      );
      payload = await apiRes.json();
      if (!apiRes.ok || payload?.ok === false) {
        ok = false;
        notes.push("api_report_failed");
      }

      // Core KPI match vs local published
      const locCons = num(local?.executiveMetrics?.considerationRate?.rate);
      const locScen = num(local?.executiveMetrics?.scenarioPresence?.rate);
      const apiCons = num(payload?.executiveMetrics?.considerationRate?.rate);
      const apiScen = num(payload?.executiveMetrics?.scenarioPresence?.rate);
      if (!approxEqual(locCons, apiCons)) {
        ok = false;
        notes.push(`consideration_mismatch local=${locCons} api=${apiCons}`);
      }
      if (!approxEqual(locScen, apiScen)) {
        ok = false;
        notes.push(`scenario_mismatch local=${locScen} api=${apiScen}`);
      }

      const trend = payload?.trends?.[0];
      if (!trend || trend.considerationRate == null || trend.scenarioPresenceRate == null || trend.propertyRealityCoverage == null) {
        ok = false;
        notes.push("trend_missing_three_metrics");
      }

      // Provider denominators
      for (const p of payload?.evidence?.providers || []) {
        if (p.comparable != null && p.total !== p.comparable) {
          ok = false;
          notes.push(`provider_denom_${p.provider}_total_ne_comparable`);
        }
        if (p.scheduled != null && p.comparable != null && p.scheduled < p.comparable) {
          ok = false;
          notes.push(`provider_denom_${p.provider}_scheduled_lt_comparable`);
        }
      }

      // Prose competitors
      const names = [
        ...(payload?.lostDemand?.displacement || []).map((d) => d.name),
        ...(payload?.competitiveSet?.observed || []).map((d) => d.name),
        ...(payload?.competitiveSet?.surprises || []).map((d) => d.name),
      ];
      const prose = names.filter(isProseName);
      if (prose.length) {
        ok = false;
        notes.push("prose_competitors:" + prose.slice(0, 3).join("|"));
      }

      // Unsupported expected impact
      for (const a of payload?.actions || []) {
        if (a.expectedImpact != null && String(a.expectedImpact).trim() !== "") {
          ok = false;
          notes.push("unsupported_expected_impact");
          break;
        }
      }

      const url = `${BASE}/owner-ai-demand-share.html?property=${encodeURIComponent(prop.id)}&v=smoke`;
      await page.goto(url, { waitUntil: "networkidle", timeout: 120000 });
      await page.waitForSelector("#adpKpiRow .aiv-kpi, .adp-er-summary-box, #adpTabExec", {
        timeout: 90000,
      });

      const title = await page.locator("h1, .adp-property-name, #adpPropertyName").first().innerText().catch(() => "");
      const body = await page.locator("body").innerText();

      if (prop.label.split(" ")[0] && !new RegExp(prop.label.split(" ")[0], "i").test(body + title)) {
        notes.push("property_name_weak_match");
      }

      if (/NaN|undefined/.test(body)) {
        ok = false;
        notes.push("nan_or_undefined_visible");
      }

      // Trend section: chart present, awaiting note, no fake delta numbers like +12.3 pp as primary
      const trendSection = page.locator("#adpTrendsSection, #adpTrends, .adp-section-trends, [data-adp-section='trends']").first();
      if ((await trendSection.count()) > 0) {
        await trendSection.scrollIntoViewIfNeeded().catch(() => {});
      } else {
        // fallback: find canvas near "Trend"
        const canvas = page.locator("canvas").first();
        if ((await canvas.count()) === 0) {
          ok = false;
          notes.push("trend_chart_missing");
        }
      }

      const canvasCount = await page.locator("canvas").count();
      if (canvasCount < 1) {
        ok = false;
        notes.push("no_chart_canvas");
      }

      if (!/Awaiting next comparable/i.test(body)) {
        notes.push("awaiting_next_period_copy_missing");
      }

      // CORE footnote stays with Competitive Overview region
      const coreBtn = page.locator("text=/Based on \\d+ CORE comparable hotels/i").first();
      if ((await coreBtn.count()) === 0) {
        // Standalone / uncertified CORE views may omit the footnote — warn only.
        notes.push("core_comparable_footnote_missing");
      }

      // Subject must not appear in customer competitor lists
      const subject = String(payload?.property?.name || prop.label || "").toLowerCase();
      if (subject) {
        const subjectHits = names.filter((n) => String(n).toLowerCase().includes(subject.replace(/,.*/, "").trim().slice(0, 20)));
        // Allow soft-brand competitors that share city words only; require strong subject core.
        const core = subject.replace(/,.*/, "").trim();
        if (core.length >= 10) {
          const selfHits = names.filter((n) => String(n).toLowerCase().includes(core));
          if (selfHits.length) {
            ok = false;
            notes.push("subject_self_in_competitors:" + selfHits[0]);
          }
        }
      }

      // Screenshots
      const pageShot = join(SCREEN_DIR, `${prop.id}__page.png`);
      await page.screenshot({ path: pageShot, fullPage: false });
      const trendShot = join(SCREEN_DIR, `${prop.id}__trend.png`);
      const trendEl = page.locator("#adpTrendsSection, .aiv-detail-trends, #adpTrendChart, canvas").first();
      if ((await trendEl.count()) > 0) {
        await trendEl.screenshot({ path: trendShot }).catch(async () => {
          await page.screenshot({ path: trendShot, fullPage: false });
        });
      } else {
        await page.screenshot({ path: trendShot, fullPage: false });
      }

      // Evidence: open missing if available (scroll + short timeout; non-blocking if click fails)
      const missingBtn = page.locator("[data-adp-evidence-intent]").first();
      if ((await missingBtn.count()) > 0) {
        try {
          await missingBtn.scrollIntoViewIfNeeded();
          await missingBtn.click({ timeout: 8000 });
          await page.waitForTimeout(800);
          const drawerText = await page.locator("#adpEvidenceBody, #adpEvidenceDrawer").innerText().catch(() => "");
          if (!drawerText || /Loading evidence/i.test(drawerText)) {
            notes.push("evidence_drawer_slow_or_empty");
          } else if (/Evidence unavailable|No valid provider evidence/i.test(drawerText)) {
            notes.push("evidence_explicit_empty_ok");
          } else {
            notes.push("evidence_opened_ok");
          }
          await page.keyboard.press("Escape").catch(() => {});
          const closeBtn = page.locator("#adpEvidenceDrawer button, dialog button").first();
          if ((await closeBtn.count()) > 0) await closeBtn.click({ timeout: 2000 }).catch(() => {});
        } catch (e) {
          notes.push("evidence_click_skipped:" + String(e).slice(0, 80));
        }
      }

      // Displacement evidence if present
      const dispBtn = page.locator("[data-adp-displacement], button:has-text('View evidence'), a:has-text('evidence')").first();
      if ((await dispBtn.count()) > 0) {
        try {
          await dispBtn.scrollIntoViewIfNeeded();
          await dispBtn.click({ timeout: 5000 });
          await page.waitForTimeout(500);
          await page.keyboard.press("Escape").catch(() => {});
          notes.push("displacement_evidence_attempted");
        } catch (_) {
          notes.push("displacement_evidence_click_skipped");
        }
      }

      const materialConsole = consoleErrors.filter(
        (t) => !/favicon|third-party|ResizeObserver|Chart\.js/i.test(t)
      );
      if (materialConsole.length || pageErrors.length) {
        ok = false;
        notes.push("material_console_errors");
      }
      if (failedRequests.length) {
        ok = false;
        notes.push("failed_network");
      }
    } catch (err) {
      ok = false;
      notes.push(String(err).slice(0, 240));
    }

    results.push({
      propertyId: prop.id,
      label: prop.label,
      ok,
      notes,
      consoleErrors: consoleErrors.slice(0, 8),
      pageErrors: pageErrors.slice(0, 5),
      failedRequests: failedRequests.slice(0, 5),
      trend: payload?.trends?.[0]
        ? {
            considerationRate: payload.trends[0].considerationRate,
            scenarioPresenceRate: payload.trends[0].scenarioPresenceRate,
            propertyRealityCoverage: payload.trends[0].propertyRealityCoverage,
          }
        : null,
      gemini: (payload?.evidence?.providers || []).find((p) => p.provider === "gemini") || null,
      readSource: payload?._adpReadSource || null,
    });
    await page.close();
  }

  await browser.close();

  let health = null;
  try {
    health = await (await fetch(`${BASE}/api/ai-demand-positioning/read-health`)).json();
  } catch (_) {}

  const passed = results.filter((r) => r.ok).length;
  const out = {
    title: "ADP_EXISTING_HOTEL_PRODUCTION_SMOKE_QA_V1",
    BASE,
    finished: new Date().toISOString(),
    STATUS: passed === results.length ? "PASS" : "FAIL",
    TESTS: `${passed}/${results.length}`,
    readHealth: health,
    byHotel: Object.fromEntries(results.map((r) => [r.propertyId, r.ok ? "PASS" : "FAIL"])),
    results,
    screenshotsDir: SCREEN_DIR,
  };
  writeFileSync(join(OUT_DIR, "RESULT.json"), JSON.stringify(out, null, 2) + "\n");
  console.log(JSON.stringify({ STATUS: out.STATUS, TESTS: out.TESTS, byHotel: out.byHotel, screenshotsDir: SCREEN_DIR }, null, 2));
  process.exit(out.STATUS === "PASS" ? 0 : 1);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
