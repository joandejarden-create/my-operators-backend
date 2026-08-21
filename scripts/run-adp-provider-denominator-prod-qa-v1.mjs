#!/usr/bin/env node
/**
 * Focused production QA: provider denominators on all 5 Existing Hotel ADP pages.
 */
import { mkdirSync, writeFileSync } from "fs";
import { join } from "path";
import { loadPublishedReport } from "../lib/ai-demand-positioning/published-snapshot.js";

const BASE =
  process.env.PRODUCTION_BASE ||
  "https://my-operators-backend-production.up.railway.app";

const IDS = [
  "adp_waterstone_boca_raton",
  "adp_renaissance_times_square",
  "adp_cambridge_beaches_bermuda",
  "adp_now_now_noho",
  "adp_hotel_phillips_kansas_city",
];

async function waitReady(maxMs = 300000) {
  const start = Date.now();
  while (Date.now() - start < maxMs) {
    try {
      const j = await (
        await fetch(`${BASE}/api/ai-demand-positioning/property/adp_hotel_phillips_kansas_city/report`)
      ).json();
      const g = (j.evidence?.providers || []).find((p) => p.provider === "gemini");
      if (g?.coverageNote || g?.incompleteCoverage || (g?.comparable === 55 && g?.presence === 49.1)) {
        return true;
      }
    } catch (_) {}
    await new Promise((r) => setTimeout(r, 15000));
  }
  return false;
}

async function main() {
  console.log("Waiting for deploy…");
  const ready = await waitReady();
  if (!ready) {
    console.log(JSON.stringify({ STATUS: "FAIL", reason: "deploy_timeout" }));
    process.exit(1);
  }

  let playwright;
  try {
    playwright = await import("playwright");
  } catch {
    console.log(JSON.stringify({ STATUS: "FAIL", reason: "no_playwright" }));
    process.exit(1);
  }

  const { chromium } = playwright;
  const browser = await chromium.launch({ headless: true });
  const results = [];
  const outDir = join(process.cwd(), "reports/ai-demand-positioning/provider-denominator-prod-qa");
  mkdirSync(outDir, { recursive: true });

  for (const id of IDS) {
    const local = loadPublishedReport(id);
    const api = await (await fetch(`${BASE}/api/ai-demand-positioning/property/${id}/report`)).json();
    const notes = [];
    let ok = true;

    for (const lp of local.evidence?.providers || []) {
      const ap = (api.evidence?.providers || []).find((p) => p.provider === lp.provider);
      if (!ap) {
        ok = false;
        notes.push(`missing_provider_${lp.provider}`);
        continue;
      }
      if (ap.total !== ap.comparable) {
        ok = false;
        notes.push(`${lp.provider}_total_ne_comparable`);
      }
      if (ap.scheduled < ap.comparable) {
        ok = false;
        notes.push(`${lp.provider}_scheduled_lt_comparable`);
      }
      if (Math.abs(Number(ap.presence) - Number(lp.presence)) > 0.15) {
        ok = false;
        notes.push(`${lp.provider}_presence_mismatch`);
      }
      if (ap.comparable === 0 && ap.presence === 0) {
        ok = false;
        notes.push(`${lp.provider}_zero_success_as_zero`);
      }
    }

    const page = await browser.newPage();
    const consoleErrors = [];
    page.on("console", (m) => {
      if (m.type() === "error") consoleErrors.push(m.text());
    });
    await page.goto(`${BASE}/owner-ai-demand-share.html?property=${id}&v=provdenom`, {
      waitUntil: "networkidle",
      timeout: 120000,
    });
    await page.waitForSelector("#adpProviderTableContainer, #adpKpiRow", { timeout: 60000 });
    const body = await page.locator("body").innerText();
    if (/NaN|undefined/.test(body)) {
      ok = false;
      notes.push("nan_undefined");
    }
    // Incomplete coverage disclosure when applicable
    const incomplete = (api.evidence?.providers || []).filter((p) => p.scheduled > p.comparable);
    for (const p of incomplete) {
      const note = `${p.comparable} of ${p.scheduled}`;
      if (!body.includes(String(p.comparable)) || !body.includes(String(p.scheduled))) {
        notes.push(`coverage_disclosure_weak_${p.provider}`);
      } else {
        notes.push(`coverage_ok_${p.provider}:${note}`);
      }
    }
    await page.screenshot({ path: join(outDir, `${id}__providers.png`), fullPage: false });
    await page.close();

    if (consoleErrors.filter((t) => !/favicon|Chart/i.test(t)).length) {
      ok = false;
      notes.push("console_errors");
    }

    results.push({
      propertyId: id,
      ok,
      notes,
      phillipsGemini:
        id === "adp_hotel_phillips_kansas_city"
          ? (api.evidence?.providers || []).find((p) => p.provider === "gemini")
          : undefined,
      consideration: api.executiveMetrics?.considerationRate?.rate,
    });
  }

  await browser.close();
  const passed = results.filter((r) => r.ok).length;
  const out = {
    STATUS: passed === results.length ? "PASS" : "FAIL",
    TESTS: `${passed}/${results.length}`,
    byHotel: Object.fromEntries(results.map((r) => [r.propertyId, r.ok ? "PASS" : "FAIL"])),
    results,
  };
  writeFileSync(join(outDir, "RESULT.json"), JSON.stringify(out, null, 2));
  console.log(JSON.stringify(out, null, 2));
  process.exit(out.STATUS === "PASS" ? 0 : 1);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
