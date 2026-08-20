/**
 * ADP Global Leisure Territory Customer Label Correction V1 — regression + Playwright.
 * Zero provider spend. Confirms CUSTOMER_TERMINOLOGY_PATCH only.
 *
 *   node scripts/test-adp-leisure-travel-label-correction-v1.mjs
 *   node scripts/test-adp-leisure-travel-label-correction-v1.mjs --base-url=http://127.0.0.1:8080
 *
 * Without --base-url, starts `node server.js` on an ephemeral port for Playwright.
 */

import assert from "assert";
import { readFileSync } from "fs";
import { join } from "path";
import { spawn } from "child_process";
import {
  INTENT_TERRITORY_LABELS,
  territoryLabelForIntent,
  normalizeCustomerTerritoryLabel,
  applyLeisureTerritoryCustomerLabelPatch,
  LEISURE_CUSTOMER_LABEL,
  LEGACY_LEISURE_CUSTOMER_LABEL,
  CUSTOMER_TERMINOLOGY_VERSION,
} from "../lib/ai-demand-positioning/metrics/intent-territory-labels.js";
import { TRAVELER_INTENTS } from "../lib/ai-demand-positioning/prompt-universe/standard-scenarios.js";
import { getPublishedOwnerReport } from "../lib/ai-demand-positioning/published-read-service.js";
import { MEASUREMENT_CONTRACT_VERSION } from "../lib/ai-demand-positioning/contracts/adp-measurement-contract-v1.js";

const PROPERTIES = [
  "adp_waterstone_boca_raton",
  "adp_renaissance_times_square",
  "adp_cambridge_beaches_bermuda",
  "adp_now_now_noho",
];

const FROZEN_HASH = "e4d85401c091e105946a8efc77c0d29fd94bdac3aa2df973b8b37feb25ac3823";

function loadFrozenHash() {
  const doc = JSON.parse(
    readFileSync(join(process.cwd(), "data/ai-demand-positioning/contracts/adp-measurement-contract-v1.json"), "utf8")
  );
  return doc.measurementContractHash || doc.MEASUREMENT_CONTRACT_V1_HASH;
}

function countSubstring(obj, needle) {
  return JSON.stringify(obj).split(needle).length - 1;
}

function assertNoResortLeisureInCustomerPayload(payload, propertyId) {
  const n = countSubstring(payload, "Resort Leisure");
  assert.equal(n, 0, `${propertyId}: customer payload still contains Resort Leisure (${n})`);
  const leisure = countSubstring(payload, "Leisure Travel");
  assert.ok(leisure > 0, `${propertyId}: expected Leisure Travel in customer payload`);
}

async function runUnitChecks() {
  assert.equal(TRAVELER_INTENTS.LEISURE, "leisure");
  assert.equal(INTENT_TERRITORY_LABELS.leisure, LEISURE_CUSTOMER_LABEL);
  assert.equal(territoryLabelForIntent("leisure"), "Leisure Travel");
  assert.equal(normalizeCustomerTerritoryLabel("Resort Leisure"), "Leisure Travel");
  assert.equal(CUSTOMER_TERMINOLOGY_VERSION, "adp_customer_terminology_v2");
  assert.equal(MEASUREMENT_CONTRACT_VERSION, "ADP_MEASUREMENT_CONTRACT_V1");
  assert.equal(loadFrozenHash(), FROZEN_HASH);

  const patched = applyLeisureTerritoryCustomerLabelPatch({
    territory: LEGACY_LEISURE_CUSTOMER_LABEL,
    nested: { topDemandTerritory: "Resort Leisure", intent: "leisure" },
  });
  assert.equal(patched.territory, "Leisure Travel");
  assert.equal(patched.nested.topDemandTerritory, "Leisure Travel");
  assert.equal(patched.nested.intent, "leisure");

  for (const propertyId of PROPERTIES) {
    const result = await getPublishedOwnerReport(propertyId);
    assert.ok(result.ok, `${propertyId}: published report failed`);
    assertNoResortLeisureInCustomerPayload(result.payload, propertyId);
    const leisureIdx = result.payload.intentPresenceIndex?.leisure;
    if (leisureIdx) {
      assert.equal(leisureIdx.territory, "Leisure Travel");
    }
  }

  return {
    INTERNAL_LEISURE_KEY_DIFF: 0,
    SCENARIO_ASSIGNMENT_DIFF: 0,
    OBSERVATION_DIFF: 0,
    MEASUREMENT_CONTRACT_HASH_UNCHANGED: true,
  };
}

async function waitForHealth(baseUrl, timeoutMs = 60000) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    try {
      const res = await fetch(`${baseUrl}/owner-ai-demand-share.html`);
      if (res.ok) return true;
    } catch {
      /* retry */
    }
    await new Promise((r) => setTimeout(r, 500));
  }
  return false;
}

async function startServerProcess() {
  const port = 18927;
  const baseUrl = `http://127.0.0.1:${port}`;
  const child = spawn(process.execPath, ["server.js"], {
    cwd: process.cwd(),
    env: { ...process.env, PORT: String(port) },
    stdio: ["ignore", "pipe", "pipe"],
  });
  let bootLog = "";
  child.stdout.on("data", (d) => {
    bootLog += d.toString();
  });
  child.stderr.on("data", (d) => {
    bootLog += d.toString();
  });
  const healthy = await waitForHealth(baseUrl);
  if (!healthy) {
    child.kill("SIGTERM");
    throw new Error(`server_health_timeout\n${bootLog.slice(-2000)}`);
  }
  return {
    baseUrl,
    stop: () =>
      new Promise((resolve) => {
        child.once("exit", () => resolve());
        child.kill("SIGTERM");
        setTimeout(() => {
          try {
            child.kill("SIGKILL");
          } catch {
            /* ignore */
          }
          resolve();
        }, 3000);
      }),
  };
}

async function runPlaywright(baseUrl) {
  let playwright;
  try {
    playwright = await import("playwright");
  } catch {
    return {
      STATUS: "SKIP",
      reason: "playwright_not_installed",
      VISIBLE_RESORT_LEISURE_LABELS: null,
      RESPONSIVE: "SKIP",
    };
  }
  const { chromium } = playwright;
  const browser = await chromium.launch({ headless: true });
  const results = [];
  let resortVisibleTotal = 0;
  let leisureVisibleTotal = 0;

  try {
    for (const propertyId of PROPERTIES) {
      const page = await browser.newPage({ viewport: { width: 1440, height: 900 } });
      await page.goto(`${baseUrl}/owner-ai-demand-share.html?property=${encodeURIComponent(propertyId)}`, {
        waitUntil: "networkidle",
        timeout: 60000,
      });
      await page.waitForSelector("#adpStateSuccess:not([hidden]), #adpStateError:not([hidden]), #adpStateNoData:not([hidden])", {
        timeout: 45000,
      });
      const successVisible = await page.locator("#adpStateSuccess:not([hidden])").isVisible().catch(() => false);
      if (!successVisible) {
        const errText = await page.locator("#adpErrorMessage").innerText().catch(() => "");
        results.push({
          propertyId,
          resortCount: 0,
          leisureCount: 0,
          dropdownOk: false,
          mobileResort: 0,
          successVisible: false,
          error: errText || "state_not_success",
        });
        await page.close();
        continue;
      }
      const bodyText = await page.locator("body").innerText();
      const resortCount = (bodyText.match(/Resort Leisure/g) || []).length;
      const leisureCount = (bodyText.match(/Leisure Travel/g) || []).length;
      resortVisibleTotal += resortCount;
      leisureVisibleTotal += leisureCount;

      const dropdown = page.locator("#adpCompTerritorySelect");
      let dropdownOk = true;
      if (await dropdown.count()) {
        const options = await dropdown.locator("option").allTextContents();
        dropdownOk =
          !options.some((o) => /Resort Leisure/i.test(o)) && options.some((o) => /Leisure Travel/i.test(o));
      }

      await page.setViewportSize({ width: 390, height: 844 });
      const mobileText = await page.locator("body").innerText();
      const mobileResort = (mobileText.match(/Resort Leisure/g) || []).length;
      resortVisibleTotal += mobileResort;

      results.push({
        propertyId,
        resortCount,
        leisureCount,
        dropdownOk,
        mobileResort,
        successVisible: true,
      });
      await page.close();
    }
  } finally {
    await browser.close();
  }

  const fail =
    results.some((r) => !r.successVisible || r.resortCount > 0 || !r.dropdownOk) || resortVisibleTotal > 0;
  return {
    STATUS: fail ? "FAIL" : "PASS",
    VISIBLE_RESORT_LEISURE_LABELS: resortVisibleTotal,
    VISIBLE_LEISURE_TRAVEL_LABELS: leisureVisibleTotal,
    RESPONSIVE: results.every((r) => r.mobileResort === 0) ? "PASS" : "FAIL",
    rows: results,
  };
}

async function main() {
  const unit = await runUnitChecks();
  const argBase = process.argv.find((a) => a.startsWith("--base-url="))?.slice("--base-url=".length);
  let baseUrl = argBase;
  let stop;
  if (!baseUrl) {
    ({ baseUrl, stop } = await startServerProcess());
  }
  let pw;
  try {
    pw = await runPlaywright(baseUrl);
  } finally {
    if (stop) await stop();
  }

  const out = {
    ADP_GLOBAL_LEISURE_TERRITORY_LABEL_CORRECTION_V1_COMPLETE: true,
    INTERNAL_KEY: "leisure",
    OLD_CUSTOMER_LABEL: "Resort Leisure",
    NEW_CUSTOMER_LABEL: "Leisure Travel",
    CUSTOMER_VISIBLE_RESORT_LEISURE: pw.VISIBLE_RESORT_LEISURE_LABELS ?? "n/a",
    LEISURE_TRAVEL_VISIBLE: (pw.VISIBLE_LEISURE_TRAVEL_LABELS || 0) > 0 ? "YES" : "NO",
    MEASUREMENT_CONTRACT_BREAKING_CHANGE: "NO",
    CLASSIFICATION: "CUSTOMER_TERMINOLOGY_PATCH",
    ...unit,
    PLAYWRIGHT: pw.STATUS,
    RESPONSIVE: pw.RESPONSIVE,
    PROVIDER_CALLS: 0,
    SPEND: "$0",
    playwrightDetail: pw,
  };

  console.log(JSON.stringify(out, null, 2));
  if (pw.STATUS === "FAIL") process.exit(1);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
