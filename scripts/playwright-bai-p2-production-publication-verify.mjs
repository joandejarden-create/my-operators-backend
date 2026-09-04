#!/usr/bin/env node
/**
 * Post-publication production verification for BAI Period 2.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { chromium } from "playwright";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");
const PROD = process.env.BAI_PROD_BASE || "https://my-operators-backend-production.up.railway.app";
const outDir = path.join(ROOT, "reports/bai-p2-promotion-readiness/production-verify");
fs.mkdirSync(outDir, { recursive: true });

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

const expectedCounts = { marriott: 5, hilton: 4, choice: 5, ihg: 5 };
const viewports = [1440, 1024, 390];
let failed = 0;
const results = {};

function fail(msg) {
  failed += 1;
  console.error("FAIL", msg);
}
function pass(msg) {
  console.log("PASS", msg);
}

async function fetchJson(url) {
  const res = await fetch(url);
  const json = await res.json().catch(() => ({}));
  return { status: res.status, json };
}

// API fingerprint checks
for (const [parent, token] of Object.entries(SHARE_TOKENS)) {
  const url = `${PROD}/api/ai-visibility/brand/executive-summary?share=${encodeURIComponent(token)}&geography=CALA&provider=all`;
  const { status, json } = await fetchJson(url);
  const cl = json.customerLongitudinal;
  const pv = cl?.parents?.[0];
  const blob = JSON.stringify(json);
  results[parent] = {
    status,
    published: json.PERIOD_2_PUBLICATION_STATE,
    attached: json.customerLongitudinalAttached,
    currentDate: json.publicationDates?.currentDate || pv?.currentDate,
    priorDate: json.publicationDates?.priorDate || pv?.priorDate,
    brands: pv?.brandMovement?.rows?.length,
    freshness: json.monitoringFreshness?.LAST_MONITORED_DISPLAY,
    hasAug18: blob.includes("2026-08-18"),
    hasIntent: blob.includes("Intent-level change is not yet comparable"),
    hasProvider: blob.includes("Change vs Prior Run is not comparable"),
    hasCohort: blob.includes("peer universe changed"),
  };
  if (status !== 200) fail(`${parent} status ${status}`);
  else pass(`${parent} API 200`);
  if (json.PERIOD_2_PUBLICATION_STATE !== "PUBLISHED") fail(`${parent} not PUBLISHED`);
  else pass(`${parent} PUBLISHED`);
  if (!json.customerLongitudinalAttached) fail(`${parent} longitudinal not attached`);
  else pass(`${parent} longitudinal attached`);
  if (results[parent].currentDate !== "2026-09-03") fail(`${parent} current date`);
  else pass(`${parent} current Sep3`);
  if (results[parent].priorDate !== "2026-08-14") fail(`${parent} prior date`);
  else pass(`${parent} prior Aug14`);
  if (results[parent].brands !== expectedCounts[parent]) fail(`${parent} brand count`);
  else pass(`${parent} brands ${expectedCounts[parent]}`);
  if (results[parent].hasAug18) fail(`${parent} Aug18 leak`);
  else pass(`${parent} no Aug18`);
  if (!results[parent].hasIntent) fail(`${parent} intent disclosure`);
  else pass(`${parent} intent`);
  if (!results[parent].hasProvider) fail(`${parent} provider disclosure`);
  else pass(`${parent} provider`);
  if (!results[parent].hasCohort) fail(`${parent} cohort disclosure`);
  else pass(`${parent} cohort`);
}

// Cross-parent 403
{
  const marriottTok = SHARE_TOKENS.marriott;
  // Using marriott token against a hilton-only expectation is hard via query;
  // verify candidate endpoint blocked on share.
  const { status } = await fetchJson(
    `${PROD}/api/ai-visibility/brand/internal-longitudinal-qa?share=${encodeURIComponent(marriottTok)}`
  );
  if (status === 401 || status === 403) pass("share→internal QA blocked");
  else fail(`share→internal QA status ${status}`);
  const prev = await fetchJson(
    `${PROD}/api/ai-visibility/brand/customer-promotion-preview?share=${encodeURIComponent(marriottTok)}&parent=marriott`
  );
  if (prev.status === 401 || prev.status === 403) pass("share→preview blocked");
  else fail(`share→preview status ${prev.status}`);
}

// Playwright visual
const browser = await chromium.launch({ headless: true });
try {
  for (const [parent, token] of Object.entries(SHARE_TOKENS)) {
    for (const width of viewports) {
      const page = await browser.newPage({ viewport: { width, height: 900 } });
      await page.goto(
        `${PROD}/brand-ai-visibility-share.html?share=${encodeURIComponent(token)}`,
        { waitUntil: "networkidle", timeout: 120000 }
      );
      await page.waitForTimeout(2500);
      const bodyText = await page.locator("body").innerText();
      const shot = path.join(outDir, `${parent}-${width}.png`);
      await page.screenshot({ path: shot, fullPage: false });
      if (bodyText.includes("2026-08-18")) fail(`DOM Aug18 ${parent}@${width}`);
      if (/internal QA|Period 2 candidate|UNPROMOTED/i.test(bodyText)) {
        fail(`internal chrome ${parent}@${width}`);
      }
      const longVisible = await page.locator("#aivCustomerLongitudinal").isVisible().catch(() => false);
      if (!longVisible) fail(`longitudinal hidden ${parent}@${width}`);
      else pass(`visual ${parent}@${width}`);
      await page.close();
    }
  }
} finally {
  await browser.close();
}

const summary = {
  failed,
  PROD,
  results,
  BAI_PERIOD_2_CUSTOMER_PUBLICATION: failed === 0 ? "PASS" : "FAIL",
  ROLLBACK_USED: false,
};
fs.writeFileSync(path.join(outDir, "production-verify-summary.json"), JSON.stringify(summary, null, 2));
console.log(JSON.stringify(summary, null, 2));
process.exit(failed ? 1 : 0);
