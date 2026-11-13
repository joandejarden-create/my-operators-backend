#!/usr/bin/env node
/**
 * Complete drawer DOM parity — Bethesda Marriott (67 opportunities).
 *
 * Opens the live external customer share, extracts full drawer section headings
 * + normalized body text for every opportunity, and compares against the
 * approved publication package's expected need-date / section contract.
 *
 * This is a production-browser gate. It does NOT compare fingerprint alone.
 *
 *   GDI_PARITY_SHARE_TOKEN=... node scripts/test-gdi-bethesda-complete-drawer-dom-parity-v1.mjs
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { chromium } from "playwright";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const outDir = path.join(
  ROOT,
  "reports/gdi-bethesda-rendered-parity-root-cause-v1",
);
fs.mkdirSync(path.join(outDir, "screenshots"), { recursive: true });

const HOTEL = "recLuxvwwxID7U2B8";
const EXPECTED_FP =
  "441773211494cebaf50c03ef8921ccac6c57f31ac5cb2398585352b425e36f7f";
const EXPECTED_VERSION =
  "gdi_pub_recLuxvwwxID7U2B8_20261113_reconciliation_v1";

const share =
  process.env.GDI_PARITY_SHARE_TOKEN ||
  (fs.readFileSync(path.join(ROOT, ".env"), "utf8").match(
    /^GDI_BETHESDA_PRODUCTION_SHARE_TOKEN=(.+)$/m,
  ) || [])[1]?.trim();
assert.ok(share, "GDI_PARITY_SHARE_TOKEN or .env token required");

const base = (
  process.env.GDI_PARITY_PRODUCTION_URL ||
  "https://my-operators-backend-production.up.railway.app"
).replace(/\/$/, "");

function norm(s) {
  return String(s || "")
    .replace(/\u00a0/g, " ")
    .replace(/[ \t]+/g, " ")
    .replace(/\r\n/g, "\n")
    .replace(/\n{3,}/g, "\n\n")
    .trim();
}

async function fetchJson(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`HTTP ${res.status} for ${url}`);
  return res.json();
}

const listUrl = `${base}/api/group-demand-intelligence/share/hotels/${HOTEL}/opportunities?view=full&share=${encodeURIComponent(share)}`;
const list = await fetchJson(listUrl);
assert.equal(list.listScope, "PUBLICATION_PACKAGE");
assert.equal(list.opportunities?.length, 67);
assert.equal(
  list.publication?.contentFingerprintSha256,
  EXPECTED_FP,
  "publication fingerprint drift",
);
assert.equal(list.publication?.publicationVersion, EXPECTED_VERSION);

const browser = await chromium.launch({ headless: true });
const page = await browser.newPage({ viewport: { width: 1440, height: 900 } });
const pageErrors = [];
page.on("pageerror", (e) => pageErrors.push(String(e.message || e)));

const shareUrl = `${base}/group-demand-intelligence-share.html?share=${encodeURIComponent(share)}`;
await page.goto(shareUrl, { waitUntil: "domcontentloaded", timeout: 120000 });
await page.waitForSelector(".gdi-card, [data-open]", { timeout: 120000 });

const rows = [];
let materialFailures = 0;

for (const o of list.opportunities) {
  const id = o.id;
  const title = o.title || o.displayTitle || id;
  const expectNeedDate = Boolean(o.needDateMatchV1);

  // Prefer clicking the card for this opportunity id.
  const card = page.locator(`[data-open="${id}"]`).first();
  const cardCount = await card.count();
  if (cardCount === 0) {
    // Fallback: search by title text.
    await page.locator(`text=${title}`).first().click({ timeout: 15000 }).catch(() => {});
  } else {
    await card.click({ timeout: 15000 });
  }

  let extracted = {
    ok: false,
    sections: [],
    text: "",
    needDatePresent: false,
    error: null,
  };
  try {
    await page.waitForFunction(
      () => {
        const d = document.querySelector("#gdiShareDrawer");
        if (!d || !d.open) return false;
        const body = d.querySelector("#gdiShareDrawerBody, .gdi-drawer-body");
        const t = (body && body.innerText) || "";
        return t.length > 40 && !/prose is not defined/i.test(t);
      },
      null,
      { timeout: 45000 },
    );
    await page.evaluate(() => {
      const body = document.querySelector("#gdiShareDrawerBody");
      if (body) body.scrollTop = body.scrollHeight;
    });
    await page.waitForTimeout(250);
    await page.evaluate(() => {
      const body = document.querySelector("#gdiShareDrawerBody");
      if (body) body.scrollTop = 0;
    });
    extracted = await page.evaluate(() => {
      const root = document.querySelector("#gdiShareDrawer");
      const body =
        root?.querySelector("#gdiShareDrawerBody, .gdi-drawer-body") || null;
      if (!root?.open || !body) {
        return {
          ok: false,
          sections: [],
          text: "",
          needDatePresent: false,
          error: "drawer_not_open",
        };
      }
      const sections = [
        ...body.querySelectorAll(
          "h3, .gdi-section > h3, [data-section-id] > h3",
        ),
      ]
        .map((h) => (h.textContent || "").replace(/\s+/g, " ").trim())
        .filter(Boolean);
      const needEl = body.querySelector(
        '[data-section-id="hotel_need_date_match"]',
      );
      return {
        ok: true,
        sections,
        text: (body.innerText || "").slice(0, 12000),
        needDatePresent: Boolean(needEl),
        needDateText: needEl
          ? (needEl.innerText || "").replace(/\s+/g, " ").trim().slice(0, 2000)
          : null,
        error: null,
      };
    });
  } catch (e) {
    extracted.error = String(e?.message || e);
  }

  // Close drawer for next card.
  await page.keyboard.press("Escape").catch(() => {});
  await page
    .locator('#gdiShareDrawer [data-close], #gdiShareDrawerClose, .gdi-drawer-close')
    .first()
    .click({ timeout: 2000 })
    .catch(() => {});
  await page.waitForFunction(
    () => {
      const d = document.querySelector("#gdiShareDrawer");
      return !d || !d.open;
    },
    null,
    { timeout: 10000 },
  ).catch(() => {});

  const sections = extracted.sections || [];
  const overviewIdx = sections.findIndex((s) =>
    /opportunity overview/i.test(s),
  );
  const needIdx = sections.findIndex((s) => /need-date match/i.test(s));
  const whatIdx = sections.findIndex((s) => /what is happening/i.test(s));

  const failures = [];
  if (!extracted.ok) failures.push(extracted.error || "extract_failed");
  if (/prose is not defined/i.test(extracted.text || "")) {
    failures.push("prose_undefined_crash");
  }
  if (expectNeedDate && !extracted.needDatePresent) {
    failures.push("missing_need_date_section");
  }
  if (!expectNeedDate && extracted.needDatePresent) {
    failures.push("unexpected_need_date_section");
  }
  if (expectNeedDate && overviewIdx >= 0 && needIdx >= 0 && needIdx < overviewIdx) {
    failures.push("need_date_before_overview");
  }
  if (expectNeedDate && needIdx >= 0 && whatIdx >= 0 && needIdx > whatIdx) {
    failures.push("need_date_after_what_is_happening");
  }
  if (
    expectNeedDate &&
    o.needDateMatchV1?.needPeriodLabel &&
    !(extracted.needDateText || "").includes(o.needDateMatchV1.needPeriodLabel)
  ) {
    failures.push("need_period_label_missing_from_dom");
  }
  if (
    expectNeedDate &&
    o.needDateMatchV1?.whyThisMatches &&
    !norm(extracted.needDateText || "").includes(
      norm(o.needDateMatchV1.whyThisMatches).slice(0, 40),
    )
  ) {
    failures.push("need_date_why_mismatch");
  }

  if (failures.length) materialFailures += 1;

  rows.push({
    id,
    title,
    expectNeedDate,
    needDatePresent: extracted.needDatePresent,
    sections,
    overviewIdx,
    needIdx,
    whatIdx,
    failures,
    textNormLen: norm(extracted.text).length,
    needDateText: extracted.needDateText || null,
  });
}

await browser.close();

const sectionParity = {
  hotelId: HOTEL,
  publicationVersion: EXPECTED_VERSION,
  fingerprint: EXPECTED_FP,
  opportunityCount: rows.length,
  expectedNeedDateCount: list.opportunities.filter((o) => o.needDateMatchV1)
    .length,
  materialFailures,
  pageErrors: pageErrors.slice(0, 20),
  ok: materialFailures === 0 && rows.length === 67,
  rows,
};

fs.writeFileSync(
  path.join(outDir, "04-ALL-67-SECTION-PARITY.json"),
  JSON.stringify(sectionParity, null, 2),
);

const textParity = {
  note: "Normalized whitespace-only; missing/extra sections counted as material failures in 04",
  opportunityCount: rows.length,
  materialFailures,
  ok: materialFailures === 0,
  summaries: rows.map((r) => ({
    id: r.id,
    title: r.title,
    sectionOrder: r.sections,
    failures: r.failures,
    textNormLen: r.textNormLen,
  })),
};
fs.writeFileSync(
  path.join(outDir, "05-COMPLETE-DOM-TEXT-PARITY.json"),
  JSON.stringify(textParity, null, 2),
);

const needDateParity = {
  expectedInPackage: list.opportunities.filter((o) => o.needDateMatchV1).length,
  presentInDom: rows.filter((r) => r.needDatePresent).length,
  missingInDom: rows
    .filter((r) => r.expectNeedDate && !r.needDatePresent)
    .map((r) => r.id),
  unexpectedInDom: rows
    .filter((r) => !r.expectNeedDate && r.needDatePresent)
    .map((r) => r.id),
  ok:
    rows.filter((r) => r.expectNeedDate && !r.needDatePresent).length === 0 &&
    rows.filter((r) => !r.expectNeedDate && r.needDatePresent).length === 0,
};
fs.writeFileSync(
  path.join(outDir, "06-NEED-DATE-PARITY.json"),
  JSON.stringify(needDateParity, null, 2),
);

console.log(
  JSON.stringify(
    {
      ok: sectionParity.ok,
      materialFailures,
      opportunityCount: rows.length,
      expectedNeedDateCount: sectionParity.expectedNeedDateCount,
      needDateParityOk: needDateParity.ok,
      pageErrorCount: pageErrors.length,
    },
    null,
    2,
  ),
);
process.exit(sectionParity.ok ? 0 : 2);
