#!/usr/bin/env node
/**
 * Browser visibility cert — Find Email & Phone must be user-visible
 * (not merely present in source). Fails if serving build lacks V2 markup.
 *
 *   DEALALITY_LOCAL_ADMIN_BASE=http://127.0.0.1:8080 \
 *     node scripts/hotel-explorer/certify-he-contact-find-button-visibility-v1.mjs
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
const HOTEL = process.env.HE_VISIBILITY_HOTEL_ID || "recLuxvwwxID7U2B8";
const OUT = path.join(
  ROOT,
  "reports/hotel-explorer/he-contact-find-button-visibility-v1",
);
const SHOTS = path.join(OUT, "SCREENSHOTS");
fs.mkdirSync(SHOTS, { recursive: true });

const VIEWPORTS = [
  { name: "1920x1080", width: 1920, height: 1080 },
  { name: "1440x900", width: 1440, height: 900 },
  { name: "1366x768", width: 1366, height: 768 },
  { name: "768x1024", width: 768, height: 1024 },
  { name: "390x844", width: 390, height: 844 },
];

const GATES = {
  HE_CONTACT_FIND_BUTTON_RENDERED: "HE_CONTACT_FIND_BUTTON_RENDERED",
  HE_CONTACT_FIND_BUTTON_VISIBLE: "HE_CONTACT_FIND_BUTTON_VISIBLE",
  HE_CONTACT_FIND_BUTTON_RIGHT_ALIGNED: "HE_CONTACT_FIND_BUTTON_RIGHT_ALIGNED",
  HE_CONTACT_FIND_BUTTON_NOT_CLIPPED: "HE_CONTACT_FIND_BUTTON_NOT_CLIPPED",
  HE_CONTACT_FIND_BUTTON_STATE_PARITY: "HE_CONTACT_FIND_BUTTON_STATE_PARITY",
  HE_CONTACT_AIRTABLE_CHANNEL_PARITY: "HE_CONTACT_AIRTABLE_CHANNEL_PARITY",
};

function isVisibleBox(r, style) {
  if (!r || r.width < 8 || r.height < 8) return false;
  if (!style) return true;
  if (style.display === "none" || style.visibility === "hidden") return false;
  if (Number(style.opacity) === 0) return false;
  return true;
}

async function launch() {
  try {
    return await chromium.launch({ headless: true });
  } catch {
    return chromium.launch({ channel: "chrome", headless: true });
  }
}

async function openContacts(page) {
  const assetMeta = [];
  page.on("response", async (res) => {
    const u = res.url();
    if (!/hotel-contact-intelligence\.(js|css)/.test(u)) return;
    const body = await res.text().catch(() => "");
    assetMeta.push({
      url: u.slice(0, 160),
      status: res.status(),
      hasV2: /gdi-list-contact-details--with-action|data-hci-hybrid-enrich/.test(body),
    });
  });
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
  await page.waitForTimeout(5000);
  await page.waitForFunction(
    () =>
      !!document.querySelector("[data-hci-contact-details-cell]") ||
      !!document.querySelector("[data-hci-hybrid-enrich]") ||
      /staging is not available|Contact Intelligence|Find Email/i.test(
        document.querySelector(".hci-root")?.innerText ||
          document.body?.innerText ||
          "",
      ),
    null,
    { timeout: 45000 },
  ).catch(() => {});
  await page.waitForTimeout(1500);
  return assetMeta;
}

async function measureRows(page) {
  return page.evaluate(() => {
    const cells = [...document.querySelectorAll("[data-hci-contact-details-cell]")];
    const detailsBtns = [...document.querySelectorAll(".hci-dir-details-btn")];
    const rows = cells.map((cell, idx) => {
      const slot = cell.querySelector("[data-hci-enrich-slot]");
      const btn =
        cell.querySelector("[data-hci-hybrid-enrich]") ||
        [...cell.querySelectorAll("button")].find((b) =>
          /find email\s*&\s*phone/i.test(b.textContent || ""),
        );
      const stack = cell.querySelector("[data-hci-channel-stack], .gdi-list-contact-channels");
      const row = cell.closest("tr");
      const name =
        row?.querySelector(".hci-dir-contact-stack__name")?.textContent?.trim() ||
        `row_${idx + 1}`;
      const text = (cell.innerText || "").replace(/\s+/g, " ");
      const hasEmail = /@/.test(text) && !/email not available/i.test(text);
      const hasPhone =
        /(\+\d{8,}|\d{3}[\s).-]*\d{3}[\s.-]*\d{4})/.test(text) &&
        !/phone not available/i.test(text);
      const expectDimmed = hasEmail && hasPhone;
      const expectActive = !expectDimmed;

      const td = cell.closest("td") || cell;
      const br = btn ? btn.getBoundingClientRect() : null;
      const sr = stack ? stack.getBoundingClientRect() : null;
      const cr = td.getBoundingClientRect();
      const bs = btn ? getComputedStyle(btn) : null;
      const detailsBtn = detailsBtns[idx];
      const dr = detailsBtn ? detailsBtn.getBoundingClientRect() : null;

      const rightAligned = !!(btn && stack && br.left >= sr.right - 4);
      // Button center must sit inside the Contact Details <td> (tolerant of subpixel + padding)
      const cx = br ? br.left + br.width / 2 : 0;
      const cy = br ? br.top + br.height / 2 : 0;
      const inCell =
        !!btn &&
        cx >= cr.left - 1 &&
        cx <= cr.right + 1 &&
        cy >= cr.top - 1 &&
        cy <= cr.bottom + 1 &&
        br.right <= cr.right + 12;
      const overlapStack =
        !!btn &&
        !!stack &&
        !(br.right <= sr.left + 1 || br.left >= sr.right - 1 || br.bottom <= sr.top || br.top >= sr.bottom);
      const overlapDetails =
        !!btn &&
        !!dr &&
        !(br.right <= dr.left + 1 || br.left >= dr.right - 1 || br.bottom <= dr.top || br.top >= dr.bottom);

      const visible =
        !!btn &&
        br.width >= 8 &&
        br.height >= 8 &&
        bs.display !== "none" &&
        bs.visibility !== "hidden" &&
        Number(bs.opacity) > 0;

      const dimmed =
        !!btn &&
        (btn.disabled ||
          btn.getAttribute("aria-disabled") === "true" ||
          btn.classList.contains("hci-btn--enrich-dimmed") ||
          btn.getAttribute("data-dimmed-complete") === "1");

      return {
        name,
        hasCell: true,
        hasSlot: !!slot,
        hasHybridAttr: !!(btn && btn.getAttribute("data-hci-hybrid-enrich") === "1"),
        hasBtn: !!btn,
        visible,
        w: br ? Math.round(br.width) : 0,
        h: br ? Math.round(br.height) : 0,
        display: bs?.display || null,
        visibility: bs?.visibility || null,
        opacity: bs ? Number(bs.opacity) : null,
        rightAligned,
        inCell,
        overlapStack,
        overlapDetails,
        hasEmail,
        hasPhone,
        expectDimmed,
        expectActive,
        dimmed,
        stateOk: expectDimmed ? dimmed === true : dimmed === false || !btn?.disabled === false,
      };
    });
    return {
      cellCount: cells.length,
      findBtnCount: cells.filter((c) =>
        c.querySelector("[data-hci-hybrid-enrich], button"),
      ).length,
      stagingEmpty: /staging is not available/i.test(
        document.querySelector(".hci-root")?.innerText || "",
      ),
      rows,
    };
  });
}

const browser = await launch();
const report = {
  version: "he-contact-find-button-visibility-v1",
  base: BASE,
  hotelId: HOTEL,
  generatedAt: new Date().toISOString(),
  viewports: [],
  gates: {},
  rootCauseHints: [],
};

let anyFail = false;

for (const vp of VIEWPORTS) {
  const page = await browser.newPage({
    viewport: { width: vp.width, height: vp.height },
  });
  const assets = await openContacts(page);
  const measured = await measureRows(page);

  if (vp.name === "1366x768") {
    await page.screenshot({
      path: path.join(SHOTS, "table_full_1366x768.png"),
      fullPage: true,
    });
    const cell = page.locator("[data-hci-contact-details-cell]").first();
    if (await cell.count()) {
      await cell.screenshot({
        path: path.join(SHOTS, "contact_details_closeup_1366x768.png"),
      });
    }
  } else {
    await page.screenshot({
      path: path.join(SHOTS, `contacts_${vp.name}.png`),
      fullPage: false,
    });
  }

  // Narrow viewports: table min-width exceeds viewport — scroll Contact Details into view
  if (vp.width <= 900 && measured.cellCount > 0) {
    await page.evaluate(() => {
      const wrap = document.querySelector(".dl-table-wrap, .hiw-table-wrap");
      const cell = document.querySelector("[data-hci-contact-details-cell]");
      if (wrap && cell) {
        const wr = wrap.getBoundingClientRect();
        const cr = cell.getBoundingClientRect();
        wrap.scrollLeft += cr.left - wr.left - 8;
      } else if (wrap) {
        wrap.scrollLeft = Math.min(wrap.scrollWidth, wrap.scrollLeft + 280);
      }
    });
    await page.waitForTimeout(250);
    // Re-measure after scroll — button must be fully inside the scrolled cell box
    const rem = await measureRows(page);
    measured.rows = rem.rows;
    measured.cellCount = rem.cellCount;
  }

  const v2Asset = assets.some((a) => a.hasV2);
  if (!v2Asset) {
    report.rootCauseHints.push(
      `${vp.name}: served hotel-contact-intelligence assets lack V2 markers (pre-#695 / stale cache)`,
    );
  }
  if (measured.cellCount === 0) {
    report.rootCauseHints.push(
      `${vp.name}: zero [data-hci-contact-details-cell] — V2 Contact Details cell not rendered`,
    );
  }

  const allRendered =
    measured.cellCount >= 1 &&
    measured.rows.every((r) => r.hasBtn && r.hasSlot && r.hasHybridAttr);
  const allVisible = measured.rows.length > 0 && measured.rows.every((r) => r.visible);
  const allRight =
    measured.rows.length > 0 && measured.rows.every((r) => r.rightAligned && r.inCell);
  const noneClipped =
    measured.rows.length > 0 &&
    measured.rows.every((r) => r.visible && !r.overlapDetails && r.inCell);
  const stateParity =
    measured.rows.length > 0 &&
    measured.rows.every((r) => {
      if (r.expectDimmed) return r.dimmed === true;
      return r.dimmed === false;
    });
  const channelParity =
    !measured.stagingEmpty &&
    measured.rows.some((r) => r.hasEmail || r.hasPhone);

  const vpResult = {
    viewport: vp.name,
    assetV2: v2Asset,
    cellCount: measured.cellCount,
    rows: measured.rows,
    gates: {
      [GATES.HE_CONTACT_FIND_BUTTON_RENDERED]: allRendered ? "PASS" : "FAIL",
      [GATES.HE_CONTACT_FIND_BUTTON_VISIBLE]: allVisible ? "PASS" : "FAIL",
      [GATES.HE_CONTACT_FIND_BUTTON_RIGHT_ALIGNED]: allRight ? "PASS" : "FAIL",
      [GATES.HE_CONTACT_FIND_BUTTON_NOT_CLIPPED]: noneClipped ? "PASS" : "FAIL",
      [GATES.HE_CONTACT_FIND_BUTTON_STATE_PARITY]: stateParity ? "PASS" : "FAIL",
      [GATES.HE_CONTACT_AIRTABLE_CHANNEL_PARITY]: channelParity ? "PASS" : "FAIL",
    },
  };
  if (Object.values(vpResult.gates).some((g) => g === "FAIL")) anyFail = true;
  report.viewports.push(vpResult);
  await page.close();
}

await browser.close();

// Aggregate: all viewports must PASS each gate
for (const g of Object.values(GATES)) {
  const ok = report.viewports.every((v) => v.gates[g] === "PASS");
  report.gates[g] = ok ? "PASS" : "FAIL";
}

fs.writeFileSync(path.join(OUT, "VISIBILITY_CERT.json"), JSON.stringify(report, null, 2) + "\n");
const md = [
  "# HE Contact Find Button Visibility Cert",
  "",
  `- Base: ${BASE}`,
  `- Hotel: ${HOTEL}`,
  `- Generated: ${report.generatedAt}`,
  "",
  "## Gates",
  ...Object.entries(report.gates).map(([k, v]) => `- **${k}**: ${v}`),
  "",
  "## Root-cause hints",
  ...(report.rootCauseHints.length
    ? report.rootCauseHints.map((h) => `- ${h}`)
    : ["- (none)"]),
  "",
  "## Viewports",
  ...report.viewports.map(
    (v) =>
      `### ${v.viewport}\n- cells=${v.cellCount} assetV2=${v.assetV2}\n` +
      Object.entries(v.gates)
        .map(([k, val]) => `  - ${k}: ${val}`)
        .join("\n"),
  ),
  "",
];
fs.writeFileSync(path.join(OUT, "VISIBILITY_CERT.md"), md.join("\n"), "utf8");
console.log(JSON.stringify({ ok: !anyFail, out: OUT, gates: report.gates, hints: report.rootCauseHints }, null, 2));
process.exit(anyFail ? 1 : 0);
