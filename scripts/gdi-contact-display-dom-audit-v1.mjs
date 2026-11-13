#!/usr/bin/env node
/**
 * DOM contact-footer audit: frozen local master ↔ local share ↔ Railway share.
 * Inspects rendered .brand-card__contact-name + .brand-card__contact-line--role.
 * Matches cards by data-open (canonical opportunity id).
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { chromium } from "playwright";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(ROOT, "reports/gdi-contact-display-parity-final-v1");
const HOTEL = "recLuxvwwxID7U2B8";
const RIC_ID = "gdi_opp_regulatory_information_conference_20260930";
const LOCAL = (process.env.GDI_PARITY_LOCAL_URL || "http://127.0.0.1:8080").replace(
  /\/$/,
  "",
);
const PROD = (
  process.env.GDI_PARITY_PRODUCTION_URL ||
  "https://my-operators-backend-production.up.railway.app"
).replace(/\/$/, "");

function readShare() {
  return (
    process.env.GDI_PARITY_SHARE_TOKEN ||
    (
      fs.readFileSync(path.join(ROOT, ".env"), "utf8").match(
        /^GDI_BETHESDA_PRODUCTION_SHARE_TOKEN=(.+)$/m,
      ) || []
    )[1]?.trim()
  );
}

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

async function scrapeContactFooters(page) {
  // Reset filters if possible so all 67 are visible
  await page.evaluate(() => {
    const reset = document.querySelector(
      '[data-gdi-reset-view], button[data-action="reset-view"], .gdi-reset-view',
    );
    if (reset) reset.click();
  });
  await page.waitForTimeout(800);
  return page.evaluate(() => {
    const cards = [...document.querySelectorAll(".brand-card.brand-card--gdi-opp")];
    return cards.map((card) => {
      const id = card.getAttribute("data-open") || null;
      const title = (
        card.querySelector(".brand-card__name")?.textContent || ""
      ).replace(/\s+/g, " ").trim();
      const name = (
        card.querySelector(".brand-card__contact-name")?.textContent || ""
      ).replace(/\s+/g, " ").trim();
      const role = (
        card.querySelector(".brand-card__contact-line--role")?.textContent || ""
      ).replace(/\s+/g, " ").trim();
      return { id, title, name, role, footer: [name, role].filter(Boolean).join(" | ") };
    });
  });
}

fs.mkdirSync(path.join(OUT, "screenshots"), { recursive: true });
const share = readShare();
if (!share) {
  console.error("FAIL missing share token");
  process.exit(2);
}

const browser = await chromium.launch({ headless: true });
const ctx = await browser.newContext({ viewport: { width: 1440, height: 1100 } });

async function load(url) {
  const page = await ctx.newPage();
  await page.goto(url, { waitUntil: "networkidle", timeout: 120000 });
  await page.waitForTimeout(2800);
  const cards = await scrapeContactFooters(page);
  return { page, cards };
}

const local = await load(
  `${LOCAL}/group-demand-intelligence.html?hotelId=${HOTEL}&live=1`,
);
await local.page.screenshot({
  path: path.join(OUT, "screenshots/A-local-master-contacts.png"),
  fullPage: true,
});

const localShare = await load(
  `${LOCAL}/group-demand-intelligence-share.html?hotelId=${HOTEL}&share=${encodeURIComponent(share)}&_cb=${Date.now()}`,
);
await localShare.page.screenshot({
  path: path.join(OUT, "screenshots/B-local-share-contacts.png"),
  fullPage: true,
});

const prod = await load(
  `${PROD}/group-demand-intelligence-share.html?hotelId=${HOTEL}&share=${encodeURIComponent(share)}&_cb=${Date.now()}`,
);
await prod.page.screenshot({
  path: path.join(OUT, "screenshots/C-production-share-contacts.png"),
  fullPage: true,
});

await browser.close();

function byId(rows) {
  const m = new Map();
  for (const r of rows) {
    if (r.id) m.set(r.id, r);
  }
  return m;
}

const a = byId(local.cards);
const b = byId(localShare.cards);
const c = byId(prod.cards);
const ids = [...new Set([...a.keys(), ...b.keys(), ...c.keys()])].sort();

const comparisons = ids.map((id) => {
  const la = a.get(id) || null;
  const lb = b.get(id) || null;
  const lc = c.get(id) || null;
  const nameAC = text(la?.name) === text(lc?.name);
  const roleAC = text(la?.role) === text(lc?.role);
  const nameAB = text(la?.name) === text(lb?.name);
  const roleAB = text(la?.role) === text(lb?.role);
  const nameBC = text(lb?.name) === text(lc?.name);
  const roleBC = text(lb?.role) === text(lc?.role);
  let status = "PASS";
  if (!la || !lc) status = "NOT_VERIFIED";
  else if (!nameAC || !roleAC) status = "FAIL";
  return {
    id,
    title: la?.title || lb?.title || lc?.title || null,
    local: la ? { name: la.name, role: la.role } : null,
    localShare: lb ? { name: lb.name, role: lb.role } : null,
    production: lc ? { name: lc.name, role: lc.role } : null,
    aVsB: Boolean(la && lb && nameAB && roleAB),
    bVsC: Boolean(lb && lc && nameBC && roleBC),
    aVsC: Boolean(la && lc && nameAC && roleAC),
    status,
  };
});

const ric = comparisons.find((x) => x.id === RIC_ID) || null;
const fails = comparisons.filter((x) => x.status === "FAIL");
const notVerified = comparisons.filter((x) => x.status === "NOT_VERIFIED");
const passes = comparisons.filter((x) => x.status === "PASS");

const groupTravelCards = comparisons.filter(
  (x) =>
    /group travel coordinator/i.test(x.local?.role || "") ||
    /group travel coordinator/i.test(x.production?.role || ""),
);

const report = {
  schema: "gdi-contact-display-dom-audit-v1",
  generatedAt: new Date().toISOString(),
  hotelId: HOTEL,
  selectors: {
    card: ".brand-card.brand-card--gdi-opp",
    idAttr: "data-open",
    name: ".brand-card__contact-name",
    role: ".brand-card__contact-line--role",
  },
  counts: {
    local: local.cards.length,
    localShare: localShare.cards.length,
    production: prod.cards.length,
    comparisons: comparisons.length,
    pass: passes.length,
    fail: fails.length,
    notVerified: notVerified.length,
  },
  ric,
  lornaExactApproved:
    Boolean(ric) &&
    text(ric.local?.name) === "Lorna Kipfer" &&
    text(ric.local?.role) === "RIC Conference Contact" &&
    text(ric.production?.name) === "Lorna Kipfer" &&
    text(ric.production?.role) === "RIC Conference Contact" &&
    ric.status === "PASS",
  groupTravelCards,
  failures: fails,
  notVerified,
  comparisons,
};

fs.writeFileSync(
  path.join(OUT, "67-CONTACT-DOM-COMPARISONS.json"),
  `${JSON.stringify(report, null, 2)}\n`,
);

console.log(
  JSON.stringify(
    {
      counts: report.counts,
      lornaExactApproved: report.lornaExactApproved,
      ric,
      failSample: fails.slice(0, 10),
      groupTravelCards: groupTravelCards.map((g) => ({
        id: g.id,
        title: g.title,
        local: g.local,
        production: g.production,
      })),
    },
    null,
    2,
  ),
);

if (
  !report.lornaExactApproved ||
  fails.length ||
  notVerified.length ||
  comparisons.length < 67
) {
  process.exit(2);
}
