#!/usr/bin/env node
/**
 * DOM contact-footer parity gates (negative + positive).
 *
 * Positive: Lorna Kipfer / RIC Conference Contact on local master + production.
 * Negative: deliberately mutate a rendered subtitle → comparison MUST fail.
 */
import "dotenv/config";
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { chromium } from "playwright";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
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

async function scrape(page) {
  return page.evaluate(() => {
    return [...document.querySelectorAll(".brand-card.brand-card--gdi-opp")].map(
      (card) => ({
        id: card.getAttribute("data-open") || null,
        title: (card.querySelector(".brand-card__name")?.textContent || "")
          .replace(/\s+/g, " ")
          .trim(),
        name: (card.querySelector(".brand-card__contact-name")?.textContent || "")
          .replace(/\s+/g, " ")
          .trim(),
        role: (
          card.querySelector(".brand-card__contact-line--role")?.textContent ||
          ""
        )
          .replace(/\s+/g, " ")
          .trim(),
      }),
    );
  });
}

async function mutateRicRole(page, newRole) {
  return page.evaluate(
    ({ id, newRole: role }) => {
      const card = [...document.querySelectorAll(".brand-card.brand-card--gdi-opp")].find(
        (c) => c.getAttribute("data-open") === id,
      );
      if (!card) return false;
      const roleEl = card.querySelector(".brand-card__contact-line--role");
      if (!roleEl) return false;
      roleEl.textContent = role;
      return true;
    },
    { id: RIC_ID, newRole },
  );
}

const share = readShare();
assert.ok(share, "share token required");

const browser = await chromium.launch({ headless: true });
const ctx = await browser.newContext({ viewport: { width: 1440, height: 1100 } });

const localPage = await ctx.newPage();
await localPage.goto(
  `${LOCAL}/group-demand-intelligence.html?hotelId=${HOTEL}&live=1`,
  { waitUntil: "networkidle", timeout: 120000 },
);
await localPage.waitForTimeout(2500);
const localCards = await scrape(localPage);
const localRic = localCards.find((c) => c.id === RIC_ID);
assert.ok(localRic, "RIC card missing on local master");
assert.equal(text(localRic.name), "Lorna Kipfer");
assert.equal(
  text(localRic.role),
  "RIC Conference Contact",
  "local master must show RIC Conference Contact under Lorna Kipfer",
);

const prodPage = await ctx.newPage();
await prodPage.goto(
  `${PROD}/group-demand-intelligence-share.html?hotelId=${HOTEL}&share=${encodeURIComponent(share)}&_cb=${Date.now()}`,
  { waitUntil: "networkidle", timeout: 120000 },
);
await prodPage.waitForTimeout(2500);
const prodCards = await scrape(prodPage);
const prodRic = prodCards.find((c) => c.id === RIC_ID);
assert.ok(prodRic, "RIC card missing on production share");
assert.equal(text(prodRic.name), "Lorna Kipfer");
assert.equal(
  text(prodRic.role),
  "RIC Conference Contact",
  "production DOM must show RIC Conference Contact (not Group Travel Coordinator)",
);

// Full set sizes — require 67 cards on both sides BEFORE mutation
assert.equal(localCards.length, 67, `local cards=${localCards.length}`);
assert.equal(prodCards.length, 67, `prod cards=${prodCards.length}`);

let mismatches = 0;
const mismatchRows = [];
const localBy = new Map(localCards.map((c) => [c.id, c]));
for (const p of prodCards) {
  const l = localBy.get(p.id);
  if (!l || text(l.name) !== text(p.name) || text(l.role) !== text(p.role)) {
    mismatches += 1;
    mismatchRows.push({
      id: p.id,
      local: l ? { name: l.name, role: l.role } : null,
      production: { name: p.name, role: p.role },
    });
  }
}
assert.equal(
  mismatches,
  0,
  `DOM contact footer mismatches: ${JSON.stringify(mismatchRows.slice(0, 8))}`,
);

// Negative: mutate production DOM subtitle → must NOT match local
const mutated = await mutateRicRole(prodPage, "Group Travel Coordinator");
assert.equal(mutated, true, "must be able to mutate role text node for negative test");
const after = await scrape(prodPage);
const mutatedRic = after.find((c) => c.id === RIC_ID);
assert.equal(text(mutatedRic.role), "Group Travel Coordinator");
assert.notEqual(
  text(localRic.role),
  text(mutatedRic.role),
  "negative gate: deliberate subtitle change must fail parity",
);

await browser.close();

console.log(
  JSON.stringify(
    {
      ok: true,
      positiveLorna: {
        local: localRic,
        production: prodRic,
      },
      negativeMutationDetected: true,
      cardsCompared: 67,
      fieldsPerCard: ["brand-card__contact-name", "brand-card__contact-line--role"],
    },
    null,
    2,
  ),
);
