#!/usr/bin/env node
/**
 * GDI complete release parity — local customer share vs Railway external share.
 *
 *   npm run test:gdi-complete-release-parity
 *
 * Compares actual HTTP-served assets, publication DTOs, browser UI, and tour
 * geometry. Never compares local↔local. Never prints secrets.
 *
 * Env:
 *   GDI_BETHESDA_PRODUCTION_SHARE_TOKEN / GDI_PARITY_SHARE_TOKEN
 *   GDI_PARITY_LOCAL_URL (default http://127.0.0.1:8080)
 *   GDI_PARITY_PRODUCTION_URL (default Railway production)
 *   GDI_PARITY_SKIP_ALL_DRAWERS=1  — sample drawers only (faster)
 *   GDI_PARITY_SKIP_TOUR=1
 */
import assert from "node:assert/strict";
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { chromium } from "playwright";
import { execSync } from "node:child_process";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(ROOT, "reports/gdi-source-asset-runtime-ui-parity-v1");
fs.mkdirSync(path.join(OUT, "screenshots"), { recursive: true });

const HOTEL_ID = "recLuxvwwxID7U2B8";
const EXPECTED_FP =
  "441773211494cebaf50c03ef8921ccac6c57f31ac5cb2398585352b425e36f7f";
const EXPECTED_PKG = "gdi_pub_recLuxvwwxID7U2B8_20261113_reconciliation_v1";

const share =
  process.env.GDI_PARITY_SHARE_TOKEN ||
  (fs.readFileSync(path.join(ROOT, ".env"), "utf8").match(
    /^GDI_BETHESDA_PRODUCTION_SHARE_TOKEN=(.+)$/m,
  ) || [])[1]?.trim();
assert.ok(share, "share token required");

const localBase = (
  process.env.GDI_PARITY_LOCAL_URL || "http://127.0.0.1:8080"
).replace(/\/$/, "");
const prodBase = (
  process.env.GDI_PARITY_PRODUCTION_URL ||
  "https://my-operators-backend-production.up.railway.app"
).replace(/\/$/, "");

assert.notEqual(
  localBase,
  prodBase,
  "local and production bases must differ — refuse local↔local comparison",
);

const SKIP_ALL_DRAWERS = process.env.GDI_PARITY_SKIP_ALL_DRAWERS === "1";
const SKIP_TOUR = process.env.GDI_PARITY_SKIP_TOUR === "1";

function sha256(buf) {
  return crypto.createHash("sha256").update(buf).digest("hex");
}
function normalizeEol(buf) {
  return Buffer.from(
    Buffer.from(buf).toString("utf8").replace(/\r\n/g, "\n").replace(/\r/g, "\n"),
  );
}
function stable(v) {
  return JSON.stringify(sortKeys(v));
}
function sortKeys(v) {
  if (Array.isArray(v)) return v.map(sortKeys);
  if (v && typeof v === "object") {
    const out = {};
    for (const k of Object.keys(v).sort()) out[k] = sortKeys(v[k]);
    return out;
  }
  return v;
}
function deepDiff(a, b, prefix = "", acc = [], limit = 80) {
  if (acc.length >= limit) return acc;
  if (stable(a) === stable(b)) return acc;
  const ta = a === null ? "null" : Array.isArray(a) ? "array" : typeof a;
  const tb = b === null ? "null" : Array.isArray(b) ? "array" : typeof b;
  if (ta !== tb || ta !== "object") {
    acc.push({ path: prefix || "(root)", local: a, production: b });
    return acc;
  }
  if (Array.isArray(a) || Array.isArray(b)) {
    const la = Array.isArray(a) ? a : [];
    const lb = Array.isArray(b) ? b : [];
    const n = Math.max(la.length, lb.length);
    for (let i = 0; i < n && acc.length < limit; i++) {
      deepDiff(la[i], lb[i], `${prefix}[${i}]`, acc, limit);
    }
    return acc;
  }
  const keys = new Set([...Object.keys(a || {}), ...Object.keys(b || {})]);
  for (const k of [...keys].sort()) {
    if (acc.length >= limit) break;
    // Operational feedback may legitimately differ — strip known mutable paths.
    if (
      /feedback|validationHistory|hotelValidation|lastValidated|updatedAt|recordedAt/i.test(
        k,
      )
    ) {
      continue;
    }
    deepDiff(a?.[k], b?.[k], prefix ? `${prefix}.${k}` : k, acc, limit);
  }
  return acc;
}

async function fetchJson(base, apiPath) {
  const sep = apiPath.includes("?") ? "&" : "?";
  const url = `${base}${apiPath}${sep}share=${encodeURIComponent(share)}`;
  const res = await fetch(url, {
    headers: { "Cache-Control": "no-cache", Pragma: "no-cache" },
  });
  const text = await res.text();
  let json = null;
  try {
    json = JSON.parse(text);
  } catch {
    json = { _parseError: true, preview: text.slice(0, 200) };
  }
  return { status: res.status, json, url: apiPath, bytes: text.length };
}

async function fetchAsset(base, urlPath) {
  const url = `${base}${urlPath}?cb=${Date.now()}`;
  const res = await fetch(url, {
    headers: { "Cache-Control": "no-cache", Pragma: "no-cache" },
  });
  const buf = Buffer.from(await res.arrayBuffer());
  return {
    status: res.status,
    bytes: buf.length,
    sha256Raw: sha256(buf),
    sha256Eol: sha256(normalizeEol(buf)),
    contentType: res.headers.get("content-type"),
    cacheControl: res.headers.get("cache-control"),
    etag: res.headers.get("etag"),
    lastModified: res.headers.get("last-modified"),
  };
}

function gitIdentity() {
  try {
    return {
      head: execSync("git rev-parse HEAD", { cwd: ROOT }).toString().trim(),
      short: execSync("git rev-parse --short HEAD", { cwd: ROOT })
        .toString()
        .trim(),
      branch: execSync("git rev-parse --abbrev-ref HEAD", { cwd: ROOT })
        .toString()
        .trim(),
      dirty: Boolean(
        execSync("git status --porcelain", { cwd: ROOT }).toString().trim(),
      ),
    };
  } catch (e) {
    return { error: String(e.message || e) };
  }
}

/** Parse share HTML for stylesheet/script deps (customer closure). */
function parseHtmlDeps(html) {
  const css = [...html.matchAll(/href="([^"]+\.css[^"]*)"/g)].map((m) => m[1]);
  const js = [...html.matchAll(/src="([^"]+\.js[^"]*)"/g)].map((m) => m[1]);
  return { css, js, all: [...css, ...js] };
}

function stripQuery(u) {
  return String(u || "").split("?")[0];
}

const failures = [];
const rootCauses = [];
function fail(code, detail) {
  failures.push({ code, detail });
}
function cause(classification, component, evidence, recommended) {
  rootCauses.push({ classification, component, evidence, recommended });
}

const report = {
  contract: "GDI_COMPLETE_RELEASE_PARITY_V1",
  generatedAt: new Date().toISOString(),
  hotelId: HOTEL_ID,
  expectedFingerprint: EXPECTED_FP,
  expectedPackage: EXPECTED_PKG,
  environments: {
    local: { base: localBase, role: "founder_approved_customer_share_preview" },
    production: {
      base: prodBase,
      role: "railway_external_customer_share",
    },
  },
  visited: { local: false, production: false },
  ok: false,
};

// ── 1. Identities ───────────────────────────────────────────────
report.git = gitIdentity();
report.health = {
  local: await fetchJson(localBase, "/health").catch((e) => ({
    error: String(e.message || e),
  })),
  production: await fetchJson(prodBase, "/health").catch((e) => ({
    error: String(e.message || e),
  })),
};
// health endpoints may not take share — use raw fetch
async function health(base) {
  try {
    const res = await fetch(`${base}/health`);
    return { status: res.status, json: await res.json() };
  } catch (e) {
    return { error: String(e.message || e) };
  }
}
report.health = {
  local: await health(localBase),
  production: await health(prodBase),
};

// ── 2. Source dependency manifest ───────────────────────────────
const shareHtmlPath = path.join(ROOT, "public/group-demand-intelligence-share.html");
const founderHtmlPath = path.join(ROOT, "public/group-demand-intelligence.html");
const shareHtmlDisk = fs.readFileSync(shareHtmlPath, "utf8");
const founderHtmlDisk = fs.readFileSync(founderHtmlPath, "utf8");
const shareDeps = parseHtmlDeps(shareHtmlDisk);
const founderDeps = parseHtmlDeps(founderHtmlDisk);

const CANONICAL_SOURCE = [
  "public/group-demand-intelligence-share.html",
  "public/group-demand-intelligence.html",
  "public/css/group-demand-intelligence.css",
  "public/css/deal-workspace-shell.css",
  "public/js/ai-visibility/ai-visibility-shared.css",
  "public/js/ai-demand-positioning/adp-guided-report-tour.css",
  "public/js/group-demand-intelligence/dealality-gdi-ui.js",
  "public/js/group-demand-intelligence/share-app.js",
  "public/js/group-demand-intelligence/app.js",
  "public/js/group-demand-intelligence/gdi-universal-presentation-controller.js",
  "public/js/group-demand-intelligence/gdi-guided-report-tour.js",
  "public/js/dealality-guided-tour/dealality-guided-tour-engine.js",
  "api/group-demand-intelligence.js",
  "lib/group-demand-intelligence/share/share-customer-presentation-parity-v1.js",
  "lib/group-demand-intelligence/share/gdi-customer-facing-opportunity-contract-v1.js",
];

report.sourceManifest = {
  shareHtmlDeps: shareDeps,
  founderHtmlDeps: founderDeps,
  canonicalFiles: CANONICAL_SOURCE.map((rel) => {
    const p = path.join(ROOT, rel);
    const exists = fs.existsSync(p);
    const buf = exists ? fs.readFileSync(p) : null;
    return {
      rel,
      exists,
      bytes: buf?.length ?? null,
      sha256Eol: buf ? sha256(normalizeEol(buf)) : null,
      headCommitHint: report.git.head || null,
    };
  }),
  duplicatedCustomerRenderers: {
    note: "Share and founder pages share dealality-gdi-ui.js + guided-tour modules; share-app.js vs app.js are entry-specific loaders (expected).",
    shareOnly: ["public/js/group-demand-intelligence/share-app.js"],
    founderOnly: ["public/js/group-demand-intelligence/app.js"],
    sharedUi: [
      "public/js/group-demand-intelligence/dealality-gdi-ui.js",
      "public/js/group-demand-intelligence/gdi-universal-presentation-controller.js",
      "public/js/group-demand-intelligence/gdi-guided-report-tour.js",
      "public/js/dealality-guided-tour/dealality-guided-tour-engine.js",
    ],
  },
};

// ── 3. Asset parity (HTTP local vs HTTP production) ─────────────
const assetPaths = [
  ...new Set(
    [...shareDeps.all, ...founderDeps.all]
      .map(stripQuery)
      .filter((u) => u.startsWith("/")),
  ),
];
// Ensure canonical CSS/JS always included even if HTML parse misses
for (const rel of CANONICAL_SOURCE.filter((r) => r.startsWith("public/"))) {
  const u = "/" + rel.replace(/^public\//, "");
  if (!assetPaths.includes(u) && /\.(css|js|html)$/.test(u)) assetPaths.push(u);
}

const assetRows = [];
for (const urlPath of assetPaths) {
  const local = await fetchAsset(localBase, urlPath);
  const production = await fetchAsset(prodBase, urlPath);
  report.visited.local = true;
  report.visited.production = true;
  const identical =
    local.status === 200 &&
    production.status === 200 &&
    local.sha256Eol === production.sha256Eol;
  assetRows.push({
    urlPath,
    local,
    production,
    identical,
  });
  if (!identical) {
    fail("DEPLOYED_ASSET_DIFFERENCE", urlPath);
    cause(
      local.status !== 200 || production.status !== 200
        ? "MISSING_DEPENDENCY"
        : "DEPLOYED_ASSET_DIFFERENCE",
      urlPath,
      {
        localStatus: local.status,
        prodStatus: production.status,
        localSha: local.sha256Eol,
        prodSha: production.sha256Eol,
      },
      "Redeploy matching tip SHA; confirm lean stage includes public assets; bust caches.",
    );
  }
}
report.assets = {
  comparedVia: "HTTP_LOCAL_VS_HTTP_PRODUCTION",
  count: assetRows.length,
  identicalCount: assetRows.filter((r) => r.identical).length,
  mismatchCount: assetRows.filter((r) => !r.identical).length,
  rows: assetRows,
};

// Cache-bust consistency on share HTML
const shareLocalHtml = await fetchAsset(
  localBase,
  "/group-demand-intelligence-share.html",
);
const shareProdHtml = await fetchAsset(
  prodBase,
  "/group-demand-intelligence-share.html",
);
const localHtmlText = (
  await (await fetch(`${localBase}/group-demand-intelligence-share.html?cb=${Date.now()}`)).text()
);
const prodHtmlText = (
  await (await fetch(`${prodBase}/group-demand-intelligence-share.html?cb=${Date.now()}`)).text()
);
report.htmlMarkers = {
  local: [...localHtmlText.matchAll(/v=([a-z0-9-]+)/gi)].map((m) => m[1]),
  production: [...prodHtmlText.matchAll(/v=([a-z0-9-]+)/gi)].map((m) => m[1]),
  eolIdentical:
    shareLocalHtml.sha256Eol === shareProdHtml.sha256Eol,
};

// ── 4. API / publication parity ─────────────────────────────────
const listPath = `/api/group-demand-intelligence/share/hotels/${HOTEL_ID}/opportunities`;
const localList = await fetchJson(localBase, listPath);
const prodList = await fetchJson(prodBase, listPath);
report.visited.local = true;
report.visited.production = true;

const localPub = localList.json?.publication || {};
const prodPub = prodList.json?.publication || {};
report.publication = {
  local: localPub,
  production: prodPub,
  fingerprintMatch:
    localPub.contentFingerprintSha256 === EXPECTED_FP &&
    prodPub.contentFingerprintSha256 === EXPECTED_FP,
  packageMatch:
    localPub.publicationVersion === EXPECTED_PKG &&
    prodPub.publicationVersion === EXPECTED_PKG,
};

if (!report.publication.fingerprintMatch) {
  fail("PUBLICATION_PACKAGE_DIFFERENCE", "fingerprint");
  cause(
    "PUBLICATION_PACKAGE_DIFFERENCE",
    "publication.contentFingerprintSha256",
    { local: localPub.contentFingerprintSha256, production: prodPub.contentFingerprintSha256 },
    "Do not alter approved package; restore expected fingerprint on both environments.",
  );
}

const localOpps = localList.json?.opportunities || [];
const prodOpps = prodList.json?.opportunities || [];
const localIds = localOpps.map((o) => o.id || o.opportunityId).filter(Boolean);
const prodIds = prodOpps.map((o) => o.id || o.opportunityId).filter(Boolean);

report.listApi = {
  localStatus: localList.status,
  prodStatus: prodList.status,
  localCount: localOpps.length,
  prodCount: prodOpps.length,
  idSetMatch: stable(localIds.slice().sort()) === stable(prodIds.slice().sort()),
  inventoryLocal: localPub.inventoryCounts || localList.json?.inventoryPopulationCounts,
  inventoryProd: prodPub.inventoryCounts || prodList.json?.inventoryPopulationCounts,
  needDateLocal: localList.json?.hotelNeedDateConfigV1 || null,
  needDateProd: prodList.json?.hotelNeedDateConfigV1 || null,
};

if (localOpps.length !== 67 || prodOpps.length !== 67) {
  fail("API_DATA_DIFFERENCE", `counts local=${localOpps.length} prod=${prodOpps.length}`);
}
if (!report.listApi.idSetMatch) {
  fail("API_DATA_DIFFERENCE", "opportunity id set mismatch");
  cause(
    "API_DATA_DIFFERENCE",
    "opportunities[].id",
    {
      onlyLocal: localIds.filter((id) => !prodIds.includes(id)),
      onlyProd: prodIds.filter((id) => !localIds.includes(id)),
    },
    "Confirm both environments load the same approved publication package.",
  );
}

// Controlled list-field diffs (exclude feedback)
const listDiffs = [];
const byProd = new Map(prodOpps.map((o) => [o.id || o.opportunityId, o]));
for (const lo of localOpps) {
  const id = lo.id || lo.opportunityId;
  const po = byProd.get(id);
  if (!po) continue;
  const diffs = deepDiff(lo, po, id, [], 20);
  if (diffs.length) listDiffs.push({ id, diffs });
}
report.listFieldDiffs = {
  mismatchedOpportunityCount: listDiffs.length,
  samples: listDiffs.slice(0, 15),
};
if (listDiffs.length) {
  fail("API_DATA_DIFFERENCE", `${listDiffs.length} opportunities differ in controlled fields`);
  cause(
    "API_DATA_DIFFERENCE",
    "list DTO controlled fields",
    { count: listDiffs.length, sample: listDiffs[0] },
    "Align DTO builders / package loader; preserve feedback-only differences.",
  );
}

// Detail drawers (all or sample)
const detailIds = SKIP_ALL_DRAWERS
  ? localIds.slice(0, 5)
  : localIds.slice();
const detailMismatches = [];
async function mapPool(items, concurrency, fn) {
  const out = new Array(items.length);
  let i = 0;
  async function worker() {
    while (i < items.length) {
      const idx = i++;
      out[idx] = await fn(items[idx], idx);
    }
  }
  await Promise.all(
    Array.from({ length: Math.min(concurrency, items.length) }, () => worker()),
  );
  return out;
}
await mapPool(detailIds, 6, async (id) => {
  const p = `/api/group-demand-intelligence/share/hotels/${HOTEL_ID}/opportunities/${encodeURIComponent(id)}`;
  const [l, r] = await Promise.all([
    fetchJson(localBase, p),
    fetchJson(prodBase, p),
  ]);
  const diffs = deepDiff(l.json?.opportunity, r.json?.opportunity, id, [], 25);
  if (l.status !== 200 || r.status !== 200 || diffs.length) {
    detailMismatches.push({
      id,
      localStatus: l.status,
      prodStatus: r.status,
      diffs: diffs.slice(0, 10),
    });
  }
});
report.detailApi = {
  compared: detailIds.length,
  mismatched: detailMismatches.length,
  samples: detailMismatches.slice(0, 10),
  mode: SKIP_ALL_DRAWERS ? "sample" : "all_67",
};
if (detailMismatches.length) {
  fail("API_DATA_DIFFERENCE", `${detailMismatches.length} detail drawers differ`);
}

// Need-date config
if (
  stable(report.listApi.needDateLocal) !== stable(report.listApi.needDateProd)
) {
  fail("API_DATA_DIFFERENCE", "hotelNeedDateConfigV1 mismatch");
  cause(
    "API_DATA_DIFFERENCE",
    "hotelNeedDateConfigV1",
    {
      local: report.listApi.needDateLocal,
      production: report.listApi.needDateProd,
    },
    "Ensure PACKAGE includes hotelNeedDateConfigV1 on both environments.",
  );
}

// ── 5. Browser UI comparison ────────────────────────────────────
const browser = await chromium.launch({ headless: true });
const context = await browser.newContext({
  viewport: { width: 1440, height: 900 },
  deviceScaleFactor: 1,
  locale: "en-US",
  timezoneId: "America/New_York",
});

async function extractUi(page, label, base) {
  const consoleErrors = [];
  page.on("pageerror", (e) => consoleErrors.push(String(e.message || e)));
  await page.goto(
    `${base}/group-demand-intelligence-share.html?share=${encodeURIComponent(share)}`,
    { waitUntil: "domcontentloaded", timeout: 120000 },
  );
  await page.waitForSelector("#gdiHowToReadReport, .brand-card, [data-open]", {
    timeout: 120000,
  });
  await page.waitForTimeout(1200);
  const reset = page.locator("#gdiResetViewBtn");
  if (await reset.count()) {
    await reset.click().catch(() => {});
    await page.waitForTimeout(600);
  }
  await page.screenshot({
    path: path.join(OUT, `screenshots/${label}-desktop-overview.png`),
    fullPage: false,
  });

  const ui = await page.evaluate(() => {
    const norm = (s) => String(s || "").replace(/\s+/g, " ").trim();
    const text = (sel) => {
      const el = document.querySelector(sel);
      return el ? norm(el.innerText || el.textContent) : null;
    };
    const cards = [...document.querySelectorAll("[data-open]")].map((el) => {
      const card = el.closest(".brand-card, .gdi-card, article") || el;
      const svg = [...card.querySelectorAll("svg")].map((s) => ({
        w: s.getAttribute("width") || s.getBoundingClientRect().width,
        h: s.getAttribute("height") || s.getBoundingClientRect().height,
        cls: s.getAttribute("class") || "",
        paths: s.querySelectorAll("path").length,
      }));
      const revenueIcons = [
        ...card.querySelectorAll(".gdi-revenue-stream-icon"),
      ].map((node) => {
        const ncs = getComputedStyle(node);
        return {
          className: String(node.className || ""),
          state: /--active/.test(node.className)
            ? "active"
            : /--muted/.test(node.className)
              ? "muted"
              : "unknown",
          color: ncs.color,
          opacity: ncs.opacity,
          title: node.getAttribute("title") || node.getAttribute("aria-label"),
        };
      });
      const contactEl =
        card.querySelector(
          ".gdi-tile-contact, .gdi-card-contact, [data-gdi-tile-contact]",
        ) || null;
      const actionEl =
        card.querySelector(
          ".gdi-pill-sales, [class*='gdi-pill-sales'], [data-commercial-action]",
        ) ||
        [...card.querySelectorAll(".gdi-pill, .badge, span")].find((n) =>
          /Pursue|Qualify|Build Venue|Watch|Partner/i.test(
            (n.textContent || "").trim(),
          ),
        );
      const contactBlock = card.querySelector(".brand-card__contact");
      const actionPill = card.querySelector(
        ".gdi-tile-pill--sales-action, [class*='gdi-pill-sales']",
      );
      return {
        id: el.getAttribute("data-open"),
        title: norm(
          card.querySelector(
            ".brand-card__name, h3, h2, .gdi-card-title, .brand-card-title",
          )?.textContent,
        ),
        body: norm(card.innerText).slice(0, 400),
        contactText: contactBlock
          ? norm(contactBlock.innerText || contactBlock.textContent)
          : contactEl
            ? norm(contactEl.innerText || contactEl.textContent)
            : null,
        actionText: actionPill
          ? norm(actionPill.textContent)
          : actionEl
            ? norm(actionEl.textContent)
            : null,
        revenueIcons,
        classes: card.className,
        rect: (() => {
          const b = card.getBoundingClientRect();
          return {
            w: Math.round(b.width),
            h: Math.round(b.height),
          };
        })(),
        svg,
      };
    });
    // unique by id
    const seen = new Set();
    const uniqueCards = [];
    for (const c of cards) {
      if (!c.id || seen.has(c.id)) continue;
      seen.add(c.id);
      uniqueCards.push(c);
    }
    const resetBtn = document.querySelector("#gdiResetViewBtn");
    const badge = document.querySelector("#gdiFilterCountBadge");
    const typeSelect = document.querySelector(
      "#gdiOpportunityTypeFilter, select[data-filter='opportunityType']",
    );
    return {
      url: location.href.split("?")[0],
      tourScript: document
        .querySelector('script[src*="gdi-guided-report-tour"]')
        ?.getAttribute("src"),
      resetPresent: Boolean(resetBtn),
      resetText: resetBtn ? norm(resetBtn.textContent) : null,
      resetClasses: resetBtn?.className || null,
      badgeText: badge ? norm(badge.textContent) : null,
      badgeVisible: badge
        ? badge.style.display !== "none" && badge.offsetParent !== null
        : false,
      opportunityType:
        typeSelect?.selectedOptions?.[0]
          ? norm(typeSelect.selectedOptions[0].textContent)
          : null,
      marketScope: text("[data-market-scope], .gdi-market-scope") ||
        ([...document.querySelectorAll("button, .chip, .segment")]
          .map((el) => norm(el.textContent))
          .find((t) => /^All\s*\(67\)/.test(t)) || null),
      needDateStatus: document
        .querySelector("[data-hotel-need-date-status]")
        ?.getAttribute("data-hotel-need-date-status"),
      needDateText: text("[data-hotel-need-date-status]"),
      showingText: (document.body.innerText.match(/Showing\s+\d+\s+of\s+\d+/i) || [])[0] || null,
      inventoryHints: {
        all67: /All\s*\(67\)/.test(document.body.innerText),
        hotel61: /Hotel\s*\(61\)/.test(document.body.innerText),
        competitive10: /Competitive\s*\(10\)/.test(document.body.innerText),
      },
      cardCount: uniqueCards.length,
      cards: uniqueCards,
      bodyHash: null,
    };
  });
  ui.bodyHash = sha256(
    normalizeEol(
      Buffer.from(
        uniqueCardFingerprint(ui.cards),
        "utf8",
      ),
    ),
  );
  ui.consoleErrors = consoleErrors.slice(0, 20);
  return ui;
}

function uniqueCardFingerprint(cards) {
  return JSON.stringify(
    (cards || []).map((c) => ({
      id: c.id,
      title: c.title,
      body: c.body,
      contactText: c.contactText,
      actionText: c.actionText,
      revenueIcons: c.revenueIcons,
      classes: c.classes,
      svg: c.svg,
    })),
  );
}

const localPage = await context.newPage();
const prodPage = await context.newPage();
const localUi = await extractUi(localPage, "local", localBase);
const prodUi = await extractUi(prodPage, "prod", prodBase);
report.visited.local = true;
report.visited.production = true;

report.ui = {
  local: {
    ...localUi,
    cards: undefined,
    cardIds: localUi.cards.map((c) => c.id),
    cardSample: localUi.cards.slice(0, 5),
  },
  production: {
    ...prodUi,
    cards: undefined,
    cardIds: prodUi.cards.map((c) => c.id),
    cardSample: prodUi.cards.slice(0, 5),
  },
};

const uiChecks = [
  ["cardCount", localUi.cardCount, prodUi.cardCount],
  ["resetPresent", localUi.resetPresent, prodUi.resetPresent],
  ["resetText", localUi.resetText, prodUi.resetText],
  ["opportunityType", localUi.opportunityType, prodUi.opportunityType],
  ["needDateStatus", localUi.needDateStatus, prodUi.needDateStatus],
  ["showingText", localUi.showingText, prodUi.showingText],
  ["bodyHash", localUi.bodyHash, prodUi.bodyHash],
];
report.uiDiffs = [];
for (const [name, l, p] of uiChecks) {
  if (stable(l) !== stable(p)) {
    report.uiDiffs.push({ field: name, local: l, production: p });
    fail("SOURCE_CODE_DIFFERENCE_OR_RUNTIME", `ui.${name}`);
    cause(
      localUi.bodyHash !== prodUi.bodyHash && name === "bodyHash"
        ? report.assets.mismatchCount
          ? "DEPLOYED_ASSET_DIFFERENCE"
          : "API_DATA_DIFFERENCE"
        : "UNKNOWN_REQUIRES_INVESTIGATION",
      `ui.${name}`,
      { local: l, production: p },
      "Compare shared dealality-gdi-ui.js rendering and DTO fields for this control.",
    );
  }
}
if (
  stable(localUi.cards.map((c) => c.id).sort()) !==
  stable(prodUi.cards.map((c) => c.id).sort())
) {
  fail("API_DATA_DIFFERENCE", "rendered card id set");
}

// Component-level card parity (contacts / SVG states / commercial actions)
const componentMismatches = [];
const localCardById = new Map(localUi.cards.map((c) => [c.id, c]));
const prodCardById = new Map(prodUi.cards.map((c) => [c.id, c]));
for (const id of localIds) {
  const l = localCardById.get(id);
  const p = prodCardById.get(id);
  if (!l || !p) {
    componentMismatches.push({
      id,
      classification: "DOM_STRUCTURE_DIFFERENCE",
      detail: "card_missing_in_one_environment",
    });
    continue;
  }
  if (l.contactText !== p.contactText) {
    componentMismatches.push({
      id,
      title: l.title,
      classification: "CONTACT_MAPPING_DIFFERENCE",
      local: l.contactText,
      production: p.contactText,
    });
  }
  if (l.actionText !== p.actionText) {
    componentMismatches.push({
      id,
      title: l.title,
      classification: "COMMERCIAL_ACTION_DIFFERENCE",
      local: l.actionText,
      production: p.actionText,
    });
  }
  if (stable(l.revenueIcons) !== stable(p.revenueIcons)) {
    componentMismatches.push({
      id,
      title: l.title,
      classification: "SVG_STATE_DIFFERENCE",
      local: l.revenueIcons,
      production: p.revenueIcons,
    });
  }
}
const FOCUS_TITLE_RE =
  /Regulatory Information Conference|Bethesda Premier Cup|Potomac Memorial Tournament|Woman'?s Club of Bethesda|ACVNU|Renal Week/i;
report.componentLevelParity = {
  compared: localIds.length,
  mismatchCount: componentMismatches.length,
  mismatches: componentMismatches,
  fixtures: {
    local: localUi.cards.filter((c) => FOCUS_TITLE_RE.test(c.title || "")),
    production: prodUi.cards.filter((c) => FOCUS_TITLE_RE.test(c.title || "")),
  },
};
if (componentMismatches.length) {
  fail(
    "COMPONENT_LEVEL_PARITY",
    `${componentMismatches.length} card component mismatches`,
  );
  for (const m of componentMismatches.slice(0, 12)) {
    cause(
      m.classification,
      m.id,
      { local: m.local, production: m.production, title: m.title },
      "Align shared customer renderer / package presentation attach; do not recolor icons without evidence.",
    );
  }
}

// Open first drawer on both and compare DOM text sections
async function openFirstDrawer(page) {
  const first = page.locator("[data-open]").first();
  await first.click();
  await page.waitForTimeout(800);
  return page.evaluate(() => {
    const drawer =
      document.getElementById("gdiShareDrawer") ||
      document.querySelector("dialog.gdi-drawer");
    const body = document.getElementById("gdiShareDrawerBody");
    const r = drawer?.getBoundingClientRect();
    const headings = [...(body?.querySelectorAll("h2,h3,h4") || [])].map((h) =>
      (h.textContent || "").replace(/\s+/g, " ").trim(),
    );
    return {
      open: Boolean(drawer?.open),
      title: (document.getElementById("gdiShareDrawerTitle")?.textContent || "")
        .replace(/\s+/g, " ")
        .trim(),
      headings,
      bodyText: (body?.innerText || "").replace(/\s+/g, " ").trim().slice(0, 2500),
      rect: r
        ? { x: Math.round(r.x), y: Math.round(r.y), w: Math.round(r.width), h: Math.round(r.height) }
        : null,
      needDateMatchPresent: /Need-?Date Match/i.test(body?.innerText || ""),
    };
  });
}
const localDrawer = await openFirstDrawer(localPage);
await localPage.screenshot({
  path: path.join(OUT, "screenshots/local-desktop-drawer.png"),
  fullPage: false,
});
const prodDrawer = await openFirstDrawer(prodPage);
await prodPage.screenshot({
  path: path.join(OUT, "screenshots/prod-desktop-drawer.png"),
  fullPage: false,
});
report.drawerSample = { local: localDrawer, production: prodDrawer };
if (localDrawer.headings.join("|") !== prodDrawer.headings.join("|")) {
  fail("API_DATA_DIFFERENCE", "drawer section headings");
  cause(
    "API_DATA_DIFFERENCE",
    "drawer headings",
    { local: localDrawer.headings, production: prodDrawer.headings },
    "Compare detail DTO section builders.",
  );
}
if (sha256(localDrawer.bodyText) !== sha256(prodDrawer.bodyText)) {
  // soft: feedback widgets may differ — only fail if need-date presence differs
  if (localDrawer.needDateMatchPresent !== prodDrawer.needDateMatchPresent) {
    fail("API_DATA_DIFFERENCE", "Need-Date Match presence");
  } else {
    report.drawerSample.bodyTextHashDiffer = true;
    report.drawerSample.note =
      "Drawer body text hash differs — inspect for feedback-only vs controlled content (see detailApi).";
  }
}

// Guided tour (desktop) — geometry-critical steps
report.tour = { skipped: SKIP_TOUR };
if (!SKIP_TOUR) {
  async function runTour(page, label) {
    await page.locator("#gdiHowToReadReport, .adp-howto-read-btn").first().click();
    await page.waitForSelector(".adp-gt__callout:not([hidden])", { timeout: 30000 });
    const steps = [];
    for (let i = 0; i < 10; i++) {
      await page.waitForTimeout(500);
      const geo = await page.evaluate(() => {
        const drawer = document.getElementById("gdiShareDrawer");
        const callout = document.querySelector(".adp-gt__callout");
        const highlight = document.querySelector(".adp-gt__highlight");
        const next = document.querySelector('[data-dgt="next"]');
        const r = (el) => {
          if (!el) return null;
          const b = el.getBoundingClientRect();
          return {
            x: Math.round(b.x),
            y: Math.round(b.y),
            w: Math.round(b.width),
            h: Math.round(b.height),
          };
        };
        const center = document.elementFromPoint(
          Math.floor(window.innerWidth / 2),
          Math.floor(window.innerHeight / 2),
        );
        return {
          title: (
            document.querySelector("#dgtTitle, .adp-gt__title")?.textContent || ""
          ).trim(),
          drawer: r(drawer),
          callout: r(callout),
          highlight: r(highlight),
          drawerOpen: Boolean(drawer?.open),
          tourNonModal: Boolean(
            drawer?.classList.contains("gdi-drawer--tour-nonmodal"),
          ),
          nextOk: Boolean(next && next.getBoundingClientRect().width > 0),
          centerClass: center?.className || null,
          centerTag: center?.tagName || null,
        };
      });
      await page.screenshot({
        path: path.join(OUT, `screenshots/${label}-tour-step-${i + 1}of10.png`),
        fullPage: false,
      });
      steps.push({ index: i + 1, geo });
      if (i === 5) {
        if (!geo.drawerOpen) fail("RESPONSIVE_LAYOUT_DIFFERENCE", `${label} step6 drawer closed`);
        if (geo.drawer && geo.drawer.y > 40) {
          fail("SOURCE_CODE_DIFFERENCE", `${label} step6 drawer_y=${geo.drawer.y}`);
          cause(
            "SOURCE_CODE_DIFFERENCE",
            "guided tour drawer pin",
            geo,
            "Ensure gdi-drawer--tour-nonmodal CSS + demote pin are deployed.",
          );
        }
        if (geo.highlight && geo.highlight.h < 500) {
          fail("SOURCE_CODE_DIFFERENCE", `${label} step6 highlight short`);
        }
        if (geo.centerTag === "HTML") {
          fail("SOURCE_CODE_DIFFERENCE", `${label} step6 blank overlay center`);
        }
      }
      if (i < 9) {
        const prev = geo.title;
        await page.locator('[data-dgt="next"]').click();
        await page
          .waitForFunction(
            (t) => {
              const el = document.querySelector("#dgtTitle, .adp-gt__title");
              return el && el.textContent !== t;
            },
            prev,
            { timeout: 15000 },
          )
          .catch(() => {});
      }
    }
    // Exit tour
    await page.locator('[data-dgt="exit"], .adp-gt__exit').first().click().catch(() => {});
    return steps;
  }
  // Fresh pages for tour to avoid drawer state bleed
  const localTourPage = await context.newPage();
  await localTourPage.goto(
    `${localBase}/group-demand-intelligence-share.html?share=${encodeURIComponent(share)}`,
    { waitUntil: "domcontentloaded", timeout: 120000 },
  );
  await localTourPage.waitForSelector("#gdiHowToReadReport", { timeout: 120000 });
  await localTourPage.waitForTimeout(800);
  const localTour = await runTour(localTourPage, "local");
  const prodTourPage = await context.newPage();
  await prodTourPage.goto(
    `${prodBase}/group-demand-intelligence-share.html?share=${encodeURIComponent(share)}`,
    { waitUntil: "domcontentloaded", timeout: 120000 },
  );
  await prodTourPage.waitForSelector("#gdiHowToReadReport", { timeout: 120000 });
  await prodTourPage.waitForTimeout(800);
  const prodTour = await runTour(prodTourPage, "prod");
  report.tour = {
    skipped: false,
    localStep6: localTour.find((s) => s.index === 6)?.geo || null,
    productionStep6: prodTour.find((s) => s.index === 6)?.geo || null,
    localTitles: localTour.map((s) => s.geo.title),
    productionTitles: prodTour.map((s) => s.geo.title),
  };
  if (stable(report.tour.localTitles) !== stable(report.tour.productionTitles)) {
    fail("SOURCE_CODE_DIFFERENCE", "tour step titles differ");
  }
}

// Mobile viewport smoke (overview + inventory chrome)
const mobileLocal = await context.newPage();
await mobileLocal.setViewportSize({ width: 390, height: 844 });
await mobileLocal.goto(
  `${localBase}/group-demand-intelligence-share.html?share=${encodeURIComponent(share)}`,
  { waitUntil: "domcontentloaded", timeout: 120000 },
);
await mobileLocal.waitForSelector("#gdiHowToReadReport", { timeout: 120000 });
await mobileLocal.waitForTimeout(800);
await mobileLocal.screenshot({
  path: path.join(OUT, "screenshots/local-mobile-overview.png"),
  fullPage: false,
});
const mobileProd = await context.newPage();
await mobileProd.setViewportSize({ width: 390, height: 844 });
await mobileProd.goto(
  `${prodBase}/group-demand-intelligence-share.html?share=${encodeURIComponent(share)}`,
  { waitUntil: "domcontentloaded", timeout: 120000 },
);
await mobileProd.waitForSelector("#gdiHowToReadReport", { timeout: 120000 });
await mobileProd.waitForTimeout(800);
await mobileProd.screenshot({
  path: path.join(OUT, "screenshots/prod-mobile-overview.png"),
  fullPage: false,
});
report.mobile = {
  localInventory: await mobileLocal.evaluate(
    () => ({
      all67: /All\s*\(67\)/.test(document.body.innerText),
      hotel61: /Hotel\s*\(61\)/.test(document.body.innerText),
      competitive10: /Competitive\s*\(10\)/.test(document.body.innerText),
    }),
  ),
  productionInventory: await mobileProd.evaluate(
    () => ({
      all67: /All\s*\(67\)/.test(document.body.innerText),
      hotel61: /Hotel\s*\(61\)/.test(document.body.innerText),
      competitive10: /Competitive\s*\(10\)/.test(document.body.innerText),
    }),
  ),
};
if (
  stable(report.mobile.localInventory) !==
  stable(report.mobile.productionInventory)
) {
  fail("RESPONSIVE_LAYOUT_DIFFERENCE", "mobile inventory chrome");
}

await browser.close();

// Guard: both environments visited
if (!report.visited.local || !report.visited.production) {
  fail("UNKNOWN_REQUIRES_INVESTIGATION", "did not visit both environments");
}

report.failures = failures;
report.rootCauses = rootCauses;
report.ok = failures.length === 0;
report.verdict = report.ok ? "CLIENT READY" : "NOT CERTIFIED";

// Write report pack
const files = {
  "00-IDENTITY.json": {
    git: report.git,
    health: report.health,
    environments: report.environments,
    visited: report.visited,
  },
  "01-SOURCE-DEPENDENCY-MANIFEST.json": report.sourceManifest,
  "02-ASSET-HASH-COMPARISON.json": report.assets,
  "03-HTML-CACHE-MARKERS.json": report.htmlMarkers,
  "04-PUBLICATION-AND-LIST-API.json": {
    publication: report.publication,
    listApi: report.listApi,
    listFieldDiffs: report.listFieldDiffs,
  },
  "05-DETAIL-API-COMPARISON.json": report.detailApi,
  "06-UI-TOOLBAR-AND-CARDS.json": report.ui,
  "07-DRAWER-SAMPLE.json": report.drawerSample,
  "08-GUIDED-TOUR.json": report.tour,
  "09-MOBILE.json": report.mobile,
  "10-ROOT-CAUSE-REGISTER.json": rootCauses,
  "11-FINAL-CERTIFICATE.json": {
    contract: report.contract,
    verdict: report.verdict,
    ok: report.ok,
    failures,
    fingerprint: EXPECTED_FP,
    package: EXPECTED_PKG,
    gitHead: report.git.head,
    generatedAt: report.generatedAt,
    environmentsVisited: report.visited,
  },
};

for (const [name, body] of Object.entries(files)) {
  fs.writeFileSync(path.join(OUT, name), JSON.stringify(body, null, 2));
}

fs.writeFileSync(
  path.join(OUT, "FOUNDER_REPORT.md"),
  `# GDI Source / Asset / Runtime / UI Parity V1

**Verdict: ${report.verdict}**

- Local: \`${localBase}\` (customer share preview)
- Production: \`${prodBase}\` (Railway external share)
- Hotel: \`${HOTEL_ID}\`
- Package: \`${EXPECTED_PKG}\`
- Fingerprint: \`${EXPECTED_FP}\`
- Git HEAD at audit: \`${report.git.head || "unknown"}\`
- Both environments visited: local=${report.visited.local} production=${report.visited.production}

## Gates

| Gate | Result |
|------|--------|
| HTTP asset parity (EOL-normalized) | ${report.assets.mismatchCount === 0 ? "PASS" : "FAIL (" + report.assets.mismatchCount + ")"} |
| Publication fingerprint | ${report.publication.fingerprintMatch ? "PASS" : "FAIL"} |
| List API 67 / id set | ${report.listApi.localCount === 67 && report.listApi.idSetMatch ? "PASS" : "FAIL"} |
| Controlled list field diffs | ${report.listFieldDiffs.mismatchedOpportunityCount === 0 ? "PASS" : "FAIL (" + report.listFieldDiffs.mismatchedOpportunityCount + ")"} |
| Detail API (${report.detailApi.mode}) | ${report.detailApi.mismatched === 0 ? "PASS" : "FAIL (" + report.detailApi.mismatched + ")"} |
| UI toolbar/cards | ${report.uiDiffs.length === 0 ? "PASS" : "FAIL"} |
| Guided tour | ${SKIP_TOUR ? "SKIPPED" : report.tour?.localStep6 && report.tour?.productionStep6 ? "EXECUTED" : "INCOMPLETE"} |
| Mobile inventory chrome | ${stable(report.mobile.localInventory) === stable(report.mobile.productionInventory) ? "PASS" : "FAIL"} |

## Failures

${
  failures.length
    ? failures.map((f) => `- \`${f.code}\`: ${f.detail}`).join("\n")
    : "_None._"
}

## Root causes

${
  rootCauses.length
    ? rootCauses
        .map(
          (c) =>
            `- **${c.classification}** · ${c.component} — ${c.recommended}`,
        )
        .join("\n")
    : "_None attributed._"
}

## Notes

- Asset comparison uses **HTTP-served bytes** from local and production (not disk-only).
- Feedback / validation history fields are excluded from controlled DTO diffs.
- Do not treat \`/health\` buildSha alone as frontend identity under lean path-as-root deploys.
`,
);

console.log(
  JSON.stringify(
    {
      ok: report.ok,
      verdict: report.verdict,
      failures: failures.map((f) => f.code),
      assetMismatches: report.assets.mismatchCount,
      listDiffs: report.listFieldDiffs.mismatchedOpportunityCount,
      detailDiffs: report.detailApi.mismatched,
      uiDiffs: report.uiDiffs.length,
      out: OUT,
      visited: report.visited,
    },
    null,
    2,
  ),
);

process.exit(report.ok ? 0 : 2);
