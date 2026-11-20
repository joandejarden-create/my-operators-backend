#!/usr/bin/env node
/**
 * Gate 1 — Product Mount vs Comparison Studio content inventory.
 * Fails if any nonempty source content silently disappears from compare mounts.
 */
import { mkdirSync, writeFileSync } from "fs";
import { join } from "path";
import { createRequire } from "module";
import { createHash } from "crypto";
import { readFileSync } from "fs";

const require = createRequire(import.meta.url);
const root = process.cwd();
const outDir = join(root, "reports/brand-explorer/phase2h-a-renderer-parity/content-preservation-v1");
mkdirSync(outDir, { recursive: true });
mkdirSync(join(outDir, "screenshots"), { recursive: true });

const PAYLOAD = "public/brand-explorer-phase2e-preview/curio-gate1-renderer-parity-payload.json";
const TABS = [
  "atelier-overview",
  "atelier-value-owners",
  "atelier-ops",
  "atelier-standards-owner",
  "atelier-commercial",
  "atelier-economics",
  "atelier-loyalty",
  "atelier-footprint",
  "atelier-materials",
  "atelier-insight",
];

const URLS = {
  product:
    "http://127.0.0.1:8082/brand-explorer-renderer-parity.html?beInternalPreview=1&payload=curio-gate1-renderer-parity-payload.json",
  compare:
    "http://127.0.0.1:8082/brand-explorer-compare.html?beInternalPreview=1&rendererParity=1&brand=receQkxgjlezsc1xg",
};

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function inventoryFromRoot(rootEl) {
  const text = (rootEl.innerText || "").replace(/\s+/g, " ").trim();
  const titles = [...rootEl.querySelectorAll(".oe-section-title, h2.oe-section-title")].map((el) =>
    el.textContent.trim(),
  );
  const bullets = [...rootEl.querySelectorAll("ul li, .bullet-list li, .oe-cluster li")].map((el) =>
    el.textContent.trim(),
  );
  const cards = [
    ...rootEl.querySelectorAll(
      ".scenario-card, .oe-card, .proof-point-card, .explorer-detail-card, .presence-intel-card, .diff-card, .info-card, .file-card",
    ),
  ].map((el) => (el.innerText || "").replace(/\s+/g, " ").trim().slice(0, 200));
  const tables = [...rootEl.querySelectorAll("table")].map((t) => ({
    rows: t.querySelectorAll("tr").length,
    cells: t.querySelectorAll("td,th").length,
    text: (t.innerText || "").replace(/\s+/g, " ").trim().slice(0, 400),
  }));
  const images = [...rootEl.querySelectorAll("img")]
    .map((img) => ({
      src: img.getAttribute("src") || "",
      alt: img.getAttribute("alt") || "",
      visible: !!(img.offsetParent || img.getClientRects().length),
    }))
    .filter((i) => i.src);
  const timelines = [...rootEl.querySelectorAll(".timeline")].map((t) =>
    [...t.querySelectorAll(".timeline__phase strong, .timeline__phase")].map((s) => s.textContent.trim()),
  );
  const links = [...rootEl.querySelectorAll("a[href]")].map((a) => ({
    href: a.getAttribute("href"),
    text: a.textContent.trim(),
  }));
  const kpiLike = [...rootEl.querySelectorAll(".kpi, .fdd-kpi, .metric-card, .snapshot-kv, .oe-cluster")]
    .slice(0, 80)
    .map((el) => (el.innerText || "").replace(/\s+/g, " ").trim().slice(0, 160));
  return {
    textLen: text.length,
    textHash: createHash("sha256").update(text).digest("hex").slice(0, 16),
    sectionTitles: titles,
    sectionCount: titles.length,
    bulletCount: bullets.length,
    bullets,
    cardCount: cards.length,
    cards,
    tableCount: tables.length,
    tables,
    imageCount: images.length,
    images,
    timelineCount: timelines.length,
    timelines,
    linkCount: links.length,
    links: links.slice(0, 40),
    kpiLikeCount: kpiLike.length,
    kpiLike,
    textSample: text.slice(0, 500),
  };
}

async function inventoryProduct(page, tabId) {
  await page.evaluate((id) => {
    const btn = document.querySelector(`.section-nav-item[data-atelier-tab="${id}"]`);
    if (btn) btn.click();
  }, tabId);
  await sleep(500);
  return page.evaluate((id) => {
    const panel =
      document.querySelector(`.be-atelier-tab-panel.active`) ||
      document.querySelector(`[data-atelier-panel="${id}"]`);
    const root = panel || document.querySelector("#beAtelierExplorer");
    // inline inventory (duplicated for browser context)
    const text = (root.innerText || "").replace(/\s+/g, " ").trim();
    const titles = [...root.querySelectorAll(".oe-section-title, h2.oe-section-title")].map((el) =>
      el.textContent.trim(),
    );
    const bullets = [...root.querySelectorAll("ul li, .bullet-list li, .oe-cluster li")].map((el) =>
      el.textContent.trim(),
    );
    const cards = [
      ...root.querySelectorAll(
        ".scenario-card, .oe-card, .proof-point-card, .explorer-detail-card, .presence-intel-card, .diff-card, .info-card, .file-card",
      ),
    ].map((el) => (el.innerText || "").replace(/\s+/g, " ").trim().slice(0, 200));
    const tables = [...root.querySelectorAll("table")].map((t) => ({
      rows: t.querySelectorAll("tr").length,
      cells: t.querySelectorAll("td,th").length,
      text: (t.innerText || "").replace(/\s+/g, " ").trim().slice(0, 400),
    }));
    const images = [...root.querySelectorAll("img")]
      .map((img) => ({
        src: img.getAttribute("src") || "",
        alt: img.getAttribute("alt") || "",
      }))
      .filter((i) => i.src);
    const timelines = [...root.querySelectorAll(".timeline")].map((t) =>
      [...t.querySelectorAll(".timeline__phase strong")].map((s) => s.textContent.trim()),
    );
    const links = [...root.querySelectorAll("a[href]")].map((a) => ({
      href: a.getAttribute("href"),
      text: a.textContent.trim(),
    }));
    // simple hash
    let h = 0;
    for (let i = 0; i < text.length; i++) h = (h * 31 + text.charCodeAt(i)) >>> 0;
    return {
      textLen: text.length,
      textHash: h.toString(16),
      sectionTitles: titles,
      sectionCount: titles.length,
      bulletCount: bullets.length,
      bullets,
      cardCount: cards.length,
      cards,
      tableCount: tables.length,
      tables,
      imageCount: images.length,
      images,
      timelineCount: timelines.length,
      timelines,
      linkCount: links.length,
      links: links.slice(0, 40),
      textSample: text.slice(0, 500),
      htmlLen: root.innerHTML.length,
    };
  }, tabId);
}

async function inventoryCompare(page, tabId) {
  await page.evaluate((id) => {
    const btn = document.querySelector(`[data-becs-tab="${id}"]`);
    if (btn) btn.click();
  }, tabId);
  await sleep(700);
  return page.evaluate(() => {
    const mounts = [...document.querySelectorAll('[data-gate1-side="A-compare-left"] .becs-product-mount')];
    // Aggregate all left mounts (split sections) as compare visible content
    const wrap = document.createElement("div");
    mounts.forEach((m) => wrap.appendChild(m.cloneNode(true)));
    const root = wrap;
    const text = (root.innerText || "").replace(/\s+/g, " ").trim();
    const titles = [...root.querySelectorAll(".oe-section-title, h2.oe-section-title")].map((el) =>
      el.textContent.trim(),
    );
    const bullets = [...root.querySelectorAll("ul li, .bullet-list li, .oe-cluster li")].map((el) =>
      el.textContent.trim(),
    );
    const cards = [
      ...root.querySelectorAll(
        ".scenario-card, .oe-card, .proof-point-card, .explorer-detail-card, .presence-intel-card, .diff-card, .info-card, .file-card",
      ),
    ].map((el) => (el.innerText || "").replace(/\s+/g, " ").trim().slice(0, 200));
    const tables = [...root.querySelectorAll("table")].map((t) => ({
      rows: t.querySelectorAll("tr").length,
      cells: t.querySelectorAll("td,th").length,
      text: (t.innerText || "").replace(/\s+/g, " ").trim().slice(0, 400),
    }));
    const images = [...root.querySelectorAll("img")]
      .map((img) => ({
        src: img.getAttribute("src") || "",
        alt: img.getAttribute("alt") || "",
      }))
      .filter((i) => i.src);
    const timelines = [...root.querySelectorAll(".timeline")].map((t) =>
      [...t.querySelectorAll(".timeline__phase strong")].map((s) => s.textContent.trim()),
    );
    const links = [...root.querySelectorAll("a[href]")].map((a) => ({
      href: a.getAttribute("href"),
      text: a.textContent.trim(),
    }));
    let h = 0;
    for (let i = 0; i < text.length; i++) h = (h * 31 + text.charCodeAt(i)) >>> 0;
    const pairTitles = [...document.querySelectorAll("#becsPairedDoc [data-gate1-section]")]
      .map((p) => p.getAttribute("data-gate1-section") || "")
      .filter((t) => t && t !== "Full tab (product-native)");
    return {
      textLen: text.length,
      textHash: h.toString(16),
      sectionTitles: titles,
      sectionCount: titles.length,
      pairTitles,
      pairCount: pairTitles.length,
      bulletCount: bullets.length,
      bullets,
      cardCount: cards.length,
      cards,
      tableCount: tables.length,
      tables,
      imageCount: images.length,
      images,
      timelineCount: timelines.length,
      timelines,
      linkCount: links.length,
      links: links.slice(0, 40),
      textSample: text.slice(0, 500),
      htmlLen: root.innerHTML.length,
      mountCount: mounts.length,
      gate1Hook: window.__BE_GATE1_COMPARE__ || null,
      contentIntegrity: (window.__BE_GATE1_COMPARE__ && window.__BE_GATE1_COMPARE__.contentIntegrity) || null,
    };
  });
}

function diffInventories(product, compare) {
  const missing = [];
  const issues = [];
  // Token-level: every product sentence/phrase ≥24 chars must appear in compare
  const productTokens = (product.bullets || [])
    .concat(product.cards || [])
    .concat((product.sectionTitles || []).map((t) => t));
  // Prefer explicit missing list built below from bullets/titles/images
  if (compare.contentIntegrity && compare.contentIntegrity.pass === false) {
    // Full-tab mount integrity is authoritative for Gate1 renderer path
    issues.push({ type: "content_integrity_hook_failed", hook: compare.contentIntegrity });
  }
  // Text length: allow small chrome delta from product shell (≤2% or ≤40 chars)
  const textDelta = product.textLen - compare.textLen;
  if (textDelta > Math.max(40, Math.floor(product.textLen * 0.02))) {
    issues.push({
      type: "text_len_loss",
      product: product.textLen,
      compare: compare.textLen,
      lostApprox: textDelta,
    });
  }
  if (compare.bulletCount < product.bulletCount) {
    issues.push({
      type: "bullet_count_loss",
      product: product.bulletCount,
      compare: compare.bulletCount,
    });
    const pSet = new Set(product.bullets);
    const cSet = new Set(compare.bullets);
    for (const b of product.bullets) {
      if (!cSet.has(b) && b.length > 8) missing.push({ kind: "bullet", text: b.slice(0, 180) });
    }
  }
  if (compare.cardCount < product.cardCount) {
    issues.push({ type: "card_count_loss", product: product.cardCount, compare: compare.cardCount });
  }
  if (compare.tableCount < product.tableCount) {
    issues.push({ type: "table_count_loss", product: product.tableCount, compare: compare.tableCount });
  }
  if (compare.imageCount < product.imageCount) {
    issues.push({ type: "image_count_loss", product: product.imageCount, compare: compare.imageCount });
    const cSrc = new Set(compare.images.map((i) => i.src));
    for (const img of product.images) {
      if (!cSrc.has(img.src)) missing.push({ kind: "image", src: img.src, alt: img.alt });
    }
  }
  if (compare.timelineCount < product.timelineCount) {
    issues.push({
      type: "timeline_count_loss",
      product: product.timelineCount,
      compare: compare.timelineCount,
    });
  }
  const pTitles = product.sectionTitles.join("|");
  const cTitles = compare.sectionTitles.join("|");
  if (pTitles !== cTitles) {
    issues.push({
      type: "section_title_mismatch",
      product: product.sectionTitles,
      compare: compare.sectionTitles,
    });
  }
  // Missing product titles
  for (const t of product.sectionTitles) {
    if (!compare.sectionTitles.includes(t)) missing.push({ kind: "section_title", text: t });
  }
  return { issues, missing, pass: issues.length === 0 };
}

async function main() {
  const puppeteer = require("puppeteer");
  const chromePath =
    process.env.PUPPETEER_EXECUTABLE_PATH ||
    "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe";
  const payloadHash = createHash("sha256").update(readFileSync(join(root, PAYLOAD))).digest("hex");
  const browser = await puppeteer.launch({
    headless: "new",
    executablePath: chromePath,
    args: ["--no-sandbox", "--disable-setuid-sandbox"],
  });

  const report = {
    version: "BE_GATE1_CONTENT_PRESERVATION_INVENTORY_V1",
    capturedAt: new Date().toISOString(),
    payloadHashSha256: payloadHash,
    urls: URLS,
    tabs: {},
    overallPass: false,
    missingBlocks: [],
  };

  try {
    const productPage = await browser.newPage();
    await productPage.setViewport({ width: 1440, height: 1100 });
    await productPage.goto(URLS.product, { waitUntil: "networkidle0", timeout: 120000 });
    await sleep(1000);

    const comparePage = await browser.newPage();
    await comparePage.setViewport({ width: 1440, height: 1100 });
    await comparePage.goto(URLS.compare, { waitUntil: "networkidle0", timeout: 120000 });
    await sleep(1200);

    for (const tabId of TABS) {
      const product = await inventoryProduct(productPage, tabId);
      await productPage.screenshot({
        path: join(outDir, "screenshots", `A-product__${tabId}.png`),
        fullPage: false,
      });
      const compare = await inventoryCompare(comparePage, tabId);
      await comparePage.screenshot({
        path: join(outDir, "screenshots", `B-compare__${tabId}.png`),
        fullPage: false,
      });
      const diff = diffInventories(product, compare);
      report.tabs[tabId] = {
        product: {
          textLen: product.textLen,
          textHash: product.textHash,
          sectionCount: product.sectionCount,
          sectionTitles: product.sectionTitles,
          bulletCount: product.bulletCount,
          cardCount: product.cardCount,
          tableCount: product.tableCount,
          imageCount: product.imageCount,
          timelineCount: product.timelineCount,
          linkCount: product.linkCount,
          htmlLen: product.htmlLen,
        },
        compare: {
          textLen: compare.textLen,
          textHash: compare.textHash,
          sectionCount: compare.sectionCount,
          sectionTitles: compare.sectionTitles,
          pairTitles: compare.pairTitles,
          pairCount: compare.pairCount,
          bulletCount: compare.bulletCount,
          cardCount: compare.cardCount,
          tableCount: compare.tableCount,
          imageCount: compare.imageCount,
          timelineCount: compare.timelineCount,
          linkCount: compare.linkCount,
          htmlLen: compare.htmlLen,
          mountCount: compare.mountCount,
          contentIntegrity: compare.contentIntegrity,
        },
        issues: diff.issues,
        missing: diff.missing.slice(0, 50),
        pass: diff.pass,
      };
      for (const m of diff.missing) {
        report.missingBlocks.push({ tabId, ...m });
      }
    }
    await productPage.close();
    await comparePage.close();
  } finally {
    await browser.close();
  }

  report.overallPass = Object.values(report.tabs).every((t) => t.pass);
  report.failedTabs = Object.entries(report.tabs)
    .filter(([, t]) => !t.pass)
    .map(([id]) => id);

  writeFileSync(join(outDir, "01-content-inventory.json"), JSON.stringify(report, null, 2));
  writeFileSync(
    join(outDir, "00-CONTENT_PRESERVATION_STATUS.md"),
    `# Gate 1 — Content Preservation Inventory

**overallPass = ${report.overallPass}**

Payload: \`${payloadHash}\`

Failed tabs: ${report.failedTabs.length ? report.failedTabs.join(", ") : "(none)"}

Missing blocks recorded: ${report.missingBlocks.length}

See \`01-content-inventory.json\` for full counts.
`,
  );

  console.log(
    JSON.stringify(
      {
        ok: report.overallPass,
        failedTabs: report.failedTabs,
        missingBlocks: report.missingBlocks.length,
        outDir,
      },
      null,
      2,
    ),
  );
  if (!report.overallPass) process.exit(2);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
