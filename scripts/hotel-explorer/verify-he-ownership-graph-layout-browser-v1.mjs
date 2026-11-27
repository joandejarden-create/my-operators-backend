#!/usr/bin/env node
/**
 * Browser visual regression: Ownership Intelligence relationship graph
 * connector-label / node / legend layout (shared OCN component).
 *
 *   DEALALITY_LOCAL_ADMIN_BASE=http://127.0.0.1:8080 \
 *     node scripts/hotel-explorer/verify-he-ownership-graph-layout-browser-v1.mjs
 *
 * Checks:
 *   - connector-label overlap with nodes
 *   - connector-label mutual overlap
 *   - node title / verification label readability
 *   - graph clipping
 *   - legend readability + About This Structure
 *   - responsive geometry (wide + narrow)
 *   - Bethesda Proposed/Verified classification preservation
 *
 * Exit 0 on PASS. Without a reachable server, exits 2 (NOT_TESTED) unless
 * --require-server is passed (then exit 1).
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { chromium } from "playwright";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const BASE = (
  process.env.DEALALITY_LOCAL_ADMIN_BASE ||
  "http://127.0.0.1:8080"
).replace(/\/$/, "");
const HOTEL =
  process.env.HE_OCN_VERIFY_HOTEL_ID ||
  process.env.HE_LOADING_VERIFY_HOTEL_ID ||
  "recLuxvwwxID7U2B8";
const requireServer = process.argv.includes("--require-server");
const OUT_DIR = path.join(ROOT, "reports", "hotel-explorer");
const SHOT_DIR = path.join(OUT_DIR, "ocn-layout-shots");
const OUT = path.join(OUT_DIR, "HE-OWNERSHIP-GRAPH-LAYOUT-BROWSER-V1.json");

const REQUIRED_EDGE_LABELS = [
  "Economic Sponsorship",
  "Legal Ownership",
  "Hotel Management",
  "Brand Affiliation",
];

async function probeServer() {
  try {
    const res = await fetch(`${BASE}/deal-capture-radar-with-ranked-list.html`, {
      method: "HEAD",
      signal: AbortSignal.timeout(4000),
    });
    return res.ok || res.status === 405 || res.status === 200;
  } catch {
    return false;
  }
}

const serverOk = await probeServer();
if (!serverOk) {
  const payload = {
    contract: "he-ownership-graph-layout-browser-v1",
    status: "NOT_TESTED",
    reason: `server_unreachable:${BASE}`,
    generatedAt: new Date().toISOString(),
  };
  fs.mkdirSync(OUT_DIR, { recursive: true });
  fs.writeFileSync(OUT, JSON.stringify(payload, null, 2));
  console.log(JSON.stringify(payload, null, 2));
  process.exit(requireServer ? 1 : 2);
}

async function launchBrowser() {
  try {
    return await chromium.launch({ headless: true });
  } catch (err) {
    const msg = String(err && err.message ? err.message : err);
    if (/Executable doesn't exist|browserType\.launch/i.test(msg)) {
      return chromium.launch({ channel: "chrome", headless: true });
    }
    throw err;
  }
}

function rectsOverlap(a, b, pad = 0) {
  return !(
    a.x + a.w + pad <= b.x ||
    b.x + b.w + pad <= a.x ||
    a.y + a.h + pad <= b.y ||
    b.y + b.h + pad <= a.y
  );
}

async function measureViewport(page, label) {
  await page.waitForSelector(".ocn[data-ocn='1'] .ocn-svg", { timeout: 45000 });
  await page.waitForSelector(".ocn-edge-label-host, .ocn-edge-label", {
    timeout: 20000,
  });
  // Allow layout + fonts to settle.
  await page.waitForTimeout(700);

  const metrics = await page.evaluate(() => {
    const root = document.querySelector(".ocn[data-ocn='1']");
    const stage = root?.querySelector(".ocn-stage");
    const svg = root?.querySelector(".ocn-svg");
    const legend = root?.querySelector(".ocn-legend");
    const about = root?.querySelector("[data-ocn-about-structure]");
    const chips = Array.from(root?.querySelectorAll("[data-ocn-edge-chip]") || []).map(
      (el) => {
        const r = el.getBoundingClientRect();
        const edge = el.closest("[data-ocn-edge]");
        const edgeClass = edge?.getAttribute("class") || "";
        return {
          label: (edge?.getAttribute("data-ocn-edge-label") || el.textContent || "")
            .trim()
            .replace(/\s+/g, " "),
          type: edge?.getAttribute("data-ocn-edge-type") || "",
          verification: edgeClass.match(/ocn-edge--(\w+)/)?.[1] || "",
          x: r.x,
          y: r.y,
          w: r.width,
          h: r.height,
        };
      }
    );
    const nodes = Array.from(root?.querySelectorAll("[data-ocn-node]") || []).map((el) => {
      const r = el.getBoundingClientRect();
      const role = (el.querySelector(".ocn-node__role")?.textContent || "").trim();
      const title = (el.querySelector(".ocn-node__label")?.textContent || "")
        .trim()
        .replace(/\s+/g, " ");
      const meta = (el.querySelector(".ocn-node__meta")?.textContent || "").trim();
      const nodeClass = el.getAttribute("class") || "";
      const ver =
        nodeClass.match(/ocn-node--(verified|proposed|historical|unresolved)/)?.[1] || "";
      return {
        id: el.getAttribute("data-ocn-node") || "",
        role,
        title,
        meta,
        verification: ver,
        x: r.x,
        y: r.y,
        w: r.width,
        h: r.height,
      };
    });
    const legendItems = Array.from(legend?.querySelectorAll(".ocn-legend__item") || []).map(
      (el) => {
        const r = el.getBoundingClientRect();
        return {
          text: (el.textContent || "").trim().replace(/\s+/g, " "),
          x: r.x,
          y: r.y,
          w: r.width,
          h: r.height,
        };
      }
    );
    const stageRect = stage?.getBoundingClientRect();
    const svgRect = svg?.getBoundingClientRect();
    const viewport = root?.querySelector(".ocn-viewport");
    let clipped = false;
    if (stageRect && viewport) {
      const vr = viewport.getBoundingClientRect();
      clipped =
        vr.left < stageRect.left - 2 ||
        vr.right > stageRect.right + 8 ||
        vr.top < stageRect.top - 2 ||
        vr.bottom > stageRect.bottom + 8;
    }
    const labelLayout = root?.getAttribute("data-ocn-label-layout") || "";
    return {
      labelLayout,
      chipCount: chips.length,
      nodeCount: nodes.length,
      chips,
      nodes,
      legendItems,
      legendReadable:
        !!legend &&
        legendItems.length >= 3 &&
        legendItems.every((i) => i.w >= 40 && i.h >= 12),
      aboutPresent: !!about,
      aboutSummary: (about?.querySelector("summary")?.textContent || "").trim(),
      stage: stageRect
        ? { w: stageRect.width, h: stageRect.height }
        : null,
      svg: svgRect ? { w: svgRect.width, h: svgRect.height } : null,
      clipped,
      edgeLabels: chips.map((c) => c.label),
    };
  });

  const labelVsNode = [];
  for (const chip of metrics.chips) {
    for (const node of metrics.nodes) {
      if (rectsOverlap(chip, node, -1)) {
        labelVsNode.push({
          label: chip.label,
          node: node.title,
          nodeRole: node.role,
        });
      }
    }
  }

  const labelVsLabel = [];
  for (let i = 0; i < metrics.chips.length; i++) {
    for (let j = i + 1; j < metrics.chips.length; j++) {
      if (rectsOverlap(metrics.chips[i], metrics.chips[j], 2)) {
        labelVsLabel.push({
          a: metrics.chips[i].label,
          b: metrics.chips[j].label,
        });
      }
    }
  }

  const nodeTitleEmpty = metrics.nodes.filter((n) => !n.title || n.title.length < 2);
  const verificationEmpty = metrics.nodes.filter((n) => !n.meta || !n.verification);

  const shotPath = path.join(SHOT_DIR, `ocn-${label}.png`);
  fs.mkdirSync(SHOT_DIR, { recursive: true });
  const ocn = page.locator(".ocn[data-ocn='1']");
  await ocn.screenshot({ path: shotPath });

  return {
    viewport: label,
    ...metrics,
    overlaps: {
      labelVsNode,
      labelVsLabel,
      nodeTitleEmpty: nodeTitleEmpty.map((n) => n.role || n.id),
      verificationEmpty: verificationEmpty.map((n) => n.title || n.id),
    },
    screenshot: path.relative(ROOT, shotPath).replace(/\\/g, "/"),
  };
}

function classifyBethesda(nodes) {
  const find = (re) =>
    nodes.find((n) => re.test(String(n.title || "").replace(/\s+/g, " ")));
  const mullen = find(/Mullen/i);
  const marbeth = find(/Marbeth/i);
  const hotel = find(/Bethesda Marriott/i);
  const operator = find(/Marriott International/i);
  const brand = nodes.find(
    (n) => /Marriott Hotels/i.test(n.title || "") && /brand/i.test(n.role || "")
  );
  return {
    mullen: mullen
      ? { title: mullen.title, verification: mullen.verification, meta: mullen.meta }
      : null,
    marbeth: marbeth
      ? {
          title: marbeth.title,
          verification: marbeth.verification,
          meta: marbeth.meta,
        }
      : null,
    hotel: hotel
      ? { title: hotel.title, verification: hotel.verification, meta: hotel.meta }
      : null,
    operator: operator
      ? {
          title: operator.title,
          verification: operator.verification,
          meta: operator.meta,
        }
      : null,
    brand: brand
      ? { title: brand.title, verification: brand.verification, meta: brand.meta }
      : null,
  };
}

const browser = await launchBrowser();
const page = await browser.newPage({ viewport: { width: 1440, height: 960 } });

const url = `${BASE}/deal-capture-radar-with-ranked-list.html?cb=${Date.now()}`;

await page.goto(url, { waitUntil: "domcontentloaded", timeout: 60000 });
await page.waitForFunction(() => typeof window.HotelExplorer === "object", null, {
  timeout: 30000,
});

await page.evaluate(async (hotelId) => {
  const hotel = {
    id: hotelId,
    recordId: hotelId,
    name: "Bethesda Marriott",
  };
  if (typeof HotelExplorer.open === "function") {
    const maybe = HotelExplorer.open(hotel, {
      tab: "ownership",
      requestedHotelId: hotelId,
    });
    if (maybe && typeof maybe.then === "function") await maybe;
  } else if (typeof HotelExplorer.openDossierById === "function") {
    await HotelExplorer.openDossierById(hotelId);
    if (typeof HotelExplorer.setTab === "function") HotelExplorer.setTab("ownership");
  }
  // Ownership fetch is async — wait for OCN host after paint.
  const deadline = Date.now() + 45000;
  while (Date.now() < deadline) {
    if (document.querySelector(".ocn[data-ocn='1'] .ocn-svg")) break;
    await new Promise((r) => setTimeout(r, 250));
  }
}, HOTEL);

const wide = await measureViewport(page, "wide-1440");

await page.setViewportSize({ width: 980, height: 900 });
await page.waitForTimeout(600);
await page.evaluate(() => {
  if (typeof HotelExplorer.setTab === "function") HotelExplorer.setTab("ownership");
  const fit = document.querySelector("[data-ocn-fit]");
  if (fit) fit.click();
});
await page.waitForTimeout(700);
const narrow = await measureViewport(page, "narrow-980");

await browser.close();

const beth = classifyBethesda(wide.nodes || []);

const checks = {
  labelLayoutV2: wide.labelLayout === "v2",
  edgeChipsPresent: wide.chipCount >= 3,
  customerEdgeLabels: REQUIRED_EDGE_LABELS.every((lab) =>
    (wide.edgeLabels || []).some((e) => e.includes(lab))
  ),
  noLabelNodeOverlapWide: (wide.overlaps.labelVsNode || []).length === 0,
  noLabelNodeOverlapNarrow: (narrow.overlaps.labelVsNode || []).length === 0,
  noLabelLabelOverlapWide: (wide.overlaps.labelVsLabel || []).length === 0,
  noLabelLabelOverlapNarrow: (narrow.overlaps.labelVsLabel || []).length === 0,
  nodeTitlesReadable: (wide.overlaps.nodeTitleEmpty || []).length === 0,
  verificationLabelsPresent: (wide.overlaps.verificationEmpty || []).length === 0,
  legendReadable: wide.legendReadable === true && narrow.legendReadable === true,
  aboutStructurePresent:
    wide.aboutPresent === true && /About This Structure/i.test(wide.aboutSummary || ""),
  notClippedWide: wide.clipped !== true,
  notClippedNarrow: narrow.clipped !== true,
  mullenProposed:
    beth.mullen &&
    (beth.mullen.verification === "proposed" || /proposed/i.test(beth.mullen.meta || "")),
  // Legal PropCo may be verified or proposed depending on live payload — never invent promotion.
  marbethNotUnresolved: beth.marbeth && beth.marbeth.verification !== "unresolved",
  hotelPresent: Boolean(beth.hotel),
  operatorVerifiedOrKnown:
    beth.operator &&
    (beth.operator.verification === "verified" || /verified/i.test(beth.operator.meta || "")),
  brandVerifiedOrKnown:
    beth.brand &&
    (beth.brand.verification === "verified" || /verified/i.test(beth.brand.meta || "")),
};

const failed = Object.entries(checks)
  .filter(([, v]) => !v)
  .map(([k]) => k);

const payload = {
  contract: "he-ownership-graph-layout-browser-v1",
  status: failed.length ? "FAIL" : "PASS",
  hotelId: HOTEL,
  base: BASE,
  checks,
  failed,
  bethesdaClassification: beth,
  viewports: {
    wide: {
      chipCount: wide.chipCount,
      nodeCount: wide.nodeCount,
      edgeLabels: wide.edgeLabels,
      overlaps: wide.overlaps,
      screenshot: wide.screenshot,
      stage: wide.stage,
      svg: wide.svg,
    },
    narrow: {
      chipCount: narrow.chipCount,
      nodeCount: narrow.nodeCount,
      edgeLabels: narrow.edgeLabels,
      overlaps: narrow.overlaps,
      screenshot: narrow.screenshot,
      stage: narrow.stage,
      svg: narrow.svg,
    },
  },
  generatedAt: new Date().toISOString(),
};

fs.mkdirSync(OUT_DIR, { recursive: true });
fs.writeFileSync(OUT, JSON.stringify(payload, null, 2));
console.log(JSON.stringify(payload, null, 2));

if (failed.length) {
  assert.fail(`he-ownership-graph-layout-browser-v1 failed: ${failed.join(", ")}`);
}
