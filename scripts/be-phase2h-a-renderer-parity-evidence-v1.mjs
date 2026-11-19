#!/usr/bin/env node
/**
 * Phase 2H-A Gate 1 — offline payload/hash + DOM fingerprint helpers.
 * Browser screenshots / computed-style captures are collected via the live preview.
 */
import { createHash } from "crypto";
import { mkdirSync, writeFileSync, readFileSync, existsSync } from "fs";
import { join } from "path";
import { pathToFileURL } from "url";

const root = process.cwd();
const outDir = join(root, "reports/brand-explorer/phase2h-a-renderer-parity");
mkdirSync(outDir, { recursive: true });

const payloadPath = join(root, "public/brand-explorer-phase2e-preview/curio-gate1-renderer-parity-payload.json");
if (!existsSync(payloadPath)) {
  console.error("Missing Gate 1 payload — run be-phase2h-a-freeze-renderer-parity-payload-v1.mjs first");
  process.exit(1);
}

const brand = JSON.parse(readFileSync(payloadPath, "utf8"));
const blocks = brand.brandExplorer?.blocks || [];

function sha256(s) {
  return createHash("sha256").update(String(s)).digest("hex");
}

function blocksHash(list) {
  const norm = (list || [])
    .map((b) => ({
      slotKey: String(b.slotKey || "").trim(),
      title: String(b.title || "").trim(),
      body: String(b.body || "").trim(),
      imageUrl: b.imageUrl || null,
      sort: typeof b.sort === "number" ? b.sort : null,
    }))
    .sort((a, b) => String(a.slotKey).localeCompare(String(b.slotKey)));
  return sha256(JSON.stringify(norm));
}

const hash = blocksHash(blocks);
const declared = brand.gate1?.presentationBlocksHashSha256 || null;

const slotInventory = blocks.reduce((acc, b) => {
  const k = b.slotKey || "(missing)";
  acc[k] = (acc[k] || 0) + 1;
  return acc;
}, {});

const gate1Slots = {
  snapshot: Object.keys(slotInventory).filter((k) => /^(snapshot|identity)\./.test(k)),
  scenarios: Object.keys(slotInventory).filter((k) => /^overview\.scenario\./.test(k)),
  bestAt: Object.keys(slotInventory).filter((k) => /^overview\.bestAt\./.test(k)),
  whyValue: Object.keys(slotInventory).filter((k) => k === "overview.why_value"),
  valueOwners: Object.keys(slotInventory).filter((k) => /^valueOwners\./.test(k)),
  loyalty: Object.keys(slotInventory).filter((k) => /^loyalty\./.test(k)),
  economics: Object.keys(slotInventory).filter((k) => /^economics\./.test(k)),
  footprint: Object.keys(slotInventory).filter((k) => /^footprint\./.test(k)),
  materials: Object.keys(slotInventory).filter((k) => /^materials\./.test(k)),
};

const evidence = {
  phase: "2H-A",
  gate: 1,
  founderApprovalStatus: "NOT_FOUNDER_APPROVED",
  contentMutation: false,
  automatedPassIsNotFounderApproval: true,
  payload: {
    path: "public/brand-explorer-phase2e-preview/curio-gate1-renderer-parity-payload.json",
    source: brand.gate1?.payloadSource || null,
    blockCount: blocks.length,
    presentationBlocksHashSha256: hash,
    declaredHashMatches: declared === hash,
    declaredHash: declared,
  },
  urls: {
    environmentA_product:
      "http://127.0.0.1:8082/brand-explorer-renderer-parity.html?beInternalPreview=1&payload=curio-gate1-renderer-parity-payload.json",
    environmentB_compare:
      "http://127.0.0.1:8082/brand-explorer-compare.html?beInternalPreview=1&rendererParity=1&brand=receQkxgjlezsc1xg",
    equalWidthGeometry:
      "http://127.0.0.1:8082/brand-explorer-compare.html?beInternalPreview=1&rendererParity=1&equalWidth=1",
    customerFacingReference:
      "http://127.0.0.1:8082/brand-explorer-gold-mock.html?id=receQkxgjlezsc1xg&beInternalPreview=1",
  },
  renderer: {
    module: "public/js/brand-explorer-atelier-from-api.js",
    css: [
      "public/css/brand-explorer-atelier-oe-panels.css",
      "public/css/brand-explorer-gold-surface.css",
      "public/css/brand-explorer-combined-detail-parity.css",
    ],
    productMount: "BrandExplorerAtelierFromApi.mountIntoRoot",
    compareMount: "BrandExplorerAtelierFromApi.renderTabInnerHtml → .oe-section pairs",
    forbidSimplifiedCompareRenderer: true,
  },
  testSections: [
    "Hero and Brand Snapshot (Overview)",
    "Where This Brand Creates the Most Value",
    "What This Brand Is Best At",
    "Owner Value Snapshot",
    "Loyalty Architecture",
    "Economics & Obligations",
    "Geographic Footprint",
    "Brand Materials / Property Examples",
  ],
  slotInventoryCounts: Object.fromEntries(Object.entries(gate1Slots).map(([k, v]) => [k, v.length])),
  note: "Browser DOM/CSS/geometry/image evidence appended by live capture into 02-browser-capture.json",
};

writeFileSync(join(outDir, "01-payload-and-renderer-evidence.json"), JSON.stringify(evidence, null, 2));
writeFileSync(
  join(outDir, "01-payload-and-renderer-evidence.md"),
  `# Phase 2H-A Gate 1 — Payload & renderer evidence

**FOUNDER_APPROVED = false** · Automated evidence ≠ approval

## Payload
- Source: \`${brand.gate1?.payloadSource}\`
- Blocks: **${blocks.length}**
- SHA-256: \`${hash}\`
- Declared hash match: **${declared === hash}**

## URLs
- A (Product): ${evidence.urls.environmentA_product}
- B (Compare): ${evidence.urls.environmentB_compare}
- Equal-width: ${evidence.urls.equalWidthGeometry}

## Canonical renderer
- JS: \`brand-explorer-atelier-from-api.js\`
- CSS: atelier OE panels (+ gold surface / combined detail parity)

Content regeneration: **disabled** for Gate 1.
`,
);

console.log(JSON.stringify({ ok: true, hash, blocks: blocks.length, outDir: "reports/brand-explorer/phase2h-a-renderer-parity" }, null, 2));
