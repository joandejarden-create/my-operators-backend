#!/usr/bin/env node
/**
 * Regression: Hotel Explorer theme tokens + no competing navy families in HE CSS.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const cssDir = path.join(ROOT, "public/css");

const tokens = fs.readFileSync(
  path.join(cssDir, "hotel-explorer-theme-tokens-v1.css"),
  "utf8",
);
assert.match(tokens, /--he-canvas:\s*#080f25/);
assert.match(tokens, /--he-section:\s*#101935/);
assert.match(tokens, /--he-elevated:\s*#212c4d/);
assert.match(tokens, /--he-table:\s*#0d152c/);
assert.match(tokens, /--hiw-elevated:\s*var\(--he-elevated\)/);
assert.match(tokens, /\.hex-root \.hex-shell/);

const explorer = fs.readFileSync(path.join(cssDir, "hotel-explorer.css"), "utf8");
assert.match(explorer, /hotel-explorer-theme-tokens-v1\.css/);
assert.doesNotMatch(
  explorer,
  /\.hex-rms-metric\s*\{[^}]*background:\s*rgba\(10,\s*18,\s*42/s,
  "hex-rms-metric must not use competing rgba(10,18,42) navy",
);

const hiw = fs.readFileSync(path.join(cssDir, "hotel-intelligence-workspace.css"), "utf8");
assert.doesNotMatch(hiw, /#1a2448/, "HIW must not use #1a2448 elevated navy");
assert.doesNotMatch(hiw, /#0b1228/, "HIW must not use #0b1228 gradient terminus");
assert.match(hiw, /--hiw-elevated:\s*#212c4d/);

const radarHtml = fs.readFileSync(
  path.join(ROOT, "public/deal-capture-radar-with-ranked-list.html"),
  "utf8",
);
const previewHtml = fs.readFileSync(
  path.join(ROOT, "public/hotel-explorer-preview.html"),
  "utf8",
);
assert.match(radarHtml, /hotel-explorer-theme-tokens-v1\.css/);
assert.match(previewHtml, /hotel-explorer-theme-tokens-v1\.css/);

function themeLoadsAfterExplorer(html) {
  const explorerIdx = html.indexOf("hotel-explorer.css");
  const themeIdx = html.lastIndexOf("hotel-explorer-theme-tokens-v1.css");
  return explorerIdx >= 0 && themeIdx > explorerIdx;
}
assert.equal(
  themeLoadsAfterExplorer(radarHtml),
  true,
  "Radar host must load theme tokens after hotel-explorer.css",
);
assert.equal(
  themeLoadsAfterExplorer(previewHtml),
  true,
  "Preview host must load theme tokens after hotel-explorer.css",
);

assert.match(
  explorer,
  /\.hex-root \.hex-scroll[\s\S]*?background:\s*var\(--he-canvas/,
  "hex-scroll must paint continuous HE canvas",
);

// Active background values only (ignore historical comments).
const activeBgForbidden = [
  /background(?:-color)?:\s*#1a2448/i,
  /background(?:-color)?:\s*#0b1228/i,
  /background(?:-color)?:\s*#0c132c/i,
  /background(?:-color)?:\s*rgba\(\s*10\s*,\s*18\s*,\s*42/i,
];
for (const re of activeBgForbidden) {
  assert.equal(re.test(explorer), false, `forbidden active background ${re}`);
}
assert.equal(hiw.includes("#1a2448"), false);
assert.equal(hiw.includes("#0b1228"), false);
assert.equal(
  /background(?:-color)?:\s*#1a2448/i.test(tokens) ||
    /background(?:-color)?:\s*#0b1228/i.test(tokens),
  false,
);

console.log(
  JSON.stringify({
    ok: true,
    gate: "test:he-unified-theme-v1",
    heUnifiedTheme: "YES",
    heElevenTabVisualConsistency: "REGRESSION_TOKEN_GATED",
    heNoBackgroundSeams: "REGRESSION_TOKEN_GATED",
  }),
);
