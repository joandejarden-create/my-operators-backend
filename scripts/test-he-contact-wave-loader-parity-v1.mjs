#!/usr/bin/env node
/**
 * Contact Intelligence loader must use the exact Dealality wave toast
 * (compact black box + animated wave), not HE elevated navy chrome.
 *
 * Reference component: DealalityWaveLoader.toastHtml /
 * HeLoadingUx.sectionHtml → public/css/dealality-wave-loader.css
 * (.dealality-wave-loader--toast { background: rgba(0, 0, 0, 0.9) })
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import vm from "node:vm";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const read = (rel) => fs.readFileSync(path.join(ROOT, rel), "utf8");

const waveCss = read("public/css/dealality-wave-loader.css");
assert.match(
  waveCss,
  /\.dealality-wave-loader--toast\s*\{[^}]*background:\s*rgba\(\s*0\s*,\s*0\s*,\s*0\s*,\s*0\.9\s*\)/s,
  "reference toast uses black rgba(0,0,0,0.9)",
);
assert.match(waveCss, /@keyframes bl-wave-1/);

const hciJs = read("public/js/hotel-contact-intelligence.js");
assert.match(hciJs, /HeLoadingUx/);
assert.match(hciJs, /Loading Contacts…/);
assert.match(hciJs, /sectionHtml|toastHtml/);
assert.doesNotMatch(hciJs, /class="hci-loading"/);

const hciCss = read("public/css/hotel-contact-intelligence.css");
assert.match(
  hciCss,
  /\.hex-root \.hci-root \.dealality-wave-loader--toast/,
  "HCI must scope toast override under .hci-root",
);
assert.match(
  hciCss,
  /background:\s*rgba\(\s*0\s*,\s*0\s*,\s*0\s*,\s*0\.9\s*\)/,
  "HCI toast must restore canonical black box",
);
assert.match(hciCss, /height:\s*auto\s*!important/);
assert.match(hciCss, /animation:\s*bl-wave-1/);
assert.doesNotMatch(
  hciCss,
  /\.hci-root[^{]*\{[^}]*dealality-wave-loader[^}]*background:\s*var\(--hdp-bg-elevated/s,
);

const radar = read("public/deal-capture-radar-with-ranked-list.html");
const preview = read("public/hotel-explorer-preview.html");
assert.match(radar, /hotel-contact-intelligence\.css\?v=18/);
assert.match(preview, /hotel-contact-intelligence\.css\?v=18/);
assert.match(radar, /\.loading:not\(\.dealality-wave-loader\)/);

const sandbox = { console, window: {}, globalThis: null };
sandbox.globalThis = sandbox;
sandbox.window = sandbox;
vm.runInNewContext(read("public/js/dealality-wave-loader.js"), sandbox);
vm.runInNewContext(read("public/js/he-loading-ux-v1.js"), sandbox);
const html = sandbox.HeLoadingUx.sectionHtml("Loading Contacts…");
assert.match(html, /dealality-wave-loader--toast/);
assert.match(html, /wave-container/);
assert.match(html, /wave wave-1/);
assert.match(html, /Loading Contacts…/);
assert.doesNotMatch(html, /height:\s*200px/);

console.log(
  JSON.stringify({
    ok: true,
    gate: "test:he-contact-wave-loader-parity-v1",
    reference: "DealalityWaveLoader.toastHtml + dealality-wave-loader.css --toast",
    heContactWaveBlackBox: "YES",
  }),
);
