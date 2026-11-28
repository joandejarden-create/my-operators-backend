#!/usr/bin/env node
/**
 * Executive Summary uses canonical DealalityWaveLoader while loading.
 *   npm run test:gdi-es-wave-loading-v1
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import vm from "node:vm";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
let failed = 0;

function check(name, fn) {
  try {
    fn();
    console.log(`PASS ${name}`);
  } catch (err) {
    failed += 1;
    console.error(`FAIL ${name}: ${err && err.message ? err.message : err}`);
  }
}

check("shell_includes_canonical_wave_loader_assets", () => {
  const html = fs.readFileSync(
    path.join(ROOT, "public/group-demand-intelligence.html"),
    "utf8",
  );
  assert.match(html, /dealality-wave-loader\.css\?v=\d+/);
  assert.match(html, /dealality-wave-loader\.js\?v=\d+/);
  assert.match(html, /dealality-shared-loading-ux-v1\.js/);
  assert.match(html, /executive-summary-tab\.js\?v=gdi-es-wave-loading-20261127/);
});

check("es_tab_uses_DealalityWaveLoader_not_generic_text", () => {
  const js = fs.readFileSync(
    path.join(ROOT, "public/js/group-demand-intelligence/executive-summary-tab.js"),
    "utf8",
  );
  const css = fs.readFileSync(
    path.join(ROOT, "public/css/group-demand-intelligence.css"),
    "utf8",
  );
  assert.match(js, /DealalityWaveLoader/);
  assert.match(js, /toastHtml/);
  assert.match(js, /executiveSummaryLoadingHtml/);
  assert.match(js, /data-gdi-es-wave-loading/);
  assert.doesNotMatch(js, /Loading executive summary…/);
  assert.doesNotMatch(js, /gdi-es-loading"/);
  assert.match(css, /\.gdi-es-loading-wave/);
  // Error / empty states preserved.
  assert.match(js, /Select a hotel to view the Executive Summary/);
  assert.match(js, /Executive Summary failed to load/);
});

check("loading_html_calls_canonical_toastHtml", () => {
  const waveSrc = fs.readFileSync(
    path.join(ROOT, "public/js/dealality-wave-loader.js"),
    "utf8",
  );
  const esSrc = fs.readFileSync(
    path.join(ROOT, "public/js/group-demand-intelligence/executive-summary-tab.js"),
    "utf8",
  );
  const sandbox = {
    window: {},
    console,
    URLSearchParams,
    encodeURIComponent,
    setTimeout,
    clearTimeout,
  };
  sandbox.global = sandbox.window;
  sandbox.window.global = sandbox.window;
  vm.runInNewContext(waveSrc, sandbox);
  assert.ok(sandbox.window.DealalityWaveLoader);
  // Extract and eval only the helper by mounting a fake controller snippet.
  assert.match(esSrc, /wave\.toastHtml\(label\)/);
  const toast = sandbox.window.DealalityWaveLoader.toastHtml(
    "Loading Executive Summary…",
  );
  assert.match(toast, /dealality-wave-loader/);
  assert.match(toast, /wave-container/);
  assert.match(toast, /wave wave-1/);
});

check("canonical_wave_loader_module_unchanged_contract", () => {
  const wave = fs.readFileSync(
    path.join(ROOT, "public/js/dealality-wave-loader.js"),
    "utf8",
  );
  assert.match(wave, /window\.DealalityWaveLoader\s*=/);
  assert.match(wave, /toastHtml/);
  assert.match(wave, /compactHtml/);
});

if (failed) {
  console.error(`\n${failed} failure(s)`);
  process.exit(1);
}
console.log("\nOK test:gdi-es-wave-loading-v1");
