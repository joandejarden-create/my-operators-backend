/**
 * HE-QL-028 — Compact Dealality loading toast (no full-width black bars).
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import vm from "node:vm";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const read = (rel) => fs.readFileSync(path.join(ROOT, rel), "utf8");

function mustInclude(hay, needle) {
  assert.ok(hay.includes(needle), `expected to include: ${needle}`);
}

let passed = 0;
function run(name, fn) {
  fn();
  passed += 1;
  console.log(`ok - ${name}`);
}

run("wave_loader_exposes_toastHtml_and_compact_not_full_width", () => {
  const js = read("public/js/dealality-wave-loader.js");
  mustInclude(js, "toastHtml");
  mustInclude(js, "dealality-wave-loader--toast");
  mustInclude(js, "dealality-loading-toast-host");
  const css = read("public/css/dealality-wave-loader.css");
  mustInclude(css, "dealality-loading-toast-host");
  mustInclude(css, "dealality-wave-loader--toast");
  mustInclude(css, "prefers-reduced-motion");
  // Compact must not force full-width black strip
  const compactBlock = css.slice(css.indexOf(".dealality-wave-loader--compact"));
  const rule = compactBlock.slice(0, compactBlock.indexOf(".dealality-loading-toast-host"));
  assert.ok(!/width:\s*100%/.test(rule), "compact loader must not be width:100%");
  assert.ok(/width:\s*auto/.test(rule), "compact loader uses width:auto");
});

run("he_loading_ux_adapter_modes", () => {
  const js = read("public/js/he-loading-ux-v1.js");
  mustInclude(js, "toastHtml");
  mustInclude(js, "waveHtml");
  mustInclude(js, "inlineHtml");
  mustInclude(js, "HeLoadingUx");
  const sandbox = {
    console,
    DealalityWaveLoader: null,
  };
  sandbox.window = sandbox;
  sandbox.globalThis = sandbox;
  // Load wave loader first
  vm.runInNewContext(read("public/js/dealality-wave-loader.js"), sandbox);
  vm.runInNewContext(js, sandbox);
  const ux = sandbox.HeLoadingUx;
  assert.ok(ux);
  const toast = ux.toastHtml("Loading Contacts…");
  mustInclude(toast, "dealality-loading-toast-host");
  mustInclude(toast, "dealality-wave-loader--toast");
  mustInclude(toast, "Loading Contacts…");
  assert.ok(!/width:\s*100%/.test(toast));
  const inline = ux.inlineHtml("Looking up…", { subText: "Surfe" });
  mustInclude(inline, "dealality-wave-loader--compact");
  assert.ok(!inline.includes("dealality-loading-toast-host"));
});

run("contact_intelligence_uses_toast_not_full_width_banner", () => {
  const hci = read("public/js/hotel-contact-intelligence.js");
  mustInclude(hci, "Loading Contacts…");
  mustInclude(hci, "HeLoadingUx");
  mustInclude(hci, "sectionHtml");
  mustInclude(hci, "he-async-loading");
  assert.ok(!/Loading Contact Intelligence…/.test(hci));
  assert.ok(
    !/class="hci-loading"/.test(hci),
    "Contact must not keep a separate .hci-loading card"
  );
  const css = read("public/css/hotel-contact-intelligence.css");
  mustInclude(css, "he-async-loading");
  assert.ok(!/\.hci-loading\s*\{/.test(css), "remove custom .hci-loading block");
});

run("he_tabs_share_same_loading_component", () => {
  const hci = read("public/js/hotel-contact-intelligence.js");
  const ws = read("public/js/hotel-intelligence-workspace.js");
  const asyncJs = read("public/js/he-async-section-v1.js");
  // Shared adapter — Contact must not invent a parallel spinner
  mustInclude(hci, "HeLoadingUx");
  mustInclude(ws, "HeLoadingUx");
  mustInclude(asyncJs, "HeLoadingUx");
  mustInclude(hci, "he-async-loading");
  mustInclude(ws, "he-async-loading");
  const hexCss = read("public/css/hotel-explorer.css");
  const waveCss = read("public/css/dealality-wave-loader.css");
  mustInclude(hexCss, ".hex-root .he-async-loading");
  mustInclude(hexCss, "dealality-wave-loader--toast");
  // Canonical toast chrome is black translucent — HE must not re-skin to elevated blue-gray
  assert.ok(
    /rgba\(\s*0,\s*0,\s*0,\s*0\.(85|9)/.test(waveCss),
    "canonical wave loader uses black translucent container"
  );
  assert.ok(
    !/\.hex-root\s+\.dealality-wave-loader--toast[\s\S]{0,220}--hdp-bg-elevated/.test(
      hexCss
    ),
    "HE must not override toast background to elevated blue-gray tokens"
  );
  assert.ok(
    !/\.hex-root\s+\.dealality-wave-loader--sm\s+\.wave-container[\s\S]{0,80}20px/.test(
      hexCss
    ),
    "HE must not shrink wave orb below canonical --sm (24px)"
  );
  assert.ok(
    /max-width:\s*min\(100%,\s*220px\)/.test(hexCss),
    "HE toast max-width stays compact"
  );
  // Neutralize Radar page `.loading { height: 200px }` conflict on wave loaders
  assert.ok(
    /height:\s*auto\s*!important/.test(hexCss),
    "HE must override host-page .loading height for wave loaders"
  );
  assert.ok(
    /dealality-wave-loader\.loading|\.loading\.dealality-wave-loader/.test(hexCss),
    "HE height reset must target .loading.dealality-wave-loader"
  );
});

run("radar_loading_selector_excludes_wave_loader", () => {
  for (const rel of [
    "public/deal-capture-radar-with-ranked-list.html",
    "public/deal-capture-radar-standalone.html",
    "public/deal-capture-radar-clean.html",
  ]) {
    const html = read(rel);
    mustInclude(html, ".loading:not(.dealality-wave-loader)");
    assert.ok(
      !/\.loading\s*\{\s*display:\s*flex;\s*justify-content:\s*center;\s*align-items:\s*center;\s*height:\s*200px/s.test(
        html
      ),
      `${rel} must not keep unscoped .loading { height: 200px }`
    );
  }
});

run("workspace_and_async_prefer_toast", () => {
  const ws = read("public/js/hotel-intelligence-workspace.js");
  mustInclude(ws, "HeLoadingUx");
  mustInclude(ws, "toastHtml");
  const asyncJs = read("public/js/he-async-section-v1.js");
  mustInclude(asyncJs, "HeLoadingUx");
  mustInclude(asyncJs, "toastHtml");
});

run("hex_css_forbids_full_width_compact_strips", () => {
  const css = read("public/css/hotel-explorer.css");
  mustInclude(css, "he-loading-toast-host");
  mustInclude(css, ".hex-root .dealality-wave-loader--compact");
  mustInclude(css, "width: auto");
});

run("quality_registry_he_ql_028", () => {
  const reg = JSON.parse(
    read("docs/ai-build-system/HOTEL_EXPLORER_FOUNDER_QUALITY_LESSONS_REGISTRY.json")
  );
  assert.ok(
    String(reg.version || "").startsWith("hotel-explorer-founder-quality-lessons-v1."),
    "registry version present"
  );
  const lesson = reg.lessons.find((l) => l.id === "HE-QL-028");
  assert.ok(lesson);
  mustInclude(lesson.approvedStandard, "HeLoadingUx");
  mustInclude(read("docs/ai-build-system/HOTEL_EXPLORER_QUALITY_STANDARD.md"), "HE-QL-028");
  assert.ok(reg.additionalGates.includes("test:he-loading-ux-toast-v1"));
});

run("preview_html_loads_shared_adapter", () => {
  const html = read("public/hotel-explorer-preview.html");
  mustInclude(html, "he-loading-ux-v1.js");
  mustInclude(html, "dealality-wave-loader.js?v=3");
  mustInclude(html, "dealality-wave-loader.css?v=3");
});

console.log(`\n${passed} checks passed (he-loading-ux-toast-v1)`);
