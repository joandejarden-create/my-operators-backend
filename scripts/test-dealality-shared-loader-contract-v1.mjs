#!/usr/bin/env node
/**
 * Shared Dealality loading component contract — GDI · ADP · BE · HE.
 *
 *   node scripts/test-dealality-shared-loader-contract-v1.mjs
 *   node scripts/test-dealality-shared-loader-contract-v1.mjs --product he
 *   node scripts/test-dealality-shared-loader-contract-v1.mjs --browser
 *
 * Browser-dependent gates return NOT_TESTED when --browser is omitted.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import vm from "node:vm";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const browserEnabled = process.argv.includes("--browser");
const productArgIdx = process.argv.indexOf("--product");
const productFilter =
  productArgIdx >= 0 ? String(process.argv[productArgIdx + 1] || "").toLowerCase() : "all";

const CONTRACT_PATH = path.join(
  ROOT,
  "configs/dealality-design-system/dealality-shared-loader-contract-v1.json",
);
const contract = JSON.parse(fs.readFileSync(CONTRACT_PATH, "utf8"));
const read = (rel) => fs.readFileSync(path.join(ROOT, rel), "utf8");

function gate(ok) {
  return ok ? "PASS" : "FAIL";
}

function includesProduct(p) {
  return productFilter === "all" || productFilter === p;
}

const waveJs = read("public/js/dealality-wave-loader.js");
const waveCss = read("public/css/dealality-wave-loader.css");
const sharedUx = read("public/js/dealality-shared-loading-ux-v1.js");
const heUx = read("public/js/he-loading-ux-v1.js");
const heCss = read("public/css/hotel-explorer.css");
const gdiUi = read("public/js/group-demand-intelligence/dealality-gdi-ui.js");
const gdiHtml = read("public/group-demand-intelligence.html");
const adpHtml = read("public/owner-ai-demand.html");
const beHtml = read("public/brand-explorer-combined.html");
const heAsync = read("public/js/he-async-section-v1.js");
const shellCss = read("public/css/deal-workspace-shell.css");

const sandbox = { console, window: {}, globalThis: null };
sandbox.globalThis = sandbox;
sandbox.window = sandbox;
vm.runInNewContext(waveJs, sandbox);
vm.runInNewContext(sharedUx, sandbox);
vm.runInNewContext(heUx, sandbox);

const toast = sandbox.DealalityWaveLoader.toastHtml("Loading Contacts…");
const sharedToast = sandbox.DealalitySharedLoadingUx.toastHtml("heContacts");
const shouldCache = sandbox.DealalitySharedLoadingUx.shouldShowLoader({
  fromCache: true,
  asyncPending: false,
});
const shouldAsync = sandbox.DealalitySharedLoadingUx.shouldShowLoader({
  asyncPending: true,
});

const parityChecks = {
  canonicalMarkup: /DealalityWaveLoader/.test(waveJs) && /toastHtml/.test(waveJs),
  sharedUxPresent: /DealalitySharedLoadingUx/.test(sharedUx),
  contractId: waveJs.includes(contract.contract) && sharedUx.includes(contract.contract),
  heDelegates: /DealalitySharedLoadingUx/.test(heUx),
  gdiUsesShared: /DealalityWaveLoader|DealalitySharedLoadingUx/.test(gdiUi),
  gdiWiresSharedScript: /dealality-shared-loading-ux-v1\.js/.test(gdiHtml),
  adpWiresWaveCss: /dealality-wave-loader\.css/.test(adpHtml),
  beWiresWave: /dealality-wave-loader/.test(beHtml),
};

const waveAnimationOk =
  /@keyframes bl-wave-1/.test(waveCss) &&
  /@keyframes bl-wave-2/.test(waveCss) &&
  /@keyframes bl-wave-3/.test(waveCss) &&
  /animation:\s*bl-wave-1/.test(waveCss) &&
  /wave wave-1/.test(toast) &&
  /wave-container/.test(toast);

const blackToastOk =
  /\.dealality-wave-loader--toast\s*\{[^}]*background:\s*rgba\(\s*0\s*,\s*0\s*,\s*0\s*,\s*0\.9\s*\)/s.test(
    waveCss,
  ) &&
  !/\.hex-root \.dealality-wave-loader--toast[\s\S]{0,200}hdp-bg-elevated/.test(heCss) &&
  /background:\s*rgba\(\s*0\s*,\s*0\s*,\s*0\s*,\s*0\.9\s*\)/.test(
    heCss.slice(heCss.indexOf("canonical Dealality black toast")),
  );

const lifecycleOk =
  /fromCache:\s*true/.test(heAsync) &&
  /onLoading/.test(heAsync) &&
  shouldCache === false &&
  shouldAsync === true &&
  /preferCache/.test(heAsync);

const noDuplicateOk =
  /DealalityWaveLoader/.test(waveJs) &&
  !/function\s+inventSpinner|createSpinnerSystem/.test(sharedUx) &&
  /bl-wave-1/.test(shellCss) &&
  /detailLoadingHtml/.test(gdiUi) &&
  /toastHtml\(DETAIL_LOADING_MESSAGE\)|shared\.toastHtml/.test(gdiUi);

const noUnnecessaryOk =
  /if \(cached != null/.test(heAsync) &&
  shouldCache === false &&
  /softRefresh/.test(heAsync);

const productParity = {
  he: includesProduct("he")
    ? gate(
        parityChecks.heDelegates &&
          /dealality-shared-loading-ux-v1\.js/.test(read("public/hotel-explorer-preview.html")),
      )
    : "SKIPPED",
  gdi: includesProduct("gdi")
    ? gate(parityChecks.gdiUsesShared && parityChecks.gdiWiresSharedScript)
    : "SKIPPED",
  adp: includesProduct("adp") ? gate(parityChecks.adpWiresWaveCss) : "SKIPPED",
  be: includesProduct("be") ? gate(parityChecks.beWiresWave) : "SKIPPED",
};

const gates = {
  DEALALITY_SHARED_LOADER_PARITY: gate(
    Object.values(parityChecks).every(Boolean) &&
      Object.values(productParity).every((v) => v === "PASS" || v === "SKIPPED"),
  ),
  DEALALITY_WAVE_ANIMATION_VALIDATED: gate(waveAnimationOk),
  DEALALITY_BLACK_TOAST_STYLE_VALIDATED: gate(blackToastOk),
  DEALALITY_LOADING_LIFECYCLE_VALIDATED: gate(lifecycleOk),
  DEALALITY_NO_DUPLICATE_LOADER_IMPLEMENTATIONS: gate(noDuplicateOk),
  DEALALITY_NO_UNNECESSARY_LOADING: gate(noUnnecessaryOk),
  DEALALITY_SHARED_LOADER_BROWSER: browserEnabled ? "NOT_IMPLEMENTED" : "NOT_TESTED",
};

const ok = Object.entries(gates)
  .filter(([k]) => !k.endsWith("_BROWSER"))
  .every(([, v]) => v === "PASS");

const report = {
  contract: contract.contract,
  generatedAt: new Date().toISOString(),
  productFilter,
  browserEnabled,
  ok,
  gates,
  productParity,
  sample: {
    toastHasWave: /wave-container/.test(toast),
    sharedMessageResolved: /Loading Contacts…/.test(sharedToast),
    cacheSkipsLoader: shouldCache === false,
  },
  FULL_HE_E2E_VALIDATED: "NO",
};

const outDir = path.join(ROOT, "reports/dealality-design-system");
fs.mkdirSync(outDir, { recursive: true });
fs.writeFileSync(
  path.join(outDir, "DEALALITY-SHARED-LOADER-CONTRACT-V1.json"),
  JSON.stringify(report, null, 2),
);
console.log(JSON.stringify(report, null, 2));

assert.equal(report.FULL_HE_E2E_VALIDATED, "NO");
if (!ok) {
  const failed = Object.entries(gates)
    .filter(([, v]) => v === "FAIL")
    .map(([k, v]) => `${k}=${v}`);
  assert.fail(`shared loader contract failed: ${failed.join(", ")}`);
}
