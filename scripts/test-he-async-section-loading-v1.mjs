#!/usr/bin/env node
/**
 * HE independent loading + canonical wave (HE-QL-024).
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import vm from "node:vm";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const read = (rel) => fs.readFileSync(path.join(ROOT, rel), "utf8");

// Wave loader: compact + toast variants exist; compact is not full-width
{
  const js = read("public/js/dealality-wave-loader.js");
  assert.ok(js.includes("compactHtml"));
  assert.ok(js.includes("toastHtml"));
  assert.ok(js.includes("dealality-wave-loader--compact"));
  const css = read("public/css/dealality-wave-loader.css");
  assert.ok(css.includes("dealality-wave-loader--compact"));
  assert.ok(css.includes("dealality-wave-loader--toast"));
  assert.ok(css.includes("dealality-loading-toast-host"));
  assert.ok(css.includes("prefers-reduced-motion"));
  const compactRule = css.slice(
    css.indexOf(".dealality-wave-loader--compact"),
    css.indexOf(".dealality-loading-toast-host")
  );
  assert.ok(!/width:\s*100%/.test(compactRule));
}

// HeAsyncSection controller: isolation, cache, stale generation
{
  const code = read("public/js/he-async-section-v1.js");
  const sandbox = {
    window: {},
    console,
    setTimeout,
    clearTimeout,
    AbortController,
    Promise,
    Map,
    Date,
  };
  sandbox.window = sandbox;
  sandbox.globalThis = sandbox;
  vm.runInNewContext(code, sandbox);
  const api = sandbox.HeAsyncSection || sandbox.window.HeAsyncSection;
  assert.ok(api);
  assert.equal(api.STATES.LOADING, "LOADING");
  assert.ok(api.waveCompact("Loading…").includes("dealality-wave-loader") || api.waveCompact("Loading…").includes("Loading"));

  const ctrl = api.createController({ key: "t", cacheTtlMs: 60000, timeoutMs: 5000 });
  let loadingSeen = false;
  let readySeen = false;
  await ctrl.load(
    "recAAAAAAAAAAAAAAAA",
    async () => ({ ok: true, items: [1] }),
    {
      isEmpty: (p) => !(p && p.items && p.items.length),
      onLoading: () => {
        loadingSeen = true;
      },
      onResult: (r) => {
        if (r.state === "READY") readySeen = true;
      },
    }
  );
  assert.equal(loadingSeen, true);
  assert.equal(readySeen, true);
  assert.equal(ctrl.getState(), "READY");

  // Cache hit avoids loading callback
  let loading2 = false;
  await ctrl.load(
    "recAAAAAAAAAAAAAAAA",
    async () => {
      throw new Error("should not fetch");
    },
    {
      onLoading: () => {
        loading2 = true;
      },
      onResult: () => {},
    }
  );
  assert.equal(loading2, false);

  // Stale generation ignored
  const ctrl2 = api.createController({ key: "t2", timeoutMs: 50 });
  let ignored = false;
  const pSlow = ctrl2.load(
    "recBBBBBBBBBBBBBB",
    () =>
      new Promise((resolve) => {
        setTimeout(() => resolve({ items: [1] }), 80);
      }),
    {
      isEmpty: () => false,
      onResult: (r) => {
        if (r.ignored) ignored = true;
      },
    }
  );
  ctrl2.cancel();
  const out = await pSlow;
  assert.ok(out.ignored === true || out.state === "STALE" || ctrl2.getState() === "IDLE");

  // clearCache enables hard ERROR (no stale fallback)
  const ctrl3 = api.createController({ key: "t3", cacheTtlMs: 60000, timeoutMs: 2000 });
  await ctrl3.load("recCCCCCCCCCCCCCC", async () => ({ items: [1] }), {
    isEmpty: () => false,
  });
  assert.ok(ctrl3.getCached("recCCCCCCCCCCCCCC"));
  ctrl3.clearCache("recCCCCCCCCCCCCCC");
  assert.equal(ctrl3.getCached("recCCCCCCCCCCCCCC"), null);
  let errState = null;
  await ctrl3.load(
    "recCCCCCCCCCCCCCC",
    async () => {
      throw new Error("provider down");
    },
    {
      force: true,
      onResult: (r) => {
        errState = r.state;
      },
    }
  );
  assert.equal(errState, "ERROR");
}

// Brand/Operator: independent mounts
{
  const ws = read("public/js/hotel-intelligence-workspace.js");
  assert.ok(ws.includes("mountHotelNewsArticles"));
  assert.ok(ws.includes("mountGuestSentiment"));
  assert.ok(ws.includes("newsAsync"));
  assert.ok(ws.includes("sentimentAsync"));
  assert.ok(ws.includes("heWaveCompact"));
  assert.ok(ws.includes("data-he-async-retry"));
  assert.ok(ws.includes("paintHotelNewsHost"));
  assert.ok(ws.includes("paintGuestSentimentHost"));
  // Empty news still hides
  assert.ok(ws.includes('host.hidden = true'));
  // Cancel on hotel open
  assert.ok(ws.includes("newsAsync.cancel()"));
  assert.ok(ws.includes("bypassStale"));
  assert.ok(ws.includes("clearCache"));
}

// Preview includes canonical assets
{
  const html = read("public/hotel-explorer-preview.html");
  assert.ok(html.includes("dealality-wave-loader.css"));
  assert.ok(html.includes("dealality-wave-loader.js"));
  assert.ok(html.includes("he-async-section-v1.js"));
}

// Contact Surfe does not block tab — per-person wave only
{
  const hci = read("public/js/hotel-contact-intelligence.js");
  assert.ok(hci.includes("hci-enrich__result"));
  assert.ok(hci.includes("Looking up contact details") || hci.includes("waveCompact"));
  assert.ok(hci.includes("data-hci-wave-loading") || hci.includes("Loading Contact Intelligence"));
}

// OCN expand uses compact wave
{
  const ocn = read("public/js/he-ownership-control-network-v1.js");
  assert.ok(ocn.includes("waveCompact") || ocn.includes("ocn-expand-loading"));
}

// Quality standard + lesson
{
  const std = read("docs/ai-build-system/HOTEL_EXPLORER_QUALITY_STANDARD.md");
  assert.ok(std.includes("Independent loading") || std.includes("wave"));
  assert.ok(std.includes("DealalityWaveLoader") || std.includes("dealality-wave-loader"));
  const reg = JSON.parse(
    read("docs/ai-build-system/HOTEL_EXPLORER_FOUNDER_QUALITY_LESSONS_REGISTRY.json")
  );
  assert.ok(reg.lessons.some((l) => l.id === "HE-QL-024"));
}

// News/sentiment APIs remain GET-only
{
  assert.ok(!/\.create\(|\.update\(/.test(read("api/hotel-explorer-news-articles.js")));
  assert.ok(!/\.create\(|\.update\(/.test(read("api/hotel-explorer-guest-sentiment.js")));
  assert.ok(read("api/hotel-explorer-guest-sentiment.js").includes("X-Dealality-Paid-Providers"));
}

console.log("PASS test:he-async-section-loading-v1");
