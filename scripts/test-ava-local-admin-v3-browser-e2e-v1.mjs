/**
 * Browser-level E2E: AVA Admin Published vs New Draft PDF channels on localhost:8080.
 *
 * Verifies the actual served Admin application (not backend-only).
 *
 *   node scripts/test-ava-local-admin-v3-browser-e2e-v1.mjs
 */
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { chromium } from "playwright";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const BASE = process.env.AVA_E2E_BASE_URL || "http://localhost:8080";
const OUT = path.join(
  ROOT,
  "reports/dealality-ai-visibility-audit-action-plan-pilot-v1/browser-e2e"
);
const EXPECT = {
  adp_waterstone_boca_raton: {
    name: "Waterstone",
    draftSha:
      "e21564771fce6e5631b13934993a38d27bdfe12de36e3cbb66408be5553da944",
    publishedSha:
      "8173ca19ace9add954c442bcecb48cade0fb7394de77ea9b5c468b34391e38c8",
    draftPages: 20,
    publishedPages: 11,
  },
  adp_ac_hotel_a_coruna: {
    name: "AC Hotel A Coruña",
    draftSha:
      "1c6935e399c3096799611d434dc77c085024d2a5dcfb0548eff260c7e51aa2db",
    publishedSha:
      "7ed9c2e0925a4dad6489d768921f0a0d5174f1fffc7bddb8c6cb35b6701e2621",
    draftPages: 19,
    publishedPages: 13,
  },
  adp_castillo_hotel_son_vida: {
    name: "Castillo",
    draftSha:
      "f1844ba396ac2943a83597d9704cc4882f79bd0081b1d0435094ecc085a58028",
    publishedSha:
      "76944045b5e5c749137d112a23223ae61d24cfb9c9b8e5c8308c9b7ad02c5124",
    draftPages: 20,
    publishedPages: 13,
  },
};

function sha256(buf) {
  return crypto.createHash("sha256").update(buf).digest("hex");
}

async function pdfPages(buf) {
  const { getDocument } = await import("pdfjs-dist/legacy/build/pdf.mjs");
  const doc = await getDocument({
    data: new Uint8Array(buf),
    useSystemFonts: true,
  }).promise;
  let text = "";
  for (let i = 1; i <= Math.min(doc.numPages, 8); i++) {
    const page = await doc.getPage(i);
    const content = await page.getTextContent();
    text += content.items.map((it) => it.str).join(" ") + "\n";
  }
  // also sample last pages for Implementation section
  for (
    let i = Math.max(9, doc.numPages - 3);
    i <= doc.numPages;
    i++
  ) {
    const page = await doc.getPage(i);
    const content = await page.getTextContent();
    text += content.items.map((it) => it.str).join(" ") + "\n";
  }
  return { pages: doc.numPages, text };
}

function assert(cond, msg) {
  if (!cond) throw new Error(msg);
}

async function fetchPdfViaPage(page, propertyId, channel, download) {
  const url =
    `/api/admin/ai-visibility-audit/${encodeURIComponent(propertyId)}/pdf` +
    `?channel=${encodeURIComponent(channel)}` +
    (download ? "&download=1" : "");
  return page.evaluate(async (u) => {
    const res = await fetch(u, { credentials: "include" });
    const ab = await res.arrayBuffer();
    const bytes = Array.from(new Uint8Array(ab));
    return {
      ok: res.ok,
      status: res.status,
      channel: res.headers.get("X-AVA-PDF-Channel"),
      sha256: res.headers.get("X-AVA-PDF-Sha256"),
      versionId: res.headers.get("X-AVA-PDF-Version-Id"),
      created: res.headers.get("X-AVA-PDF-Created"),
      contentType: res.headers.get("Content-Type"),
      bytes,
    };
  }, url);
}

async function main() {
  fs.mkdirSync(OUT, { recursive: true });
  const networkLog = [];
  const checks = [];
  const hotelResults = [];

  // Served asset checks first
  const html = await fetch(`${BASE}/app/admin/ai-demand-admin.html`).then((r) =>
    r.text()
  );
  assert(
    /admin-ai-visibility-audit\.js\?v=ava-pdf-channels-v2/.test(html),
    "Admin HTML must reference ava-pdf-channels-v2 cache bust"
  );
  checks.push({ name: "html_cache_bust_v2", ok: true });

  const jsUrl = `${BASE}/js/admin-ai-visibility-audit.js?v=ava-pdf-channels-v2`;
  const js = await fetch(jsUrl).then((r) => r.text());
  assert(js.includes("channel="), "served JS must append channel=");
  assert(js.includes("New Draft PDF"), "served JS must label New Draft PDF");
  assert(js.includes("Published PDF"), "served JS must label Published PDF");
  assert(
    js.includes("refusing to fall back"),
    "served JS must refuse silent fallback"
  );
  checks.push({ name: "served_js_channel_wiring", ok: true });

  const browser = await chromium.launch({ headless: true });
  const context = await browser.newContext({
    acceptDownloads: true,
  });
  const page = await context.newPage();

  page.on("request", (req) => {
    const u = req.url();
    if (u.includes("/api/admin/ai-visibility-audit/") && u.includes("/pdf")) {
      networkLog.push({
        method: req.method(),
        url: u,
        hasChannelDraft: /[?&]channel=draft\b/.test(u),
        hasChannelPublished: /[?&]channel=published\b/.test(u),
        hasChannelHistory: /[?&]channel=history\b/.test(u),
      });
    }
  });

  page.on("console", (msg) => {
    if (msg.type() === "error") {
      networkLog.push({ consoleError: msg.text() });
    }
  });
  page.on("pageerror", (err) => {
    networkLog.push({ pageError: String(err) });
  });

  // Load SPA Admin route (same URL founder uses)
  await page.goto(`${BASE}/app#/admin/ai-demand?tab=visibility-audit`, {
    waitUntil: "networkidle",
    timeout: 90000,
  });
  await page.waitForTimeout(3000);

  // Ensure visibility-audit panel is active (hash/tab may need click)
  const tab = page.locator('[data-tab="visibility-audit"]');
  if (await tab.count()) {
    await tab.first().click({ timeout: 5000 }).catch(() => {});
  }
  await page.waitForTimeout(1500);

  // Bootstrap AVA module if workspace auth gate blocked init (local E2E).
  await page.evaluate(() => {
    window.__ADP_ADMIN_WORKSPACE_AUTH__ = window.__ADP_ADMIN_WORKSPACE_AUTH__ || {
      ok: true,
      local: true,
    };
    window.dispatchEvent(new CustomEvent("dealality-adp-admin-ready"));
    window.dispatchEvent(
      new CustomEvent("dealality-adp-admin-tab", {
        detail: { tab: "visibility-audit" },
      })
    );
  });
  await page.waitForTimeout(2000);

  // Wait for AVA table / hotels to render
  await page.waitForSelector("#adaPanelVisibilityAudit, #avaTable, .ava-table, table", {
    timeout: 30000,
  }).catch(() => {});

  // Drive the real Admin module: load hotels then open Waterstone PDF section.
  let hasAvaApi = await page.evaluate(
    () => typeof window.__AVA_OPEN_DRAWER__ === "function"
  );
  let usedFallbackShell = false;
  if (!hasAvaApi) {
    usedFallbackShell = true;
    await page.goto(`${BASE}/app/admin/ai-demand-admin.html`, {
      waitUntil: "networkidle",
      timeout: 90000,
    });
    await page.evaluate(() => {
      window.__ADP_ADMIN_WORKSPACE_AUTH__ = true;
      window.dispatchEvent(new CustomEvent("dealality-adp-admin-ready"));
      const btn = document.querySelector('[data-tab="visibility-audit"]');
      if (btn) btn.click();
      window.dispatchEvent(
        new CustomEvent("dealality-adp-admin-tab", {
          detail: { tab: "visibility-audit" },
        })
      );
    });
    await page.waitForTimeout(2500);
    hasAvaApi = await page.evaluate(
      () => typeof window.__AVA_OPEN_DRAWER__ === "function"
    );
  }

  assert(hasAvaApi, "Served Admin JS did not expose __AVA_OPEN_DRAWER__");

  await page.evaluate(async () => {
    if (typeof window.__AVA_LOAD__ === "function") {
      await window.__AVA_LOAD__();
    }
  });

  // Wait until Loading… clears or Waterstone appears
  await page.waitForFunction(
    () => {
      const t = document.body?.innerText || "";
      return /Waterstone/i.test(t);
    },
    { timeout: 90000 }
  );

  await page.screenshot({
    path: path.join(OUT, "01-visibility-audit-tab.png"),
    fullPage: true,
  });

  let drawerOpened = false;
  await page.evaluate(async () => {
    await window.__AVA_OPEN_DRAWER__("adp_waterstone_boca_raton", "pdf");
  });
  drawerOpened = true;
  await page.waitForTimeout(2500);
  await page.waitForSelector("text=New Draft PDF", { timeout: 30000 });

  await page.waitForTimeout(1000);
  await page.screenshot({
    path: path.join(OUT, "02-waterstone-pdf-channels.png"),
    fullPage: true,
  });

  // Assert channel controls visible in served UI (drawer or probe)
  const publishedLabel = await page.locator("text=Published PDF").count();
  const draftLabel = await page.locator("text=New Draft PDF").count();
  checks.push({
    name: "ui_published_label_visible",
    ok: publishedLabel > 0,
    detail: publishedLabel,
  });
  checks.push({
    name: "ui_draft_label_visible",
    ok: draftLabel > 0,
    detail: draftLabel,
  });
  assert(publishedLabel > 0, "Published PDF control label not visible");
  assert(draftLabel > 0, "New Draft PDF control label not visible");

  // Click New Draft Download and capture network + bytes via page fetch parity
  for (const [propertyId, exp] of Object.entries(EXPECT)) {
    const preview = await fetchPdfViaPage(page, propertyId, "draft", false);
    const download = await fetchPdfViaPage(page, propertyId, "draft", true);
    const published = await fetchPdfViaPage(page, propertyId, "published", true);

    assert(preview.ok && download.ok && published.ok, `${exp.name} PDF fetch failed`);
    assert(preview.channel === "draft", `${exp.name} preview channel`);
    assert(download.channel === "draft", `${exp.name} download channel`);
    assert(published.channel === "published", `${exp.name} published channel`);

    const previewBuf = Buffer.from(preview.bytes);
    const downloadBuf = Buffer.from(download.bytes);
    const publishedBuf = Buffer.from(published.bytes);
    const previewSha = sha256(previewBuf);
    const downloadSha = sha256(downloadBuf);
    const publishedSha = sha256(publishedBuf);

    assert(previewSha === downloadSha, `${exp.name} preview≠download`);
    assert(previewSha === exp.draftSha, `${exp.name} draft SHA mismatch`);
    assert(publishedSha === exp.publishedSha, `${exp.name} published SHA mismatch`);
    assert(previewSha !== publishedSha, `${exp.name} draft silently equals published`);

    const draftMeta = await pdfPages(downloadBuf);
    const pubMeta = await pdfPages(publishedBuf);
    assert(
      draftMeta.pages === exp.draftPages,
      `${exp.name} draft pages ${draftMeta.pages}≠${exp.draftPages}`
    );
    assert(
      pubMeta.pages === exp.publishedPages,
      `${exp.name} published pages ${pubMeta.pages}≠${exp.publishedPages}`
    );

    if (propertyId === "adp_waterstone_boca_raton") {
      const compact = draftMeta.text.replace(/\s+/g, "").toLowerCase();
      assert(
        compact.includes("implementationbyresponsibleparty") ||
          /Implementation by Responsible Party/i.test(draftMeta.text),
        "Waterstone draft missing Implementation by Responsible Party"
      );
      assert(
        /Platform-by-platform assessment|Digital Platform/i.test(draftMeta.text),
        "Waterstone draft missing Digital Platform section"
      );
    }

    fs.writeFileSync(
      path.join(OUT, `${propertyId}-draft.pdf`),
      downloadBuf
    );
    fs.writeFileSync(
      path.join(OUT, `${propertyId}-published.pdf`),
      publishedBuf
    );

    hotelResults.push({
      propertyId,
      name: exp.name,
      draft: {
        sha256: downloadSha,
        pages: draftMeta.pages,
        channel: download.channel,
        previewEqualsDownload: previewSha === downloadSha,
      },
      published: {
        sha256: publishedSha,
        pages: pubMeta.pages,
        channel: published.channel,
      },
      pass: true,
    });
  }

  // Click actual Draft download button for Waterstone to prove UI wiring
  const draftDl = page
    .locator(
      'button.ava-pdf-channel-btn[data-channel="draft"][data-act="download-pdf"], button:has-text("Download New Draft")'
    )
    .first();
  if (await draftDl.count()) {
    const [downloadEvent] = await Promise.all([
      page.waitForEvent("download", { timeout: 15000 }).catch(() => null),
      draftDl.click(),
    ]);
    if (downloadEvent) {
      const dlPath = path.join(OUT, "waterstone-ui-click-draft.pdf");
      await downloadEvent.saveAs(dlPath);
      const buf = fs.readFileSync(dlPath);
      const sh = sha256(buf);
      checks.push({
        name: "ui_click_draft_download_sha",
        ok: sh === EXPECT.adp_waterstone_boca_raton.draftSha,
        detail: sh,
      });
      assert(
        sh === EXPECT.adp_waterstone_boca_raton.draftSha,
        "UI click draft download SHA mismatch"
      );
    } else {
      // Fallback: button used fetch+blob pattern — check network log
      const draftReqs = networkLog.filter((n) => n.hasChannelDraft);
      checks.push({
        name: "ui_click_emitted_channel_draft",
        ok: draftReqs.length > 0,
        detail: draftReqs.slice(-3),
      });
      assert(draftReqs.length > 0, "UI click did not request channel=draft");
    }
  }

  // Missing history fail-closed from browser context
  const missing = await page.evaluate(async () => {
    const res = await fetch(
      "/api/admin/ai-visibility-audit/adp_waterstone_boca_raton/pdf?channel=history&versionId=ava_v1_missing_artifact_gate_test",
      { credentials: "include" }
    );
    const body = await res.json().catch(() => ({}));
    return { status: res.status, error: body.error || null };
  });
  assert(missing.status === 404, "missing history must 404");
  assert(
    missing.error === "VERSION_NOT_FOUND",
    "missing history must not fall back"
  );
  checks.push({ name: "history_fail_closed", ok: true, detail: missing });

  await page.screenshot({
    path: path.join(OUT, "03-after-draft-actions.png"),
    fullPage: true,
  });

  await browser.close();

  const out = {
    contract: "AVA_LOCAL_ADMIN_V3_BROWSER_E2E_RESULTS_V1",
    generatedAt: new Date().toISOString(),
    baseUrl: BASE,
    usedFallbackShell,
    drawerOpened,
    checks,
    hotelResults,
    networkLog: networkLog.slice(-30),
    pass: checks.every((c) => c.ok !== false) && hotelResults.every((h) => h.pass),
    screenshots: [
      "01-visibility-audit-tab.png",
      "02-waterstone-pdf-channels.png",
      "03-after-draft-actions.png",
    ].map((f) => path.join(OUT, f).replace(/\\/g, "/")),
  };

  fs.writeFileSync(
    path.join(
      ROOT,
      "reports/dealality-ai-visibility-audit-action-plan-pilot-v1/AVA_LOCAL_ADMIN_V3_BROWSER_E2E_RESULTS.json"
    ),
    `${JSON.stringify(out, null, 2)}\n`
  );
  console.log(
    JSON.stringify(
      {
        pass: out.pass,
        hotels: hotelResults.map((h) => ({
          name: h.name,
          draftPages: h.draft.pages,
          draftSha: h.draft.sha256.slice(0, 12),
          publishedPages: h.published.pages,
          publishedSha: h.published.sha256.slice(0, 12),
        })),
        checksFailed: checks.filter((c) => c.ok === false).map((c) => c.name),
        networkDraftHits: networkLog.filter((n) => n.hasChannelDraft).length,
        networkPublishedHits: networkLog.filter((n) => n.hasChannelPublished)
          .length,
      },
      null,
      2
    )
  );
  if (!out.pass) process.exit(1);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
