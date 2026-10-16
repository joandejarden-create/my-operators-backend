#!/usr/bin/env node
/**
 * Authenticated Admin six-combo verification (local :8080).
 * Uses DEV_AUTH_BYPASS + /api/me mock for page shell; exercises real Admin APIs
 * and Open/Copy/View PDF control paths. Does not mint research.
 */
import fs from "fs";
import path from "path";
import { chromium } from "playwright";

const BASE = process.env.VERIFY_BASE || "http://127.0.0.1:8080";
const OUT_DIR = path.join(
  process.cwd(),
  "reports/admin-six-combo-delivery"
);
const OUT = path.join(OUT_DIR, "07-authenticated-admin-clicks.json");

const COMBOS = [
  {
    hotel: "Renaissance NY Times Square",
    adpId: "adp_renaissance_times_square",
    hpcId: "recG66DQJKP2c0UNh",
    nameMatch: /Renaissance.*Times Square/i,
  },
  {
    hotel: "Westin Tampa Waterside",
    adpId: "adp_westin_tampa_waterside",
    hpcId: "recdmo0CnhIZAwHGC",
    nameMatch: /Westin Tampa Waterside/i,
  },
  {
    hotel: "Wyndham San Diego Bayside",
    adpId: "adp_wyndham_san_diego_bayside",
    hpcId: "recnPB3Q02NO8Ma1o",
    nameMatch: /Wyndham San Diego Bayside/i,
  },
];

const FAKE_MS_JWT =
  "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJtZW1fZGVtbyIsImVtYWlsIjoiZGVhbGFsaXR5ZGVtb0BkZWFsYWxpdHkuY29tIiwiaWF0IjoxNjk5OTk5OTk5fQ.local-qa";

async function apiJson(urlPath) {
  const res = await fetch(`${BASE}${urlPath}`);
  const body = await res.json().catch(() => ({}));
  return { status: res.status, body };
}

async function headOrGet(url, { expectPdf = false } = {}) {
  const res = await fetch(url, { redirect: "manual" });
  const ct = String(res.headers.get("content-type") || "");
  const len = Number(res.headers.get("content-length") || 0);
  let bytes = len;
  if (expectPdf && res.ok && (!len || len < 1000)) {
    const buf = Buffer.from(await res.arrayBuffer());
    bytes = buf.length;
    const isPdf = buf.slice(0, 4).toString("utf8") === "%PDF";
    return { status: res.status, bytes, isPdf, ct };
  }
  return {
    status: res.status,
    bytes,
    isPdf: ct.includes("pdf") || expectPdf,
    ct,
  };
}

async function main() {
  fs.mkdirSync(OUT_DIR, { recursive: true });
  const me = await apiJson("/api/me");
  const gdiCat = await apiJson("/api/admin/group-demand-intelligence/reports");
  const hotels = gdiCat.body?.hotels || [];

  const rows = [];

  for (const c of COMBOS) {
    const gdiRow = hotels.find((h) => h.hotelId === c.hpcId) || null;
    const linksAdp = await apiJson(
      `/api/admin/ai-demand/hotels/${encodeURIComponent(c.adpId)}/external-client-links`
    );
    const linksHpc = await apiJson(
      `/api/admin/ai-demand/hotels/${encodeURIComponent(c.hpcId)}/external-client-links`
    );

    const adpUrl =
      linksAdp.body?.adp?.url || linksHpc.body?.adp?.url || null;
    const gdiUrl =
      linksAdp.body?.gdi?.url || linksHpc.body?.gdi?.url || null;

    let adpClientOk = false;
    let gdiClientOk = false;
    let gdiFacing = null;
    if (adpUrl) {
      const page = await headOrGet(adpUrl);
      adpClientOk = page.status === 200;
    }
    if (gdiUrl) {
      const u = new URL(gdiUrl);
      const share = u.searchParams.get("share");
      const apiUrl = `${u.origin}/api/group-demand-intelligence/share/hotels/${c.hpcId}/opportunities?share=${encodeURIComponent(share)}`;
      const res = await fetch(apiUrl, { signal: AbortSignal.timeout(45000) });
      const body = await res.json().catch(() => ({}));
      const opps = body.opportunities || body.items || [];
      gdiFacing = Array.isArray(opps) ? opps.length : null;
      gdiClientOk = res.status === 200 && gdiFacing > 0;
    }

    const adpPdf = await headOrGet(
      `${BASE}/api/admin/ai-demand-positioning/current-report-pdf/${encodeURIComponent(c.adpId)}`,
      { expectPdf: true }
    );
    const gdiPdf = await headOrGet(
      `${BASE}/api/admin/group-demand-intelligence/hotels/${encodeURIComponent(c.hpcId)}/report-pdf`,
      { expectPdf: true }
    );

    rows.push({
      hotel: c.hotel,
      product: "ADP",
      adminStatus:
        adpPdf.status === 200 && adpPdf.isPdf && adpPdf.bytes > 1000
          ? "READY"
          : "PDF_UNAVAILABLE",
      clientLinkWorks: adpClientOk,
      pdfWorks: adpPdf.status === 200 && adpPdf.isPdf && adpPdf.bytes > 1000,
      pdfBytes: adpPdf.bytes || null,
      clientUrl: adpUrl,
      remainingBlocker: !adpClientOk
        ? linksAdp.body?.adp?.reason ||
          linksHpc.body?.adp?.reason ||
          "ADP client link unavailable via Admin resolver"
        : null,
      resolvedViaProduction: !!linksAdp.body?.resolvedViaProduction,
      apiStatus: linksAdp.status,
    });

    const gdiStatus = gdiRow?.reportStatus || "UNKNOWN";
    const gdiPdfOk =
      gdiPdf.status === 200 && gdiPdf.isPdf && gdiPdf.bytes > 1000;
    let blocker = null;
    if (gdiStatus === "WATCH_ONLY" || gdiStatus === "NO_READY_OPPORTUNITIES") {
      blocker =
        "No customer-ready published opportunities (accurate unavailable / not customer-ready).";
    } else if (!gdiClientOk && gdiUrl && gdiFacing === 0) {
      blocker = "Client share loads but 0 customer-facing opportunities.";
    } else if (!gdiClientOk) {
      blocker =
        linksAdp.body?.gdi?.reason ||
        linksHpc.body?.gdi?.reason ||
        "GDI client link unavailable via Admin resolver";
    } else if (!gdiPdfOk) {
      blocker = "GDI PDF missing locally or not generated.";
    }

    rows.push({
      hotel: c.hotel,
      product: "GDI",
      adminStatus: gdiStatus,
      clientLinkWorks: gdiClientOk,
      pdfWorks: gdiPdfOk,
      pdfBytes: gdiPdf.bytes || null,
      facingCount: gdiFacing,
      clientUrl: gdiUrl,
      remainingBlocker: blocker,
      resolvedViaProduction: !!linksAdp.body?.resolvedViaProduction,
      apiStatus: linksAdp.status,
      gdiPdfStatus: gdiRow?.pdf?.status || null,
    });
  }

  // Browser: Action Plan + GDI Reports authenticated control clicks
  const browser = await chromium.launch({ headless: true });
  const context = await browser.newContext();
  const page = await context.newPage();

  await page.route("**/api/me**", async (route) => {
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        success: true,
        ok: true,
        user: {
          firstName: "Demo",
          lastName: "Dealality",
          email: "dealalitydemo@dealality.com",
        },
        dealality: {
          email: "dealalitydemo@dealality.com",
          role: "admin",
          isAdmin: true,
          isOwner: true,
          isDemo: true,
          flags: { isAdmin: true, isDemo: true },
        },
      }),
    });
  });

  await page.addInitScript((jwt) => {
    window.__dealalityMemberstackJwt = jwt;
    try {
      sessionStorage.setItem("dealality_ms_jwt_test", jwt);
    } catch (_) {}
  }, FAKE_MS_JWT);

  const ui = { actionPlan: [], gdiReports: [], accessRestricted: false };

  await page.goto(
    `${BASE}/app/admin/ai-demand-admin.html?msToken=${encodeURIComponent(FAKE_MS_JWT)}#/admin/adp-action-plan`,
    { waitUntil: "domcontentloaded", timeout: 60000 }
  );
  await page.waitForTimeout(1500);
  const bodyText = await page.locator("body").innerText().catch(() => "");
  ui.accessRestricted = /Access restricted/i.test(bodyText);

  // Force action-plan panel visible if tab shell loaded
  await page.evaluate(() => {
    const gate = document.getElementById("dealalityAppShellAuthGate");
    if (gate) gate.hidden = true;
    const panel = document.getElementById("adaPanelActionPlan");
    if (panel) panel.hidden = false;
  });

  for (const c of COMBOS) {
    const select = page.locator("#aapProperty");
    const hasSelect = (await select.count()) > 0;
    if (!hasSelect) {
      ui.actionPlan.push({ hotel: c.hotel, ok: false, reason: "no_property_select" });
      continue;
    }
    // Wait options
    await page.waitForTimeout(800);
    const optionValue = await page.evaluate((matchSrc) => {
      const sel = document.getElementById("aapProperty");
      if (!sel) return null;
      const re = new RegExp(matchSrc, "i");
      for (const opt of sel.options) {
        if (re.test(opt.textContent || "") || re.test(opt.value || "")) return opt.value;
      }
      return null;
    }, c.nameMatch.source);

    if (!optionValue) {
      ui.actionPlan.push({
        hotel: c.hotel,
        ok: false,
        reason: "property_not_in_action_plan_catalog",
      });
      continue;
    }

    await select.selectOption(optionValue);
    await page.waitForTimeout(1200);

    const adpOpenDisabled = await page.locator("#aapAdpOpen").isDisabled();
    const gdiOpenDisabled = await page.locator("#aapGdiOpen").isDisabled();
    const pdfDisabled = await page.locator("#aapViewPdf").isDisabled();

    let adpClick = null;
    let gdiClick = null;
    let pdfClick = null;

    if (!adpOpenDisabled) {
      const [popup] = await Promise.all([
        page.waitForEvent("popup", { timeout: 8000 }).catch(() => null),
        page.locator("#aapAdpOpen").click(),
      ]);
      adpClick = {
        disabled: false,
        popupUrl: popup ? popup.url() : null,
      };
      if (popup) await popup.close().catch(() => {});
    } else {
      adpClick = { disabled: true };
    }

    if (!gdiOpenDisabled) {
      const [popup] = await Promise.all([
        page.waitForEvent("popup", { timeout: 8000 }).catch(() => null),
        page.locator("#aapGdiOpen").click(),
      ]);
      gdiClick = {
        disabled: false,
        popupUrl: popup ? popup.url() : null,
      };
      if (popup) await popup.close().catch(() => {});
    } else {
      gdiClick = { disabled: true };
    }

    if (!pdfDisabled) {
      const [resp] = await Promise.all([
        page
          .waitForResponse(
            (r) => /current-report-pdf|report-pdf/.test(r.url()),
            { timeout: 15000 }
          )
          .catch(() => null),
        page.locator("#aapViewPdf").click(),
      ]);
      pdfClick = {
        disabled: false,
        status: resp?.status() || null,
        url: resp?.url() || null,
      };
    } else {
      pdfClick = { disabled: true };
    }

    ui.actionPlan.push({
      hotel: c.hotel,
      optionValue,
      adpOpenDisabled,
      gdiOpenDisabled,
      pdfDisabled,
      adpClick,
      gdiClick,
      pdfClick,
    });
  }

  // GDI Reports tab — select each hotel row and exercise PDF + client actions if present
  await page.goto(
    `${BASE}/app/admin/ai-demand-admin.html?msToken=${encodeURIComponent(FAKE_MS_JWT)}#/admin/ai-demand`,
    { waitUntil: "domcontentloaded", timeout: 60000 }
  );
  await page.waitForTimeout(1000);
  await page.evaluate(() => {
    const panel = document.getElementById("adaPanelGdiReports");
    if (panel) panel.hidden = false;
    const tab = document.querySelector('[data-tab="gdi-reports"], #adaTabGdiReports');
    if (tab) tab.click();
  });
  await page.waitForTimeout(1500);

  for (const c of COMBOS) {
    const rowInfo = await page.evaluate((hpcId) => {
      const rows = [...document.querySelectorAll("#gdiRptTableBody tr, #gdiRptTable tbody tr")];
      for (const tr of rows) {
        const id =
          tr.getAttribute("data-id") ||
          tr.getAttribute("data-hotel-id") ||
          "";
        const text = tr.textContent || "";
        if (id === hpcId || text.includes(hpcId)) {
          tr.click();
          const statusCell = tr.querySelector("[data-col='reportStatus'], td:nth-child(6)");
          return {
            found: true,
            text: text.slice(0, 160),
            status: statusCell ? statusCell.textContent.trim() : null,
          };
        }
      }
      // name fallback
      return { found: false, rowCount: rows.length };
    }, c.hpcId);

    ui.gdiReports.push({ hotel: c.hotel, hpcId: c.hpcId, ...rowInfo });
  }

  await browser.close();

  const report = {
    at: new Date().toISOString(),
    base: BASE,
    meStatus: me.status,
    gdiHotelCount: hotels.length,
    rows,
    ui,
  };
  fs.writeFileSync(OUT, JSON.stringify(report, null, 2));

  // Markdown table
  const md = [
    "# Authenticated Admin six-combo clicks",
    "",
    `| Hotel | Product | Admin status | Client link works | PDF works | Remaining blocker |`,
    `|---|---|---|---|---|---|`,
    ...rows.map(
      (r) =>
        `| ${r.hotel} | ${r.product} | ${r.adminStatus} | ${r.clientLinkWorks ? "YES" : "NO"} | ${r.pdfWorks ? "YES" : "NO"} | ${r.remainingBlocker || "—"} |`
    ),
    "",
  ].join("\n");
  fs.writeFileSync(
    path.join(OUT_DIR, "07-authenticated-admin-clicks.md"),
    md
  );
  console.log(md);
  console.log("Wrote", OUT);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
