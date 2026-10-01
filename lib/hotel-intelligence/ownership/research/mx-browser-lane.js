/**
 * Mexico browser lane — RNT consulta via Playwright (P1.5A).
 * Apify-first audit: no existing Apify actor for SIGER/RNT; use local Playwright
 * following harvest-guatemala-inguat-registry.mjs pattern.
 *
 * Gated: OWNERSHIP_MX_BROWSER_ENABLE=1
 * Does not bypass captcha/login; marks access issues explicitly.
 */

import { chromium } from "playwright";
import {
  MX_RNT_PORTAL_OFFICIAL,
} from "../../../gtm-owner-target/adapters/mx-rnt-portal-config.js";
import { MX_SIGER_URL, MX_SAT_RFC_VALIDATOR_URL } from "../../../gtm-owner-target/adapters/mx-siger-registry.js";

export const MX_BROWSER_LANE_VERSION = "ownership-mx-browser-v1";

/**
 * Probe RNT portal accessibility and capture structure metadata.
 * @param {{ headed?: boolean, hotelName?: string, timeoutMs?: number }} [opts]
 */
export async function probeMexicoRntBrowserAccess(opts = {}) {
  const started = Date.now();
  const result = {
    ok: false,
    access_path: "playwright_chromium",
    portal_url: MX_RNT_PORTAL_OFFICIAL,
    captcha_detected: false,
    login_required: false,
    js_required: true,
    search_available: false,
    response_structure: null,
    evidence_fields: [],
    legal_semantics: "RNT registration identifies lodging operator/company — NOT PropCo owner",
    automation_reliability: "unknown",
    time_ms: 0,
    cost_usd: 0,
    notes: [],
    sample_html_length: 0,
  };

  const enabled =
    String(process.env.OWNERSHIP_MX_BROWSER_ENABLE || "0").trim() === "1";
  if (!enabled) {
    result.notes.push("browser_lane_disabled:set OWNERSHIP_MX_BROWSER_ENABLE=1");
    result.automation_reliability = "not_attempted";
    result.time_ms = Date.now() - started;
    return result;
  }

  let browser;
  try {
    browser = await chromium.launch({
      headless: !opts.headed,
    });
    const context = await browser.newContext({
      userAgent:
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
      locale: "es-MX",
    });
    const page = await context.newPage();
    page.setDefaultTimeout(opts.timeoutMs || 90000);

    await page.goto(MX_RNT_PORTAL_OFFICIAL, {
      waitUntil: "domcontentloaded",
      timeout: opts.timeoutMs || 90000,
    });

    const bodyText = await page.evaluate(() => document.body?.innerText || "");
    result.sample_html_length = bodyText.length;

    const lower = bodyText.toLowerCase();
    result.captcha_detected =
      /captcha|recaptcha|hcaptcha|verificaci[oó]n de seguridad/i.test(bodyText) ||
      /cloudflare/i.test(bodyText);
    result.login_required =
      /iniciar sesi[oó]n|login|acceso restringido|credenciales/i.test(lower);

    const hasSearch =
      (await page.locator('input[type="search"], input[name*="buscar"], input[id*="buscar"], input[placeholder*="buscar" i]').count()) >
      0;
    result.search_available = hasSearch;

    result.evidence_fields = inferRntFields(bodyText);
    result.response_structure = {
      has_form: hasSearch,
      page_title: await page.title(),
      url_after_load: page.url(),
    };

    if (opts.hotelName && hasSearch && !result.captcha_detected && !result.login_required) {
      const searchResult = await tryRntSearch(page, opts.hotelName);
      result.search_attempt = searchResult;
      result.ok = searchResult.rows_found > 0;
      if (searchResult.rows_found > 0) {
        result.automation_reliability = "partial — search returned rows";
      } else if (searchResult.error) {
        result.automation_reliability = "low — search failed";
        result.notes.push(searchResult.error);
      } else {
        result.automation_reliability = "medium — portal loads, no match";
        result.ok = true;
      }
    } else if (result.captcha_detected || result.login_required) {
      result.automation_reliability = "blocked — captcha or login";
      result.notes.push(
        result.captcha_detected ? "captcha_or_bot_protection" : "login_required"
      );
    } else if (bodyText.length > 500) {
      result.ok = true;
      result.automation_reliability = "medium — page loads without search test";
    }

    await browser.close();
  } catch (err) {
    result.notes.push(String(err.message || err).slice(0, 200));
    result.automation_reliability = "failed — navigation error";
    if (browser) await browser.close().catch(() => {});
  }

  result.time_ms = Date.now() - started;
  return result;
}

async function tryRntSearch(page, hotelName) {
  const out = { rows_found: 0, fields: [], error: null };
  try {
    const input = page.locator(
      'input[type="search"], input[name*="buscar"], input[id*="buscar"], input[placeholder*="buscar" i]'
    ).first();
    await input.fill(String(hotelName).slice(0, 80));
    await input.press("Enter");
    await page.waitForTimeout(4000);

    const rows = await page.evaluate(() => {
      const tables = [...document.querySelectorAll("table tr")];
      if (tables.length > 1) {
        return tables.slice(1, 6).map((tr) =>
          [...tr.querySelectorAll("td")].map((td) => td.innerText?.trim() || "")
        );
      }
      return [];
    });
    out.rows_found = rows.length;
    out.fields = rows[0] || [];
  } catch (err) {
    out.error = String(err.message || err).slice(0, 120);
  }
  return out;
}

function inferRntFields(text) {
  const fields = [];
  const cues = [
    ["RFC", /RFC/i],
    ["Razón social", /raz[oó]n social/i],
    ["Nombre comercial", /nombre comercial/i],
    ["RNT", /RNT|Registro Nacional de Turismo/i],
    ["Estado", /estado|entidad/i],
    ["Municipio", /municipio/i],
  ];
  for (const [label, re] of cues) {
    if (re.test(text)) fields.push(label);
  }
  return fields;
}

/**
 * Lookup Mexico RNT via browser when enabled.
 * Returns evidence candidates compatible with country adapter contract.
 */
export async function lookupMexicoRntViaBrowser(hotel, env = process.env) {
  const enabled = String(env.OWNERSHIP_MX_BROWSER_ENABLE || "0").trim() === "1";
  if (!enabled) {
    return { ok: false, candidates: [], probe: null, notes: ["mx_browser_disabled"] };
  }

  const name = String(hotel.hotel_name || hotel.name || "").trim();
  const probe = await probeMexicoRntBrowserAccess({ hotelName: name });
  const notes = [...(probe.notes || [])];

  if (probe.captcha_detected || probe.login_required) {
    notes.push("mx_rnt_not_automatable_responsibly");
    return { ok: false, candidates: [], probe, notes };
  }

  const search = probe.search_attempt;
  if (!search?.rows_found) {
    return { ok: probe.ok, candidates: [], probe, notes };
  }

  // Map first row heuristically — company name field varies by portal version
  const row = search.fields || [];
  const companyName = row.find((c) => c && c.length > 5 && !/^\d+$/.test(c)) || null;
  if (!companyName) {
    return { ok: true, candidates: [], probe, notes: [...notes, "mx_rnt_row_unparsed"] };
  }

  return {
    ok: true,
    candidates: [
      {
        entity_candidate: {
          legal_name: companyName,
          display_name: companyName,
          jurisdiction: "MX",
          identifiers: [],
        },
        supported_relationship: null,
        source_provider: "mx_rnt_browser",
        source_url: probe.portal_url,
        source_authority: 0.75,
        source_semantics:
          "RNT registered lodging company — identity resolution only, NOT PropCo ownership",
        max_verification: "needs_review",
        extracted_claim: `RNT browser match: ${companyName}`,
      },
    ],
    probe,
    notes,
  };
}

/**
 * Fetch-only accessibility probe (no Playwright). Used to compare vs browser.
 * @param {string} url
 * @param {{ timeoutMs?: number }} [opts]
 */
export async function probeMexicoUrlFetch(url, opts = {}) {
  const started = Date.now();
  const timeoutMs = Number(opts.timeoutMs || 20000);
  const out = {
    url,
    ok: false,
    http_status: null,
    captcha_detected: false,
    login_required: false,
    bytes: 0,
    latency_ms: 0,
    error: null,
  };
  try {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    const res = await fetch(url, {
      signal: controller.signal,
      redirect: "follow",
      headers: {
        Accept: "text/html",
        "User-Agent":
          "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
      },
    });
    clearTimeout(timer);
    const text = await res.text();
    out.http_status = res.status;
    out.bytes = text.length;
    out.ok = res.ok;
    const lower = text.toLowerCase();
    out.captcha_detected = /captcha|recaptcha|hcaptcha|cloudflare/i.test(text);
    out.login_required = /iniciar sesi[oó]n|login|acceso restringido|credenciales/i.test(lower);
  } catch (err) {
    out.error = String(err?.message || err).slice(0, 160);
  }
  out.latency_ms = Date.now() - started;
  return out;
}

/**
 * Combined Mexico public-system probe: fetch + optional Playwright.
 * Does not add Browser Use. Does not solve CAPTCHA.
 */
export async function probeMexicoPublicSystems(opts = {}) {
  const started = Date.now();
  const targets = [
    { id: "rnt_consulta", url: MX_RNT_PORTAL_OFFICIAL },
    { id: "siger", url: MX_SIGER_URL },
    { id: "sat_rfc_validator", url: MX_SAT_RFC_VALIDATOR_URL },
  ];
  const fetch_probes = [];
  for (const t of targets) {
    fetch_probes.push({ id: t.id, ...(await probeMexicoUrlFetch(t.url, opts)) });
  }

  let rnt_browser = null;
  const browserEnabled =
    opts.forceBrowser ||
    String(process.env.OWNERSHIP_MX_BROWSER_ENABLE || "0").trim() === "1";
  if (browserEnabled) {
    rnt_browser = await probeMexicoRntBrowserAccess({
      headed: opts.headed,
      hotelName: opts.hotelName || "Hotel Xcaret",
      timeoutMs: opts.timeoutMs,
    });
  }

  const fetchOk = fetch_probes.filter((p) => p.ok).length;
  return {
    version: MX_BROWSER_LANE_VERSION,
    cost_usd: 0,
    apify: {
      attempted: false,
      reason: "no_in_repo_siger_rnt_actor; MCP namespace needsAuth this session",
    },
    fetch_probes,
    fetch_success_pct: Math.round((fetchOk / targets.length) * 100),
    rnt_browser,
    browser_use_added: false,
    recommendation:
      rnt_browser?.captcha_detected || rnt_browser?.login_required || fetch_probes.every((p) => !p.ok)
        ? "existing_tooling_insufficient_for_registry_do_not_add_browser_use_prioritize_corporate_web"
        : rnt_browser?.ok
          ? "playwright_partial_rnt_only"
          : "fetch_limited_corporate_web_first",
    latency_ms: Date.now() - started,
  };
}
