/**
 * Official page fetch with static → structured → rendered fallback.
 * Render only when official source + thin body + dynamic content likely.
 */

import {
  fetchResearchPage,
  htmlToSearchableText,
} from "../hotel-intelligence/room-count-research/fetch.js";
import { scoreOfficialSource } from "./official-source-ranking.js";

export const OFFICIAL_PAGE_FETCH_VERSION = "gdi_official_page_fetch_v1";

export const FETCH_METHOD = Object.freeze({
  STATIC_HTML: "STATIC_HTML",
  STRUCTURED_JSONLD: "STRUCTURED_JSONLD",
  RENDERED_PLAYWRIGHT: "RENDERED_PLAYWRIGHT",
  FAILED: "FAILED",
});

export const BODY_QUALITY = Object.freeze({
  STATIC_HTML_OK: "STATIC_HTML_OK",
  STATIC_HTML_EMPTY: "STATIC_HTML_EMPTY",
  JS_RENDER_REQUIRED: "JS_RENDER_REQUIRED",
  STRUCTURED_ENDPOINT_AVAILABLE: "STRUCTURED_ENDPOINT_AVAILABLE",
  OTHER: "OTHER",
});

const THIN_TEXT_CHARS = 800;
const RENDER_BUDGET_DEFAULT = 15;

function extractJsonLd(html) {
  const out = [];
  const re =
    /<script[^>]*type=["']application\/ld\+json["'][^>]*>([\s\S]*?)<\/script>/gi;
  let m;
  while ((m = re.exec(String(html || "")))) {
    try {
      const parsed = JSON.parse(m[1]);
      out.push(parsed);
    } catch {
      /* ignore */
    }
  }
  return out;
}

function flattenJsonLdEvents(nodes, acc = []) {
  const list = Array.isArray(nodes) ? nodes : [nodes];
  for (const n of list) {
    if (!n || typeof n !== "object") continue;
    const type = String(n["@type"] || "");
    if (/Event|BusinessEvent|EducationEvent|SportsEvent/i.test(type)) {
      acc.push(n);
    }
    if (n["@graph"]) flattenJsonLdEvents(n["@graph"], acc);
    if (Array.isArray(n.itemListElement)) {
      for (const it of n.itemListElement) {
        flattenJsonLdEvents(it.item || it, acc);
      }
    }
  }
  return acc;
}

function extractEventDetailLinks(html, baseUrl) {
  const links = [];
  const re = /href=["']([^"']+)["']/gi;
  let m;
  let base;
  try {
    base = new URL(baseUrl);
  } catch {
    return links;
  }
  while ((m = re.exec(String(html || "")))) {
    const href = m[1];
    if (!href || href.startsWith("#") || href.startsWith("mailto:")) continue;
    if (
      !/event|meeting|symposium|summit|conference|calendar|registration|housing|accommodation|tournament/i.test(
        href
      )
    ) {
      continue;
    }
    try {
      const abs = new URL(href, base).href;
      if (abs.startsWith("http") && links.length < 20) links.push(abs);
    } catch {
      /* ignore */
    }
  }
  return [...new Set(links)];
}

/**
 * Assess whether static HTML is enough or render is warranted.
 */
export function assessOfficialPageBody(html, text, url) {
  const textLen = String(text || "").trim().length;
  const htmlLen = String(html || "").length;
  const rank = scoreOfficialSource(url);
  const looksJsShell =
    htmlLen > 2000 &&
    textLen < THIN_TEXT_CHARS &&
    (/__NEXT_DATA__|ng-app|data-reactroot|webpackJsonp|window\.__INITIAL/i.test(
      html
    ) ||
      /<noscript>/i.test(html));

  if (textLen >= THIN_TEXT_CHARS) {
    return {
      quality: BODY_QUALITY.STATIC_HTML_OK,
      textLen,
      htmlLen,
      renderRecommended: false,
      officialScore: rank.score,
    };
  }
  if (looksJsShell && rank.score >= 20) {
    return {
      quality: BODY_QUALITY.JS_RENDER_REQUIRED,
      textLen,
      htmlLen,
      renderRecommended: true,
      officialScore: rank.score,
    };
  }
  if (textLen < 200) {
    return {
      quality: BODY_QUALITY.STATIC_HTML_EMPTY,
      textLen,
      htmlLen,
      renderRecommended: rank.score >= 35,
      officialScore: rank.score,
    };
  }
  return {
    quality: BODY_QUALITY.OTHER,
    textLen,
    htmlLen,
    renderRecommended: false,
    officialScore: rank.score,
  };
}

async function renderWithPlaywright(url, { timeoutMs = 45000 } = {}) {
  const { chromium } = await import("playwright");
  const browser = await chromium.launch({ headless: true });
  try {
    const page = await browser.newPage();
    await page.goto(url, {
      waitUntil: "domcontentloaded",
      timeout: timeoutMs,
    });
    // Allow SPA calendars to settle briefly
    await page.waitForTimeout(2500);
    const html = await page.content();
    return { ok: true, html, url: page.url() };
  } finally {
    await browser.close();
  }
}

/**
 * @param {string} url
 * @param {{ allowRender?: boolean, renderBudget?: { remaining: number }, timeoutMs?: number }} [opts]
 */
export async function fetchOfficialPage(url, opts = {}) {
  const allowRender = opts.allowRender !== false;
  const started = Date.now();
  const staticPage = await fetchResearchPage(url, {
    timeoutMs: opts.timeoutMs ?? 20000,
  });

  const base = {
    url,
    fetchVersion: OFFICIAL_PAGE_FETCH_VERSION,
    latencyMs: Date.now() - started,
    jsonLdEvents: [],
    eventDetailLinks: [],
  };

  if (!staticPage.ok) {
    // Official .gov / known institute hosts: try rendered fallback even when static fetch fails (bot blocks).
    const authority = scoreOfficialSource(url, {});
    const worthRender =
      allowRender &&
      (authority.tier === "PRIMARY_OFFICIAL" ||
        authority.tier === "LIKELY_OFFICIAL" ||
        /\.gov\b/i.test(url)) &&
      (opts.renderBudget?.remaining ?? RENDER_BUDGET_DEFAULT) > 0;
    if (worthRender) {
      try {
        if (opts.renderBudget) opts.renderBudget.remaining -= 1;
        const rendered = await renderWithPlaywright(url, {
          timeoutMs: opts.renderTimeoutMs || 45000,
        });
        const rHtml = rendered.html || "";
        const rText = htmlToSearchableText(rHtml).replace(/\s+/g, " ").trim();
        const rJsonLd = flattenJsonLdEvents(extractJsonLd(rHtml));
        if (rText.length >= 200 || rJsonLd.length > 0) {
          return {
            ...base,
            ok: true,
            fetchMethod: FETCH_METHOD.RENDERED_PLAYWRIGHT,
            bodyQuality: BODY_QUALITY.JS_RENDER_REQUIRED,
            text: rText.slice(0, 120000),
            html: rHtml.slice(0, 500000),
            finalUrl: rendered.url || url,
            jsonLdEvents: rJsonLd,
            eventDetailLinks: extractEventDetailLinks(rHtml, rendered.url || url),
            assessment: { quality: BODY_QUALITY.JS_RENDER_REQUIRED, renderRecommended: true },
            browserUsed: true,
            staticFailed: true,
            staticError: staticPage.error || `status_${staticPage.status}`,
          };
        }
      } catch {
        /* fall through to FAILED */
      }
    }
    return {
      ...base,
      ok: false,
      fetchMethod: FETCH_METHOD.FAILED,
      bodyQuality: BODY_QUALITY.OTHER,
      error: staticPage.error || `status_${staticPage.status}`,
      text: "",
      html: "",
    };
  }

  const html = staticPage.text || "";
  const text = htmlToSearchableText(html).replace(/\s+/g, " ").trim();
  const jsonLd = extractJsonLd(html);
  const jsonLdEvents = flattenJsonLdEvents(jsonLd);
  const eventDetailLinks = extractEventDetailLinks(html, staticPage.url || url);
  const assessment = assessOfficialPageBody(html, text, staticPage.url || url);

  if (jsonLdEvents.length >= 1 && text.length < THIN_TEXT_CHARS) {
    const fromLd = jsonLdEvents
      .map((e) =>
        [e.name, e.startDate, e.endDate, e.location?.name || e.location, e.description]
          .filter(Boolean)
          .join(" | ")
      )
      .join("\n");
    return {
      ...base,
      ok: true,
      fetchMethod: FETCH_METHOD.STRUCTURED_JSONLD,
      bodyQuality: BODY_QUALITY.STRUCTURED_ENDPOINT_AVAILABLE,
      text: `${text}\n${fromLd}`.trim(),
      html,
      finalUrl: staticPage.url || url,
      jsonLdEvents,
      eventDetailLinks,
      assessment,
      browserUsed: false,
    };
  }

  if (
    allowRender &&
    assessment.renderRecommended &&
    (opts.renderBudget?.remaining ?? RENDER_BUDGET_DEFAULT) > 0
  ) {
    try {
      if (opts.renderBudget) opts.renderBudget.remaining -= 1;
      const rendered = await renderWithPlaywright(staticPage.url || url, {
        timeoutMs: opts.renderTimeoutMs || 45000,
      });
      const rHtml = rendered.html || "";
      const rText = htmlToSearchableText(rHtml).replace(/\s+/g, " ").trim();
      const rJsonLd = flattenJsonLdEvents(extractJsonLd(rHtml));
      return {
        ...base,
        ok: rText.length >= 200 || rJsonLd.length > 0,
        fetchMethod: FETCH_METHOD.RENDERED_PLAYWRIGHT,
        bodyQuality: BODY_QUALITY.JS_RENDER_REQUIRED,
        text: rText.slice(0, 120000),
        html: rHtml.slice(0, 500000),
        finalUrl: rendered.url || url,
        jsonLdEvents: rJsonLd,
        eventDetailLinks: extractEventDetailLinks(rHtml, rendered.url || url),
        assessment,
        browserUsed: true,
        staticTextLen: text.length,
        renderedTextLen: rText.length,
      };
    } catch (err) {
      return {
        ...base,
        ok: text.length >= 200,
        fetchMethod: FETCH_METHOD.STATIC_HTML,
        bodyQuality: assessment.quality,
        text: text.slice(0, 120000),
        html: html.slice(0, 500000),
        finalUrl: staticPage.url || url,
        jsonLdEvents,
        eventDetailLinks,
        assessment,
        browserUsed: false,
        renderError: String(err?.message || err).slice(0, 200),
      };
    }
  }

  return {
    ...base,
    ok: text.length >= 100 || jsonLdEvents.length > 0,
    fetchMethod: FETCH_METHOD.STATIC_HTML,
    bodyQuality: assessment.quality,
    text: text.slice(0, 120000),
    html: html.slice(0, 500000),
    finalUrl: staticPage.url || url,
    jsonLdEvents,
    eventDetailLinks,
    assessment,
    browserUsed: false,
  };
}
