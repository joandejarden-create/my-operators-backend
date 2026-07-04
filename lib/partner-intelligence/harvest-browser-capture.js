/**

 * Shared Puppeteer helpers for exhaustive reference material capture.

 */

import fs from "fs";

import path from "path";

import * as cheerio from "cheerio";

import {

  buildReferenceMaterialPaths,

  ensureReferenceDirectory,

  writeCaptureReadme,

  appendCaptureLog,

  resolveReferenceRoot,
  resolveOperatorReferenceRoot,

  sanitizeFileName,

} from "./reference-material-paths.js";

export { resolveOperatorReferenceRoot };



export const DEFAULT_UA =

  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 DealalityReferenceCapture/1.0";



export function sleep(ms) {

  return new Promise((r) => setTimeout(r, ms));

}



/**

 * Make saved HTML render when opened from disk (file://).

 * Inserts <base href="origin/"> so relative /themes/... assets load from the live site.

 * Requires internet when viewing; use alsoPdf for offline visual archive.

 */

export function prepareOfflineHtml(html, pageUrl) {
  const origin = new URL(pageUrl).origin;
  const $ = cheerio.load(html, { decodeEntities: false });

  function toAbsolute(val) {
    if (!val || /^(data:|mailto:|tel:|javascript:|#)/i.test(val.trim())) return val;
    try {
      return new URL(val, pageUrl).href;
    } catch {
      return val;
    }
  }

  $("base").remove();
  if ($("head").length) {
    $("head").prepend(`<base href="${origin}/">`);
  } else {
    $("html").prepend(`<head><base href="${origin}/"></head>`);
  }

  const attrPairs = [
    ["link", "href"],
    ["script", "src"],
    ["img", "src"],
    ["source", "src"],
    ["video", "src"],
    ["audio", "src"],
    ["image", "href"],
    ["use", "href"],
    ["a", "href"],
  ];
  for (const [tag, attr] of attrPairs) {
    $(`${tag}[${attr}]`).each((_, el) => {
      const val = el.attribs[attr];
      if (val) $(el).attr(attr, toAbsolute(val));
    });
  }

  $("[srcset]").each((_, el) => {
    const srcset = el.attribs.srcset;
    if (!srcset) return;
    const fixed = srcset
      .split(",")
      .map((part) => {
        const bits = part.trim().split(/\s+/);
        bits[0] = toAbsolute(bits[0]);
        return bits.join(" ");
      })
      .join(", ");
    $(el).attr("srcset", fixed);
  });

  function rewriteCssUrls(css) {
    if (!css) return css;
    return css.replace(/url\(\s*(['"]?)([^'")]+)\1\s*\)/gi, (_m, quote, rawUrl) => {
      const trimmed = rawUrl.trim();
      if (/^(data:|https?:|#)/i.test(trimmed)) return `url(${quote}${trimmed}${quote})`;
      return `url(${quote}${toAbsolute(trimmed)}${quote})`;
    });
  }

  $("style").each((_, el) => {
    const css = $(el).html();
    if (css) $(el).html(rewriteCssUrls(css));
  });

  $("[style]").each((_, el) => {
    const inline = el.attribs.style;
    if (inline) $(el).attr("style", rewriteCssUrls(inline));
  });

  return $.html();
}

/** Inline linked stylesheets so saved HTML renders without separate CSS requests (Drive preview, offline). */
export async function inlineExternalStylesheets(page) {
  await page.evaluate(async () => {
    const links = Array.from(document.querySelectorAll('link[rel="stylesheet"][href]'));
    for (const link of links) {
      try {
        const res = await fetch(link.href, { credentials: "same-origin" });
        if (!res.ok) continue;
        const css = await res.text();
        const style = document.createElement("style");
        style.setAttribute("data-inlined-from", link.href);
        style.textContent = css;
        link.replaceWith(style);
      } catch {
        /* keep external link as fallback */
      }
    }
  });
}

/** @param {import('puppeteer').Page} page @param {string} outputPath */
export async function captureMhtmlSnapshot(page, outputPath) {
  const client = await page.createCDPSession();
  const { data } = await client.send("Page.captureSnapshot", { format: "mhtml" });
  fs.writeFileSync(outputPath, data);
}



/** @param {import('puppeteer').Page} page */

export async function dismissCommonCookieBanners(page) {

  try {

    await page.evaluate(() => {

      const labels = /accept all cookies|accept cookies|i agree|agree and proceed|allow all/i;

      for (const el of document.querySelectorAll("button, a, [role='button']")) {

        const text = (el.textContent || "").trim();

        if (labels.test(text)) {

          el.click();

          return;

        }

      }

    });

    await sleep(800);

  } catch {

    /* non-fatal */

  }

}



/** @param {string} title @param {string} html */

export function isBlockedOrErrorPage(title, html) {

  const t = `${title} ${html.slice(0, 8000)}`.toLowerCase();

  if (/\b404\b/.test(title) || /page not found|error 404/i.test(title)) return "404 or not found";

  if (/rate limited|access denied|cf-browser-verification|error 1015|just a moment/i.test(t)) {

    return "Blocked by Cloudflare or access denied";

  }

  return null;

}



/** @param {string} sitemapUrl */

export async function fetchSitemapUrls(sitemapUrl, { max = 200 } = {}) {

  try {

    const res = await fetch(sitemapUrl, { headers: { "User-Agent": DEFAULT_UA } });

    if (!res.ok) return [];

    const xml = await res.text();

    const urls = [...xml.matchAll(/<loc>([^<]+)<\/loc>/g)].map((m) => m[1].trim());

    return urls.slice(0, max);

  } catch {

    return [];

  }

}



/**

 * @param {import('puppeteer').Browser} browser

 * @param {{ url: string, title: string, companyFolder: string, typeKey?: string, brand?: string, category?: string, alsoPdf?: boolean }} opts

 */

async function gotoWithOptionalWarmup(page, url, { gotoTimeout, warmOrigin }) {
  if (warmOrigin) {
    try {
      await page.goto(warmOrigin, { waitUntil: "domcontentloaded", timeout: gotoTimeout });
      await sleep(3500);
    } catch {
      /* non-fatal warmup */
    }
  }
  await page.goto(url, { waitUntil: "networkidle2", timeout: gotoTimeout });
}

export async function captureHtmlWithBrowser(browser, opts) {

  const page = await browser.newPage();
  const refRoot = opts.referenceRoot || resolveReferenceRoot();

  await page.setUserAgent(DEFAULT_UA);
  const gotoTimeout = opts.gotoTimeout || 120000;
  const warmOrigin = opts.warmOrigin || null;
  try {
    await gotoWithOptionalWarmup(page, opts.url, { gotoTimeout, warmOrigin });

    await sleep(2000);

    await dismissCommonCookieBanners(page);

    await sleep(1500);

    await inlineExternalStylesheets(page);

    await sleep(500);

    const html = await page.content();

    const title = await page.title();

    const blockReason = isBlockedOrErrorPage(title, html);

    if (blockReason) throw new Error(blockReason);



    const prepared = prepareOfflineHtml(html, opts.url).replace(/^\s*<!DOCTYPE[^>]*>\s*/i, "");

    const wrapped = `<!DOCTYPE html>

<!-- Captured ${new Date().toISOString()} from ${opts.url} -->

<!-- Category: ${opts.category || "web page"} -->

<!-- Page title: ${title} -->

<!-- Offline: open the matching (archive).mhtml in Chrome/Edge for full offline; (snapshot).pdf for print view; .html has inlined CSS but images may still load from the live site when online -->

${prepared}`;



    const paths = buildReferenceMaterialPaths({

      companyFolder: opts.companyFolder,

      brandName: opts.brand,

      typeKey: opts.typeKey || "development-brochure",

      title: sanitizeFileName(opts.title || title || "page"),

      ext: ".html",

      referenceRoot: refRoot,

    });

    ensureReferenceDirectory(paths.absoluteDir);

    writeCaptureReadme(opts.companyFolder, path.join(refRoot, opts.companyFolder));

    fs.writeFileSync(paths.absoluteFile, wrapped, "utf8");

    appendCaptureLog(opts.companyFolder, {

      url: opts.url,

      relativePath: paths.relativePath,

      format: "html",

      category: opts.category,

      pageTitle: title,

    }, refRoot);



    let pdfRelativePath;
    let mhtmlRelativePath;

    if (opts.alsoMhtml) {
      page.setDefaultTimeout(Math.max(gotoTimeout, 90000));
      const mhtmlAbs = paths.absoluteFile.replace(/\.html$/i, " (archive).mhtml");
      await captureMhtmlSnapshot(page, mhtmlAbs);
      mhtmlRelativePath = mhtmlAbs.replace(refRoot + path.sep, "").replace(/\\/g, "/");
      appendCaptureLog(opts.companyFolder, {
        url: opts.url,
        relativePath: mhtmlRelativePath,
        format: "mhtml",
        category: opts.category,
        pageTitle: title,
      }, refRoot);
    }

    if (opts.alsoPdf) {
      page.setDefaultTimeout(Math.max(gotoTimeout, 90000));
      const pdfAbs = paths.absoluteFile.replace(/\.html$/i, " (snapshot).pdf");

      await page.pdf({

        path: pdfAbs,

        format: "A4",

        printBackground: true,

        margin: { top: "12mm", bottom: "12mm", left: "10mm", right: "10mm" },

      });

      pdfRelativePath = pdfAbs.replace(refRoot + path.sep, "").replace(/\\/g, "/");

      appendCaptureLog(opts.companyFolder, {

        url: opts.url,

        relativePath: pdfRelativePath,

        format: "pdf-snapshot",

        category: opts.category,

        pageTitle: title,

      }, refRoot);

    }



    return {
      relativePath: paths.relativePath,
      bytes: wrapped.length,
      pageTitle: title,
      pdfRelativePath,
      mhtmlRelativePath,
    };

  } finally {

    await page.close();

  }

}



/**

 * Save Showpad interactive share as rendered HTML when no PDF exists.

 * @param {import('puppeteer').Browser} browser

 */

export async function captureShowpadHtml(browser, opts) {

  const page = await browser.newPage();

  await page.setUserAgent(DEFAULT_UA);

  try {

    await page.goto(opts.sourceUrl, { waitUntil: "networkidle2", timeout: 120000 });

    await sleep(5000);

    const html = await page.content();

    const title = await page.title();

    const blockReason = isBlockedOrErrorPage(title, html);

    if (blockReason) throw new Error(blockReason);



    const prepared = prepareOfflineHtml(html, opts.sourceUrl).replace(/^\s*<!DOCTYPE[^>]*>\s*/i, "");

    const wrapped = `<!DOCTYPE html>

<!-- Showpad interactive capture ${new Date().toISOString()} -->

<!-- Source: ${opts.sourceUrl} -->

<!-- Original title: ${opts.title} -->

<!-- Offline viewing: assets load from ${new URL(opts.sourceUrl).origin} when online -->

${prepared}`;



    const paths = buildReferenceMaterialPaths({

      companyFolder: opts.companyFolder,

      brandName: opts.brand,

      typeKey: opts.typeKey || "development-brochure",

      title: sanitizeFileName(`${opts.title} (Showpad interactive)`),

      ext: ".html",

    });

    ensureReferenceDirectory(paths.absoluteDir);

    fs.writeFileSync(paths.absoluteFile, wrapped, "utf8");

    appendCaptureLog(opts.companyFolder, {

      url: opts.sourceUrl,

      relativePath: paths.relativePath,

      format: "showpad-html",

      title: opts.title,

    });

    return { relativePath: paths.relativePath, bytes: wrapped.length };

  } finally {

    await page.close();

  }

}



/** Filter sitemap URLs to development-relevant pages */

export function filterDevelopmentPages(urls, hostPattern = /development\.ihg\.com/i) {

  return [...new Set(urls.filter((u) => hostPattern.test(u) && !/\.(pdf|jpg|png|svg|css|js|xml|kml)(\?|$)/i.test(u)))];

}



/**

 * Crawl a seed page in-browser and return unique internal links matching pathPattern.

 * @param {import('puppeteer').Browser} browser

 * @param {{ seedUrl: string, pathPattern?: RegExp, waitMs?: number }} opts

 */

export async function discoverInternalLinks(browser, { seedUrl, pathPattern, waitMs = 2500 }) {

  const page = await browser.newPage();

  await page.setUserAgent(DEFAULT_UA);

  try {

    await page.goto(seedUrl, { waitUntil: "networkidle2", timeout: 120000 });

    await sleep(waitMs);

    const origin = new URL(seedUrl).origin;

    const links = await page.evaluate((base, patternSource) => {

      const pattern = patternSource ? new RegExp(patternSource.slice(1, patternSource.lastIndexOf("/"))) : null;

      return [...document.querySelectorAll("a[href]")]

        .map((a) => {

          try {

            return new URL(a.href, base).href;

          } catch {

            return "";

          }

        })

        .filter((h) => h.startsWith(base) && (!pattern || pattern.test(h)) && !h.includes("#"));

    }, origin, pathPattern?.toString());

    return [...new Set(links)];

  } finally {

    await page.close();

  }

}



/** @param {string} url */

export function slugToTitle(url) {

  const slug = decodeURIComponent(url.split("/").filter(Boolean).pop() || "page");

  return slug.replace(/-/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());

}


