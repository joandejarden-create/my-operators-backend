/**
 * Resolve hero image URL from a choicehotels.com property page.
 */
import puppeteer from "puppeteer";

const CHOICE_HOST_RE = /^https?:\/\/(www\.)?choicehotels\.com\//i;

function decodeHtml(str) {
  return String(str || "")
    .replace(/&amp;/g, "&")
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">");
}

export function pickImageFromHtml(html, pageUrl) {
  const og = html.match(/<meta[^>]+property=["']og:image["'][^>]+content=["']([^"']+)["']/i);
  if (og?.[1]) return new URL(decodeHtml(og[1]), pageUrl).toString();
  const tw = html.match(/<meta[^>]+name=["']twitter:image["'][^>]+content=["']([^"']+)["']/i);
  if (tw?.[1]) return new URL(decodeHtml(tw[1]), pageUrl).toString();

  const scripts = [...html.matchAll(/<script[^>]*type=["']application\/ld\+json["'][^>]*>([\s\S]*?)<\/script>/gi)];
  for (const s of scripts) {
    try {
      const json = JSON.parse(s[1]);
      const arr = Array.isArray(json) ? json : [json];
      for (const obj of arr) {
        const image = obj?.image;
        if (typeof image === "string" && /^https?:\/\//i.test(image)) return image;
        if (Array.isArray(image) && typeof image[0] === "string") return image[0];
        if (image && typeof image === "object" && typeof image.url === "string") return image.url;
      }
    } catch {
      /* ignore */
    }
  }
  return "";
}

export async function fetchHotelImageFromPage(pageUrl) {
  const url = String(pageUrl || "").trim();
  if (!url || !CHOICE_HOST_RE.test(url)) return "";

  try {
    const res = await fetch(url, {
      redirect: "follow",
      headers: { "User-Agent": "Mozilla/5.0 (compatible; DealCapture/1.0)" },
    });
    if (res.ok) {
      const html = await res.text();
      const fromHtml = pickImageFromHtml(html, url);
      if (fromHtml) return fromHtml;
    }
  } catch {
    /* fall through to browser */
  }
  return "";
}

/**
 * @param {import('puppeteer').Page} page
 * @param {string} pageUrl
 */
export async function fetchHotelImageWithBrowser(page, pageUrl) {
  try {
    await page.goto(pageUrl, { waitUntil: "domcontentloaded", timeout: 45000 });
    await page.waitForTimeout(1200);
    return await page.evaluate(() => {
      const abs = (u) => {
        try {
          return new URL(u, window.location.href).toString();
        } catch {
          return "";
        }
      };
      const bySel = (sel, attr = "content") => {
        const el = document.querySelector(sel);
        const v = el?.getAttribute(attr) || "";
        return v ? abs(v) : "";
      };
      const og = bySel('meta[property="og:image"]');
      if (og) return og;
      const tw = bySel('meta[name="twitter:image"]');
      if (tw) return tw;
      const imgs = [...document.querySelectorAll("img")]
        .map((i) => i.currentSrc || i.src || "")
        .map(abs)
        .filter(Boolean)
        .filter((u) => !/logo|icon|sprite|pixel/i.test(u))
        .filter((u) => /\.(jpg|jpeg|png|webp)(\?|$)/i.test(u));
      return imgs[0] || "";
    });
  } catch {
    return "";
  }
}

export async function resolveHotelImageForPropertyUrl(pageUrl, browserPage = null) {
  let imageUrl = await fetchHotelImageFromPage(pageUrl);
  if (!imageUrl && browserPage) {
    imageUrl = await fetchHotelImageWithBrowser(browserPage, pageUrl);
  }
  return imageUrl || "";
}

export function extractPropertyUrlFromBody(body) {
  const m = String(body || "").match(/(https:\/\/www\.choicehotels\.com\/[^\s)\]]+)/i);
  return m ? m[1].trim() : "";
}
