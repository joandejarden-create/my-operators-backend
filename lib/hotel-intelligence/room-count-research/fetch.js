/**
 * Safe single-page fetch for room-count research (not a crawler).
 */

const FETCH_HEADERS = {
  "User-Agent":
    "DealalityHotelIntelligence/1.0 (+room-count-research; research-only)",
  Accept: "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
  "Accept-Language": "en,es;q=0.9,pt;q=0.8,fr;q=0.7",
};

/**
 * @param {string} url
 * @param {{ timeoutMs?: number }} [opts]
 */
export async function fetchResearchPage(url, opts = {}) {
  const controller = new AbortController();
  const timeoutMs = opts.timeoutMs ?? 25000;
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  const started = Date.now();
  try {
    const res = await fetch(url, {
      headers: FETCH_HEADERS,
      redirect: "follow",
      signal: controller.signal,
    });
    const text = await res.text();
    const blocked =
      res.status === 403 ||
      res.status === 429 ||
      /<title[^>]*>\s*access denied/i.test(text) ||
      /cf-challenge|attention required|akamai\s*block/i.test(text);
    return {
      ok: res.ok && !blocked,
      status: res.status,
      url: res.url || url,
      text: text.slice(0, 500_000), // cap memory
      blocked,
      length: text.length,
      latency_ms: Date.now() - started,
    };
  } catch (err) {
    return {
      ok: false,
      status: 0,
      url,
      text: "",
      blocked: false,
      error: String(err?.message || err).slice(0, 200),
      length: 0,
      latency_ms: Date.now() - started,
    };
  } finally {
    clearTimeout(timer);
  }
}

/**
 * Strip tags lightly for snippet/quote work (not a full HTML parser).
 * @param {string} html
 */
export function htmlToSearchableText(html) {
  return String(html || "")
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<style[\s\S]*?<\/style>/gi, " ")
    .replace(/<[^>]+>/g, " ")
    .replace(/&nbsp;/g, " ")
    .replace(/&amp;/g, "&")
    .replace(/\s+/g, " ")
    .trim();
}
