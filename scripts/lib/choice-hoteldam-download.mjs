import puppeteer from "puppeteer";

const UA =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36";

const BROWSER_HEADERS = {
  "User-Agent": UA,
  Accept: "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
  Referer: "https://www.choicehotels.com/",
};

/**
 * @param {string} imageUrl
 * @returns {Promise<{ buffer: Buffer, contentType: string } | null>}
 */
/**
 * @param {string} imageUrl
 * @param {string} [propertyPageUrl]
 */
/**
 * @param {string} imageUrl
 * @param {string} [propertyPageUrl]
 * @param {{ usePuppeteer?: boolean }} [opts]
 */
/**
 * Download hoteldam image bytes (direct fetch, then optional Puppeteer).
 * @param {string} imageUrl
 * @param {string} [propertyPageUrl]
 * @param {{ usePuppeteer?: boolean }} [opts]
 * @returns {Promise<{ buffer: Buffer, contentType: string } | null>}
 */
export async function fetchHoteldamImageBytes(imageUrl, propertyPageUrl = "", opts = {}) {
  const direct = await fetchBytes(imageUrl);
  if (direct) return direct;
  if (!opts.usePuppeteer || !propertyPageUrl) return null;
  return downloadFromPropertyPage(propertyPageUrl, imageUrl);
}

export async function downloadHoteldamImage(imageUrl, propertyPageUrl = "", opts = {}) {
  return fetchHoteldamImageBytes(imageUrl, propertyPageUrl, opts);
}

/**
 * @param {string} imageUrl
 */
async function fetchBytes(imageUrl) {
  const attempts = [
    () => fetch(imageUrl, { redirect: "follow" }),
    () => fetch(imageUrl, { redirect: "follow", headers: BROWSER_HEADERS }),
  ];
  for (const get of attempts) {
    try {
      const res = await get();
      if (!res.ok) continue;
      const contentType = res.headers.get("content-type") || "";
      if (!contentType.includes("image")) continue;
      const buffer = Buffer.from(await res.arrayBuffer());
      if (buffer.length) return { buffer, contentType };
    } catch {
      // try next attempt
    }
  }
  return null;
}

/**
 * Load property page and capture hoteldam image from network or HTML.
 * @param {string} propertyPageUrl
 * @param {string} [preferredImageUrl]
 */
export async function downloadFromPropertyPage(propertyPageUrl, preferredImageUrl = "") {
  const browser = await puppeteer.launch({ headless: "new" });
  try {
    const page = await browser.newPage();
    await page.setUserAgent(UA);
    const hits = new Set();
    page.on("response", async (res) => {
      const u = res.url();
      if (!/hoteldam/i.test(u) || !/\.(jpe?g|png|webp)/i.test(u)) return;
      if (preferredImageUrl && u.split("?")[0] !== preferredImageUrl.split("?")[0]) {
        if (!/exterior|aerial|hero|facade|pool|terrace/i.test(u)) return;
      }
      hits.add(u.split("?")[0]);
    });

    await page.goto(propertyPageUrl, { waitUntil: "networkidle2", timeout: 90000 });
    await new Promise((r) => setTimeout(r, 5000));

    if (preferredImageUrl) {
      const norm = preferredImageUrl.split("?")[0];
      if (hits.has(norm)) {
        const got = await fetchBytes(norm);
        if (got) return got;
      }
    }

    const html = await page.content();
    const found = [
      ...html.matchAll(
        /https:\/\/www\.choicehotels\.com\/hoteldam\/[^"'\\s]+?\.(?:jpg|jpeg|png|webp)/gi
      ),
    ].map((m) => m[0].split("?")[0]);
    const pick =
      found.find((u) => preferredImageUrl && u === preferredImageUrl.split("?")[0]) ||
      found.find((u) => /exterior|aerial|hero|facade/i.test(u)) ||
      [...hits][0] ||
      found[0];

    if (!pick) return null;
    return fetchBytes(pick);
  } finally {
    await browser.close();
  }
}
