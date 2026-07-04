/**
 * Attach hotel images to materials.caseStudy rows from choicehotels.com pages.
 * Updates only rows where Image is empty.
 *
 * Image source precedence: og:image -> twitter:image -> ld+json image.
 *
 * Usage:
 *   node scripts/attach-choice-case-study-images.mjs --dry-run
 *   node scripts/attach-choice-case-study-images.mjs
 */
import "../load-env.js";
import Airtable from "airtable";
import puppeteer from "puppeteer";

const TABLE = "Brand Setup - Brand Explorer Presentation";
const SLOT = "materials.caseStudy";
const CHOICE_HOST_RE = /^https?:\/\/(www\.)?choicehotels\.com\//i;

function parseArgs(argv) {
  return { dryRun: argv.includes("--dry-run") };
}

function extractFirstUrl(text) {
  const s = String(text || "");
  const m = s.match(/https?:\/\/[^\s)]+/i);
  return m ? m[0] : "";
}

function decodeHtml(str) {
  return String(str || "")
    .replace(/&amp;/g, "&")
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">");
}

function pickImageFromHtml(html, pageUrl) {
  const og = html.match(/<meta[^>]+property=["']og:image["'][^>]+content=["']([^"']+)["']/i);
  if (og?.[1]) return new URL(decodeHtml(og[1]), pageUrl).toString();
  const tw = html.match(/<meta[^>]+name=["']twitter:image["'][^>]+content=["']([^"']+)["']/i);
  if (tw?.[1]) return new URL(decodeHtml(tw[1]), pageUrl).toString();

  const scripts = [...html.matchAll(/<script[^>]*type=["']application\/ld\+json["'][^>]*>([\s\S]*?)<\/script>/gi)];
  for (const s of scripts) {
    const raw = s[1];
    try {
      const json = JSON.parse(raw);
      const arr = Array.isArray(json) ? json : [json];
      for (const obj of arr) {
        const image = obj?.image;
        if (typeof image === "string" && /^https?:\/\//i.test(image)) return image;
        if (Array.isArray(image) && typeof image[0] === "string") return image[0];
        if (image && typeof image === "object" && typeof image.url === "string") return image.url;
      }
    } catch {
      // ignore invalid json blocks
    }
  }
  return "";
}

async function fetchHotelImage(url) {
  const res = await fetch(url, { redirect: "follow" });
  if (!res.ok) return "";
  const html = await res.text();
  const imageUrl = pickImageFromHtml(html, url);
  if (!imageUrl) return "";
  return imageUrl;
}

async function fetchHotelImageWithBrowser(page, url) {
  try {
    await page.goto(url, { waitUntil: "domcontentloaded", timeout: 45000 });
    await page.waitForTimeout(1200);
    const imageUrl = await page.evaluate(() => {
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
    return imageUrl || "";
  } catch {
    return "";
  }
}

async function listCaseStudyRows(base) {
  return base(TABLE)
    .select({
      filterByFormula: `{Slot Key} = "${SLOT}"`,
      maxRecords: 1000,
    })
    .all();
}

async function main() {
  const { dryRun } = parseArgs(process.argv);
  const key = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!key || !baseId) throw new Error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID");
  const base = new Airtable({ apiKey: key }).base(baseId);

  const rows = await listCaseStudyRows(base);
  const targets = rows.filter((r) => {
    const imgs = r.get("Image");
    return !Array.isArray(imgs) || imgs.length === 0;
  });
  console.log(`${dryRun ? "[dry-run] " : ""}Found ${targets.length} caseStudy row(s) without images.`);

  let updated = 0;
  let skippedNoUrl = 0;
  let skippedNoImage = 0;
  const browser = await puppeteer.launch({ headless: true });
  const page = await browser.newPage();
  await page.setViewport({ width: 1600, height: 1200 });
  await page.setUserAgent(
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
  );
  for (const row of targets) {
    const brand = String(row.get("Brand Name") || "").trim() || "(no brand)";
    const title = String(row.get("Title") || "").trim() || "(no title)";
    const body = String(row.get("Body") || "");
    const pageUrl = extractFirstUrl(body);
    if (!pageUrl || !CHOICE_HOST_RE.test(pageUrl)) {
      console.log(`- ${brand}: skip "${title}" (no choicehotels.com URL in body)`);
      skippedNoUrl += 1;
      continue;
    }
    let imageUrl = "";
    try {
      imageUrl = await fetchHotelImage(pageUrl);
    } catch {
      imageUrl = "";
    }
    if (!imageUrl) {
      imageUrl = await fetchHotelImageWithBrowser(page, pageUrl);
    }
    if (!imageUrl) {
      console.log(`- ${brand}: skip "${title}" (no image metadata found)`);
      skippedNoImage += 1;
      continue;
    }

    console.log(`- ${brand}: attach image for "${title}"`);
    console.log(`  page: ${pageUrl}`);
    console.log(`  image: ${imageUrl}`);
    if (!dryRun) {
      await base(TABLE).update(row.id, {
        Image: [{ url: imageUrl }],
      });
    }
    updated += 1;
  }
  await browser.close();

  console.log(
    `${dryRun ? "Would update" : "Updated"} ${updated} row(s). Skipped: ${skippedNoUrl} (no URL), ${skippedNoImage} (no image).`
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});

