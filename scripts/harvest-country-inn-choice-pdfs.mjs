/**
 * Discover + download public Country Inn & Suites brand/development PDFs.
 * node scripts/harvest-country-inn-choice-pdfs.mjs --dry-run
 * node scripts/harvest-country-inn-choice-pdfs.mjs --apply
 */
import fs from "fs";
import path from "path";
import * as cheerio from "cheerio";

const BASE = "https://www.choicehotelsdevelopment.com";
const DAM = "https://www.choicehotels.com/content/dam/chhdcweb/choicehotelsdevelopment";
const OUT_DIR =
  process.env.CHOICE_COUNTRY_INN_MATERIALS_DIR ||
  "G:\\My Drive\\Dealality™\\Platform Design & Build\\Brand Reference Material\\Choice Hotels International\\Country Inn & Suites";
const CHI_ROOT =
  process.env.CHOICE_BRAND_REFERENCE_ROOT ||
  "G:\\My Drive\\Dealality™\\Platform Design & Build\\Brand Reference Material\\Choice Hotels International";
const APPLY = process.argv.includes("--apply");
const UA = "DealalityReferenceCapture/1.0";

const SEED_PAGES = [
  `${BASE}/our-brands/upper-midscale/country-inn-and-suites`,
  `${BASE}/our-brands/upper-midscale`,
  `${BASE}/our-brands`,
  `${BASE}/international/canada`,
];

const CANDIDATE_PDFS = [
  `${DAM}/CIS_OnePager_2024.pdf`,
  `${DAM}/CIS_OnePager_2024_PRINT.pdf`,
  `${DAM}/CIS_OnePager_2025.pdf`,
  `${DAM}/CIS_OnePager_2025_PRINT.pdf`,
  `${DAM}/brochure--country-inn.pdf`,
  `${DAM}/brochure--country-inn-and-suites.pdf`,
  `${DAM}/brochure--country-inn-suites.pdf`,
  `${DAM}/CHD_Country_TargetMarkets_WEB.pdf`,
  `${DAM}/Country_Inn_Suites_Development_Presentation.pdf`,
  `${DAM}/Country_Inn_Development.pdf`,
  `${DAM}/country-inn-development.pdf`,
  `${DAM}/COUNTRY_INN_OnePager.pdf`,
  `${DAM}/CIS_Development_Brochure.pdf`,
  `${BASE}/sfsites/c/resource/brochure--country-inn`,
];

function sanitize(name) {
  return String(name).replace(/[<>:"/\\|?*]/g, "").replace(/\s+/g, " ").trim();
}

function isPdf(buf) {
  return buf.length > 4 && buf.slice(0, 4).toString() === "%PDF";
}

async function fetchText(url) {
  const res = await fetch(url, { headers: { "User-Agent": UA }, redirect: "follow" });
  if (!res.ok) return { ok: false, status: res.status, text: "" };
  return { ok: true, status: res.status, text: await res.text() };
}

async function probePdf(url) {
  try {
    const res = await fetch(url, { headers: { "User-Agent": UA }, redirect: "follow" });
    if (!res.ok) return null;
    const ct = String(res.headers.get("content-type") || "").toLowerCase();
    const buf = Buffer.from(await res.arrayBuffer());
    if (!isPdf(buf) && !ct.includes("pdf")) return null;
    const cd = res.headers.get("content-disposition") || "";
    const fnMatch = cd.match(/filename\*?=(?:UTF-8''|")?([^";]+)/i);
    const filename = sanitize(fnMatch?.[1] || path.basename(new URL(url).pathname) || "download.pdf");
    return { url, filename, bytes: buf.length, buffer: buf };
  } catch {
    return null;
  }
}

function extractPdfUrls(html, pageUrl) {
  const $ = cheerio.load(html);
  const found = new Set();
  $("a[href], link[href], source[src]").each((_, el) => {
    const href = $(el).attr("href") || $(el).attr("src") || "";
    if (/\.pdf(\?|$)/i.test(href) || /content\/dam/i.test(href)) {
      try {
        found.add(new URL(href, pageUrl).toString().split("#")[0]);
      } catch {}
    }
  });
  const inline = [...html.matchAll(/https?:\/\/[^"'\\s)]+\.pdf[^"'\\s)]*/gi)].map((m) => m[0]);
  for (const u of inline) found.add(u.split("#")[0]);
  return [...found].filter((u) => /country|cis|country-inn/i.test(u) || /\.pdf$/i.test(u));
}

async function discover() {
  /** @type {Map<string, { url: string, filename: string, bytes: number, buffer?: Buffer, source: string }>} */
  const hits = new Map();

  for (const page of SEED_PAGES) {
    const { ok, text } = await fetchText(page);
    if (!ok) continue;
    for (const u of extractPdfUrls(text, page)) {
      if (!/country|cis|targetmarket|brochure|development|prototype|onepager|one-pager/i.test(u)) continue;
      const hit = await probePdf(u);
      if (hit) hits.set(hit.filename.toLowerCase(), { ...hit, source: `page:${page}` });
    }
    const viewMatch = text.match(/country_Inn_and_Suites_view/);
    if (viewMatch) {
      const viewUrlMatch = text.match(/\/webruntime\/view\/[a-f0-9]+\/prod\/en-US\/country_Inn_and_Suites_view/);
      if (viewUrlMatch) {
        const viewUrl = `${BASE}${viewUrlMatch[0]}`;
        const view = await fetchText(viewUrl);
        if (view.ok) {
          for (const u of extractPdfUrls(view.text, viewUrl)) {
            const hit = await probePdf(u);
            if (hit) hits.set(hit.filename.toLowerCase(), { ...hit, source: `view:${viewUrl}` });
          }
        }
      }
    }
  }

  for (const url of CANDIDATE_PDFS) {
    const hit = await probePdf(url);
    if (hit) hits.set(hit.filename.toLowerCase(), { ...hit, source: `candidate:${url}` });
  }

  // Consolidate PDFs already elsewhere in Dealality CHI tree
  const localSources = [
    { rel: "Comfort Inn/CIS_OnePager_2024.pdf", name: "CIS_OnePager_2024.pdf" },
    { rel: "FDDs/Country Inn & Suites by Radisson FDD 2026 (35772-202604-09).pdf", name: "Country Inn & Suites by Radisson FDD 2026.pdf" },
    { rel: "brochure--country-inn.pdf", name: "brochure--country-inn.pdf" },
    { rel: "CIS_OnePager_2025.pdf", name: "CIS_OnePager_2025.pdf" },
  ];
  for (const row of localSources) {
    const src = path.join(CHI_ROOT, row.rel);
    if (!fs.existsSync(src)) continue;
    const key = row.name.toLowerCase();
    if (hits.has(key)) continue;
    const buf = fs.readFileSync(src);
    if (!isPdf(buf)) continue;
    hits.set(key, { url: src, filename: row.name, bytes: buf.length, buffer: buf, source: `local:${src}` });
  }

  return [...hits.values()];
}

async function main() {
  if (!fs.existsSync(OUT_DIR)) fs.mkdirSync(OUT_DIR, { recursive: true });

  console.log(`${APPLY ? "APPLY" : "DRY RUN"} → ${OUT_DIR}\n`);
  const discovered = await discover();
  if (!discovered.length) {
    console.log("No new Country Inn PDFs discovered online.");
    process.exit(0);
  }

  let saved = 0;
  let skipped = 0;
  for (const hit of discovered) {
    const dest = path.join(OUT_DIR, hit.filename);
    if (fs.existsSync(dest)) {
      const existing = fs.statSync(dest);
      if (existing.size === hit.bytes) {
        console.log(`skip (exists): ${hit.filename}`);
        skipped++;
        continue;
      }
    }
    console.log(`${APPLY ? "save" : "would save"}: ${hit.filename} (${(hit.bytes / (1024 * 1024)).toFixed(2)} MB) ← ${hit.source}`);
    if (APPLY) {
      const buf = hit.buffer || Buffer.from(await (await fetch(hit.url)).arrayBuffer());
      fs.writeFileSync(dest, buf);
      saved++;
    } else {
      saved++;
    }
  }

  const manifest = {
    generated: new Date().toISOString().slice(0, 10),
    outDir: OUT_DIR,
    files: discovered.map((h) => ({ filename: h.filename, bytes: h.bytes, source: h.source })),
  };
  const manifestPath = path.join(OUT_DIR, "harvest-manifest.json");
  if (APPLY) fs.writeFileSync(manifestPath, `${JSON.stringify(manifest, null, 2)}\n`, "utf8");

  console.log(`\n${APPLY ? "Saved" : "Would save"} ${saved}; skipped ${skipped}.`);
  if (APPLY) console.log(`Manifest: ${manifestPath}`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
