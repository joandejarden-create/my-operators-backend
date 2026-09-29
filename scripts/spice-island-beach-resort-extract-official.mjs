/**
 * Extract Spice Island Beach Resort first-party facts.
 *   node scripts/spice-island-beach-resort-extract-official.mjs
 */
import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const OUT = path.join(
  __dirname,
  "../reports/hotel-census/spice-island-beach-resort-onboarding-v1"
);

async function fetchText(url) {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), 30000);
  try {
    const res = await fetch(url, {
      signal: ctrl.signal,
      redirect: "follow",
      headers: {
        "User-Agent": "DealalityHotelIntelligence/1.0 (+research)",
        Accept: "text/html",
      },
    });
    clearTimeout(t);
    const text = await res.text();
    return { ok: res.ok, status: res.status, url: res.url || url, bytes: text.length, text };
  } catch (err) {
    clearTimeout(t);
    return { ok: false, status: 0, url, error: err.message || String(err) };
  }
}

const urls = {
  home: "https://www.spiceislandbeachresort.com/",
  about: "https://www.spiceislandbeachresort.com/about/",
  accommodations: "https://www.spiceislandbeachresort.com/accommodations/",
  contact: "https://www.spiceislandbeachresort.com/contact/",
  team: "https://www.spiceislandbeachresort.com/our-team/",
  management: "https://www.spiceislandbeachresort.com/management/",
  weddings: "https://www.spiceislandbeachresort.com/weddings/",
};

const pages = {};
for (const [k, u] of Object.entries(urls)) {
  pages[k] = await fetchText(u);
}

const corpus = Object.values(pages)
  .filter((p) => p.ok)
  .map((p) => p.text)
  .join("\n");

function find(re) {
  const m = corpus.match(re);
  return m ? m[0].slice(0, 200) : null;
}
function numNear(patterns) {
  for (const re of patterns) {
    const m = corpus.match(re);
    if (m) {
      const n = Number(String(m[1]).replace(/,/g, ""));
      if (Number.isFinite(n)) return { value: n, match: m[0].slice(0, 80) };
    }
  }
  return null;
}

const leadershipMentions = [];
for (const name of [
  "Janelle M. Hopkin",
  "Janelle Hopkin",
  "Nerissa Hopkin",
  "Betty Hopkin",
  "Lady Betty Hopkin",
  "Sheldon Keens-Douglas",
  "Sheldon Keens Douglas",
]) {
  const re = new RegExp(name.replace(/[.*+?^${}()|[\]\\]/g, "\\$&") + "[^.<]{0,120}", "i");
  const m = corpus.match(re);
  if (m) leadershipMentions.push({ name, snippet: m[0].replace(/\s+/g, " ").trim() });
}

const report = {
  generatedAt: new Date().toISOString(),
  hotel: "Spice Island Beach Resort",
  pages: Object.fromEntries(
    Object.entries(pages).map(([k, v]) => [
      k,
      { ok: v.ok, status: v.status, bytes: v.bytes, url: v.url, error: v.error || null },
    ])
  ),
  extracted: {
    suites: numNear([
      /(\d{2,3})\s*suites?/i,
      /(\d{2,3})\s*(?:luxury\s+)?(?:beachfront\s+)?suites?/i,
    ]),
    rooms: numNear([/(\d{2,3})\s*(?:guest\s*)?rooms?/i]),
    allInclusive: /all[- ]inclusive/i.test(corpus),
    beachfront: /beachfront|grand anse beach/i.test(corpus),
    familyOwned: /family[- ]owned|hopkin family/i.test(corpus),
    independent: /independent/i.test(corpus),
    grandAnse: /grand anse/i.test(corpus),
    grenada: /grenada/i.test(corpus),
    phone: find(/(?:\+|00)?\s*1?[\s.-]?\(?473\)?[\s.-]?\d[\d\s.-]{6,}/),
    email: find(/[a-z0-9._%+-]+@spiceislandbeachresort\.com/i),
    addressSnippet: find(/Grand Anse[^<]{0,80}/i),
    spa: /spa/i.test(corpus),
    weddings: /wedding|honeymoon/i.test(corpus),
    leadershipMentions,
  },
};

fs.mkdirSync(OUT, { recursive: true });
fs.writeFileSync(path.join(OUT, "PHASE2_OFFICIAL_EXTRACT.json"), JSON.stringify(report, null, 2) + "\n");
console.log(
  JSON.stringify(
    {
      pagesOk: Object.fromEntries(Object.entries(report.pages).map(([k, v]) => [k, v.ok])),
      extracted: report.extracted,
    },
    null,
    2
  )
);
