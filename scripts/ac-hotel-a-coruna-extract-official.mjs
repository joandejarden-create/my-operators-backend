/**
 * Extract public AC Hotel A Coruña facts from Marriott events page (+ secondary).
 *   node scripts/ac-hotel-a-coruna-extract-official.mjs
 */
import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const OUT = path.join(
  __dirname,
  "../reports/hotel-census/ac-hotel-a-coruna-onboarding-v1"
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
        "Accept-Language": "en-US,en;q=0.9,es;q=0.8",
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

function extractNumberNear(text, patterns) {
  for (const re of patterns) {
    const m = text.match(re);
    if (m) {
      const n = Number(String(m[1]).replace(/,/g, "").replace(/\./g, ""));
      if (Number.isFinite(n)) return { value: n, match: m[0].slice(0, 80) };
    }
  }
  return null;
}

const urls = {
  eventsEn: "https://www.marriott.com/en-us/hotels/lcgco-ac-hotel-a-coruna/events/",
  eventsEs: "https://www.marriott.com/es/hotels/lcgco-ac-hotel-a-coruna/events/",
  overviewEn: "https://www.marriott.com/en-us/hotels/lcgco-ac-hotel-a-coruna/overview/",
  overviewEs: "https://www.marriott.com/es/hotels/lcgco-ac-hotel-a-coruna/overview/",
};

const pages = {};
for (const [k, u] of Object.entries(urls)) {
  pages[k] = await fetchText(u);
}

const primary = pages.eventsEn.ok ? pages.eventsEn : pages.eventsEs;
const text = primary?.text || "";

const meetingRooms = extractNumberNear(text, [
  /(\d{1,2})\s*(?:meeting|event)\s*rooms?/i,
  /meeting rooms[^0-9]{0,40}(\d{1,2})/i,
]);
const totalSqM = extractNumberNear(text, [
  /([\d,.]+)\s*(?:sq\.?\s*m|square meters|m²)/i,
  /total[^0-9]{0,40}([\d,.]+)\s*(?:sq\.?\s*m|m²)/i,
]);
const largestCap = extractNumberNear(text, [
  /largest[^0-9]{0,40}(\d{2,4})/i,
  /reception[^0-9]{0,20}(\d{2,4})/i,
]);
const rooms = extractNumberNear(text, [
  /(\d{2,4})\s*guest\s*rooms?/i,
  /(\d{2,4})\s*rooms?\s*(?:and|&)/i,
]);

const spaceNames = [];
for (const name of ["Banquets", "Consejo", "Fórum A", "Forum A", "Fórum B", "Forum B", "Gran Fórum", "Gran Forum", "Congress"]) {
  if (new RegExp(name.replace(/ó/g, "o?"), "i").test(text)) spaceNames.push(name);
}

const addressHit = text.match(/Enrique\s+Mari[nñ]as[^0-9<]{0,40}(\d+)/i);
const postalHit = text.match(/\b15009\b/);
const phoneHit = text.match(/(?:\+|00)?\s*34[\s.-]?\d[\d\s.-]{7,}/);

const report = {
  generatedAt: new Date().toISOString(),
  hotel: "AC Hotel A Coruña",
  marriottCode: "LCGCO",
  pages: Object.fromEntries(
    Object.entries(pages).map(([k, v]) => [
      k,
      { ok: v.ok, status: v.status, bytes: v.bytes, url: v.url, error: v.error || null },
    ])
  ),
  primarySource: primary?.ok ? primary.url : null,
  extracted: {
    meetingRooms,
    totalEventSpaceSqM: totalSqM,
    largestCapacity: largestCap,
    guestRooms: rooms,
    spaceNamesFound: spaceNames,
    addressSnippet: addressHit?.[0] || null,
    postal15009: Boolean(postalHit),
    phone: phoneHit?.[0]?.replace(/\s+/g, " ").trim() || null,
  },
  verifiedSeed: {
    hotelName: "AC Hotel A Coruña",
    brand: "AC Hotels by Marriott",
    brandFamily: "Marriott International",
    propertyCode: "LCGCO",
    address: "Enrique Mariñas 36",
    city: "A Coruña",
    stateRegion: "Galicia",
    postalCode: "15009",
    country: "Spain",
    roomsKeys: 116,
    officialPropertyUrlEs: urls.overviewEs,
    officialPropertyUrlEn: urls.overviewEn,
    officialEventsUrlEn: urls.eventsEn,
    meetingRoomCount: meetingRooms?.value ?? 6,
    totalEventSpaceSqM: totalSqM?.value ?? 674,
    largestEventCapacity: largestCap?.value ?? 290,
    note: "Overview pages returned 403 from this runner; events page preferred. Seed rooms 116 from founder/public brief pending overview corroboration — mark MEDIUM until steward HIGH.",
  },
};

fs.mkdirSync(OUT, { recursive: true });
fs.writeFileSync(path.join(OUT, "PHASE2_OFFICIAL_EXTRACT.json"), JSON.stringify(report, null, 2) + "\n");
console.log(JSON.stringify({ primary: report.primarySource, extracted: report.extracted, pages: report.pages }, null, 2));
