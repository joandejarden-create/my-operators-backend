#!/usr/bin/env node
/**
 * Sample IHG brand hoteldetail sitemaps + destinations.en sitemap for CALA.
 */
import { writeFileSync } from "node:fs";

const UA =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36";

function extractLocs(xml) {
  return [...String(xml || "").matchAll(/<loc>([^<]+)<\/loc>/gi)].map((m) => m[1].trim());
}

async function fetchText(url) {
  const r = await fetch(url, { headers: { "User-Agent": UA, Accept: "*/*" } });
  return { status: r.status, text: await r.text() };
}

/**
 * Parse: /{brand}/hotels/{region}/{locale}/{city}/{mnemonic}/hoteldetail
 */
function parseHoteldetailUrl(url) {
  try {
    const u = new URL(url);
    const parts = u.pathname.split("/").filter(Boolean);
    // brand, hotels, region, locale, city, mnemonic, hoteldetail
    const hi = parts.indexOf("hoteldetail");
    if (hi < 0) return null;
    const mnemonic = parts[hi - 1] || "";
    const citySlug = parts[hi - 2] || "";
    const brand = parts[0] || "";
    if (!/^[a-z0-9]{4,6}$/i.test(mnemonic)) return null;
    return {
      url: u.href.replace(/\/hoteldetail\/.*$/i, "/hoteldetail").replace(/\/$/, "") ,
      brand,
      citySlug,
      mnemonic: mnemonic.toUpperCase(),
      canonical: `https://www.ihg.com/${brand}/hotels/us/en/${citySlug}/${mnemonic.toLowerCase()}/hoteldetail`,
    };
  } catch {
    return null;
  }
}

async function main() {
  const bin = await fetchText("https://www.ihg.com/bin/sitemapindex.xml");
  const all = extractLocs(bin.text);
  const enUsHoteldetail = all.filter((u) => /\.en-us\.hoteldetail\.xml$/i.test(u));
  console.log("en-us hoteldetail sitemaps:", enUsHoteldetail.length);
  for (const u of enUsHoteldetail) console.log(" ", u);

  /** @type {object[]} */
  const hotels = [];
  const byMnemonic = new Map();
  for (const sm of enUsHoteldetail) {
    const r = await fetchText(sm);
    const locs = extractLocs(r.text);
    let added = 0;
    for (const loc of locs) {
      if (!/\/hoteldetail\/?$/i.test(loc) && !/\/hoteldetail$/i.test(loc.split("?")[0])) {
        // include only exact hoteldetail, not hotel-reviews subpages
        if (!/\/hoteldetail$/i.test(loc.replace(/\/$/, ""))) continue;
      }
      const path = loc.replace(/\/$/, "");
      if (/\/hoteldetail\/.+/i.test(path)) continue; // subpages
      const parsed = parseHoteldetailUrl(path);
      if (!parsed) continue;
      if (byMnemonic.has(parsed.mnemonic)) continue;
      byMnemonic.set(parsed.mnemonic, parsed);
      hotels.push(parsed);
      added++;
    }
    console.log(sm.split("/").pop(), "locs", locs.length, "hotels added", added);
  }

  console.log("unique hotels", hotels.length);
  console.log("sample", hotels.slice(0, 8));

  // Destinations sitemap — find CALA country hotel list pages
  const dest = await fetchText("https://www.ihg.com/services/sitemaps/destinations.en.sitemap.xml");
  const destLocs = extractLocs(dest.text);
  const calaKeywords = [
    "mexico",
    "dominican-republic",
    "colombia",
    "brazil",
    "argentina",
    "chile",
    "peru",
    "panama",
    "costa-rica",
    "jamaica",
    "puerto-rico",
    "bahamas",
    "barbados",
    "aruba",
    "curacao",
    "guatemala",
    "honduras",
    "el-salvador",
    "nicaragua",
    "belize",
    "ecuador",
    "uruguay",
    "cuba",
    "haiti",
    "trinidad",
    "cayman",
    "turks",
    "saint-lucia",
    "antigua",
    "grenada",
    "dominica",
    "martinique",
    "guadeloupe",
    "bonaire",
    "virgin-islands",
    "saint-kitts",
    "saint-vincent",
  ];
  const calaDest = destLocs.filter((u) => {
    const low = u.toLowerCase();
    return calaKeywords.some((k) => low.includes(k)) && /hotels?$/i.test(u.replace(/\/$/, ""));
  });
  console.log("dest total", destLocs.length, "cala-ish country hotel pages", calaDest.length);
  console.log(calaDest.slice(0, 40));

  writeFileSync(
    "reports/ihg-hoteldetail-sitemap-sample.json",
    JSON.stringify(
      {
        generatedAt: new Date().toISOString(),
        enUsSitemapCount: enUsHoteldetail.length,
        hotelCount: hotels.length,
        sampleHotels: hotels.slice(0, 50),
        calaDestinationPages: calaDest,
        brandSitemaps: enUsHoteldetail,
      },
      null,
      2
    )
  );
  console.log("wrote reports/ihg-hoteldetail-sitemap-sample.json");
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
