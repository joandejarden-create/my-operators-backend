#!/usr/bin/env node
/**
 * Probe IHG sitemap indexes + destination pages for hoteldetail URL patterns.
 */
import { writeFileSync, mkdirSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPORTS = join(__dirname, "..", "reports");
const UA =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36";

async function fetchText(url) {
  const r = await fetch(url, {
    headers: { "User-Agent": UA, Accept: "*/*", "Accept-Language": "en-US,en;q=0.9" },
  });
  return { status: r.status, url: r.url, text: await r.text() };
}

function extractLocs(xml) {
  return [...String(xml || "").matchAll(/<loc>([^<]+)<\/loc>/gi)].map((m) => m[1].trim());
}

async function main() {
  mkdirSync(REPORTS, { recursive: true });
  const out = { generatedAt: new Date().toISOString() };

  const idx = await fetchText("https://www.ihg.com/services/sitemaps/sitemap-index.xml");
  const childLocs = extractLocs(idx.text);
  out.servicesSitemapIndex = { status: idx.status, childCount: childLocs.length, children: childLocs };
  console.log("services sitemap children:", childLocs.length);
  for (const c of childLocs) console.log(" ", c);

  // Sample a few child sitemaps
  out.childSamples = {};
  for (const child of childLocs.slice(0, 8)) {
    const r = await fetchText(child);
    const locs = extractLocs(r.text);
    const hoteldetail = locs.filter((u) => /hoteldetail/i.test(u));
    const destinations = locs.filter((u) => /destinations/i.test(u));
    out.childSamples[child] = {
      status: r.status,
      locCount: locs.length,
      hoteldetailCount: hoteldetail.length,
      destinationCount: destinations.length,
      sampleHoteldetail: hoteldetail.slice(0, 5),
      sampleDest: destinations.slice(0, 5),
      sampleLocs: locs.slice(0, 8),
    };
    console.log(
      "child",
      child.split("/").pop(),
      "locs",
      locs.length,
      "hoteldetail",
      hoteldetail.length,
      "dest",
      destinations.length
    );
  }

  // Also scan bin sitemapindex for hoteldetail-looking child names
  const bin = await fetchText("https://www.ihg.com/bin/sitemapindex.xml");
  const binLocs = extractLocs(bin.text);
  const interesting = binLocs.filter((u) =>
    /hotel|destination|property|brand|holiday|indigo|kimpton|crowne|voco|intercontinental/i.test(u)
  );
  out.binInteresting = interesting.slice(0, 80);
  console.log("bin interesting:", interesting.length);
  for (const u of interesting.slice(0, 40)) console.log(" ", u);

  // Probe destination Mexico HTML for URL structure
  const mex = await fetchText("https://www.ihg.com/destinations/us/en/mexico-hotels");
  const detailUrls = [
    ...mex.text.matchAll(/href="([^"]*hoteldetail[^"]*)"/gi),
  ].map((m) => m[1]);
  const abs = uniq(
    detailUrls.map((u) => (u.startsWith("http") ? u : `https://www.ihg.com${u.startsWith("/") ? "" : "/"}${u}`))
  );
  out.mexicoHoteldetailSample = abs.slice(0, 20);
  out.mexicoHoteldetailCount = abs.length;

  // Parse hotel cards more carefully
  const cards = [];
  const cardRe =
    /data-hotel-mnemonic="([A-Z0-9]+)"[^>]*data-hotel-countryCode="([A-Z]{2})"[^>]*data-filters="([^"]*)"/gi;
  let m;
  while ((m = cardRe.exec(mex.text))) {
    cards.push({ mnemonic: m[1], countryCode: m[2], filters: m[3] });
  }
  // Also try alternate attribute order
  const cardRe2 =
    /data-hotel-countryCode="([A-Z]{2})"[^>]*data-hotel-mnemonic="([A-Z0-9]+)"/gi;
  while ((m = cardRe2.exec(mex.text))) {
    cards.push({ mnemonic: m[2], countryCode: m[1], filters: "" });
  }
  out.mexicoCards = cards.slice(0, 30);
  out.mexicoCardCount = cards.length;
  console.log("mexico hoteldetail unique", abs.length, "cards", cards.length);
  console.log("sample urls", abs.slice(0, 5));

  // Try GraphQL with API key from public page config
  const gqlUrl = "https://apis.ihg.com/graphql/v1/hotels";
  const apiKey = "hsL8GAAz7drdIbEjBCejdA1Ud2MVhxoo";
  try {
    const r = await fetch(gqlUrl, {
      method: "POST",
      headers: {
        "User-Agent": UA,
        "Content-Type": "application/json",
        Accept: "application/json",
        Origin: "https://www.ihg.com",
        Referer: "https://www.ihg.com/explore",
        apikey: apiKey,
        "x-ihg-api-key": apiKey,
      },
      body: JSON.stringify({
        query: `query Hotel($mnemonic: String!) {
          hotel(mnemonic: $mnemonic) {
            mnemonic
            name
            brandCode
          }
        }`,
        variables: { mnemonic: "SDQHI" },
      }),
    });
    const text = await r.text();
    out.graphqlWithKey = { status: r.status, head: text.slice(0, 800) };
    console.log("graphql with key", r.status, text.slice(0, 300));
  } catch (err) {
    out.graphqlWithKey = { error: String(err?.message || err) };
    console.log("graphql with key ERR", err.message);
  }

  writeFileSync(join(REPORTS, "ihg-sitemap-structure-probe.json"), JSON.stringify(out, null, 2));
  console.log("wrote reports/ihg-sitemap-structure-probe.json");
}

function uniq(arr) {
  return [...new Set(arr.filter(Boolean))];
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
