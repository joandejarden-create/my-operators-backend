#!/usr/bin/env node
/**
 * Discover official IHG directory/sitemap/API sources for census Phase 0.
 */
import { writeFileSync, mkdirSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPORTS = join(__dirname, "..", "reports");
const UA =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36";

async function fetchText(url, opts = {}) {
  const r = await fetch(url, {
    redirect: "follow",
    headers: {
      "User-Agent": UA,
      Accept: opts.accept || "*/*",
      "Accept-Language": "en-US,en;q=0.9",
      ...(opts.headers || {}),
    },
  });
  const text = await r.text();
  return { status: r.status, url: r.url, text, headers: Object.fromEntries(r.headers) };
}

function uniq(arr) {
  return [...new Set(arr.filter(Boolean))];
}

async function main() {
  mkdirSync(REPORTS, { recursive: true });
  const out = { generatedAt: new Date().toISOString(), probes: {} };

  const robots = await fetchText("https://www.ihg.com/robots.txt");
  out.probes.robots = {
    status: robots.status,
    sitemapLines: robots.text.split(/\r?\n/).filter((l) => /sitemap/i.test(l)),
    text: robots.text,
  };
  console.log("robots sitemaps:", out.probes.robots.sitemapLines);

  for (const sm of out.probes.robots.sitemapLines) {
    const url = sm.replace(/^Sitemap:\s*/i, "").trim();
    if (!url) continue;
    const r = await fetchText(url);
    out.probes[`sitemap:${url}`] = {
      status: r.status,
      len: r.text.length,
      head: r.text.slice(0, 400),
      locCount: [...r.text.matchAll(/<loc>/gi)].length,
    };
    console.log("sitemap", url, r.status, "locs", out.probes[`sitemap:${url}`].locCount);
  }

  const explore = await fetchText("https://www.ihg.com/explore");
  const t = explore.text;
  out.probes.explore = {
    status: explore.status,
    len: t.length,
    graphql: uniq([...t.matchAll(/https?:\/\/[^"'\\\s]*apis\.ihg\.com[^"'\\\s]*/gi)].map((m) => m[0])).slice(0, 30),
    hotelMnemonics: uniq([...t.matchAll(/data-hotel-mnemonic="([A-Z0-9]+)"/gi)].map((m) => m[1])).slice(0, 50),
    countryCodes: uniq([...t.matchAll(/data-hotel-countryCode="([A-Z]{2})"/gi)].map((m) => m[1])).slice(0, 40),
    scripts: uniq([...t.matchAll(/src=["']([^"']+\.js[^"']*)/gi)].map((m) => m[1])).slice(0, 25),
  };
  console.log("explore graphql", out.probes.explore.graphql);
  console.log("explore mnemonics sample", out.probes.explore.hotelMnemonics.slice(0, 10));

  // Probe GraphQL endpoint with introspection-ish and hotel lookup
  const gqlUrl = "https://apis.ihg.com/graphql/v1/hotels";
  const gqlBodies = [
    {
      name: "hotelByMnemonic",
      body: {
        query: `query($mnemonic:String!){ hotel(mnemonic:$mnemonic){ mnemonic name brandCode address { city countryCode } } }`,
        variables: { mnemonic: "SDQHI" },
      },
    },
    {
      name: "hotelsSearch",
      body: {
        query: `query { hotels(destination:"Mexico", limit:5){ mnemonic name } }`,
      },
    },
    {
      name: "simplePing",
      body: { query: "{ __typename }" },
    },
  ];

  out.probes.graphql = {};
  for (const g of gqlBodies) {
    try {
      const r = await fetch(gqlUrl, {
        method: "POST",
        headers: {
          "User-Agent": UA,
          "Content-Type": "application/json",
          Accept: "application/json",
          Origin: "https://www.ihg.com",
          Referer: "https://www.ihg.com/explore",
        },
        body: JSON.stringify(g.body),
      });
      const text = await r.text();
      out.probes.graphql[g.name] = {
        status: r.status,
        head: text.slice(0, 500),
      };
      console.log("gql", g.name, r.status, text.slice(0, 200));
    } catch (err) {
      out.probes.graphql[g.name] = { error: err.message };
      console.log("gql", g.name, "ERR", err.message);
    }
  }

  // Destination / destination pages often list hotels
  const destUrls = [
    "https://www.ihg.com/destinations/us/en/mexico-hotels",
    "https://www.ihg.com/destinations/us/en/dominican-republic-hotels",
    "https://www.ihg.com/holidayinn/hotels/us/en/reservation",
    "https://www.ihg.com/content/us/en/customer-care/sitemap.html",
  ];
  for (const url of destUrls) {
    const r = await fetchText(url);
    const mnemonics = uniq([...r.text.matchAll(/data-hotel-mnemonic="([A-Z0-9]+)"/gi)].map((m) => m[1]));
    const hoteldetail = uniq([
      ...r.text.matchAll(/https?:\/\/www\.ihg\.com\/[^"'\\\s]+\/hoteldetail/gi),
      ...r.text.matchAll(/\/[a-z0-9-]+\/hotels\/us\/en\/[^"'\\\s]+\/hoteldetail/gi),
    ].map((m) => m[0]));
    out.probes[`page:${url}`] = {
      status: r.status,
      finalUrl: r.url,
      len: r.text.length,
      mnemonicCount: mnemonics.length,
      mnemonics: mnemonics.slice(0, 20),
      hoteldetailSample: hoteldetail.slice(0, 10),
    };
    console.log("page", url, r.status, "mnemonics", mnemonics.length, "hoteldetail", hoteldetail.length);
  }

  const path = join(REPORTS, "ihg-directory-source-probe.json");
  writeFileSync(path, JSON.stringify(out, null, 2));
  console.log("Wrote", path);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
