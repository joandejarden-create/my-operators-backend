#!/usr/bin/env node
/**
 * Probe Marriott GraphQL / property media endpoints for Wave 16A Stage 2B.
 */
import fs from "node:fs";

const UA =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36";

const CODES = [
  "CUNFI",
  "NYCTS",
  "MIAFO",
  "MEXFO",
  "BOGFO",
  "BOGFP",
  "MIAFA",
  "SDQFP",
  "CUNFP",
  "LAXFP",
  "YYZDA",
  "YVRDL",
  "YYCDL",
  "CUNDL",
  "MEXDL",
];

const endpoints = (code) => [
  `https://www.marriott.com/mi/query/v1/graphql`,
  `https://www.marriott.com/content/data/${code}.json`,
  `https://www.marriott.com/mi/cloud/data/propertyDetails?propertyCode=${code}`,
  `https://www.marriott.com/en-us/hotels/${code.toLowerCase()}/photos/`,
  `https://www.marriott.com/en-us/hotels/${code.toLowerCase()}-x/overview/`,
];

async function tryGet(url, opts = {}) {
  try {
    const r = await fetch(url, {
      method: opts.method || "GET",
      headers: {
        "User-Agent": UA,
        Accept: opts.accept || "application/json,text/html,*/*",
        "Content-Type": opts.body ? "application/json" : undefined,
        ...(opts.headers || {}),
      },
      body: opts.body ? JSON.stringify(opts.body) : undefined,
      redirect: "follow",
    });
    const ct = r.headers.get("content-type") || "";
    const text = await r.text();
    return { status: r.status, ct, len: text.length, text: text.slice(0, 2000) };
  } catch (e) {
    return { status: 0, error: e.message };
  }
}

const out = {};

for (const code of CODES.slice(0, 3)) {
  out[code] = {};
  for (const url of endpoints(code).slice(1)) {
    const res = await tryGet(url);
    console.log(code, url.slice(0, 90), res.status, res.len || 0, (res.ct || "").slice(0, 40));
    out[code][url] = { status: res.status, len: res.len, ct: res.ct, sample: res.text?.slice(0, 300) };
  }
}

// GraphQL hotel media query (common pattern)
const gql = {
  operationName: "hotelMedia",
  variables: { propertyId: "BOGFP" },
  query: `query hotelMedia($propertyId: String!) {
    property(id: $propertyId) {
      id
      name
      media {
        primaryImage { edges { node { title imageUrls { wideHorizontal classicHorizontal square } } } }
        images { edges { node { title alternateDescription imageUrls { wideHorizontal classicHorizontal square } } } }
      }
    }
  }`,
};

const gqlRes = await tryGet("https://www.marriott.com/mi/query/v1/graphql", {
  method: "POST",
  body: gql,
  accept: "application/json",
  headers: { "x-request-id": "wave16a-stage2b", Origin: "https://www.marriott.com", Referer: "https://www.marriott.com/" },
});
console.log("gql", gqlRes.status, gqlRes.len, gqlRes.text?.slice(0, 500));
out.graphql = gqlRes;

// Also try Wayback CDX for a few property pages
async function cdx(url) {
  const api = `https://web.archive.org/cdx/search/cdx?url=${encodeURIComponent(url)}&output=json&fl=timestamp,original,statuscode,mimetype&filter=statuscode:200&limit=5`;
  const r = await fetch(api, { headers: { "User-Agent": UA } });
  const j = await r.json();
  return j;
}

for (const url of [
  "https://www.marriott.com/en-us/hotels/bogfp-four-points-bogota/overview/",
  "https://www.marriott.com/en-us/hotels/miafa-four-points-miami-airport/overview/",
  "https://www.marriott.com/en-us/hotels/yvrdl-delta-hotels-vancouver-downtown/overview/",
  "https://www.marriott.com/en-us/hotels/nycts-fairfield-inn-and-suites-new-york-manhattan-times-square-south/overview/",
]) {
  try {
    const rows = await cdx(url);
    console.log("cdx", url.split("/hotels/")[1]?.slice(0, 40), "rows", rows.length - 1);
    out[`cdx:${url}`] = rows.slice(0, 6);
  } catch (e) {
    console.log("cdx err", e.message);
  }
}

fs.writeFileSync("reports/_tmp-wave16a-stage2b-api-probe.json", JSON.stringify(out, null, 2));
