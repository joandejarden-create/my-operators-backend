import fs from "fs";
import { fetchHiltonLocationsPage, brandIndexUrl } from "../lib/hilton-brand-directory-extract.js";

const { pageData } = (await fetchHiltonLocationsPage(brandIndexUrl("curio-collection"))).pageData
  ? await fetchHiltonLocationsPage(brandIndexUrl("curio-collection"))
  : {};

// re-fetch properly
const page = await fetchHiltonLocationsPage(brandIndexUrl("curio-collection"));
const data = JSON.parse(
  (await (await fetch(brandIndexUrl("curio-collection"), {
    headers: {
      "User-Agent":
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
      Accept: "text/html",
    },
  })).text()).match(/<script id="__NEXT_DATA__"[^>]*>([\s\S]*?)<\/script>/i)[1]
);

const queries = data.props?.pageProps?.dehydratedState?.queries || [];
console.log("queries", queries.length);

function walk(obj, path = "", hits = [], depth = 0) {
  if (depth > 8 || !obj || typeof obj !== "object") return hits;
  if (Array.isArray(obj)) {
    for (const item of obj.slice(0, 3)) walk(item, path, hits, depth + 1);
    return hits;
  }
  for (const [k, v] of Object.entries(obj)) {
    const p = path ? `${path}.${k}` : k;
    if (typeof v === "string" && v.length > 80 && /1930|pedestrian|National Theatre|fitness center/i.test(v)) {
      hits.push({ path: p, v: v.slice(0, 300) });
    }
    if (/description|overview/i.test(k) && typeof v === "string" && v.length > 40) {
      hits.push({ path: p, v: v.slice(0, 300) });
    }
    walk(v, p, hits, depth + 1);
  }
  return hits;
}

const textHits = walk(data);
console.log("Text hits in locations __NEXT_DATA__:", textHits.length);
textHits.slice(0, 5).forEach((h) => console.log(h.path, h.v));

// Try graphql with hotel prop code
const gqlUrl = "https://www.hilton.com/graphql/customer";
const gran = page.pageData.hotelSummaryOptions.hotels.find((h) => h.ctyhocn === "SJOCUQQ");
console.log("\nGran ctyhocn", gran?.ctyhocn, gran?.facilityOverview?.homeUrlTemplate);

const attempts = [
  {
    operationName: "hotel",
    query: `query hotel($ctyhocn: String!, $language: String!) { hotel(ctyhocn: $ctyhocn, language: $language) { name overview { shortDescription longDescription } } }`,
    variables: { ctyhocn: "SJOCUQQ", language: "en" },
  },
  {
    operationName: "property",
    query: `query property($propCode: String!) { property(propCode: $propCode) { name description } }`,
    variables: { propCode: "SJOCUQQ" },
  },
];

for (const body of attempts) {
  try {
    const res = await fetch(gqlUrl, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 Chrome/124",
        Accept: "application/json",
        Origin: "https://www.hilton.com",
        Referer: "https://www.hilton.com/en/hotels/sjocuqq-gran-hotel-costa-rica/",
      },
      body: JSON.stringify(body),
    });
    const text = await res.text();
    console.log(`\nGQL ${body.operationName} [${res.status}]`, text.slice(0, 500));
  } catch (e) {
    console.log("GQL error", e.message);
  }
}

fs.writeFileSync("reports/_hilton-gql-probe.json", JSON.stringify({ queryCount: queries.length, textHits }, null, 2));
