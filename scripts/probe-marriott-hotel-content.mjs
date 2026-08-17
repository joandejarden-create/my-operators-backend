#!/usr/bin/env node
/**
 * Probe Marriott hotel content endpoints for overview + amenities.
 */
import {
  MARRIOTT_FETCH_HEADERS,
  MARRIOTT_ORIGIN,
  parseNextDataFromHtml,
} from "../lib/marriott-brand-directory-extract.js";

const MARSHA = process.argv[2] || "POPLC";
const SLUG =
  process.argv[3] ||
  "poplc-the-ocean-club-a-luxury-collection-resort-costa-norte";
const overviewUrl = `${MARRIOTT_ORIGIN}/en-us/hotels/${SLUG}/overview/`;

async function getSession() {
  const res = await fetch(`${MARRIOTT_ORIGIN}/mi/phoenix-gateway/session`, {
    headers: {
      ...MARRIOTT_FETCH_HEADERS,
      Accept: "application/json, text/plain, */*",
      Origin: MARRIOTT_ORIGIN,
      Referer: `${MARRIOTT_ORIGIN}/default.mi`,
    },
  });
  const json = JSON.parse(await res.text());
  const cookies = (res.headers.getSetCookie?.() || []).map((c) => c.split(";")[0]).join("; ");
  return { token: json.sessionToken, cookies };
}

function walkFind(obj, pred, depth = 0, out = []) {
  if (!obj || depth > 16) return out;
  if (pred(obj)) out.push(obj);
  if (Array.isArray(obj)) {
    for (const x of obj) walkFind(x, pred, depth + 1, out);
  } else if (obj && typeof obj === "object") {
    for (const v of Object.values(obj)) walkFind(v, pred, depth + 1, out);
  }
  return out;
}

const { token, cookies } = await getSession();
console.log("session ok", token?.slice(0, 8), "cookies", cookies.length);

const headers = {
  ...MARRIOTT_FETCH_HEADERS,
  Cookie: cookies,
  Origin: MARRIOTT_ORIGIN,
  Referer: overviewUrl,
  "Accept-Language": "en-US,en;q=0.9",
};

const attempts = [
  ["overview html", overviewUrl, { headers: { ...headers, Accept: "text/html" } }],
  [
    "overview graphql v1",
    `${MARRIOTT_ORIGIN}/v1/graph/query`,
    {
      method: "POST",
      headers: { ...headers, "Content-Type": "application/json", sessionToken: token },
      body: JSON.stringify({
        operationName: "phoenixHotelOverview",
        variables: { propertyCode: MARSHA, locale: "en-US" },
        query: `query phoenixHotelOverview($propertyCode: String!, $locale: String!) {
          property(propertyCode: $propertyCode, locale: $locale) {
            name
            description
            overview
            amenities { name category }
          }
        }`,
      }),
    },
  ],
  [
    "mi query property",
    `${MARRIOTT_ORIGIN}/mi/query`,
    {
      method: "POST",
      headers: { ...headers, "Content-Type": "application/json", sessionToken: token },
      body: JSON.stringify({
        operationName: "PropertyOverview",
        variables: { marshaCode: MARSHA },
        query: `query PropertyOverview($marshaCode: String!) {
          hotel(marshaCode: $marshaCode) {
            name
            overview
            amenities
          }
        }`,
      }),
    },
  ],
  [
    "aem content dam",
    `https://www.marriott.com/content/dam/marriott-digital/mi/global/en_us/hotels/${MARSHA.toLowerCase()}.en-us.json`,
    { headers },
  ],
  [
    "cache property",
    `https://cache.marriott.com/content/marriottdata/en-us/hotels/${MARSHA.toLowerCase()}.json`,
    { headers: { ...MARRIOTT_FETCH_HEADERS, Accept: "application/json" } },
  ],
  [
    "hotel-info mi",
    `${MARRIOTT_ORIGIN}/mi/hotelinformation/${MARSHA}.json`,
    { headers: { ...headers, Accept: "application/json" } },
  ],
];

for (const [label, url, init] of attempts) {
  try {
    const res = await fetch(url, init);
    const text = await res.text();
    console.log(`\n=== ${label} ===`);
    console.log("status", res.status, "len", text.length, "denied", /access denied/i.test(text));
    if (res.status === 200 && text.startsWith("{")) {
      const j = JSON.parse(text);
      console.log("json keys", Object.keys(j).slice(0, 10));
      const overviewHits = walkFind(j, (o) =>
        typeof o?.overview === "string" && o.overview.length > 40
      );
      const amenityHits = walkFind(
        j,
        (o) => Array.isArray(o?.amenities) && o.amenities.length > 3
      );
      console.log("overview hits", overviewHits.length, overviewHits[0]?.overview?.slice(0, 120));
      console.log("amenity hits", amenityHits.length, amenityHits[0]?.amenities?.slice(0, 5));
    } else if (/<script id="__NEXT_DATA__"/i.test(text)) {
      const d = parseNextDataFromHtml(text);
      const str = JSON.stringify(d?.props?.pageProps || {});
      console.log("pageProps len", str.length);
      console.log("has overview word", /overview/i.test(str));
      console.log("has amenities word", /amenit/i.test(str));
      const m = str.match(/15-minute drive from Puerto Plata/i);
      console.log("sample overview text found", Boolean(m));
    } else {
      console.log(text.slice(0, 280).replace(/\s+/g, " "));
    }
  } catch (e) {
    console.log(`\n=== ${label} === ERR`, e.message);
  }
}
