import {
  MARRIOTT_FETCH_HEADERS,
  MARRIOTT_ORIGIN,
} from "../lib/marriott-brand-directory-extract.js";

const PROPERTIES = [
  {
    brand: "fairfield-by-marriott",
    marsha: "CUNFO",
    slug: "cunfo-fairfield-cancun-airport",
    name: "Fairfield by Marriott Cancun Airport",
    market: "Cancún, Mexico",
    geographyLabel: "CALA",
  },
  {
    brand: "fairfield-by-marriott",
    marsha: "SDQFO",
    slug: "sdqfo-fairfield-santo-domingo",
    name: "Fairfield by Marriott Santo Domingo",
    market: "Santo Domingo, Dominican Republic",
    geographyLabel: "CALA",
  },
  {
    brand: "fairfield-by-marriott",
    marsha: "NYCTS",
    slug: "nycts-fairfield-inn-and-suites-new-york-manhattan-times-square-south",
    name: "Fairfield Inn & Suites New York Manhattan/Times Square South",
    market: "New York, USA",
    geographyLabel: "International Reference",
  },
  {
    brand: "four-points-by-sheraton",
    marsha: "CUNFP",
    slug: "cunfp-four-points-cancun-centro",
    name: "Four Points by Sheraton Cancun Centro",
    market: "Cancún, Mexico",
    geographyLabel: "CALA",
  },
  {
    brand: "four-points-by-sheraton",
    marsha: "BOGFP",
    slug: "bogfp-four-points-bogota",
    name: "Four Points by Sheraton Bogota",
    market: "Bogotá, Colombia",
    geographyLabel: "CALA",
  },
  {
    brand: "four-points-by-sheraton",
    marsha: "MIAFP",
    slug: "miafp-four-points-miami-airport",
    name: "Four Points by Sheraton Miami Airport",
    market: "Miami, USA",
    geographyLabel: "International Reference",
  },
  {
    brand: "delta-hotels-by-marriott",
    marsha: "CUNDL",
    slug: "cundl-delta-hotels-cancun-inn",
    name: "Delta Hotels Cancun Inn",
    market: "Cancún, Mexico",
    geographyLabel: "CALA",
  },
  {
    brand: "delta-hotels-by-marriott",
    marsha: "MEXDL",
    slug: "mexdl-delta-hotels-mexico-city-metropolitan",
    name: "Delta Hotels by Marriott Mexico City Metropolitan",
    market: "Mexico City, Mexico",
    geographyLabel: "CALA",
  },
  {
    brand: "delta-hotels-by-marriott",
    marsha: "YYZDL",
    slug: "yyzdl-delta-hotels-toronto-airport-and-conference-centre",
    name: "Delta Hotels by Marriott Toronto Airport & Conference Centre",
    market: "Toronto, Canada",
    geographyLabel: "International Reference",
  },
];

function walkUrls(obj, out = [], depth = 0) {
  if (!obj || depth > 24) return out;
  if (typeof obj === "string") {
    if (/cache\.marriott\.com|marriott\.com\/.*\.(jpg|jpeg|png|webp)/i.test(obj)) out.push(obj);
    return out;
  }
  if (Array.isArray(obj)) {
    for (const x of obj) walkUrls(x, out, depth + 1);
    return out;
  }
  if (typeof obj === "object") {
    for (const v of Object.values(obj)) walkUrls(v, out, depth + 1);
  }
  return out;
}

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

const { token, cookies } = await getSession();
console.log("session", Boolean(token), "cookieLen", cookies.length);

const headers = {
  ...MARRIOTT_FETCH_HEADERS,
  Cookie: cookies,
  Origin: MARRIOTT_ORIGIN,
  "Accept-Language": "en-US,en;q=0.9",
  sessionToken: token,
  "Content-Type": "application/json",
};

const results = [];
for (const p of PROPERTIES) {
  const overviewUrl = `${MARRIOTT_ORIGIN}/en-us/hotels/${p.slug}/overview/`;
  const attempts = [];

  // HTML with session cookie
  try {
    const r = await fetch(overviewUrl, {
      headers: { ...headers, Accept: "text/html", Referer: overviewUrl },
    });
    const html = await r.text();
    const urls = [
      ...html.matchAll(/https?:\/\/cache\.marriott\.com\/[^\"'\\s>]+/g),
    ].map((m) => m[0]);
    attempts.push({ kind: "html", status: r.status, urls: [...new Set(urls)].slice(0, 20) });
  } catch (e) {
    attempts.push({ kind: "html", error: e.message });
  }

  // GraphQL photo-ish queries
  const queries = [
    {
      name: "photos",
      body: {
        operationName: "phoenixHotelPhotos",
        variables: { propertyCode: p.marsha, locale: "en-US" },
        query: `query phoenixHotelPhotos($propertyCode: String!, $locale: String!) {
          property(propertyCode: $propertyCode, locale: $locale) {
            name
            photos { url category caption title }
            images { url category caption }
            media { url type category }
          }
        }`,
      },
    },
    {
      name: "overview",
      body: {
        operationName: "phoenixHotelOverview",
        variables: { propertyCode: p.marsha, locale: "en-US" },
        query: `query phoenixHotelOverview($propertyCode: String!, $locale: String!) {
          property(propertyCode: $propertyCode, locale: $locale) {
            name
            primaryImage { url }
            heroImage { url }
            images { url category }
          }
        }`,
      },
    },
  ];

  for (const q of queries) {
    try {
      const r = await fetch(`${MARRIOTT_ORIGIN}/mi/query`, {
        method: "POST",
        headers: { ...headers, Referer: overviewUrl },
        body: JSON.stringify(q.body),
      });
      const text = await r.text();
      let json = null;
      try {
        json = JSON.parse(text);
      } catch {
        json = { raw: text.slice(0, 300) };
      }
      const urls = walkUrls(json);
      attempts.push({
        kind: q.name,
        status: r.status,
        urls: [...new Set(urls)].slice(0, 20),
        errors: json?.errors?.map((e) => e.message) || null,
      });
    } catch (e) {
      attempts.push({ kind: q.name, error: e.message });
    }
  }

  console.log(
    p.marsha,
    attempts.map((a) => `${a.kind}:${a.status || "err"}:${(a.urls || []).length}`).join(" | ")
  );
  for (const a of attempts) {
    for (const u of (a.urls || []).slice(0, 5)) console.log("  ", a.kind, u.slice(0, 140));
  }
  results.push({ ...p, attempts });
}

import fs from "node:fs";
fs.writeFileSync(
  "reports/_tmp-wave16a-stage2b-property-photos.json",
  JSON.stringify(results, null, 2)
);
