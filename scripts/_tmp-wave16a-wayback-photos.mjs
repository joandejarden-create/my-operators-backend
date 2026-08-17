import fs from "node:fs";

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
    brand: "fairfield-by-marriott",
    marsha: "MIAFO",
    slug: "miafo-fairfield-inn-and-suites-miami-airport-west-doral",
    name: "Fairfield Inn & Suites Miami Airport West/Doral",
    market: "Miami, USA",
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
    brand: "four-points-by-sheraton",
    marsha: "NYCFM",
    slug: "nycfm-four-points-midtown-new-york",
    name: "Four Points by Sheraton Midtown New York",
    market: "New York, USA",
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
  {
    brand: "delta-hotels-by-marriott",
    marsha: "YVRDL",
    slug: "yvrdl-delta-hotels-vancouver-downtown",
    name: "Delta Hotels by Marriott Vancouver Downtown",
    market: "Vancouver, Canada",
    geographyLabel: "International Reference",
  },
];

function collapse(urls) {
  const byBase = new Map();
  for (const raw of urls) {
    let u = String(raw || "").replace(/\\u002F/g, "/").replace(/\\/g, "");
    if (!/^https?:/i.test(u)) continue;
    if (!/cache\.marriott\.com/i.test(u)) continue;
    if (/favicon|logo|1x1|pixel|spacer/i.test(u)) continue;
    const bare = u.split("?")[0];
    const base = bare.replace(/:(Feature-Hor|Wide-Hor|Square|Classic-Hor|Pano-Hor).*$/i, "");
    if (!byBase.has(base)) {
      const out = /marriotts7prod/i.test(base)
        ? `${base}:Wide-Hor?wid=1600&fit=constrain`
        : bare;
      byBase.set(base, out);
    }
  }
  return [...byBase.values()];
}

async function cdxSnapshots(path) {
  const url = `http://web.archive.org/cdx/search/cdx?url=www.marriott.com${path}&output=json&filter=statuscode:200&limit=8&fl=timestamp,original,statuscode`;
  const r = await fetch(url, { headers: { "User-Agent": "Mozilla/5.0 DealalityBot" } });
  const j = await r.json().catch(() => []);
  if (!Array.isArray(j) || j.length < 2) return [];
  return j.slice(1).map((row) => ({ ts: row[0], original: row[1] }));
}

async function harvestProperty(p) {
  const path = `/en-us/hotels/${p.slug}/overview/`;
  let snaps = await cdxSnapshots(path);
  if (!snaps.length) {
    // try without en-us
    snaps = await cdxSnapshots(`/hotels/travel/${p.slug}/`);
  }
  console.log(p.marsha, "snaps", snaps.length);
  const images = new Set();
  for (const snap of snaps.slice(0, 4)) {
    const wayback = `https://web.archive.org/web/${snap.ts}id_/https://www.marriott.com${path}`;
    try {
      const r = await fetch(wayback, {
        headers: { "User-Agent": "Mozilla/5.0 DealalityBot" },
        redirect: "follow",
      });
      const html = await r.text();
      const urls = [
        ...html.matchAll(/https?:\/\/cache\.marriott\.com\/[^\"'\\s>]+/g),
      ].map((m) => m[0]);
      const collapsed = collapse(urls);
      console.log(" ", snap.ts, r.status, "raw", urls.length, "uniq", collapsed.length);
      for (const u of collapsed) images.add(u);
      if (images.size >= 8) break;
    } catch (e) {
      console.log(" ", snap.ts, "ERR", e.message);
    }
  }
  return {
    ...p,
    sourcePageUrl: `https://www.marriott.com/en-us/hotels/${p.slug}/overview/`,
    images: [...images],
  };
}

const out = [];
for (const p of PROPERTIES) {
  out.push(await harvestProperty(p));
}

fs.writeFileSync(
  "reports/_tmp-wave16a-stage2b-wayback-photos.json",
  JSON.stringify(out, null, 2)
);
for (const r of out) {
  console.log("SUMMARY", r.marsha, r.images.length, r.name);
}
