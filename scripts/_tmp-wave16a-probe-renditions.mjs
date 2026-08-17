import fs from "node:fs";

const HEADERS = {
  "User-Agent":
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
  Accept: "image/*,*/*",
};

const PROPERTIES = [
  {
    brand: "fairfield-by-marriott",
    marsha: "CUNFI",
    name: "Fairfield Inn & Suites Cancun Airport",
    market: "Cancún, Mexico",
    geographyLabel: "CALA",
    sourcePageUrl:
      "https://www.marriott.com/en-us/hotels/cunfi-fairfield-inn-and-suites-cancun-airport/overview/",
  },
  {
    brand: "fairfield-by-marriott",
    marsha: "NYCTS",
    name: "Fairfield Inn & Suites New York Manhattan/Times Square South",
    market: "New York, USA",
    geographyLabel: "International Reference",
    sourcePageUrl:
      "https://www.marriott.com/en-us/hotels/nycts-fairfield-inn-and-suites-new-york-manhattan-times-square-south/overview/",
  },
  {
    brand: "fairfield-by-marriott",
    marsha: "MIAFO",
    name: "Fairfield Inn & Suites Miami Airport West/Doral",
    market: "Miami, USA",
    geographyLabel: "International Reference",
    sourcePageUrl:
      "https://www.marriott.com/en-us/hotels/miafo-fairfield-inn-and-suites-miami-airport-west-doral/overview/",
  },
  {
    brand: "four-points-by-sheraton",
    marsha: "BOGFP",
    name: "Four Points by Sheraton Bogota",
    market: "Bogotá, Colombia",
    geographyLabel: "CALA",
    sourcePageUrl: "https://www.marriott.com/en-us/hotels/bogfp-four-points-bogota/overview/",
  },
  {
    brand: "four-points-by-sheraton",
    marsha: "MIAFA",
    name: "Four Points by Sheraton Miami Airport",
    market: "Miami, USA",
    geographyLabel: "International Reference",
    sourcePageUrl: "https://www.marriott.com/en-us/hotels/miafa-four-points-miami-airport/overview/",
  },
  {
    brand: "four-points-by-sheraton",
    marsha: "SDQFP",
    name: "Four Points by Sheraton Santo Domingo",
    market: "Santo Domingo, Dominican Republic",
    geographyLabel: "CALA",
    sourcePageUrl:
      "https://www.marriott.com/en-us/hotels/sdqfp-four-points-santo-domingo/overview/",
  },
  {
    brand: "delta-hotels-by-marriott",
    marsha: "YYZDA",
    name: "Delta Hotels Toronto Airport & Conference Centre",
    market: "Toronto, Canada",
    geographyLabel: "International Reference",
    sourcePageUrl:
      "https://www.marriott.com/en-us/hotels/yyzda-delta-hotels-toronto-airport-and-conference-centre/overview/",
  },
  {
    brand: "delta-hotels-by-marriott",
    marsha: "YVRDL",
    name: "Delta Hotels Vancouver Downtown",
    market: "Vancouver, Canada",
    geographyLabel: "International Reference",
    sourcePageUrl:
      "https://www.marriott.com/en-us/hotels/yvrdl-delta-hotels-vancouver-downtown/overview/",
  },
  {
    brand: "delta-hotels-by-marriott",
    marsha: "YYCDL",
    name: "Delta Hotels Calgary Downtown",
    market: "Calgary, Canada",
    geographyLabel: "International Reference",
    sourcePageUrl:
      "https://www.marriott.com/en-us/hotels/yycdl-delta-hotels-calgary-downtown/overview/",
  },
];

const ROLES = [
  ["exterior", "exterior_arrival"],
  ["lobby", "public_space_lobby"],
  ["guestroom", "guest_room"],
  ["guest-room", "guest_room"],
  ["king-guestroom", "guest_room"],
  ["suite", "guest_room_suite"],
  ["pool", "amenity_pool"],
  ["fitness", "amenity_fitness"],
  ["restaurant", "food_beverage_experience"],
  ["bar", "food_beverage_experience"],
  ["breakfast", "food_beverage_experience"],
  ["meeting", "meeting_space"],
  ["ballroom", "meeting_space"],
  ["dining", "food_beverage_experience"],
  ["reception", "public_space_lobby"],
  ["aerial", "property_setting"],
];

const ID_GUESSES = [4001, 4002, 4003, 4004, 4005, 1000, 1001, 2000, 2001, 3000, 3001, 5000, 5001, 7590, 7592, 5444, 8860, 8774];

async function headOk(url) {
  try {
    const r = await fetch(url, { method: "HEAD", headers: HEADERS, redirect: "follow" });
    const ct = r.headers.get("content-type") || "";
    return r.ok && /image/i.test(ct);
  } catch {
    return false;
  }
}

const hits = [];
for (const p of PROPERTIES) {
  const code = p.marsha.toLowerCase();
  const found = [];
  for (const [desc, role] of ROLES) {
    for (const id of ID_GUESSES) {
      const variants = [
        `https://cache.marriott.com/content/dam/marriott-renditions/${p.marsha}/${code}-${desc}-${id}-hor-wide.jpg`,
        `https://cache.marriott.com/content/dam/marriott-renditions/${p.marsha}/${code}-${desc}-${id}-hor-feat.jpg`,
        `https://cache.marriott.com/content/dam/marriott-renditions/${p.marsha}/${code}-${desc}-${id}-sq.jpg`,
      ];
      for (const url of variants) {
        if (await headOk(url)) {
          found.push({
            ...p,
            propertyKey: code,
            propertyName: p.name,
            marketCity: p.market.split(",")[0],
            imageUrl: url,
            label: "property_rendition",
            role,
          });
          console.log("HIT", p.marsha, role, url.split("/").pop());
        }
      }
    }
  }
  console.log(p.marsha, "found", found.length);
  hits.push(...found);
}

fs.writeFileSync(
  "reports/_tmp-wave16a-stage2b-rendition-hits.json",
  JSON.stringify(hits, null, 2)
);
console.log("TOTAL", hits.length);
