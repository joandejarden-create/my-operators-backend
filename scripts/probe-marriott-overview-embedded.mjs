#!/usr/bin/env node
const SLUG = "poplc-the-ocean-club-a-luxury-collection-resort-costa-norte";
const pages = ["experiences", "rooms", "dining", "events", "reviews"];
const H = {
  "User-Agent":
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
  Accept: "text/html",
};

for (const p of pages) {
  const html = await fetch(`https://www.marriott.com/en-us/hotels/${SLUG}/${p}/`, { headers: H }).then(
    (r) => r.text()
  );
  console.log("\n", p, html.length);
  for (const needle of [
    "15-minute drive",
    "private beach",
    "Free high-speed internet",
    "Pet friendly",
    "Airport shuttle",
    "Meeting event space",
    "Housekeeping service daily",
    "overviewDescription",
    "hotelOverview",
    "featuredAmenities",
    "amenitiesOverview",
  ]) {
    if (html.includes(needle)) console.log("  FOUND", needle);
  }
}

// Search for escaped unicode overview in any script
const exp = await fetch(`https://www.marriott.com/en-us/hotels/${SLUG}/experiences/`, { headers: H }).then(
  (r) => r.text()
);
const escaped = [...exp.matchAll(/\\u003c[^\\]{0,20}Overview|\\"overview\\"|overviewText/gi)].slice(0, 10);
console.log("\nescaped overview refs", escaped.length);

// Try marriott catalog API
for (const url of [
  "https://www.marriott.com/mi/query/phoenixShopPropertyByMarsha",
  "https://www.marriott.com/mi/query/phoenixShopPropertyDetails",
]) {
  const body = {
    operationName: "phoenixShopPropertyDetails",
    variables: { marshaCode: "POPLC", locale: "en-US" },
    query: `query phoenixShopPropertyDetails($marshaCode: String!, $locale: String!) {
      property(marshaCode: $marshaCode, locale: $locale) {
        name
        overview
        description
        amenities { name }
      }
    }`,
  };
  const r = await fetch(url, {
    method: "POST",
    headers: {
      ...H,
      "Content-Type": "application/json",
      Origin: "https://www.marriott.com",
      Referer: `https://www.marriott.com/en-us/hotels/${SLUG}/experiences/`,
    },
    body: JSON.stringify(body),
  });
  console.log("\n", url.split("/").pop(), r.status, (await r.text()).slice(0, 200));
}
