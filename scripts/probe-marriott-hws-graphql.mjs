#!/usr/bin/env node
const H = {
  "User-Agent": "Mozilla/5.0",
  Accept: "application/json",
  "Content-Type": "application/json",
  Origin: "https://www.marriott.com",
  Referer: "https://www.marriott.com/en-us/hotels/poplc-the-ocean-club-a-luxury-collection-resort-costa-norte/experiences/",
};

const queries = [
  {
    url: "https://www.marriott.com/services/marriott-hws/graphql",
    body: {
      query: `query($marsha: String!) { property(marshaCode: $marsha) { name overview amenities { name } } }`,
      variables: { marsha: "POPLC" },
    },
  },
  {
    url: "https://www.marriott.com/services/marriott-hws/propertyOverview/?marsha=POPLC&locale=en-US",
    body: null,
  },
  {
    url: "https://www.marriott.com/services/marriott-hws/property/?marsha=POPLC&locale=en-US",
    body: null,
  },
  {
    url: "https://www.marriott.com/mi/query",
    body: {
      operationName: "phoenixHotelOverview",
      variables: { propertyCode: "POPLC", locale: "en-US" },
      query: `query phoenixHotelOverview($propertyCode: String!, $locale: String!) {
        property(propertyCode: $propertyCode, locale: $locale) {
          name description overview amenities { name }
        }
      }`,
    },
  },
];

for (const q of queries) {
  const r = await fetch(q.url, {
    method: q.body ? "POST" : "GET",
    headers: H,
    body: q.body ? JSON.stringify(q.body) : undefined,
  });
  const t = await r.text();
  console.log("\n", q.url.replace("https://www.marriott.com", ""), r.status, t.slice(0, 250).replace(/\s+/g, " "));
}
