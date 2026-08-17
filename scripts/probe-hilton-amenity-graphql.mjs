import {
  HILTON_GRAPHQL_URL,
  HILTON_GRAPHQL_HEADERS,
} from "../lib/hilton-hotel-description-fetch.js";

const ctyhocn = process.argv[2] || "PUJMIQQ";

const queries = [
  {
    name: "amenityIds",
    query: `query q($ctyhocn: String!, $language: String!) {
      hotel(ctyhocn: $ctyhocn, language: $language) {
        ctyhocn name amenityIds
      }
    }`,
  },
  {
    name: "amenities",
    query: `query q($ctyhocn: String!, $language: String!) {
      hotel(ctyhocn: $ctyhocn, language: $language) {
        ctyhocn name
        amenities { id name icon { url altText } }
      }
    }`,
  },
  {
    name: "facilityAmenities",
    query: `query q($ctyhocn: String!, $language: String!) {
      hotel(ctyhocn: $ctyhocn, language: $language) {
        facilityOverview { amenities { id name icon { url } } }
      }
    }`,
  },
];

for (const { name, query } of queries) {
  const res = await fetch(HILTON_GRAPHQL_URL, {
    method: "POST",
    headers: {
      ...HILTON_GRAPHQL_HEADERS,
      Referer: `https://www.hilton.com/en/hotels/${ctyhocn.toLowerCase()}-hotel/`,
    },
    body: JSON.stringify({
      operationName: "q",
      query,
      variables: { ctyhocn, language: "en" },
    }),
  });
  const json = await res.json();
  console.log("\n===", name, "===", res.status);
  if (json.errors) console.log("errors:", json.errors.map((e) => e.message).join("; "));
  console.log(JSON.stringify(json.data, null, 2).slice(0, 2500));
}
