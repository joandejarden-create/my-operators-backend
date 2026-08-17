const gqlUrl = "https://www.hilton.com/graphql/customer";
const headers = {
  "Content-Type": "application/json",
  "User-Agent": "Mozilla/5.0 Chrome/124",
  Accept: "application/json",
  Origin: "https://www.hilton.com",
  Referer: "https://www.hilton.com/en/hotels/sjocuqq-gran-hotel-costa-rica/",
};

async function gql(body) {
  const res = await fetch(gqlUrl, { method: "POST", headers, body: JSON.stringify(body) });
  const text = await res.text();
  return { status: res.status, text, json: JSON.parse(text) };
}

// Introspect HotelOverview
const intro = await gql({
  query: `{
    __type(name: "HotelOverview") {
      name
      fields { name type { kind name ofType { kind name ofType { name } } } }
    }
  }`,
});
console.log("HotelOverview fields:");
for (const f of intro.json?.data?.__type?.fields || []) {
  console.log(" ", f.name, JSON.stringify(f.type));
}

// Introspect Hotel type
const hotelType = await gql({
  query: `{
    __type(name: "Hotel") {
      fields { name type { kind name ofType { name } } }
    }
  }`,
});
console.log("\nHotel type fields (sample):");
for (const f of (hotelType.json?.data?.__type?.fields || []).filter((x) =>
  /overview|desc|name|cty/i.test(x.name)
)) {
  console.log(" ", f.name);
}

// Try hotel query with overview only
const variants = [
  `query { hotel(ctyhocn: "SJOCUQQ", language: "en") { name overview { description } } }`,
  `query { hotel(ctyhocn: "SJOCUQQ", language: "en") { name overview { hotelDescription } } }`,
  `query { hotel(ctyhocn: "SJOCUQQ", language: "en") { name overview { marketingDescription } } }`,
  `query { hotel(ctyhocn: "SJOCUQQ", language: "en") { name overview { briefDescription } } }`,
  `query { hotel(ctyhocn: "SJOCUQQ", language: "en") { name overview { leadIn } } }`,
  `query { hotel(ctyhocn: "SJOCUQQ", language: "en") { name overview { headline } } }`,
];

for (const query of variants) {
  const r = await gql({ query });
  const err = r.json?.errors?.[0]?.message;
  const data = r.json?.data?.hotel;
  if (data) {
    console.log("\nSUCCESS:", query.match(/overview \{ (\w+)/)?.[1]);
    console.log(JSON.stringify(data, null, 2).slice(0, 800));
    break;
  }
  console.log("fail", query.match(/overview \{ (\w+)/)?.[1], err?.slice(0, 80));
}
