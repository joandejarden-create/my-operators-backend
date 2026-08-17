const gqlUrl = "https://www.hilton.com/graphql/customer";
const headers = {
  "Content-Type": "application/json",
  "User-Agent": "Mozilla/5.0 Chrome/124",
  Accept: "application/json",
  Origin: "https://www.hilton.com",
  Referer: "https://www.hilton.com/en/hotels/sjocuqq-gran-hotel-costa-rica/",
};

async function gql(query) {
  const res = await fetch(gqlUrl, {
    method: "POST",
    headers,
    body: JSON.stringify({ query }),
  });
  return res.json();
}

for (const typeName of ["Hotel", "FacilityOverview", "HotelFacilityOverview", "PropertyOverview", "HotelContent"]) {
  const j = await gql(`{ __type(name: "${typeName}") { name fields { name type { kind name ofType { name } } } } }`);
  const fields = j?.data?.__type?.fields;
  if (!fields) {
    console.log(`\n${typeName}: not found`);
    continue;
  }
  console.log(`\n${typeName} (${fields.length} fields):`);
  for (const f of fields) {
    if (/desc|overview|marketing|lead|head|story|about|brief|welcome/i.test(f.name)) {
      console.log(" *", f.name, JSON.stringify(f.type));
    }
  }
  console.log("  all:", fields.map((f) => f.name).join(", "));
}

// Full hotel query shallow
const hotel = await gql(`{
  hotel(ctyhocn: "SJOCUQQ", language: "en") {
    name
    ctyhocn
    facilityOverview { homeUrlTemplate allowAdultsOnly }
    overview { _id resortFeeDisclosureDesc }
  }
}`);
console.log("\nhotel sample:", JSON.stringify(hotel, null, 2).slice(0, 1500));

// Search Query type for hotel-related operations
const queryType = await gql(`{
  __type(name: "Query") {
    fields { name }
  }
}`);
const qfields = queryType?.data?.__type?.fields?.map((f) => f.name) || [];
console.log("\nQuery fields with hotel/prop:", qfields.filter((n) => /hotel|prop|facility|content|overview/i.test(n)).join(", "));
