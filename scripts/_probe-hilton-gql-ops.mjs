const headers = {
  "User-Agent": "Mozilla/5.0 Chrome/124",
  Accept: "text/html",
};
const html = await (await fetch("https://www.hilton.com/en/locations/costa-rica/curio-collection/", { headers })).text();
const data = JSON.parse(html.match(/<script id="__NEXT_DATA__"[^>]*>([\s\S]*?)<\/script>/i)[1]);

// Search dehydrated queries for hotel detail operations
const queries = data.props?.pageProps?.dehydratedState?.queries || [];
for (const q of queries) {
  const key = q.queryKey?.[0];
  const op = key?.operationName || key?.operationString?.slice(0, 80);
  if (op) console.log(op);
}

// Grep entire __NEXT_DATA__ for interesting operation names
const blob = html.match(/<script id="__NEXT_DATA__"[^>]*>([\s\S]*?)<\/script>/i)[1];
const ops = [...blob.matchAll(/operationName":"([^"]+)"/g)].map((m) => m[1]);
console.log("\nUnique operationNames in page:", [...new Set(ops)].sort().join(", "));

// Also search script tags for graphql endpoint
const gqlUrls = [...html.matchAll(/https?:\/\/[^"'\s]+graphql[^"'\s]*/gi)].map((m) => m[0]);
console.log("\ngraphql URLs:", [...new Set(gqlUrls)]);

const dx = [...html.matchAll(/https?:\/\/[^"'\s]*hilton[^"'\s]*api[^"'\s]*/gi)].slice(0, 20).map((m) => m[0]);
console.log("api-ish:", [...new Set(dx)].slice(0, 10));

// search for checkIn in full html outside next data
for (const term of ["checkInTime", "checkOutTime", "Asamblea", "TripAdvisor", "hotelInfo", "nearbyPlaces"]) {
  console.log(term, "in html:", html.includes(term));
}
