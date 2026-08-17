#!/usr/bin/env node
const BASE = "https://www.marriott.com";
const HEADERS = {
  "User-Agent":
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
  Accept: "application/json, text/plain, */*",
  "Accept-Language": "en-US,en;q=0.9",
  Origin: BASE,
  Referer: `${BASE}/`,
};

let cookie = "";
async function get(path, extra = {}) {
  const res = await fetch(`${BASE}${path}`, {
    headers: { ...HEADERS, ...(cookie ? { Cookie: cookie } : {}), ...extra },
    redirect: "follow",
  });
  const set = res.headers.getSetCookie?.() || [];
  if (set.length) cookie = set.map((c) => c.split(";")[0]).join("; ");
  return { res, text: await res.text() };
}

console.log("1) Session");
const s1 = await get("/mi/phoenix-gateway/session");
console.log(" status", s1.res.status, "cookie len", cookie.length, "body", s1.text.slice(0, 200));

console.log("\n2) Overview page");
const url = "/en-us/hotels/midcy-courtyard-merida-downtown/overview/";
const s2 = await get(url, { Accept: "text/html,application/xhtml+xml,*/*;q=0.8" });
console.log(" status", s2.res.status, "len", s2.text.length, "denied", /access denied/i.test(s2.text));
const next = s2.text.match(/<script id="__NEXT_DATA__"[^>]*>([\s\S]*?)<\/script>/i);
console.log("__NEXT_DATA__", Boolean(next));
if (next) {
  const d = JSON.parse(next[1]);
  console.log("keys", Object.keys(d?.props?.pageProps || {}));
}

console.log("\n3) GraphQL query");
const gqlBody = {
  operationName: "phoenixShopPropertiesByDestination",
  variables: {
    destination: { country: "DO" },
    limit: 20,
    offset: 0,
  },
  query: `query phoenixShopPropertiesByDestination($destination: DestinationInput!, $limit: Int, $offset: Int) {
    search {
      propertiesByDestination(destination: $destination, limit: $limit, offset: $offset) {
        total
        edges { node { id name marshaCode brand { name } } }
      }
    }
  }`,
};
const s3 = await fetch(`${BASE}/mi/query`, {
  method: "POST",
  headers: {
    ...HEADERS,
    "Content-Type": "application/json",
    ...(cookie ? { Cookie: cookie } : {}),
  },
  body: JSON.stringify(gqlBody),
});
const gtext = await s3.text();
console.log(" gql status", s3.status, gtext.slice(0, 400));
