#!/usr/bin/env node
const BASE = "https://www.marriott.com";
const HEADERS = {
  "User-Agent":
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
  Accept: "*/*",
  "Accept-Language": "en-US,en;q=0.9",
  Origin: BASE,
  Referer: `${BASE}/default.mi`,
};

const sessionRes = await fetch(`${BASE}/mi/phoenix-gateway/session`, { headers: HEADERS });
const sessionJson = JSON.parse(await sessionRes.text());
const token = sessionJson.sessionToken;
const cookies = (sessionRes.headers.getSetCookie?.() || []).map((c) => c.split(";")[0]).join("; ");
console.log("session", token?.slice(0, 8));

const authHeaders = {
  ...HEADERS,
  Cookie: cookies,
  "Content-Type": "application/json",
  sessionToken: token,
  "x-session-id": token,
  "X-Session-Token": token,
};

const attempts = [
  ["POST /v1/graph/query", `${BASE}/v1/graph/query`, {
    method: "POST",
    headers: authHeaders,
    body: JSON.stringify({
      operationName: "phoenixShopPropertiesByDestination",
      variables: { search: { destination: { country: "DO" }, options: { startIndex: 0, count: 5 } } },
      query: "query phoenixShopPropertiesByDestination($search: PropertySearchInput!) { searchPropertiesByDestination(input: $search) { total properties { id name marshaCode } } }",
    }),
  }],
  ["GET property content", `${BASE}/mi/property/MIDCY`, { headers: { ...HEADERS, Cookie: cookies } }],
  ["GET aem hotel", `https://cache.marriott.com/content/marriott-hotel-sites/en-us/midcy.hotelInformation.json`, { headers: HEADERS }],
];

for (const [label, url, init] of attempts) {
  try {
    const r = await fetch(url, init);
    const t = await r.text();
    console.log("\n", label, r.status, t.slice(0, 250).replace(/\s+/g, " "));
  } catch (e) {
    console.log("\n", label, "ERR", e.message);
  }
}
