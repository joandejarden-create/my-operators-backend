/** Probe Hilton DX / image endpoints and Choice hotel search for Radisson Collection. */
async function get(url, headers = {}) {
  const res = await fetch(url, {
    redirect: "follow",
    headers: {
      "User-Agent":
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
      Accept: "application/json,text/html,*/*",
      ...headers,
    },
  });
  const text = await res.text();
  return { status: res.status, text, len: text.length, url: res.url };
}

function grab(text, re) {
  return [...new Set([...text.matchAll(re)].map((m) => m[1] || m[0]))].slice(0, 30);
}

const hiltonUrls = [
  "https://www.hilton.com/graphql/customer?app=dx&operationName=hotelQuery&variables=%7B%22language%22%3A%22en%22%2C%22ctyhocn%22%3A%22SAVVYUP%22%7D&extensions=%7B%22persistedQuery%22%3A%7B%22version%22%3A1%2C%22sha256Hash%22%3A%22deadbeef%22%7D%7D",
  "https://www.hilton.com/en/hotels/savvyup-the-cotton-sail-hotel-savannah/index.json",
  "https://www.hilton.com/dx-customer/graphql",
];

for (const url of hiltonUrls) {
  process.stdout.write(`H ${url.slice(0, 70)}... `);
  try {
    const r = await get(url);
    console.log(r.status, r.len, r.text.slice(0, 180).replace(/\s+/g, " "));
  } catch (e) {
    console.log("ERR", e.message);
  }
}

// Choice hotel locator search API patterns
const choiceApis = [
  "https://www.choicehotels.com/webapi/hotel/hotelInfo?adults=1&checkInDate=2026-09-01&checkOutDate=2026-09-02&hotelIds=SE054",
  "https://www.choicehotels.com/webapi/search/hotels?adults=1&placeId=ChIJywtkGTF2X0YRZnedUaT0I3I&radius=50&checkInDate=2026-09-01&checkOutDate=2026-09-02&siteName=us&locale=en-us&brandCodes=RC",
];

for (const url of choiceApis) {
  process.stdout.write(`C ${url.slice(0, 80)}... `);
  try {
    const r = await get(url, { Accept: "application/json" });
    console.log(r.status, r.len);
    const imgs = grab(r.text, /https?:\\?\/\\?\/[^"\\]+\.(?:jpg|jpeg|png|webp)/gi).map((u) =>
      u.replace(/\\\//g, "/")
    );
    const hoteldam = grab(r.text, /hoteldam[^"\\]+/gi);
    console.log("  imgs", imgs.slice(0, 8));
    console.log("  hoteldam", hoteldam.slice(0, 8));
    if (r.status === 200) console.log("  head", r.text.slice(0, 250).replace(/\s+/g, " "));
  } catch (e) {
    console.log("ERR", e.message);
  }
}

// Accor handwritten hotel name resolution for titles
const codes = ["B9F3", "C139", "C013", "C1U0", "C150", "B7A6", "C2L2", "C160"];
for (const code of codes.slice(0, 3)) {
  const r = await get(`https://all.accor.com/hotel/${code}/index.en.shtml`);
  const title = (r.text.match(/<title>([^<]+)<\/title>/i) || [])[1] || "";
  const h1 = (r.text.match(/<h1[^>]*>([\s\S]*?)<\/h1>/i) || [])[1]?.replace(/<[^>]+>/g, "").trim();
  console.log(code, "title=", title.slice(0, 80), "h1=", (h1 || "").slice(0, 80));
}
