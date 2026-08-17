const url =
  process.argv[2] ||
  "https://www.hilton.com/en/hotels/pujmiqq-zemi-miches-all-inclusive-resort/";

const res = await fetch(url, {
  headers: {
    "User-Agent":
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    Accept: "text/html,application/xhtml+xml",
  },
});
console.log("status", res.status, url);
const html = await res.text();
const m = html.match(/<script id="__NEXT_DATA__"[^>]*>([\s\S]*?)<\/script>/i);
if (!m) {
  console.log("no __NEXT_DATA__");
  process.exit(1);
}
const data = JSON.parse(m[1]);
const pageData = data?.props?.pageProps?.pageData || {};
const hotel = pageData.hotel || pageData;

console.log("pageData keys:", Object.keys(pageData).slice(0, 30));
console.log("amenityIds:", hotel.amenityIds);
console.log("amenities count:", hotel.amenities?.length);

if (Array.isArray(hotel.amenities)) {
  for (const a of hotel.amenities.slice(0, 12)) {
    console.log(JSON.stringify(a));
  }
}

// search for icon urls in page
const iconUrls = [...html.matchAll(/https:\/\/[^"'\s]+amenit[^"'\s]*/gi)].map((x) => x[0]);
console.log("icon url samples:", [...new Set(iconUrls)].slice(0, 10));
