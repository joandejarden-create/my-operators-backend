#!/usr/bin/env node
const url = process.argv[2] || "https://www.marriott.com/en-us/hotels/midcy-courtyard-merida-downtown/overview/";
const res = await fetch(url, {
  headers: {
    "User-Agent":
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    Accept: "text/html,application/xhtml+xml",
  },
  redirect: "follow",
});
console.log("status", res.status, "url", res.url);
const html = await res.text();
console.log("html length", html.length);

const next = html.match(/<script id="__NEXT_DATA__"[^>]*>([\s\S]*?)<\/script>/i);
const ldBlocks = [...html.matchAll(/<script type="application\/ld\+json">([\s\S]*?)<\/script>/gi)];
console.log("__NEXT_DATA__", Boolean(next), "ld+json blocks", ldBlocks.length);

if (next) {
  const data = JSON.parse(next[1]);
  console.log("next pageProps keys", Object.keys(data?.props?.pageProps || {}));
}

for (const m of ldBlocks.slice(0, 2)) {
  try {
    const j = JSON.parse(m[1]);
    console.log("ld+json @type", j["@type"], "name", j.name?.slice?.(0, 60));
    if (j.description) console.log("description", j.description.slice(0, 120));
  } catch (e) {
    console.log("ld parse err", e.message);
  }
}

const marsha = url.match(/\/hotels\/([a-z0-9]+)-/i);
console.log("marsha from url", marsha?.[1]);
