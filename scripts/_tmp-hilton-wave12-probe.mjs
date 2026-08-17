#!/usr/bin/env node
async function tryFetch(url) {
  const headers = {
    "User-Agent":
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    Accept: "text/html",
    Referer: "https://www.hilton.com/",
  };
  const r = await fetch(url, { headers, redirect: "follow" });
  const t = await r.text();
  const re =
    /https:\/\/www\.hilton\.com\/im\/en\/[A-Z0-9]+\/\d+\/[^"'\\\s<>?]+\.(?:jpg|jpeg|png|webp)/gi;
  const imgs = [...t.matchAll(re)].map((m) => m[0]);
  const wa =
    /https:\/\/web\.archive\.org\/web\/\d+(?:im_)?\/(https:\/\/www\.hilton\.com\/im\/en\/[A-Z0-9]+\/\d+\/[^"'\\\s<>?]+\.(?:jpg|jpeg|png|webp))/gi;
  const fromWa = [...t.matchAll(wa)].map((m) => m[1]);
  console.log(
    r.status,
    url.slice(0, 90),
    "liveImgs",
    imgs.length,
    "waImgs",
    fromWa.length,
    "sample",
    (imgs[0] || fromWa[0] || "").slice(0, 100)
  );
}

const pages = [
  "https://www.hilton.com/en/hotels/cnypycc-canopy-by-hilton-pittsburgh-downtown/",
  "https://web.archive.org/web/20240101000000/https://www.hilton.com/en/hotels/cnypycc-canopy-by-hilton-pittsburgh-downtown/",
  "https://www.hilton.com/en/hotels/czmtuua-motto-tulum/",
  "https://web.archive.org/web/20240101000000/https://www.hilton.com/en/hotels/czmtuua-motto-tulum/",
  "https://www.hilton.com/en/hotels/nycnptp-tempo-by-hilton-times-square/",
  "https://web.archive.org/web/20240101000000/https://www.hilton.com/en/hotels/nycnptp-tempo-by-hilton-times-square/",
];

for (const u of pages) {
  try {
    await tryFetch(u);
  } catch (e) {
    console.log("ERR", u.slice(0, 80), e.message);
  }
}
