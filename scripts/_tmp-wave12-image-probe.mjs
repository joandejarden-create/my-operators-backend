#!/usr/bin/env node
async function probe(label, url) {
  try {
    const res = await fetch(url, {
      headers: { "User-Agent": "Mozilla/5.0", Accept: "text/html" },
      redirect: "follow",
    });
    const html = await res.text();
    const og = [
      ...html.matchAll(/property=["']og:image["'][^>]*content=["']([^"']+)["']/gi),
      ...html.matchAll(/content=["']([^"']+)["'][^>]*property=["']og:image["']/gi),
    ].map((m) => m[1]);
    const marriott = [...html.matchAll(/https:\/\/cache\.marriott\.com\/[^"'\\\s<>]+/gi)];
    const ihg = [...html.matchAll(/https:\/\/digital\.ihg\.com\/[^"'\\\s<>]+/gi)];
    const hilton = [
      ...html.matchAll(/https:\/\/(?:assets\.)?hiltonstatic\.com\/[^"'\\\s<>]+/gi),
      ...html.matchAll(/https:\/\/www\d*\.hilton\.com\/im\/[^"'\\\s<>]+/gi),
    ];
    const bunk = [
      ...html.matchAll(
        /https:\/\/[^"'\\\s<>]*bunkhouse[^"'\\\s<>]+\.(?:jpg|jpeg|png|webp)[^"'\\\s<>]*/gi
      ),
      ...html.matchAll(/https:\/\/[^"'\\\s<>]+\.(?:jpg|jpeg|png|webp)(?:\?[^"'\\\s<>]*)?/gi),
    ].filter((u) => /bunkhouse|wp-content|cloudinary|imgix|cdn/i.test(u));
    console.log("===", label, "status", res.status, "len", html.length);
    console.log("og", [...new Set(og)].slice(0, 4));
    console.log("marriott", [...new Set(marriott)].slice(0, 4));
    console.log("ihg", [...new Set(ihg)].slice(0, 4));
    console.log("hilton", [...new Set(hilton)].slice(0, 4));
    console.log("bunkish", [...new Set(bunk)].slice(0, 4));
  } catch (e) {
    console.log(label, "ERR", e.message);
  }
}

await probe(
  "voco-reforma",
  "https://www.ihg.com/voco/hotels/us/en/ciudad-de-mexico/mexvc/hoteldetail"
);
await probe(
  "even-nyc",
  "https://www.ihg.com/evenhotels/hotels/us/en/new-york/nycme/hoteldetail"
);
await probe(
  "courtyard",
  "https://www.marriott.com/en-us/hotels/cuncy-courtyard-cancun/overview/"
);
await probe("motto", "https://www.hilton.com/en/hotels/sjumbup-motto-san-juan/");
await probe("bunkhouse", "https://bunkhousehotels.com/hotels/hotel-san-cristobal/");
