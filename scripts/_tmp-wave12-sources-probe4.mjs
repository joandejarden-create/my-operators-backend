#!/usr/bin/env node
async function fetchText(url) {
  const r = await fetch(url, {
    headers: {
      "User-Agent":
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
      Accept: "text/html",
    },
    redirect: "follow",
  });
  return { status: r.status, text: await r.text(), finalUrl: r.url };
}

function imgs(text) {
  return [
    ...new Set(
      [...text.matchAll(/https:\/\/[^"'\\\s<>]+\.(?:jpg|jpeg|png|webp)(?:\?[^"'\\\s<>]*)?/gi)].map(
        (m) => m[0].replace(/[),.;]+$/, "")
      )
    ),
  ].filter((u) => !/logo|favicon|sprite|icon|1x1|pixel|avatar|wp-includes|404-image/i.test(u));
}

function hiltonIm(text) {
  return [
    ...new Set(
      [
        ...text.matchAll(
          /https:\/\/www\.hilton\.com\/im\/en\/[A-Z0-9]+\/\d+\/[^"'\\\s<>?]+\.(?:jpg|jpeg|png|webp)/gi
        ),
      ].map((m) => m[0])
    ),
  ];
}

// Bunkhouse dedicated sites
const bunkPages = [
  "https://www.hotelsaintcecilia.com/",
  "https://www.hotelsancristobal.com/",
  "https://www.hotelsanfernando.com/",
  "https://hotelsaintcecilia.com/",
  "https://hotelsancristobalbaja.com/",
  "https://www.hotel-san-cristobal.com/",
  "https://bunkhousehotels.com/",
  "https://www.bunkhousehotels.com/",
];

for (const u of bunkPages) {
  try {
    const { status, text, finalUrl } = await fetchText(u);
    const list = imgs(text);
    console.log("\nBUNK", status, finalUrl.slice(0, 70), "n", list.length);
    console.log(list.slice(0, 10).join("\n"));
  } catch (e) {
    console.log("ERR", u, e.message);
  }
}

// EVEN via booking / hotels.com / expedia
const evenPages = [
  "https://www.booking.com/hotel/us/even-hotel-new-york-times-square-south.html",
  "https://www.hotels.com/ho447156/even-hotel-new-york-times-square-south-new-york-united-states-of-america/",
  "https://www.expedia.com/New-York-Hotels-EVEN-Hotel-New-York-Times-Square-South.h9271444.Hotel-Information",
  "https://www.ihg.com/evenhotels/hotels/us/en/new-york/nycep/hoteldetail#/hotel-details",
];

for (const u of evenPages) {
  try {
    const { status, text, finalUrl } = await fetchText(u);
    const list = imgs(text).filter((x) =>
      /even|ihg|trvl|media-cdn|tacdn|ak-d\.tripcdn|images\.trvl/i.test(x)
    );
    const ihg = [
      ...new Set(
        [...text.matchAll(/https:\/\/digital\.ihg\.com\/is\/image\/ihg\/[^"'\\\s<>?]+/gi)].map(
          (m) => m[0]
        )
      ),
    ];
    console.log("\nEVEN", status, finalUrl.slice(0, 80), "imgs", list.length, "ihg", ihg.length);
    console.log(list.slice(0, 8).join("\n"));
    console.log(ihg.slice(0, 8).join("\n"));
  } catch (e) {
    console.log("ERR", u, e.message);
  }
}

// More Hilton stories pages + brand pages via archive
const moreStories = [
  "https://stories.hilton.com/releases/hilton-opens-canopy-by-hilton-reykjavik-city-centre",
  "https://stories.hilton.com/releases/canopy-by-hilton-opens-in-washington-dc",
  "https://stories.hilton.com/brands/canopy",
  "https://stories.hilton.com/brands/tempo",
  "https://stories.hilton.com/brands/motto",
  "https://www.hilton.com/en/brands/canopy-by-hilton/",
  "https://www.hilton.com/en/brands/tempo-by-hilton/",
  "https://www.hilton.com/en/brands/motto-by-hilton/",
];

for (const u of moreStories) {
  try {
    const { status, text } = await fetchText(u);
    const list = [
      ...hiltonIm(text),
      ...imgs(text).filter((x) =>
        /canopy|tempo|motto|hilton|stories-editor/i.test(x)
      ),
    ];
    const uniq = [...new Set(list)].filter((x) => !/logo|hilton_black|icon/i.test(x));
    console.log("\nHIL", status, u.slice(-55), "n", uniq.length);
    console.log(uniq.slice(0, 8).join("\n"));
  } catch (e) {
    console.log("ERR", u, e.message);
  }
}

// Probe hilton CDN for known codes with numeric id ranges from tempo harvest
async function exists(url) {
  try {
    const r = await fetch(url, {
      method: "GET",
      headers: { "User-Agent": "Mozilla/5.0", Range: "bytes=0-32", Referer: "https://www.hilton.com/" },
      redirect: "follow",
    });
    return r.ok || r.status === 206;
  } catch {
    return false;
  }
}

const codes = ["WASWHCP", "REKPCP", "PDXPDCP", "CZMTUUA", "CUZINUA", "DCAMTMT", "BNAPOPO", "RDUTPUP", "TYTSPUP"];
// From tempo harvest we know BNAPOPO/18516072/bnapo-exterior2.jpg works via CDN
const sample = "https://www.hilton.com/im/en/BNAPOPO/18516072/bnapo-exterior2.jpg?impolicy=ratio&rw=1200&rh=800";
console.log("\nlive tempo sample", await exists(sample), sample);
