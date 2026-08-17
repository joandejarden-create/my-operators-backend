/**
 * Extract Marriott / Hilton / Accor / IHG / Radisson image candidates from property pages.
 */
function decode(s) {
  return String(s || "")
    .replace(/&amp;/g, "&")
    .replace(/\\u002F/g, "/")
    .replace(/\\\//g, "/");
}

async function probe(url) {
  const res = await fetch(url, {
    headers: {
      "User-Agent":
        "Mozilla/5.0 (compatible; DealalityBrandExplorer/1.0; +https://dealality.com)",
      Accept: "text/html,application/xhtml+xml",
    },
    redirect: "follow",
  });
  const html = await res.text();
  const decoded = decode(html);
  const marriott = [
    ...decoded.matchAll(/https:\/\/cache\.marriott\.com\/[^\s"'<>]+/gi),
  ].map((m) => m[0].replace(/[),.;]+$/, ""));
  const scene7 = marriott.filter((u) => /is\/image\/marriotts7prod/i.test(u) || /marriott-renditions/i.test(u));
  const ahstatic = [...decoded.matchAll(/https:\/\/www\.ahstatic\.com\/photos\/[^\s"'<>]+/gi)].map(
    (m) => m[0]
  );
  const ihg = [...decoded.matchAll(/https:\/\/digital\.ihg\.com\/is\/image\/[^\s"'<>]+/gi)].map(
    (m) => m[0].replace(/[),.;]+$/, "")
  );
  const ogs = [];
  for (const re of [
    /property=["']og:image["'][^>]*content=["']([^"']+)["']/gi,
    /content=["']([^"']+)["'][^>]*property=["']og:image["']/gi,
  ]) {
    for (const m of decoded.matchAll(re)) ogs.push(decode(m[1]));
  }
  return {
    url,
    status: res.status,
    og: [...new Set(ogs)].slice(0, 6),
    scene7: [...new Set(scene7)].slice(0, 20),
    ahstatic: [...new Set(ahstatic)].slice(0, 12),
    ihgProperty: [...new Set(ihg)].filter((u) => !/logo|chiclet|primary_logo/i.test(u)).slice(0, 20),
  };
}

const targets = [
  ["autograph", "https://www.marriott.com/en-us/hotels/mspak-emery-autograph-collection/photos/"],
  ["autograph", "https://www.marriott.com/en-us/hotels/chidx-hotel-emc2-autograph-collection/photos/"],
  ["autograph", "https://www.marriott.com/en-us/hotels/mciak-the-raphael-hotel-autograph-collection/photos/"],
  ["autograph", "https://www.marriott.com/en-us/hotels/laxak-hotel-figueroa-autograph-collection/photos/"],
  ["autograph", "https://www.marriott.com/en-us/hotels/sfocm-hotel-adagio-autograph-collection/photos/"],
  ["autograph", "https://www.marriott.com/en-us/hotels/bosak-the-liberty-a-luxury-collection-hotel-boston/photos/"],
  ["vignette", "https://www.ihg.com/vignettecollection/hotels/us/en/nairobi/nbovc/hoteldetail"],
  ["handwritten", "https://all.accor.com/hotel/B8P4/index.en.shtml"],
  ["handwritten", "https://all.accor.com/hotel/B0I2/index.en.shtml"],
];

const out = [];
for (const [brand, url] of targets) {
  try {
    const r = await probe(url);
    out.push({ brand, ...r });
    console.log(brand, r.status, "og", r.og.length, "scene7", r.scene7.length, "ah", r.ahstatic.length, "ihg", r.ihgProperty.length);
    if (r.og[0]) console.log("  og0", r.og[0].slice(0, 120));
    if (r.scene7[0]) console.log("  s70", r.scene7[0].slice(0, 120));
    if (r.ihgProperty[0]) console.log("  ihg0", r.ihgProperty[0].slice(0, 120));
    if (r.ahstatic[0]) console.log("  ah0", r.ahstatic[0].slice(0, 120));
  } catch (e) {
    console.log(brand, url, "ERR", e.message);
  }
  await new Promise((r) => setTimeout(r, 250));
}

import fs from "fs";
fs.writeFileSync("reports/_tmp-lane2-image-probe.json", JSON.stringify(out, null, 2));
console.log("wrote reports/_tmp-lane2-image-probe.json");
