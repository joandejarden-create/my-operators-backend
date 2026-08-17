#!/usr/bin/env node
/** Expand Canopy / thin Hilton pools from stories.hilton.com search + brand pages. */
async function fetchHtml(url) {
  const r = await fetch(url, {
    headers: {
      "User-Agent":
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
      Accept: "text/html",
    },
    redirect: "follow",
  });
  return { status: r.status, html: await r.text(), url: r.url };
}

function extract(html, brandRe) {
  const out = [];
  for (const m of html.matchAll(
    /https:\/\/(?:stories-editor\.hilton\.com|stories\.hilton\.com)\/[^"'\\\s<>]+\.(?:jpg|jpeg|png|webp)/gi
  )) {
    const u = m[0].replace(/&amp;/g, "&").replace(/\?.*$/, "");
    if (/logo|hilton_black|icon|sprite/i.test(u)) continue;
    if (!brandRe.test(u)) continue;
    if (/curio|tapestry|tempo|motto|waldorf|conrad|casa-marina/i.test(u) && !brandRe.test(u)) continue;
    out.push(u);
  }
  return [...new Set(out)];
}

const pages = [
  "https://stories.hilton.com/brands/canopy",
  "https://stories.hilton.com/canopy-by-hilton-fact-sheet",
  "https://stories.hilton.com/?s=Canopy+by+Hilton",
  "https://stories.hilton.com/releases?s=Canopy",
  "https://stories.hilton.com/destination-spotlight-exploring-south-african-culture-from-canopy-cape-town-longkloof",
];

const all = new Set();
for (const u of pages) {
  try {
    const { status, html } = await fetchHtml(u);
    const imgs = extract(html, /canopy/i);
    console.log(status, u.slice(-60), imgs.length);
    imgs.forEach((i) => all.add(i));
  } catch (e) {
    console.log("ERR", u, e.message);
  }
}
console.log("\nTOTAL", all.size);
for (const u of [...all].slice(0, 40)) console.log(u);
