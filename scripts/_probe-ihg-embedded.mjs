import fs from "fs";

const UA = "Dealality/1.0";
const urls = [
  "https://development.ihg.com/",
  "https://development.ihg.com/resources",
  "https://development.ihg.com/regions/americas",
];

for (const url of urls) {
  const html = await fetch(url, { headers: { "User-Agent": UA } }).then((r) => r.text());
  const files = [...html.matchAll(/\/sites\/ihgplc\/files[^"'\\s<>]+\.pdf/gi)].map((m) => decodeURIComponent(m[0]));
  const nextData = html.match(/<script id="__NEXT_DATA__"[^>]*>([\s\S]*?)<\/script>/);
  const nuxt = html.match(/window\.__NUXT__=([\s\S]*?)<\/script>/);
  const jsonLd = [...html.matchAll(/application\/ld\+json[^>]*>([\s\S]*?)<\/script>/g)].map((m) => m[1]);
  console.log("\n===", url, "===");
  console.log("embedded pdfs", [...new Set(files)].length);
  for (const f of [...new Set(files)].slice(0, 30)) console.log(" ", f);
  console.log("next data", !!nextData, "nuxt", !!nuxt, "jsonLd", jsonLd.length);
  if (nextData) {
    const pdfsInNext = [...nextData[1].matchAll(/\.pdf[^"\\]*/gi)].map((m) => m[0]);
    console.log("pdfs in __NEXT_DATA__", pdfsInNext.slice(0, 20));
  }
  // __NUXT__ or similar
  const allPdfPaths = [...html.matchAll(/IHG\/Resources[^"'\\]+\.pdf/gi)].map((m) => m[0]);
  console.log("IHG/Resources paths", [...new Set(allPdfPaths)].slice(0, 30));
}
