const index = await fetch("https://www.ritzcarlton.com/sitemap-index.xml").then((r) => r.text());
console.log(index.slice(0, 2000));
const urls = [...index.matchAll(/<loc>([^<]+)<\/loc>/gi)].map((m) => m[1]);
console.log("sitemaps:", urls.length);
for (const u of urls.slice(0, 20)) console.log(u);
