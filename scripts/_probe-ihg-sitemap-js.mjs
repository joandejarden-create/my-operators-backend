const UA = "Dealality/1.0";

async function main() {
  const sm = await fetch("https://development.ihg.com/sitemap.xml", { headers: { "User-Agent": UA } });
  const xml = await sm.text();
  const locs = [...xml.matchAll(/<loc>([^<]+)<\/loc>/g)].map((m) => m[1]);
  console.log("sitemap urls", locs.length);

  const pdfLocs = locs.filter((u) => /\.pdf/i.test(u) || /\/files\//i.test(u));
  console.log("pdf/file locs", pdfLocs.length);
  for (const u of pdfLocs.slice(0, 50)) console.log(u);

  // Fetch main JS chunks from resources page
  const page = await fetch("https://development.ihg.com/resources", { headers: { "User-Agent": UA } });
  const html = await page.text();
  const scripts = [...html.matchAll(/src="([^"]+\.js[^"]*)"/g)].map((m) => m[1]);
  console.log("\nscript count", scripts.length);

  for (const src of scripts.slice(0, 15)) {
    const url = src.startsWith("http") ? src : `https://development.ihg.com${src.startsWith("/") ? "" : "/"}${src}`;
    try {
      const js = await fetch(url, { headers: { "User-Agent": UA } }).then((r) => r.text());
      const pdfs = [...js.matchAll(/\/sites\/ihgplc\/files[^\s"'\\)]+\.pdf/gi)].map((m) => m[0]);
      const apis = [...js.matchAll(/\/api\/[a-z0-9/_-]+/gi)].map((m) => m[0]);
      if (pdfs.length || apis.length > 1) {
        console.log("\nJS", url.slice(-60));
        console.log("  pdfs", [...new Set(pdfs)].slice(0, 10));
        console.log("  apis", [...new Set(apis)].slice(0, 10));
      }
    } catch (_) {}
  }

  // Try common Drupal media paths
  const guesses = [
    "https://development.ihg.com/sites/ihgplc/files/IHG/Resources/Americas/",
  ];
  for (const g of guesses) {
    const r = await fetch(g, { headers: { "User-Agent": UA } });
    console.log("dir probe", g, r.status, r.headers.get("content-type"));
  }
}

main();
