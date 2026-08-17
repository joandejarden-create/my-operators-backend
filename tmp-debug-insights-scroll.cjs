const https = require("https");
const fs = require("fs");

function get(u) {
  return new Promise((res, rej) => {
    https
      .get(u, { headers: { "Cache-Control": "no-cache", Pragma: "no-cache" } }, (r) => {
        let d = "";
        r.on("data", (c) => (d += c));
        r.on("end", () => res({ status: r.statusCode, headers: r.headers, d }));
      })
      .on("error", rej);
  });
}

(async () => {
  const { d: html } = await get("https://www.dealality.com/old-home?cb=" + Date.now());
  fs.writeFileSync("tmp-old-home-live-full.html", html);

  // Extract insights section
  const i = html.indexOf('id="insights"');
  const j = html.indexOf('id="cta-band"');
  const section = html.slice(i, j > i ? j : i + 20000);
  fs.writeFileSync("tmp-insights-section.html", section);

  // Button markup
  const prev = section.match(/id="insights-prev"[^>]*>/);
  const next = section.match(/id="insights-next"[^>]*>/);
  console.log("prev tag", prev && prev[0]);
  console.log("next tag", next && next[0]);

  // CSS rules affecting nav
  const styleMatch = html.match(/<style>([\s\S]*?)<\/style>/g) || [];
  console.log("style blocks", styleMatch.length);
  for (const block of styleMatch) {
    if (block.includes("insights-prev") || block.includes("insights-grid") || block.includes("360px")) {
      const hits = block.match(/#[^#{]*insights[^}]+}/g) || [];
      console.log("--- style hits", hits.length);
      hits.slice(0, 30).forEach((h) => console.log(h.slice(0, 220)));
    }
  }

  // Footer script carousel portion
  const si = html.indexOf("function scrollMax");
  console.log("scrollMax idx", si);
  if (si > 0) console.log(html.slice(si - 200, si + 1200));

  // Check pointer-events conflicts
  console.log(
    "pointer-events none on disabled",
    /insights-prev[^}]*pointer-events:\s*none/.test(html)
  );
  console.log(
    "pointer-events auto important",
    /insights-prev[^}]*pointer-events:\s*auto\s*!important/.test(html)
  );

  // CDN css link
  const css = html.match(/dealality-old-home-dark[^"'\\s]+/g) || [];
  console.log("css", [...new Set(css)]);
})();
