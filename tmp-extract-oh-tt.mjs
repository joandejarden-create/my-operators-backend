import fs from "fs";
const html = fs.readFileSync("tmp-old-home-tt-live.html", "utf8");
const m = html.match(/<style id=["']oh-tt["'][\s\S]*?<\/style>/i);
fs.writeFileSync("tmp-oh-tt-live.css", m ? m[0] : "NO");
console.log(m ? `oh-tt len=${m[0].length}` : "NO oh-tt");
const quotes = [...html.matchAll(/<blockquote[^>]*>([\s\S]*?)<\/blockquote>/gi)].map((x) =>
  x[1].replace(/<[^>]+>/g, "").replace(/\s+/g, " ").trim()
);
const attrs = [...html.matchAll(/<article[\s\S]*?<p>([\s\S]*?)<\/p>/gi)].map((x) =>
  x[1].replace(/<[^>]+>/g, "").replace(/\s+/g, " ").trim()
);
console.log(JSON.stringify({ quoteCount: quotes.length, quotes, attrs }, null, 2));
const boot = [...html.matchAll(/old-home-[a-z0-9.-]+\.v2026[0-9a-z]+\.(?:js|css)/gi)].map((x) => x[0]);
console.log("assets", [...new Set(boot)].slice(0, 40));
