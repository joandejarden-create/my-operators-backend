const https = require("https");
const fs = require("fs");

function get(u) {
  return new Promise((res, rej) => {
    https
      .get(u, (r) => {
        let d = "";
        r.on("data", (c) => (d += c));
        r.on("end", () => res(d));
      })
      .on("error", rej);
  });
}

(async () => {
  const html = await get("https://www.dealality.com/old-home");
  for (const id of [
    "ins-1",
    "ins-2",
    "ins-3",
    "ins-4",
    "ins-5",
    "ins-6",
    "insights-grid",
    "scrollMax",
    "flex-wrap:nowrap",
    "oh-ins-card",
  ]) {
    console.log(id, (html.match(new RegExp(id.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "g")) || []).length);
  }
  const css = html.match(/dealality-old-home-dark[^"'\\s]+/g) || [];
  console.log("css", [...new Set(css)]);
  const hasFooterScript = html.includes("scrollMax") || html.includes("insights-prev");
  console.log("has insights-prev", html.includes("insights-prev"));
  console.log("has scrollMax", html.includes("scrollMax"));
  console.log("has nowrap append", html.includes("flex-wrap:nowrap") || html.includes("flex-wrap: nowrap"));
  const idx = html.indexOf('id="insights-grid"');
  if (idx >= 0) {
    const chunk = html.slice(idx, idx + 3500);
    fs.writeFileSync("tmp-insights-live-chunk.html", chunk);
    console.log("cards in chunk", (chunk.match(/id="ins-/g) || []).length);
  }
})();
