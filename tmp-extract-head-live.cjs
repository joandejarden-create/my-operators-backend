const fs = require("fs");
const https = require("https");

function get(url) {
  return new Promise((resolve, reject) => {
    https
      .get(url, { headers: { "Cache-Control": "no-cache", Pragma: "no-cache" } }, (res) => {
        let d = "";
        res.on("data", (c) => (d += c));
        res.on("end", () => resolve(d));
      })
      .on("error", reject);
  });
}

(async () => {
  const html = await get("https://www.dealality.com/old-home");
  const start = html.indexOf('<link rel="preconnect" href="https://fonts.googleapis.com">');
  const marker = "/* Insights carousel";
  const mi = html.indexOf(marker);
  const styleEnd = html.indexOf("</style>", mi);
  if (start < 0 || mi < 0 || styleEnd < 0) {
    console.log({ start, mi, styleEnd });
    process.exit(1);
  }
  const head = html.slice(start, styleEnd + 8);
  fs.writeFileSync("tmp-old-home-head-live.txt", head);
  console.log({
    len: head.length,
    starts: head.slice(0, 70),
    ends: head.slice(-50),
    has360: head.includes("flex:0 0 360px"),
  });
})().catch((e) => {
  console.error(e);
  process.exit(1);
});
