import https from "https";
import fs from "fs";

function get(url) {
  return new Promise((resolve, reject) => {
    https
      .get(url, (res) => {
        let d = "";
        res.on("data", (c) => (d += c));
        res.on("end", () => resolve({ status: res.statusCode, body: d }));
      })
      .on("error", reject);
  });
}

const { status, body } = await get(
  "https://www.dealality.com/old-home?v=" + Date.now()
);
fs.writeFileSync("tmp-old-home-live-now.html", body);
console.log("status", status, "len", body.length);
console.log("pricing", body.includes('id="pricing"'));
console.log(
  "pricing css",
  body.match(/dealality-old-home-pricing[^"']+/g) || []
);
console.log("footer", body.match(/old-home-footer[^"']+/g) || []);
console.log(
  "head css",
  body.match(/dealality-old-home-freeform-head[^"']+/g) || []
);
const i = body.indexOf('id="pricing"');
if (i >= 0) {
  const end = body.indexOf("</section>", i);
  fs.writeFileSync(
    "tmp-pricing-section.html",
    body.slice(Math.max(0, i - 8), (end > i ? end : i + 12000) + 10)
  );
  console.log("wrote pricing section", end - i);
}
