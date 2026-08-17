import fs from "fs";
import https from "https";

function get(url) {
  return new Promise((resolve, reject) => {
    https
      .get(url, (res) => {
        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          return get(res.headers.location).then(resolve, reject);
        }
        const chunks = [];
        res.on("data", (c) => chunks.push(c));
        res.on("end", () => resolve(Buffer.concat(chunks).toString("utf8")));
      })
      .on("error", reject);
  });
}

const html = await get("https://www.dealality.com/");
fs.writeFileSync("tmp-dealality-home.html", html);
const heads = [...html.matchAll(/dealality-old-home-freeform-head[^"'\\\s>]+/g)].map((m) => m[0]);
const footers = [...html.matchAll(/old-home-footer-oh[^"'\\\s>]+/g)].map((m) => m[0]);
const testi = [...html.matchAll(/dealality-old-home-testimonials[^"'\\\s>]+/g)].map((m) => m[0]);
console.log({ heads, footers, testi, len: html.length });
const i = html.indexOf('id="testimonials-viewport"');
console.log("viewport idx", i);
if (i >= 0) console.log(html.slice(i, i + 1800));
