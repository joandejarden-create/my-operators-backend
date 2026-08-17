import https from "https";
import fs from "fs";

function get(url) {
  return new Promise((resolve, reject) => {
    https
      .get(url, { headers: { "User-Agent": "dealality-qa" } }, (res) => {
        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          return get(new URL(res.headers.location, url).href).then(resolve, reject);
        }
        const chunks = [];
        res.on("data", (c) => chunks.push(c));
        res.on("end", () =>
          resolve({
            status: res.statusCode,
            url,
            body: Buffer.concat(chunks).toString("utf8"),
          })
        );
      })
      .on("error", reject);
  });
}

const page = await get("https://www.dealality.com/old-home");
fs.writeFileSync("tmp-old-home-live-verify.html", page.body);
const checks = {
  status: page.status,
  len: page.body.length,
  hasViewport: page.body.includes("testimonials-viewport"),
  hasOhTt: page.body.includes("oh-tt") || page.body.includes("grid-template-columns:repeat(2"),
  hasBootGuard: page.body.includes("oldhomebootguardw19") || page.body.includes("OldHomeBootGuard"),
  hasW19: page.body.includes("v20260729w19.css"),
  hasW4: page.body.includes("testimonials.v20260729w4.js"),
  hasSingleHide: page.body.includes("article:nth-child(n+2){display:none"),
  joan: page.body.includes("Joan Dejarden"),
};

const css = await get(
  "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6a1d8ac5f82dea4aac9efb_dealality-old-home-freeform-head.v20260729w19.css"
);
checks.cssStatus = css.status;
checks.cssTwoCol = css.body.includes("grid-template-columns:repeat(2,minmax(0,1fr))");
checks.cssNoHide = !css.body.includes("article:nth-child(n+2){display:none");
checks.cssFixedH = css.body.includes("height:320px!important");
checks.cssFaqGlow = css.body.includes("#6C72FF 50%,transparent");

console.log(JSON.stringify(checks, null, 2));
