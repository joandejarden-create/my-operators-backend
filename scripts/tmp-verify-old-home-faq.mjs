import https from "https";
import fs from "fs";

function get(url) {
  return new Promise((resolve, reject) => {
    https
      .get(url, {
        headers: {
          "Cache-Control": "no-cache",
          Pragma: "no-cache",
          "user-agent": "Mozilla/5.0",
        },
      }, (res) => {
        let d = "";
        res.on("data", (c) => (d += c));
        res.on("end", () => resolve({ status: res.statusCode, body: d }));
      })
      .on("error", reject);
  });
}

const page = await get(`https://www.dealality.com/old-home?nocache=${Date.now()}`);
fs.writeFileSync("tmp-old-home-live.html", page.body);

const checks = [
  "v20260728g.css",
  "v20260728f.css",
  "Questions Stakeholders Actually Ask",
  "Is Dealality a broker?",
  "Who sees my project?",
  "private beta",
  "Who is Dealality for?",
  'id="faq"',
  "dc-premium",
];

const cssMatches = [...page.body.matchAll(/dealality-old-home-dark[^"'\\s]+/g)].map((m) => m[0]);

console.log(
  JSON.stringify(
    {
      status: page.status,
      len: page.body.length,
      cssMatches,
      checks: Object.fromEntries(checks.map((c) => [c, page.body.includes(c)])),
    },
    null,
    2
  )
);
