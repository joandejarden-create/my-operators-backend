import fs from "fs";
import https from "https";

function fetch(url) {
  return new Promise((resolve, reject) => {
    https
      .get(url, (res) => {
        let data = "";
        res.on("data", (c) => (data += c));
        res.on("end", () => resolve({ status: res.statusCode, headers: res.headers, body: data }));
      })
      .on("error", reject);
  });
}

const home = await fetch("https://www.dealality.com/");
fs.writeFileSync("tmp-home-live-nav.html", home.body);
console.log("home status", home.status, "loc", home.headers.location || "");

const h = home.body;
for (const label of ["Insights", "FAQ", "About", "Process"]) {
  const re = new RegExp(`href="([^"]*)"[^>]*>\\s*${label}\\s*<`, "gi");
  let m;
  while ((m = re.exec(h))) console.log(label, "=>", m[1]);
}

const old = fs.readFileSync("tmp-old-home-live-nav.html", "utf8");
console.log("old-home sections", {
  insights: /id="insights"/.test(old),
  faq: /id="faq"/.test(old),
});
const nav = old.match(/id="nav-links"[\s\S]{0,900}/);
console.log(nav ? nav[0].replace(/\s+/g, " ").slice(0, 500) : "no nav-links");
