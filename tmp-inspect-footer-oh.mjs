import https from "https";

function get(url) {
  return new Promise((resolve, reject) => {
    https
      .get(url, (res) => {
        let d = "";
        res.on("data", (c) => (d += c));
        res.on("end", () => resolve(d));
      })
      .on("error", reject);
  });
}

const js = await get(
  "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a69fe92f7d4f50fb6112eb9_old-home-footer-oh-20260729b.js"
);
const hits = [...js.matchAll(/freeform-head[^"'`\s)]+/g)].map((m) => m[0]);
console.log("hits", [...new Set(hits)]);
console.log("len", js.length);
console.log(js.slice(0, 400));
