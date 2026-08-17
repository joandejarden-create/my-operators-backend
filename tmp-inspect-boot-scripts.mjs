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

const urls = [
  "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a%2F689e5ba67671442434f3ca35%2F6a6a23d33f4842a30976ec30%2Foldhomebootguardw19-1.0.0.js",
  "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a%2F689e5ba67671442434f3ca35%2F6a6a21dc2ed81046fccea458%2Foldhomeassetbootw19-1.0.0.js",
  "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a%2F689e5ba67671442434f3ca35%2F6a6a22cdba5c2e3ed95d0b00%2Fohmodulestabfixw16-1.0.0.js",
];

for (const url of urls) {
  const js = await get(url);
  const name = url.split("%2F").pop();
  const hits = [...js.matchAll(/freeform-head[^"'`\s)]+|footer-oh[^"'`\s)]+|w1[6-9]|w20/g)].map(
    (m) => m[0]
  );
  console.log("\n==", name, "len", js.length);
  console.log([...new Set(hits)].slice(0, 40));
  const cssIdx = js.search(/freeform-head|stylesheet/i);
  if (cssIdx >= 0) console.log(js.slice(cssIdx, cssIdx + 350));
}
