import https from "https";
import http from "http";

function get(url) {
  return new Promise((resolve, reject) => {
    const lib = url.startsWith("https") ? https : http;
    lib
      .get(url, (res) => {
        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          return get(res.headers.location).then(resolve, reject);
        }
        let d = "";
        res.on("data", (c) => (d += c));
        res.on("end", () => resolve({ status: res.statusCode, d, url }));
      })
      .on("error", reject);
  });
}

const page = await get("https://www.dealality.com/old-home");
const css = [...page.d.matchAll(/href="([^"]+\.css[^"]*)"/g)].map((m) => m[1]);
console.log("css count", css.length);
for (const u of css) {
  const abs = u.startsWith("http") ? u : `https://www.dealality.com${u}`;
  try {
    const { d, status } = await get(abs);
    if (status !== 200) continue;
    if (/0\.72|manual-label|rgba\(255,\s*255,\s*255,\s*0\.72\)/.test(d)) {
      const lines = [
        ...d.matchAll(
          /[^\n]{0,100}(?:0\.72|manual-label|rgba\(255,\s*255,\s*255,\s*0\.72\))[^\n]{0,100}/g
        ),
      ]
        .slice(0, 8)
        .map((m) => m[0]);
      console.log("\nHIT", abs.split("/").pop());
      for (const line of lines) console.log(" ", line);
    }
  } catch (e) {
    console.log("fail", abs, e.message);
  }
}
