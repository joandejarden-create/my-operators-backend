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

const html = await get("https://www.dealality.com/old-home?cb=" + Date.now());
const scripts = [...html.matchAll(/<script[^>]+src="([^"]+)"/g)].map((m) => m[1]);
const interesting = scripts.filter((u) =>
  /boot|footer-oh|freeform|problem|old-home|guard/i.test(u)
);
console.log(interesting.join("\n"));
console.log("---inline boot?---");
console.log(/OldHomeBoot|BootGuard|freeform-head/i.test(html));
const idx = html.search(/OldHomeBoot|BootGuardW19|freeform-head\.v/i);
console.log("idx", idx);
if (idx >= 0) console.log(html.slice(Math.max(0, idx - 120), idx + 300));
