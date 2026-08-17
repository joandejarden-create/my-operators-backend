import https from "https";

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

const html = await get("https://www.dealality.com/old-home?cb=" + Date.now());
const scripts = [...html.matchAll(/<script[^>]+src="([^"]+)"/g)].map((m) => m[1]);
const interesting = scripts.filter((u) =>
  /boot|guard|hero|globe|old-home|problem|platform|pricing|footer-oh|freeform|noloader|pin-dim|fit/i.test(u)
);
console.log("INTERESTING SCRIPTS:");
interesting.forEach((s) => console.log(s));
console.log("---CSS LINKS---");
const links = [...html.matchAll(/<link[^>]+href="([^"]+)"/g)]
  .map((m) => m[1])
  .filter((u) => /old-home|freeform|hero|problem|platform|pricing|dark|benefits|perspectives/i.test(u));
links.forEach((s) => console.log(s));
console.log("---FLAGS---");
console.log("oh-ready", /oh-ready/.test(html));
console.log("BootGuard", /BootGuard|OldHomeBoot/.test(html));
console.log("publicnoloader", /publicnoloader|noloader/.test(html));
console.log("createElement link", /createElement\(['\"]link['\"]\)/.test(html));
const idx = html.search(/BootGuard|OldHomeBoot|asset-boot|oh-ready|injectCss|createElement\(['\"]link['\"]\)/i);
if (idx >= 0) console.log("snippet:\n", html.slice(Math.max(0, idx - 80), idx + 500));

const inlineScripts = [...html.matchAll(/<script(?![^>]*src=)[^>]*>([\s\S]*?)<\/script>/gi)]
  .map((m) => m[1])
  .filter((s) => /old-home|BootGuard|inject|createElement\(['\"]link|oh-|hero-fit|problem-v2|platform-features/i.test(s));
console.log("---INLINE MATCH COUNT---", inlineScripts.length);
inlineScripts.slice(0, 5).forEach((s, i) => {
  console.log(`\n---INLINE ${i} (${s.length} chars)---`);
  console.log(s.slice(0, 800));
});
