const fs = require("fs");
const https = require("https");

function get(u) {
  return new Promise((res, rej) => {
    https
      .get(u, (r) => {
        let d = "";
        r.on("data", (c) => (d += c));
        r.on("end", () => res(d));
      })
      .on("error", rej);
  });
}

(async () => {
  const html = await get("https://www.dealality.com/old-home?cb=" + Date.now());
  const scripts = [...html.matchAll(/<script(?:\s[^>]*)?>([\s\S]*?)<\/script>/gi)];
  console.log("inline scripts", scripts.length);
  for (let i = 0; i < scripts.length; i++) {
    const src = scripts[i][1];
    if (!src || src.trim().startsWith("window.") && src.length < 50) continue;
    if (!src.includes("insights") && !src.includes("scrollMax") && !src.includes("fsw-") && !src.includes("rotator")) {
      // still try parse all long ones
    }
    try {
      // wrap as function body check via Function constructor
      // eslint-disable-next-line no-new-func
      new Function(src);
      if (src.includes("scrollMax") || src.includes("insights-grid")) {
        console.log("OK insights script", i, "len", src.length);
      }
    } catch (e) {
      console.log("FAIL script", i, "len", src.length, e.message);
      // find position
      const m = /Unexpected token/.test(e.message);
      fs.writeFileSync("tmp-bad-script-" + i + ".js", src);
      // try to locate near )
      const lines = src.split(/\n/);
      console.log("lines", lines.length);
      // binary search
      let lo = 0,
        hi = lines.length;
      while (lo < hi - 1) {
        const mid = Math.floor((lo + hi) / 2);
        try {
          new Function(lines.slice(0, mid).join("\n"));
          lo = mid;
        } catch {
          hi = mid;
        }
      }
      console.log("fails around line", hi);
      console.log(lines.slice(Math.max(0, hi - 5), hi + 3).join("\n"));
      console.log("--- context ---");
      console.log(lines.slice(Math.max(0, hi - 15), Math.min(lines.length, hi + 10)).join("\n"));
    }
  }
})();
